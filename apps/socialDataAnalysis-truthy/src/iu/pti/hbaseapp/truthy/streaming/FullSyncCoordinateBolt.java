package iu.pti.hbaseapp.truthy.streaming;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;
import iu.pti.hbaseapp.truthy.streaming.FullSyncMessage.FullSyncClusterInfo;
import iu.pti.hbaseapp.truthy.streaming.MemeClusteringTester.GlobalClusteringParams;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class FullSyncCoordinateBolt extends BaseRichBolt {
	private static final long serialVersionUID = -7072120930901493649L;

	protected static final Log LOG = LogFactory.getLog(FullSyncCoordinateBolt.class);
	
	/** For parsing tuples received from the clustering bolts */
	protected Gson gson;

	/** For acknowledging tuples received from the clustering bolts */
	protected OutputCollector collector;
	
	/** Length of a time step in milliseconds */
	protected long timeStepInMilli;
	
	/** Length of the siding window in number of time steps */
	protected int slidingWinLenInSteps;
	
	/** The start time (in milliseonds) of 'current' time step */ 
	protected long curWinStartMilli;
	
	/** The end time (in milliseonds) of 'current' time step */
	protected long curWinEndMilli;
	
	/** The ealiest tweet creation time in the bootstrap clusters */
	protected long bootstrapStartMilli;
	
	/** The base number for cluster IDs of outlier clusters */ 
	protected int outlierClusterIdBase;
	
	/** List of outlier clusters newly created in this batch */
	protected List<ProtomemeCluster> outlierClusters;
	
	/** List of clusters */
	protected List<ProtomemeCluster> clusters;
	
	/** Queue of lists of protomemes for different time steps */
	ArrayDeque<List<ProtoMeme>> pmLists;
	
	/** A map recording the number of maintained protomemes sharing the same markers */
	Map<String, Integer> pmMarkerCount;
	
	/** A map from protomeme markers to cluster IDs */
	Map<String, Integer> pmMarkerClusterMap;
	
	/** temporary map from outlier protomeme markers to outlier cluster IDs */ 
	protected Map<String, Integer> markerOutlierClusterMap;
	
	/** Number of protomemes in a batch */
	protected int numPmsInBatch;
	
	/** number of protomemes processed as read from the bootstrap info file */
	protected int numPmsInBootstrap;
	
	/** current batchId received from the clustering bolts, also used for identifying synchronizations */
	protected int batchId = -1;
	
	/** number of PMADD messages received in this batch */
	protected int pmAddCount = 0;
	
	/** number of OUTLIER messages received in this batch */
	protected int outlierCount = 0;
	
	/** total number of parallel clustering bolt tasks */
	protected int numClusteringBoltTasks;
	
	/** number of protomemes expected from all clustering bolts in a batch */
	protected int numPmsAllBoltsInBatch;
	
	/** number of padding messages received in this batch */
	protected int numPaddingReceived = 0;
	
	/** Set of task IDs from which synchronization requests have been received */
	protected Set<Integer> syncReqTaksIds;
	
	/** Set of task IDs from which IP reports have been received */
	protected Set<Integer> ipReportTaksIds;
	
	/** a map from IP addresses to the min task ID of cbolts running on each IP */
	protected Map<String, Integer> ipMinTaskIdMap;
	
	/** Number of "end of stream" sync requests received so far */
	protected int numEosReceived = 0;
	
	/** Number of sync requests needed before sending the next synchronization message */
	protected int numSyncReqNeeded;
	
	/** for converting time between string representations and milliseconds */
	protected Calendar calTmp;
	
	/** global parameters for the clustering process */
	protected GlobalClusteringParams globalParams;
	
	/** mean of max similarities in current batch */
	protected double simMean;
	
	/** standard deviation of max similarities in current batch */
	protected double simStdDev;
	
	/** ActiveMQ connection */
	protected Connection amqConnection;
	
	/** ActiveMQ session */
	protected Session amqSyncSession;
	
	/** ActiveMQ producer for sending synchronization messages */
	protected MessageProducer amqSyncSender;
	
	/** ActiveMQ elect session */
	protected Session amqElectSession;
	
	/** ActiveMQ producer for sending elect messages */
	protected MessageProducer amqElectSender;
	
	/** ActiveMQ report session */
	protected Session amqReportSession;
	
	/** ActiveMQ producer for sending reports to the protomeme generator */
	protected MessageProducer amqReportSender;
	
	/** Path of the final results file */
	protected String finalResultsPath;
	
	/** Initial sleep time to wait for all clustering bolts to be up */
	protected long initSleepMilli;
	
	/** Some functions in this helper will be useful for generating protomemes */
	protected MemeClusteringTester helper;
	
	@Override
	public void execute(Tuple input) {
		try {
			int taskId = input.getSourceTask();
			int msgType = input.getIntegerByField(StreamClusteringTopology.FIELD_MSG_TYPE);
			int batchId = input.getIntegerByField(StreamClusteringTopology.FIELD_BATCH_ID);
			ProtoMeme pm = null;
			
			if (initSleepMilli > 0) {
				Utils.sleep(1000);
				initSleepMilli -= 1000;
			}			
			if (this.batchId < 0) {
				this.batchId = batchId;
			} else if (this.batchId != batchId && msgType != StreamClusteringTopology.CBOLT_MSG_PADDING) {
				throw new Exception("Received inconsistent batch ID: " + batchId + "; expected: " + this.batchId);
			}
			switch (msgType) {
			case StreamClusteringTopology.CBOLT_MSG_IP_REPORT:
				ipReportTaksIds.add(taskId);
				String ip = input.getString(2);
				Integer curMin = ipMinTaskIdMap.get(ip);
				if (curMin == null || taskId < curMin) {
					ipMinTaskIdMap.put(ip, taskId);
				}
				if (ipReportTaksIds.size() >= numClusteringBoltTasks) {
					sendElectMsg();
				}
				break;
			case StreamClusteringTopology.CBOLT_MSG_PADDING:
				numPaddingReceived++;
				if (numPaddingReceived % 100 == 0) {
					LOG.info("received PADDING from Task " + taskId + ". total number of PADDING received: " + numPaddingReceived);
				}
				break;
			case StreamClusteringTopology.CBOLT_MSG_SYNC_REQ:
				boolean endOfStream = input.getBoolean(2);
				if (endOfStream) {
					numEosReceived++;
				}
				LOG.info("received SYNC request from Task " + taskId + ", end of stream: " + endOfStream);
				if (syncReqTaksIds.contains(taskId)) {
					LOG.warn("Received repeated synchronization request from task " + taskId);
				} else {
					syncReqTaksIds.add(taskId);
					if (syncReqTaksIds.size() >= numSyncReqNeeded) {
						syncThroughActiveMQ();
					}
				}
				break;
			case StreamClusteringTopology.CBOLT_MSG_PMADD:
				pm = gson.fromJson(input.getStringByField(StreamClusteringTopology.FIELD_PM), ProtoMeme.class);
				addPmToPmLists(pm);
				if (pm.earliestTweetTs >= curWinStartMilli) {
					ProtomemeCluster cluster = clusters.get(pm.clusterId);
					if (cluster != null) {
						cluster.addProtoMeme(pm);
						Integer count = pmMarkerCount.get(pm.mainMarker);
						if (count == null) {
							pmMarkerCount.put(pm.mainMarker, 1);
						} else {
							pmMarkerCount.put(pm.mainMarker, count + 1);
						}
						pmMarkerClusterMap.put(pm.mainMarker, pm.clusterId);
					} else {						
						throw new Exception("Received PMADD for null cluster: " + pm.clusterId);
					}
				} else {
					LOG.warn("Received PMADD for protomeme older than current time window.");
				}
				globalParams.nPmsProcessed++;
				if (!pm.isClusteredByMarker) {
					globalParams.nSimsForMeanStd++;
					globalParams.sumSimForMeanStd += pm.similarityToCluster;
					globalParams.ssSimForMeanStd += pm.similarityToCluster * pm.similarityToCluster;
				}
				pmAddCount++;
				break;
			case StreamClusteringTopology.CBOLT_MSG_OUTLIER:
				// process outliers reported by all clustering bolts with a global view
				pm = gson.fromJson(input.getStringByField(StreamClusteringTopology.FIELD_PM), ProtoMeme.class);
				adjustTimeWindow(pm);
				Integer clusterId = markerOutlierClusterMap.get(pm.mainMarker);
				if (clusterId != null) {
					ProtomemeCluster c = outlierClusters.get(clusterId);
					c.addProtoMeme(pm);
					pm.clusterId = clusterId;
					pm.similarityToCluster = c.computeSimilarity(pm);
				} else {
					int idClosestCluster = -1;
					double maxSimilarity = 0;
					for (int i = 0; i < outlierClusters.size(); i++) {
						double sim = outlierClusters.get(i).computeSimilarity(pm);
						if (sim >= maxSimilarity) {
							maxSimilarity = sim;
							idClosestCluster = i;
						}
					}
					if (maxSimilarity > 0 && maxSimilarity > simMean - globalParams.outlierThd * simStdDev) {
						outlierClusters.get(idClosestCluster).addProtoMeme(pm);
						pm.clusterId = idClosestCluster;
						pm.similarityToCluster = maxSimilarity;
						markerOutlierClusterMap.put(pm.mainMarker, idClosestCluster);
						globalParams.nSimsForMeanStd++;
						globalParams.sumSimForMeanStd += pm.similarityToCluster;
						globalParams.ssSimForMeanStd += pm.similarityToCluster * pm.similarityToCluster;
					} else {
						ProtomemeCluster pmc = new ProtomemeCluster(pm);
						outlierClusters.add(pmc);
						pm.clusterId = outlierClusters.size() - 1;
						pm.similarityToCluster = 1.0;
						markerOutlierClusterMap.put(pm.mainMarker, outlierClusters.size() - 1);
					}
				}
				globalParams.nPmsProcessed++;
				globalParams.outlierCount++;
				outlierCount++;
				break;
			default:
				throw new Exception("Unsupported message type: " + msgType);
			}
			if (pmAddCount + outlierCount == numPmsAllBoltsInBatch) {
				sendAmqSyncInit();
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.equals("Exception when processing tuple: " + e.getMessage());
		}
	}

	/**
	 * Add <b>pm</b> to the protomeme lists of the current time window. If the given protomeme <b>pm</b> is from a new time step, 
	 * delete old protomemes falling out of the sliding window.
	 * @param pm
	 *  A new coming protomeme.
	 * @throws Exception
	 */
	protected void addPmToPmLists(ProtoMeme pm) throws Exception {
		adjustTimeWindow(pm);
		Object[] pmListArray = pmLists.toArray();
		int stepIdx = (int)((pm.earliestTweetTs - curWinStartMilli) / timeStepInMilli);
		((List<ProtoMeme>)pmListArray[stepIdx]).add(pm);
	}

	/**
	 * Check if <b>pm</b> starts a new time window. If so, move the time window and delete any old protomemes.
	 * @param pm
	 *  A new coming protomeme.
	 * @throws Exception
	 */
	protected void adjustTimeWindow(ProtoMeme pm) throws Exception {
		while (pm.earliestTweetTs > curWinEndMilli) {
			LOG.info("new time window started");
			List<ProtoMeme> pmList = new LinkedList<ProtoMeme>();
			pmLists.add(pmList);
			if (pmLists.size() > slidingWinLenInSteps) {
				List<ProtoMeme> oldPms = pmLists.remove();
				deleteOldPms(oldPms);
			}
			curWinStartMilli += timeStepInMilli;
			curWinEndMilli += timeStepInMilli;
		}
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		try {
			gson = new Gson();
			this.collector = collector;
			helper = new MemeClusteringTester();
			// initialize parallel algorithm configurations
			numClusteringBoltTasks = Integer.parseInt((String) conf.get(StreamClusteringTopology.CBOLT_PARA_CONFKEY));
			numSyncReqNeeded = numClusteringBoltTasks - numEosReceived;
			numPmsInBatch = Integer.parseInt((String) conf.get(StreamClusteringTopology.CBOLT_BATCH_CONFKEY));
			syncReqTaksIds = new HashSet<Integer>();
			ipReportTaksIds = new HashSet<Integer>();
			ipMinTaskIdMap = new HashMap<String, Integer>();
			numPmsAllBoltsInBatch = numPmsInBatch * numClusteringBoltTasks;
			finalResultsPath = (String)conf.get(StreamClusteringTopology.RESULT_PATH_CONFKEY);
			initSleepMilli = Integer.parseInt((String)conf.get(StreamClusteringTopology.SYNC_COORD_WAIT_CONFKEY)) * 1000;

			// initialize information about sliding time window
			timeStepInMilli = Long.parseLong((String)conf.get(StreamClusteringTopology.TIME_STEP_SEC_CONFKEY)) * 1000;
			slidingWinLenInSteps = Integer.parseInt((String)conf.get(StreamClusteringTopology.SLIDE_WIN_LEN_CONFKEY));
			String startingTweetTime = (String) conf.get(StreamClusteringTopology.FIRST_TWEET_TIME_CONFKEY);
			calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
			GeneralHelpers.setDateTimeByString(calTmp, startingTweetTime);
			long curStepStartMilli = calTmp.getTimeInMillis();
			curWinStartMilli = curStepStartMilli - (slidingWinLenInSteps - 1) * timeStepInMilli;
			curWinEndMilli = curStepStartMilli + timeStepInMilli - 1;
			
			// initialize clusters
			pmLists = new ArrayDeque<List<ProtoMeme>>(slidingWinLenInSteps + 1);
			pmMarkerCount = new HashMap<String, Integer>();
			pmMarkerClusterMap = new HashMap<String, Integer>();
			outlierClusterIdBase = Integer.parseInt((String)conf.get(StreamClusteringTopology.OUTLIER_CID_CONFKEY));
			outlierClusters = new ArrayList<ProtomemeCluster>(numPmsInBatch);
			markerOutlierClusterMap = new HashMap<String, Integer>();
			String bootstrapStartTime = (String) conf.get(StreamClusteringTopology.BOOT_TWEET_TIME_CONFKEY);
			GeneralHelpers.setDateTimeByString(calTmp, bootstrapStartTime);
			bootstrapStartMilli = calTmp.getTimeInMillis();
			String bootstrapInfoPath = (String)conf.get(StreamClusteringTopology.BOOTSTRAP_PATH_CONFKEY);
			readBootstrapInfo(bootstrapInfoPath);
			
			// initialize activeMQ for sync message passing
			String amqUri = (String)conf.get(StreamClusteringTopology.ACTIVEMQ_URI_CONFKEY);
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(amqUri);
			amqConnection = connectionFactory.createConnection();
			amqConnection.start();
            amqSyncSession = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = amqSyncSession.createTopic((String)conf.get(StreamClusteringTopology.SYNC_TOPIC_CONFKEY));
            amqSyncSender = amqSyncSession.createProducer(topic);
            amqSyncSender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            amqElectSession = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic electTopic = amqElectSession.createTopic((String)conf.get(StreamClusteringTopology.ELECT_TOPIC_CONFKEY));
            amqElectSender = amqElectSession.createProducer(electTopic);
            amqElectSender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            amqReportSession = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic reportTopic = amqReportSession.createTopic((String)conf.get(StreamClusteringTopology.REPORT_TOPIC_CONFKEY));
            amqReportSender = amqReportSession.createProducer(reportTopic);
            amqReportSender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Exception in prepare(): " + e.getMessage());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}
	
	@Override
	public void cleanup() {
		try {
			if (amqSyncSender != null) {
				amqSyncSender.close();
			}
			if (amqSyncSession != null) {
				amqSyncSession.close();
			}
			if (amqElectSender != null) {
				amqElectSender.close();
			}
			if (amqElectSession != null) {
				amqElectSession.close();
			}
			if (amqReportSender != null) {
				amqReportSender.close();
			}
			if (amqReportSession != null) {
				amqReportSession.close();
			}
			if (amqConnection != null) {
				amqConnection.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Exception when closing ActiveMQ session and connection: " + e.getMessage());
		}
	}
	
	/**
	 * Read global parameter values from the bootstrap information file path
	 * @param bootstrapInfoPath
	 *  Path to the bootstrap information file
	 * @throws Exception
	 */
	protected void readBootstrapInfo(String bootstrapInfoPath) throws Exception {
		ArrayList<List<ProtoMeme>> tmpPmLists = new ArrayList<List<ProtoMeme>>(slidingWinLenInSteps);
		for (int i=0; i<slidingWinLenInSteps; i++) {
			tmpPmLists.add(new LinkedList<ProtoMeme>());
		}
		BufferedReader brBoot = null;
		if (bootstrapInfoPath.endsWith(".gz")) {
			brBoot = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(bootstrapInfoPath)), "UTF-8"));
		} else {
			brBoot = new BufferedReader(new InputStreamReader(new FileInputStream(bootstrapInfoPath), "UTF-8"));
		}
		//1st line: "Final values of global parameters:"
		String line = brBoot.readLine();
		
		//2nd line: JSON string of GlobalClusteringParams
		line = brBoot.readLine();
		globalParams = gson.fromJson(line, GlobalClusteringParams.class);
		numPmsInBootstrap = globalParams.nPmsProcessed;
		simMean = globalParams.sumSimForMeanStd / globalParams.nSimsForMeanStd;
		simStdDev = Math.sqrt(globalParams.ssSimForMeanStd / globalParams.nSimsForMeanStd - simMean * simMean);
		
		//3rd line: "Protomemes in the final 120 clusters:"
		line = brBoot.readLine();
		int idx2 = line.lastIndexOf(' ');
		int idx1 = line.lastIndexOf(' ', idx2-1);
		int clusterNum = Integer.parseInt(line.substring(idx1+1, idx2));
		clusters = new ArrayList<ProtomemeCluster>((int)(globalParams.initClusterNum * globalParams.clusterNumMultiplier));
		for (int i=0; i<clusterNum; i++) {
			clusters.add(null);
		}
		
		ProtomemeCluster tmpCluster = null;
		// from now on: either "Cluster-*" or JSON string for a protomeme in Cluster-*
		line = brBoot.readLine();
		while (line != null) {
			if (line.startsWith("Cluster-")) {
				tmpCluster = new ProtomemeCluster();
				int clusterId = Integer.parseInt(line.substring("Cluster-".length()));
				clusters.set(clusterId, tmpCluster);
			} else {
				ProtoMeme pm = gson.fromJson(line, ProtoMeme.class);
				if (pm.earliestTweetTs >= curWinStartMilli) {
					tmpCluster.addProtoMeme(pm);
					int timeStepIdx = (int) ((pm.earliestTweetTs - curWinStartMilli) / timeStepInMilli);
					tmpPmLists.get(timeStepIdx).add(pm);
					pmMarkerClusterMap.put(pm.mainMarker, pm.clusterId);
					Integer count = pmMarkerCount.get(pm.mainMarker);
					if (count == null) {
						pmMarkerCount.put(pm.mainMarker, 1);
					} else {
						pmMarkerCount.put(pm.mainMarker, count + 1);
					}
				}
			}
			line = brBoot.readLine();
		}
		brBoot.close();
		pmLists.addAll(tmpPmLists);
		LOG.info("clusters info after reading bootstrap:");
		helper.printFinalAndDeletedClusters(clusters, null, false);
	}
	
	/**
	 * Send a sync initialization message to all clustering bolts.
	 * @throws Exception
	 */
	protected void sendAmqSyncInit() throws Exception {
		LOG.info("sending sync init message. total pms in this batch: " + (pmAddCount + outlierCount) + ", limit: " + numPmsAllBoltsInBatch);
		TextMessage msg = amqElectSession.createTextMessage(StreamClusteringTopology.SYNC_INIT_MSG);
        amqElectSender.send(msg);
        TextMessage reportMsg = amqReportSession.createTextMessage(Integer.toString(globalParams.nPmsProcessed - numPmsInBootstrap));
        amqReportSender.send(reportMsg);
	}
	
	/**
	 * Send elect message about IP addresses and smallest task IDs on each IP to the cbolts.
	 * @throws Excepiton
	 */
	protected void sendElectMsg() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(StreamClusteringTopology.ELECT_MSG_MARKER + ":[");
		for (Map.Entry<String, Integer> e : ipMinTaskIdMap.entrySet()) {
			sb.append(e.getKey()).append(':').append(e.getValue()).append(',');
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append(']');
		TextMessage msg = amqElectSession.createTextMessage(sb.toString());
		amqElectSender.send(msg);
		LOG.info("sent elect message.");
	}
	
	/**
	 * Send synchronization message through ActiveMQ.
	 * @throws Exception
	 */
	protected void syncThroughActiveMQ() throws Exception {
		LOG.info("Starting synchronization for batch " + batchId + ". PMADD: " + pmAddCount + ", OUTLIER: " + outlierCount +
				", number of oulier clusters: " + outlierClusters.size());
		long time0 = System.currentTimeMillis();
		// first, sort the deltas of existing clusters and outlier clusters by their latest update time
		TreeSet<FullSyncClusterInfo> allClustersInfo = new TreeSet<FullSyncClusterInfo>();
		ArrayDeque<Integer> usableIdsForOutlier = new ArrayDeque<Integer>(clusters.size());
		for (int i=0; i<clusters.size(); i++) {
			ProtomemeCluster cluster = clusters.get(i);
			if (cluster != null && cluster.protomemes.size() > 0) {
				allClustersInfo.add(new FullSyncClusterInfo(cluster, i));
			} else {
				usableIdsForOutlier.add(i);
			}
		}
		for (int i=0; i<outlierClusters.size(); i++) {
			ProtomemeCluster c = outlierClusters.get(i);
			allClustersInfo.add(new FullSyncClusterInfo(c, i + outlierClusterIdBase));
		}
		
		// generate the synchronization message. allDeltas now contains all the cluster deltas sorted by latest update time
		FullSyncMessage msg = new FullSyncMessage(clusters.size());
		adjustOutlierThreshold(globalParams);
		msg.params = globalParams;
		int nOldEles = allClustersInfo.size() - clusters.size();
		int i = 0;
		for (FullSyncClusterInfo ele : allClustersInfo) {
			i++;
			if (i <= nOldEles) {
				if (ele.clusterId < outlierClusterIdBase) {
					usableIdsForOutlier.add(ele.clusterId);
				}
			} else {
				if (ele.clusterId < outlierClusterIdBase) {
					ProtomemeCluster c = clusters.get(ele.clusterId);
					ele.fillPmMarkers(c);
					msg.clusters.set(ele.clusterId, ele);
				} else {
					// put the outlier cluster to the proper position of the normal cluster list
					ProtomemeCluster olc = outlierClusters.get(ele.clusterId - outlierClusterIdBase);
					int newCid = usableIdsForOutlier.remove();
					replaceOldCluster(newCid, olc);
					
					// Put the cluster info into the sync message
					ele.clusterId = newCid + outlierClusterIdBase;
					ele.centTidVector = olc.centTidVector;
					ele.centUserVector = olc.centUserVector;
					ele.centWordVector = olc.centWordVector;
					ele.centDiffusionVector = olc.centDiffusionVector;
					ele.fillPmMarkers(olc);
					msg.clusters.set(newCid, ele);
				}
			}
		}
		
		// send the synchronization message
		String msgStr = gson.toJson(msg, FullSyncMessage.class);
		TextMessage message = amqSyncSession.createTextMessage(msgStr);
        amqSyncSender.send(message);
        long msgGenTime = System.currentTimeMillis() - time0;
        LOG.info("sent synchronization message with length " + msgStr.length() + ". Time spent on generating and sending message: " +
        		(msgGenTime / 1000.0) + "s.");
        helper.printFinalAndDeletedClusters(clusters, null, false);
        
        // reset to get ready for the next batch
        batchId++;
        pmAddCount = outlierCount = 0;
        numPaddingReceived = 0;
        numSyncReqNeeded = numClusteringBoltTasks - numEosReceived;
        simMean = globalParams.sumSimForMeanStd / globalParams.nSimsForMeanStd;
		simStdDev = Math.sqrt(globalParams.ssSimForMeanStd / globalParams.nSimsForMeanStd - simMean * simMean);
        markerOutlierClusterMap.clear();
        outlierClusters.clear();
        syncReqTaksIds.clear();
        if (numEosReceived == numClusteringBoltTasks) {
        	cleanup();
        	writeFinalResults(finalResultsPath);
			LOG.info("Written results to " + finalResultsPath);
        }
	}

	/**
	 * Replace an old cluster at the position of <b>oldClusterId</b> with an outlier cluster.
	 * @param oldClusterId
	 *  The ID of the old cluster.
	 * @param outlierCluster
	 *  The outlierCluster.
	 */
	protected void replaceOldCluster(int oldClusterId, ProtomemeCluster outlierCluster) throws Exception {
		// delete the old cluster
		ProtomemeCluster deleted = clusters.get(oldClusterId);
		if (deleted != null) {
			for (ProtoMeme p : deleted.protomemes) {
				p.clusterId = -1;
				p.similarityToCluster = 0;
				pmMarkerClusterMap.remove(p.mainMarker);
			}
		}
		
		// check if the outlier cluster contains any old protomemes
		List<ProtoMeme> oldPms = new ArrayList<ProtoMeme>(outlierCluster.protomemes.size() / 4);
		for (ProtoMeme pm : outlierCluster.protomemes) {
			if (pm.earliestTweetTs < curWinStartMilli) {
				oldPms.add(pm);
			}
		}
		for (ProtoMeme pm : oldPms) {
			outlierCluster.removeProtoMeme(pm);
		}
		
		// add all the good protomemes in the cluster to the global variables 
		Object[] pmListArray = pmLists.toArray();
		for (ProtoMeme pm : outlierCluster.protomemes) {
			if (pm.earliestTweetTs > curWinEndMilli) {
				throw new Exception("Found protomeme newer than current time window in outlier cluster.");
			}
			pm.clusterId = oldClusterId;
			int stepIdx = (int)((pm.earliestTweetTs - curWinStartMilli) / timeStepInMilli);
			((List<ProtoMeme>)pmListArray[stepIdx]).add(pm);
			pmMarkerClusterMap.put(pm.mainMarker, pm.clusterId);
			Integer count = pmMarkerCount.get(pm.mainMarker);
			if (count == null) {
				pmMarkerCount.put(pm.mainMarker, 1);
			} else {
				pmMarkerCount.put(pm.mainMarker, count + 1);
			}			
		}
		clusters.set(oldClusterId, outlierCluster);
	}
	
	/**
	 * Dynamically adjust the outlier threshold value in <b>para</b> based on the current mean and stdDev.
	 * @param param
	 *  An object containing the dynamic global parameters during the whole clustering process.
	 */
	protected void adjustOutlierThreshold(GlobalClusteringParams param) {
		simMean = param.sumSimForMeanStd / param.nSimsForMeanStd;
		simStdDev = Math.sqrt(param.ssSimForMeanStd / param.nSimsForMeanStd - simMean * simMean);
		if (simMean - param.outlierThd * simStdDev < 0 && param.outlierThd > 0) {
			param.outlierThd -= param.outlierThdAdjStep;
			if (param.outlierThd < 0) {
				param.outlierThd = 0;
			}
		} else if (param.outlierThd < param.initOutlierThd && simMean - param.outlierThd * simStdDev > param.outlierThdAdjStep * simStdDev) {
			param.outlierThd += param.outlierThdAdjStep;
			if (param.outlierThd > param.initOutlierThd) {
				param.outlierThd = param.initOutlierThd;
			}
		}
	}
	
	/**
	 * Delete old protomemes falling out of the sliding window.
	 * @param oldPms
	 */
	protected void deleteOldPms(List<ProtoMeme> oldPms) throws Exception {
		for (ProtoMeme pm : oldPms) {
			// Delete pm from its cluster. If the cluster becomes empty, delete the cluster from the cluster list.
			if (pm.clusterId >= 0) {
				ProtomemeCluster c = clusters.get(pm.clusterId);
				c.removeProtoMeme(pm);
				pm.clusterId = -1;
				pm.similarityToCluster = 0;
			}

			// Decrease the protomeme count for the marker. If there are no more protomemes using this marker, remove the marker
			// from the marker-cluster map.
			Integer markerCount = pmMarkerCount.get(pm.mainMarker);
			if (markerCount != null) {
				markerCount--;
				if (markerCount == 0) {
					pmMarkerCount.remove(pm.mainMarker);
					pmMarkerClusterMap.remove(pm.mainMarker);
				} else {
					pmMarkerCount.put(pm.mainMarker, markerCount);
				}
			}
		}
	}
	
	/**
	 * Write the final global parameters and clusters to <b>finalResultsPath</b>.
	 * @param finalResultsPath
	 *  Path to the final results file.
	 * @throws Exception
	 */
	protected void writeFinalResults(String finalResultsPath) throws Exception {
		PrintWriter pwRes = new PrintWriter(new FileWriter(finalResultsPath));
		pwRes.println("Final values of global parameters:");
		pwRes.println(gson.toJson(globalParams, GlobalClusteringParams.class));
		pwRes.println("Protomemes in the final " + clusters.size() + " clusters:");
		for (int i=0; i<clusters.size(); i++) {
			ProtomemeCluster c = clusters.get(i);
			if (c != null) {
				pwRes.println("Cluster-" + i);
				for (ProtoMeme pm : c.protomemes) {
					pwRes.println(gson.toJson(pm, ProtoMeme.class));
				}
			}
		}
		pwRes.close();
	}
}
