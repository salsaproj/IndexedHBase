package iu.pti.hbaseapp.truthy.streaming;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.truthy.streaming.MemeClusteringTester.GlobalClusteringParams;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
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

public class SyncCoordinateBolt extends BaseRichBolt {
	private static final long serialVersionUID = -4410734423569406087L;
	
	protected static final Log LOG = LogFactory.getLog(SyncCoordinateBolt.class);
	
	/** For parsing tuples received from the clustering bolts */
	protected Gson gson;

	/** For acknowledging tuples received from the clustering bolts */
	protected OutputCollector collector;
	
	/** The base number for cluster IDs of outlier clusters */ 
	protected int outlierClusterIdBase;
	
	/** List of outlier clusters newly created in this batch */
	protected List<ProtomemeCluster> outlierClusters;
	
	/** List of cluster changes received in this batch */
	protected List<ProtomemeClusterDelta> clusterDeltas;
	
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
	
	/** ActiveMQ sync session */
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
	
	/** Initial sleep time to wait for all clustering bolts to be up */
	protected long initSleepMilli;
	
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
				ProtomemeClusterDelta delta = clusterDeltas.get(pm.clusterId);
				if (delta == null) {
					delta = new ProtomemeClusterDelta(pm.clusterId, pm.latestTweetTs, pm);
					clusterDeltas.set(pm.clusterId, delta);
				} else {
					delta.addProtomeme(pm);
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
				Integer clusterId = markerOutlierClusterMap.get(pm.mainMarker);
				if (clusterId != null) {
					pm.isClusteredByMarker = true;
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

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		try {
			gson = new Gson();
			this.collector = collector;
			numClusteringBoltTasks = Integer.parseInt((String)conf.get(StreamClusteringTopology.CBOLT_PARA_CONFKEY));
			numSyncReqNeeded = numClusteringBoltTasks - numEosReceived;
			numPmsInBatch = Integer.parseInt((String)conf.get(StreamClusteringTopology.CBOLT_BATCH_CONFKEY));
			numPmsAllBoltsInBatch = numPmsInBatch * numClusteringBoltTasks;
			outlierClusterIdBase = Integer.parseInt((String)conf.get(StreamClusteringTopology.OUTLIER_CID_CONFKEY));
			outlierClusters = new ArrayList<ProtomemeCluster>(numPmsInBatch);
			markerOutlierClusterMap = new HashMap<String, Integer>();
			calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
			String bootstrapInfoPath = (String) conf.get(StreamClusteringTopology.BOOTSTRAP_PATH_CONFKEY);
			readBootstrapInfo(bootstrapInfoPath);
			syncReqTaksIds = new HashSet<Integer>();
			ipReportTaksIds = new HashSet<Integer>();
			ipMinTaskIdMap = new HashMap<String, Integer>();
			String amqUri = (String)conf.get(StreamClusteringTopology.ACTIVEMQ_URI_CONFKEY);
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(amqUri);
			amqConnection = connectionFactory.createConnection();
			amqConnection.start();
            amqSyncSession = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic syncTopic = amqSyncSession.createTopic((String)conf.get(StreamClusteringTopology.SYNC_TOPIC_CONFKEY));
            amqSyncSender = amqSyncSession.createProducer(syncTopic);
            amqSyncSender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            amqElectSession = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic electTopic = amqElectSession.createTopic((String)conf.get(StreamClusteringTopology.ELECT_TOPIC_CONFKEY));
            amqElectSender = amqElectSession.createProducer(electTopic);
            amqElectSender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            amqReportSession = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic reportTopic = amqReportSession.createTopic((String)conf.get(StreamClusteringTopology.REPORT_TOPIC_CONFKEY));
            amqReportSender = amqReportSession.createProducer(reportTopic);
            amqReportSender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            initSleepMilli = Integer.parseInt((String)conf.get(StreamClusteringTopology.SYNC_COORD_WAIT_CONFKEY)) * 1000;
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
		clusterDeltas = new ArrayList<ProtomemeClusterDelta>((int)(globalParams.initClusterNum * globalParams.clusterNumMultiplier));
		for (int i=0; i<clusterNum; i++) {
			clusterDeltas.add(null);
		}
		
		// from now on: either "Cluster-*" or JSON string for a protomeme in Cluster-*
		ProtomemeClusterDelta tmpDelta = null;
		line = brBoot.readLine();
		while (line != null) {
			if (line.startsWith("Cluster-")) {
				tmpDelta = new ProtomemeClusterDelta();
				int clusterId = Integer.parseInt(line.substring("Cluster-".length()));
				tmpDelta.clusterId = clusterId;
				clusterDeltas.set(clusterId, tmpDelta);
			} else {
				ProtoMeme pm = gson.fromJson(line, ProtoMeme.class);
				if (pm.latestTweetTs > tmpDelta.latestUpdateMilli) {
					tmpDelta.latestUpdateMilli = pm.latestTweetTs;
				}
			}
			line = brBoot.readLine();
		}
		brBoot.close();
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
		TreeSet<ProtomemeClusterDelta> allDeltas = new TreeSet<ProtomemeClusterDelta>();
		ArrayDeque<Integer> usableIdsForOutlier = new ArrayDeque<Integer>(clusterDeltas.size());
		for (int i=0; i<clusterDeltas.size(); i++) {
			ProtomemeClusterDelta delta = clusterDeltas.get(i);
			if (delta != null) {
				allDeltas.add(delta);
			} else {
				usableIdsForOutlier.add(i);
			}
		}
		for (int i=0; i<outlierClusters.size(); i++) {
			ProtomemeCluster c = outlierClusters.get(i);
			ProtomemeClusterDelta d = new ProtomemeClusterDelta();
			d.clusterId = i + outlierClusterIdBase;
			d.latestUpdateMilli = c.latestUpdateTime;
			d.pms.addAll(c.protomemes);
			allDeltas.add(d);
		}
		
		// generate the synchronization message. allDeltas now contains all the cluster deltas sorted by latest update time
		Map<Integer, Long> replacedClusterNewTime = new HashMap<Integer, Long>();
		SyncMessage msg = new SyncMessage(clusterDeltas.size());
		adjustOutlierThreshold(globalParams);
		msg.params = globalParams;
		// since centUpdateTime is not used in the parallel version, we use it to piggyback number of end of streams (finished bolts)
		msg.params.centUpdateTime = numEosReceived;
		int nOldDeltas = allDeltas.size() - clusterDeltas.size();
		int i = 0;
		for (ProtomemeClusterDelta delta : allDeltas) {
			i++;
			if (i <= nOldDeltas) {
				if (delta.clusterId < outlierClusterIdBase) {
					usableIdsForOutlier.add(delta.clusterId);
				}
			} else {
				if (delta.clusterId >= outlierClusterIdBase) {
					// change the cluster ID of an outlier cluster to a usable one
					int newCid = usableIdsForOutlier.remove();
					delta.clusterId = newCid + outlierClusterIdBase;
					for (ProtoMeme pm : delta.pms) {
						pm.clusterId = newCid;
					}
					// if an old cluster is replaced by an outlier, its corresponding delta information should also be replaced
					replacedClusterNewTime.put(newCid, delta.latestUpdateMilli);
				}
				if (delta.pms.size() > 0) {
					msg.deltas.add(delta);
				}
			}
		}
		
		// send the synchronization message
		String msgJson = gson.toJson(msg, SyncMessage.class);
		TextMessage syncMsg = amqSyncSession.createTextMessage(msgJson);
        amqSyncSender.send(syncMsg);
        long msgGenTime = System.currentTimeMillis() - time0;
        LOG.info("sent synchronization message with length " + msgJson.length() + ". Number of EOS received: " + msg.params.centUpdateTime + 
        		". Time spent on generating and sending message: " + (msgGenTime / 1000.0) + "s.");
        
        // reset to get ready for the next batch
        batchId++;
        pmAddCount = outlierCount = 0;
        numPaddingReceived = 0;
        numSyncReqNeeded = numClusteringBoltTasks - numEosReceived;
        simMean = globalParams.sumSimForMeanStd / globalParams.nSimsForMeanStd;
		simStdDev = Math.sqrt(globalParams.ssSimForMeanStd / globalParams.nSimsForMeanStd - simMean * simMean);
        for (int cid=0; cid<clusterDeltas.size(); cid++) {
        	ProtomemeClusterDelta d = clusterDeltas.get(cid);
        	Long newUpdateMilli = replacedClusterNewTime.get(cid);
        	if (d != null) {
        		d.pms.clear();
        		if (newUpdateMilli != null) {
        			// outlier replace an old cluster
        			d.latestUpdateMilli = newUpdateMilli;
        		}
        	} else if (newUpdateMilli != null) {
        		// outlier takes an empty spot
        		d = new ProtomemeClusterDelta();
        		d.clusterId = cid;
        		d.latestUpdateMilli = newUpdateMilli;
        		clusterDeltas.set(cid, d);
        	}
        }
        markerOutlierClusterMap.clear();
        outlierClusters.clear();
        syncReqTaksIds.clear();
        if (numEosReceived == numClusteringBoltTasks) {
        	cleanup();
        }
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
}
