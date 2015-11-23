package iu.pti.hbaseapp.truthy.streaming;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;
import iu.pti.hbaseapp.truthy.streaming.MemeClusteringTester.GlobalClusteringParams;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class ProtomemeClusteringBolt extends BaseRichBolt implements MessageListener {
	private static final long serialVersionUID = -2644203764187310059L;
	
	private static final Log LOG = LogFactory.getLog(ProtomemeClusteringBolt.class);
	
	/** sync message shared among all clustering bolt threads on the same node */
	protected static String syncMsgStr;
	
	/** Lock object shared among all clustering bolt threads on the same node. Used to wake all threads up after receiving syncMsg */
	protected static Object syncLock = new Object();
	
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
	
	/** Global clustering parameters */
	protected GlobalClusteringParams params;
	
	/** mean of similarities so far */
	protected double simMean;
	
	/** standard deviation of similarities so far */
	protected double simStdDev;
	
	/** The base number for cluster IDs of outlier clusters */ 
	protected int outlierClusterIdBase;
	
	/** List of clusters */
	protected List<ProtomemeCluster> clusters;
	
	/** Queue of lists of protomemes for different time steps */
	protected ArrayDeque<List<ProtoMeme>> pmLists;
	
	/** A map recording the number of maintained protomemes sharing the same markers */
	protected Map<String, Integer> pmMarkerCount;
	
	/** A map from protomeme markers to cluster IDs */
	protected Map<String, Integer> pmMarkerClusterMap;
	
	/** A temporary map from protomeme markers to clusters, used between two synchronizations */ 
	protected Map<String, Integer> tmpMarkerClusterMap;
	
	/** Useful for conversion between time and string */
	protected Calendar calTmp;
	
	/** Used to parse the JSON string of a protomeme */
	protected Gson gson;
	
	/** number of protomemes processed in this batch */
	protected int pmCount = 0;
	
	/** number of PMADD tuples emitted in this batch */
	protected int pmAddCount = 0;
	
	/** number of PMADD tuples emitted in this batch */
	protected int outlierCount = 0;
	
	/** Storm task ID */
	protected int taskId;
	
	/** IP address of the node this cbolt runs on */
	protected String ipAddr;
	
	/** Index of this task in the list of all tasks for the ProtomemeClusteringBolt component */
	protected int taskIdxForComponent;
	
	/** marks the ID of this 'batch of protomemes' before the next synchronization happens */
	protected int batchId = 0;
	
	/** Number of protomemes in a batch */
	protected int numPmsInBatch;
	
	/** number of protomeme clustering bolts */
	protected int numClusteringBoltTasks;
	
	/** Number of padding tuples to emit to 'push' the sync requests out */
	protected int paddingCount;
	
	/** Output collector for this bolt task */
	protected OutputCollector collector;
	
	/** Tells if the bolt is done with one batch of protomemes and waiting for synchronization */
	protected boolean waitingForSync;
	
	/** ActiveMQ connection */
	protected Connection amqConnection;
	
	/** ActiveMQ session */
	protected Session amqElectSession;
	
	/** ActiveMQ receiver for synchronization messages */
	protected MessageConsumer amqElectReceiver;
	
	/** whether the elect message has been processed */
	protected boolean electMsgProcessed;
	
	/** ActiveMQ session */
	protected Session amqSyncSession;
	
	/** ActiveMQ receiver for synchronization messages */
	protected MessageConsumer amqSyncReceiver;
	
	/** ActiveMQ topic for representative cbolt election */
	protected String electTopicStr;
	
	/** ActiveMQ topic for synchronization */
	protected String syncTopicStr;
	
	/** Tells whether an "end of stream" message has been received from the spout */
	protected boolean endOfStream = false;
	
	/** Path of the final results file */
	protected String finalResultsPath;
	
	/** computing start time for each batch */
	protected long compStartTime;
	
	/** start time for each synchronization */
	protected long syncStartTime;
	
	/** total time spent on computing */
	protected long totalCompTime;
	
	/** total time spent on synchronization */
	protected long totalSyncTime;

	/** whether a synch initialization message has been received from the sync coordinator */
	protected boolean syncInitReceived = false;
	
	/** log once for how many protomemes */
	protected int numPmsForLog;
	
	/** Some functions in this helper will be useful for generating protomemes */
	protected MemeClusteringTester helper;
	
	@Override
	public void execute(Tuple input) {
		try {
			if (!electMsgProcessed) {
				Utils.sleep(1000);
			}
			
			if (input.getSourceStreamId().equals(StreamClusteringTopology.CONTROL_STREAM_NAME)) {
				int msgType = input.getIntegerByField(StreamClusteringTopology.FIELD_MSG_TYPE);
				if (msgType == StreamClusteringTopology.PMGEN_MSG_ENDSTREAM) {
					collector.ack(input);
					endOfStream = true;
					List<Object> tuple = new ArrayList<Object>(3);
					tuple.add(StreamClusteringTopology.CBOLT_MSG_SYNC_REQ);
					tuple.add(batchId);
					tuple.add(endOfStream);
					long compTime = System.currentTimeMillis() - compStartTime;
					totalCompTime += compTime;
					LOG.info("Task " + taskId + ": emitting SYNC request for end of stream, computing time for batch " + batchId +
							": " + (compTime / 1000.0) + "s.");
					collector.emit(tuple);
					synchronized (syncLock) {
						syncStartTime = System.currentTimeMillis();
						waitingForSync = true;
						syncLock.wait();
					}
					waitingForSync = false;
					processSyncMsg(syncMsgStr);
					long syncTime = System.currentTimeMillis() - syncStartTime;
					totalSyncTime += syncTime;
					LOG.info("Task " + taskId + ": Sync time for last batch: " + (syncTime / 1000.0) + "s. Total compute time: " + 
							(totalCompTime / 1000.0) + "s, total synchronization time: " + (totalSyncTime / 1000.0) + "s.");
					compStartTime = System.currentTimeMillis();
				} else {
					LOG.error("Task " + taskId + ": Unsupported message type from the control stream of protomeme generator: " + msgType);
				}
			} else {
				String pmJson = input.getStringByField(StreamClusteringTopology.FIELD_PM);
				ProtoMeme pm = gson.fromJson(pmJson, ProtoMeme.class);
				processProtomeme(pm, input);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Task " + taskId + ": Exception when processing protomeme: " + e.getMessage());
		}
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		try {
			gson = new Gson();
			taskId = context.getThisTaskId();
			taskIdxForComponent = context.getThisTaskIndex();
			this.collector = collector;
			helper = new MemeClusteringTester();
			numPmsForLog = Integer.parseInt((String) conf.get(StreamClusteringTopology.CBOLT_LOG_CONFKEY));
			timeStepInMilli = Long.parseLong((String) conf.get(StreamClusteringTopology.TIME_STEP_SEC_CONFKEY)) * 1000;
			slidingWinLenInSteps = Integer.parseInt((String) conf.get(StreamClusteringTopology.SLIDE_WIN_LEN_CONFKEY));
			pmLists = new ArrayDeque<List<ProtoMeme>>(slidingWinLenInSteps * 3 / 2);
			numClusteringBoltTasks = Integer.parseInt((String) conf.get(StreamClusteringTopology.CBOLT_PARA_CONFKEY));
			numPmsInBatch = Integer.parseInt((String) conf.get(StreamClusteringTopology.CBOLT_BATCH_CONFKEY));
			paddingCount = Integer.parseInt((String) conf.get(StreamClusteringTopology.CBOLT_PADDING_CONFKEY));
			outlierClusterIdBase = Integer.parseInt((String) conf.get(StreamClusteringTopology.OUTLIER_CID_CONFKEY));
			String startingTweetTime = (String) conf.get(StreamClusteringTopology.FIRST_TWEET_TIME_CONFKEY);
			calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
			GeneralHelpers.setDateTimeByString(calTmp, startingTweetTime);
			long curStepStartMilli = calTmp.getTimeInMillis();
			curWinStartMilli = curStepStartMilli - (slidingWinLenInSteps - 1) * timeStepInMilli;
			curWinEndMilli = curStepStartMilli + timeStepInMilli - 1;
			pmMarkerCount = new HashMap<String, Integer>();
			pmMarkerClusterMap = new HashMap<String, Integer>();
			tmpMarkerClusterMap = new HashMap<String, Integer>();
			String bootstrapStartTime = (String) conf.get(StreamClusteringTopology.BOOT_TWEET_TIME_CONFKEY);
			GeneralHelpers.setDateTimeByString(calTmp, bootstrapStartTime);
			bootstrapStartMilli = calTmp.getTimeInMillis();
			String bootstrapInfoPath = (String) conf.get(StreamClusteringTopology.BOOTSTRAP_PATH_CONFKEY);
			readBootstrapInfo(bootstrapInfoPath);
			finalResultsPath = (String) conf.get(StreamClusteringTopology.RESULT_PATH_CONFKEY);
			waitingForSync = false;
			String amqUri = (String) conf.get(StreamClusteringTopology.ACTIVEMQ_URI_CONFKEY);
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(amqUri);
			amqConnection = connectionFactory.createConnection();
			amqConnection.start();
			electTopicStr = (String) conf.get(StreamClusteringTopology.ELECT_TOPIC_CONFKEY);
			syncTopicStr = (String) conf.get(StreamClusteringTopology.SYNC_TOPIC_CONFKEY);
			amqElectSession = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Topic electTopic = amqElectSession.createTopic(electTopicStr);
			amqElectReceiver = amqElectSession.createConsumer(electTopic);
			amqElectReceiver.setMessageListener(this);
			ipAddr = InetAddress.getLocalHost().getHostAddress() + "-" + context.getThisWorkerPort();
			electMsgProcessed = false;
			reportIPToSyncCoord();
			compStartTime = System.currentTimeMillis();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Task " + taskId + ": Exception in prepare(): " + e.getMessage());
		}
	}

	/**
	 * Emit a tuple to the sync coordinator to report the IP address of this cbolt task.
	 */
	protected void reportIPToSyncCoord() {
		List<Object> tuple = new ArrayList<Object>(3);
		tuple.add(StreamClusteringTopology.CBOLT_MSG_IP_REPORT);
		tuple.add(batchId);
		tuple.add(ipAddr);
		collector.emit(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(StreamClusteringTopology.FIELD_MSG_TYPE, StreamClusteringTopology.FIELD_BATCH_ID,
				StreamClusteringTopology.FIELD_PM));
	}
	
	@Override
	public void cleanup() {
		try {
			if (amqElectReceiver != null) {
				amqElectReceiver.close();
			}
			if (amqElectSession != null) {
				amqElectSession.close();
			}
			if (amqSyncReceiver != null) {
				amqSyncReceiver.close();
			}
			if (amqSyncSession != null) {
				amqSyncSession.close();
			}
			if (amqConnection != null) {
				amqConnection.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Task " + taskId + ": Exception when closing ActiveMQ session and connection: " + e.getMessage());
		}
	}
	
	/**
	 * Read global parameter values and initial clusters from the bootstrap information file path
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
		params = gson.fromJson(line, GlobalClusteringParams.class);
		simMean = params.sumSimForMeanStd / params.nSimsForMeanStd;
		simStdDev = Math.sqrt(params.ssSimForMeanStd / params.nSimsForMeanStd - simMean * simMean);
		
		//3rd line: "Protomemes in the final 120 clusters:"
		line = brBoot.readLine();
		int idx2 = line.lastIndexOf(' ');
		int idx1 = line.lastIndexOf(' ', idx2-1);
		int clusterNum = Integer.parseInt(line.substring(idx1+1, idx2));
		clusters = new ArrayList<ProtomemeCluster>((int)(params.initClusterNum * params.clusterNumMultiplier));
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
					int timeStepIdx = (int)((pm.earliestTweetTs - curWinStartMilli) / timeStepInMilli);
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
		if (taskIdxForComponent == 0) {
			LOG.info("clusters info after reading bootstrap:");
			helper.printFinalAndDeletedClusters(clusters, null, false);
		}
	}
	
	/**
	 * Process a protomeme received from the spout.
	 * @param pm
	 *  A received protomeme.
	 * @param input
	 *  The input tuple to anchor and ack.
	 * @throws Exception
	 */
	protected void processProtomeme(ProtoMeme pm, Tuple input) throws Exception {
//		if (numClusteringBoltTasks > 48 && batchId == 0 && pmCount >= numPmsInBatch) {
//			Utils.sleep(300);
//		}
		// first, check if a new time step starts. If so, delete old protomemes falling out of the sliding window
		while (pm.earliestTweetTs > curWinEndMilli) {
			LOG.info("Task " + taskId + ": new time window started");
			List<ProtoMeme> pmList = new LinkedList<ProtoMeme>();
			pmLists.add(pmList);
			if (pmLists.size() > slidingWinLenInSteps) {
				List<ProtoMeme> oldPms = pmLists.remove();
				deleteOldPms(oldPms);
			}
			curWinStartMilli += timeStepInMilli;
			curWinEndMilli += timeStepInMilli;
		}

		// then, check if the protomeme falls into any existing clusters
		Integer clusterId = pmMarkerClusterMap.get(pm.mainMarker);
		if (clusterId == null) {
			clusterId = tmpMarkerClusterMap.get(pm.mainMarker);
		}
		if (clusterId != null) {
			pm.clusterId = clusterId;
			pm.isClusteredByMarker = true;
			ProtomemeCluster c = clusters.get(clusterId);
			pm.similarityToCluster = c.computeSimilarity(pm);
			// send an "adding protomeme to cluster" message to the coordinator bolt
			List<Object> tuple = new ArrayList<Object>(3);
			tuple.add(StreamClusteringTopology.CBOLT_MSG_PMADD);
			tuple.add(batchId);
			tuple.add(gson.toJson(pm, ProtoMeme.class));
			// we don't anchor the output tuple here because we want to avoid replaying 'old' protomemes that
			// could potentially fall out of the sliding time window. Missing a protomeme is tolerable.
			collector.emit(tuple);
			pmAddCount++;
		} else {
			// assign pm to the closest cluster, or report an outlier to the coordinator bolt
			int idClosestCluster = -1;
			double maxSimilarity = 0;
			for (int i=0; i<clusters.size(); i++) {
				ProtomemeCluster c = clusters.get(i);
				if (c == null) {
					continue;
				}
				double sim = c.computeSimilarity(pm);
				if (sim >= maxSimilarity) {
					maxSimilarity = sim;
					idClosestCluster = i;
				}
			}
			if (maxSimilarity > 0 && maxSimilarity > simMean - params.outlierThd * simStdDev) {
				pm.clusterId = idClosestCluster;
				pm.similarityToCluster = maxSimilarity;
				tmpMarkerClusterMap.put(pm.mainMarker, idClosestCluster);
				List<Object> tuple = new ArrayList<Object>(3);
				tuple.add(StreamClusteringTopology.CBOLT_MSG_PMADD);
				tuple.add(batchId);
				tuple.add(gson.toJson(pm, ProtoMeme.class));
				collector.emit(tuple);
				pmAddCount++;
			} else {
				pm.clusterId = - 1;
				pm.similarityToCluster = 0.0;
				List<Object> tuple = new ArrayList<Object>(3);
				tuple.add(StreamClusteringTopology.CBOLT_MSG_OUTLIER);
				tuple.add(batchId);
				tuple.add(gson.toJson(pm, ProtoMeme.class));
				collector.emit(tuple);
				outlierCount++;
			}
		}
		pmCount++;
		if (pmCount % numPmsForLog == 0) {
			LOG.info("Task " + taskId + ": processed " + pmCount + " messages in batch " + batchId + ", PMADD: " + pmAddCount + ", OUTLIER: "
					+ outlierCount);
		}
		if (syncInitReceived && !endOfStream) {
			List<Object> tuple = new ArrayList<Object>(3);
			tuple.add(StreamClusteringTopology.CBOLT_MSG_SYNC_REQ);
			tuple.add(batchId);
			tuple.add(endOfStream);
			long compTime = System.currentTimeMillis() - compStartTime;
			totalCompTime += compTime;
			LOG.info("Taks " + taskId + ": emitting SYNC request. Finished " + pmCount + " messages in batch " + batchId + ", PMADD: " + pmAddCount
					+ ", OUTLIER: " + outlierCount + ", computing time: " + (compTime / 1000.0) + "s, number of paddings: " + paddingCount);
			collector.emit(tuple);
			for (int i=0; i<paddingCount; i++) {
				tuple = new ArrayList<Object>(3);
				tuple.add(StreamClusteringTopology.CBOLT_MSG_PADDING);
				tuple.add(batchId);
				tuple.add(gson.toJson(pm, ProtoMeme.class));
				collector.emit(tuple);
			}
			syncInitReceived = false;
			synchronized (syncLock) {
				syncStartTime = System.currentTimeMillis();
				waitingForSync = true;
				syncLock.wait();
			}
			waitingForSync = false;
			processSyncMsg(syncMsgStr);
			long syncTime = System.currentTimeMillis() - syncStartTime;
			totalSyncTime += syncTime;
			LOG.info("Task " + taskId + ": Sync time for last batch: " + (syncTime / 1000.0) + "s. Total compute time: " + (totalCompTime / 1000.0) +
					"s. Total sync time: " + (totalSyncTime / 1000.0) + "s.");
			compStartTime = System.currentTimeMillis();
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
				if (c.protomemes.size() <= 0) {
					clusters.set(pm.clusterId, null);
				}
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
	 * Process synchronization message received from ActiveMQ.
	 */
	@Override
	public void onMessage(Message amqMessage) {
		if (!(amqMessage instanceof TextMessage)) {
			LOG.info("Task " + taskId + ": Type of received ActiveMQ message is not TextMessage");
			return;
		}
		
		try {
			TextMessage textMessage = (TextMessage) amqMessage;
			String msgStr = textMessage.getText();
			if (msgStr.startsWith(StreamClusteringTopology.ELECT_MSG_MARKER)) {
				LOG.info("Task " + taskId + ": received elect message. Time since start working: " + 
						(System.currentTimeMillis() - compStartTime) / 1000.0 + "s." );
				processElectMsg(msgStr);
				return;
			}
			
			if (msgStr.equals(StreamClusteringTopology.SYNC_INIT_MSG)) {
				LOG.info("Task " + taskId + ": received sync init.");
				syncInitReceived = true;
				return;
			}

			if (!waitingForSync && !endOfStream) {
				LOG.error("Task " + taskId + ": received ActiveMQ synch message when waitingForSync is " + waitingForSync);
				return;
			}
			// read deltas from the synchronization message and update the clusters
			syncMsgStr = textMessage.getText();
			boolean processHere = !waitingForSync && endOfStream;
			synchronized (syncLock) {
				syncLock.notifyAll();
			}
			if (processHere) {
				processSyncMsg(syncMsgStr);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Task " + taskId + ": Exception when processing received ActiveMQ message: " + e.getMessage());
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
		pwRes.println(gson.toJson(params, GlobalClusteringParams.class));
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
	
	/** 
	 * Read IP addresses and taskIds from the electMsg, and decide whether this cblolt should
	 * be elected as the sync representative on this node.
	 * @param electMsg
	 */
	protected void processElectMsg(String electMsg) throws Exception {
		// electMsg is in the form of  "elect:[ip:taskId,ip:taskId,...]
		int idx = electMsg.indexOf(':');
		String ipTaskIdPairs = electMsg.substring(idx + 2, electMsg.length() - 1);
		for (String pair : ipTaskIdPairs.split(",")) {
			idx = pair.indexOf(':');
			String ip = pair.substring(0, idx);
			int taskId = Integer.parseInt(pair.substring(idx + 1));
			if (ip.equals(this.ipAddr) && taskId == this.taskId) {
				amqSyncSession = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	            Topic syncTopic = amqSyncSession.createTopic(syncTopicStr);
	            amqSyncReceiver = amqSyncSession.createConsumer(syncTopic);
	            amqSyncReceiver.setMessageListener(this);
	            LOG.info("Task " + this.taskId + ": selected as repsentative on " + this.ipAddr);
	            break;
			}
		}
		electMsgProcessed = true;
	}
	
	/**
	 * Process the synchronization message string received from the sync coordinator.
	 * @param syncMsgStr
	 * @throws Exception
	 */
	protected void processSyncMsg(String syncMsgStr) throws Exception {
		SyncMessage syncMsg = gson.fromJson(syncMsgStr, SyncMessage.class);
		params = syncMsg.params;
		List<ProtoMeme> newPms = new LinkedList<ProtoMeme>();
		int nOutlier = 0;
		for (ProtomemeClusterDelta delta : syncMsg.deltas) {
			ProtomemeCluster targetCluster = null;
			if (delta.clusterId < outlierClusterIdBase) {
				// update an existing cluster
				targetCluster = clusters.get(delta.clusterId);
				if (targetCluster == null) {
					// this could happen when all the protomemes in targetCluster are removed after some PMADD have been sent.
					targetCluster = new ProtomemeCluster();
					clusters.set(delta.clusterId, targetCluster);
				}
			} else {
				// an outlier cluster replaces an existing one 
				delta.clusterId -= outlierClusterIdBase;
				targetCluster = new ProtomemeCluster();
				clusters.set(delta.clusterId, targetCluster);
				nOutlier++;
			}
			for (ProtoMeme pm : delta.pms) {
				if (pm.earliestTweetTs < curWinStartMilli) {
					LOG.warn("Task " + taskId + ": Received protomeme older than current sliding window.");
					continue;
				}
				targetCluster.addProtoMeme(pm);
				Integer count = pmMarkerCount.get(pm.mainMarker);
				if (count == null) {
					pmMarkerCount.put(pm.mainMarker, 1);
				} else {
					pmMarkerCount.put(pm.mainMarker, count + 1);
				}
				pmMarkerClusterMap.put(pm.mainMarker, delta.clusterId);
				newPms.add(pm);
				while (pm.earliestTweetTs > curWinEndMilli) {
					// temporarily increase pmLists to make space for new protomemes
					List<ProtoMeme> pmList = new LinkedList<ProtoMeme>();
					pmLists.add(pmList);						
					curWinEndMilli += timeStepInMilli;
				}
			}
			targetCluster.latestUpdateTime = delta.latestUpdateMilli;
		}
		
		// add new protomemes to the temporarily increased pmLists
		Object[] pmListArray = pmLists.toArray();
		for (ProtoMeme pm : newPms) {
			int stepIdx = (int)((pm.earliestTweetTs - curWinStartMilli) / timeStepInMilli);
			((List<ProtoMeme>)pmListArray[stepIdx]).add(pm);
		}
		// shrink pmLists and remove old protomemes
		while (pmLists.size() > slidingWinLenInSteps) {
			List<ProtoMeme> oldPms = pmLists.remove();
			deleteOldPms(oldPms);
			curWinStartMilli += timeStepInMilli;
		}
		
		if (!endOfStream) {
			LOG.info("Task " + taskId + ": processed synchronization message, number of outlier clusters: " + nOutlier + ". Starting new batch..");
			if (taskIdxForComponent == 0) {
				helper.printFinalAndDeletedClusters(clusters, null, false);
			}
		} else if ((int)params.centUpdateTime == numClusteringBoltTasks) {
			// params.centUpdateTime here has the piggybacked 'number of finished clustering bolt tasks' information
			cleanup();
			if (taskIdxForComponent == 0) {
				writeFinalResults(finalResultsPath);
				LOG.info("Task " + taskId + ": processed final synchronization message, number of outlier clusters: " + nOutlier +
						". Written results to " + finalResultsPath);
			}
		}
		batchId++;
		pmCount = 0;
		pmAddCount = 0;
		outlierCount = 0;
		tmpMarkerClusterMap.clear();
		simMean = params.sumSimForMeanStd / params.nSimsForMeanStd;
		simStdDev = Math.sqrt(params.ssSimForMeanStd / params.nSimsForMeanStd - simMean * simMean);
	}
}