package iu.pti.hbaseapp.truthy.streaming;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class StreamClusteringTopology {
	public static final String TOPO_DEBUG_CONFKEY = "topology.debug.mode";
	public static final String LOCAL_MODE_CONFKEY = "run.local.mode";
	public static final String SYNC_MODE_CONFKEY = "sync.centroids.transfer.mode";
	public static final String MAX_UNACK_CONFKEY = "max.unack.msg.count";
	public static final String NUM_ACKER_CONFKEY = "acker.count";
	public static final String JSONGZ_PATH_CONFKEY = "json.gz.path";
	public static final String GT_HASHTAG_CONFKEY = "ground.truth.hahstags";
	public static final String TIME_STEP_SEC_CONFKEY = "time.step.seconds";
	public static final String SLIDE_WIN_LEN_CONFKEY = "sliding.window.in.steps";
	public static final String FIRST_TWEET_TIME_CONFKEY = "start.tweet.time";
	public static final String BOOT_TWEET_TIME_CONFKEY = "bootstrap.tweet.time";
	public static final String SYNC_COORD_WAIT_CONFKEY = "sync.coord.wait.seconds";
	public static final String BOOTSTRAP_PATH_CONFKEY = "bootstrap.cluster.info.path";
	public static final String NUM_WORKER_CONFKEY = "topology.num.workers";
	public static final String CBOLT_PARA_CONFKEY = "cluster.bolt.parallelism";
	public static final String CBOLT_BATCH_CONFKEY = "cluster.bolt.batch.pms";
	public static final String CBOLT_PADDING_CONFKEY = "cluster.bolt.padding";
	public static final String CBOLT_LOG_CONFKEY = "cluster.bolt.log.pms";
	public static final String OUTLIER_CID_CONFKEY = "outlier.cluster.id.base";
	public static final String ACTIVEMQ_URI_CONFKEY = "activemq.broker.uri";
	public static final String SYNC_TOPIC_CONFKEY = "activemq.sync.topic";
	public static final String ELECT_TOPIC_CONFKEY = "activemq.elect.topic";
	public static final String REPORT_TOPIC_CONFKEY = "activemq.report.topic";
	public static final String RESULT_PATH_CONFKEY = "final.results.path";
	public static final String SYNC_MODE_DELTA = "delta";
	public static final String SYNC_MODE_WHOLE = "full";
	public static final String PM_GEN_SPOUT_NAME = "pmGenerator";
	public static final String PM_ECHO_BOLT_NAME = "pmEcho";
	public static final String PM_CLUSTER_BOLT_NAME = "pmCluster";
	public static final String SYNC_COORD_BOLT_NAME = "syncCoord";
	public static final String SYNC_INIT_MSG = "syncInit";
	public static final String ELECT_MSG_MARKER = "elect";
	public static final String PM_STREAM_NAME = "pmStream";
	public static final String CONTROL_STREAM_NAME = "controlStream";
	public static final String FIELD_MARKER = "marker";
	public static final String FIELD_PM = "pm";
	public static final String FIELD_MSG_TYPE = "type";
	public static final String FIELD_BATCH_ID = "batch";
	public static final String CLUSTERING_TOPO_NAME = "tweetStreamClusteringTopology";
	public static final String TEST_TOPO_NAME = "testTopology";
	/** message type of tuples emitted by the clustering bolt: add protomeme to cluster */
	public static final int CBOLT_MSG_PMADD = 1;
	/** message type of tuples emitted by the clustering bolt: delete protomeme from cluster */
	public static final int CBOLT_MSG_PMDEL = 2;
	/** message type of tuples emitted by the clustering bolt: synchronization request */
	public static final int CBOLT_MSG_SYNC_REQ = 3;
	/** message type of tuples emitted by the clustering bolt: potential outlier */
	public static final int CBOLT_MSG_OUTLIER = 4;
	/** message type of tuples emitted by the clustering bolt: padding for pushing tuples out */
	public static final int CBOLT_MSG_PADDING = 5;
	/** message type of tuples emitted by the proromeme generator spout: end of stream reached */
	public static final int PMGEN_MSG_ENDSTREAM = 6;
	/** message type of tuples emitted by the clustering bolt: reporting IP address */
	public static final int CBOLT_MSG_IP_REPORT = 7;
	
	/**
	 * Read configuration proterties from <b>confPath</b> to <b>conf</b>.
	 * @param confPath
	 *  Path to the configuration file.
	 * @param conf
	 *  The configuration object.
	 */
	public static void readConfig(String confPath, Map<String, Object> conf) throws Exception {
		BufferedReader brConf = new BufferedReader(new FileReader(confPath));
		String line = brConf.readLine();
		while (line != null) {
			line = line.trim();
			if (line.length() > 0) {
				int idx = line.indexOf('=');
				if (idx < 0) {
					brConf.close();
					throw new Exception("Bad line in configuration file: " + line);
				}
				conf.put(line.substring(0, idx), line.substring(idx + 1));
			}
			line = brConf.readLine();
		}
		brConf.close();
	}
	
	public static void main(String[] args) {
		try {
			String confPath = args[0];
			Config conf = new Config();
			readConfig(confPath, conf);
			conf.setDebug(Boolean.parseBoolean((String)conf.get(TOPO_DEBUG_CONFKEY)));
			boolean localMode = Boolean.parseBoolean((String)conf.get(LOCAL_MODE_CONFKEY));
			int cboltPara = Integer.parseInt((String)conf.get(CBOLT_PARA_CONFKEY));
			conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
			conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
			conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 32768);
			conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 32768);
			conf.setNumAckers(Integer.parseInt((String)conf.get(NUM_ACKER_CONFKEY)));
			conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 120000);
			conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 120000);
			
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout(PM_GEN_SPOUT_NAME, new FileProtomemeSpout(), 1);
			if (conf.get(SYNC_MODE_CONFKEY).equals(StreamClusteringTopology.SYNC_MODE_DELTA)) {
				BoltDeclarer cboltDeclarer = builder.setBolt(PM_CLUSTER_BOLT_NAME, new ProtomemeClusteringBolt(), cboltPara);
				cboltDeclarer.fieldsGrouping(PM_GEN_SPOUT_NAME, StreamClusteringTopology.PM_STREAM_NAME, new Fields(FIELD_MARKER));
				cboltDeclarer.allGrouping(PM_GEN_SPOUT_NAME, StreamClusteringTopology.CONTROL_STREAM_NAME);
				builder.setBolt(SYNC_COORD_BOLT_NAME, new SyncCoordinateBolt(), 1).globalGrouping(PM_CLUSTER_BOLT_NAME);
			} else {
				BoltDeclarer cboltDeclarer = builder.setBolt(PM_CLUSTER_BOLT_NAME, new FullSyncPmClusteringBolt(), cboltPara);
				cboltDeclarer.fieldsGrouping(PM_GEN_SPOUT_NAME, StreamClusteringTopology.PM_STREAM_NAME, new Fields(FIELD_MARKER));
				cboltDeclarer.allGrouping(PM_GEN_SPOUT_NAME, StreamClusteringTopology.CONTROL_STREAM_NAME);
				builder.setBolt(SYNC_COORD_BOLT_NAME, new FullSyncCoordinateBolt(), 1).globalGrouping(PM_CLUSTER_BOLT_NAME);
			}
			if (!localMode) {
				conf.setNumWorkers(Integer.parseInt((String)conf.get(StreamClusteringTopology.NUM_WORKER_CONFKEY)));
				StormSubmitter.submitTopologyWithProgressBar(CLUSTERING_TOPO_NAME, conf, builder.createTopology());
			} else {
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(TEST_TOPO_NAME, conf, builder.createTopology());
				Utils.sleep(120000);
				cluster.killTopology(TEST_TOPO_NAME);
				cluster.shutdown();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
