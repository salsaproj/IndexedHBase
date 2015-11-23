package iu.pti.hbaseapp.truthy.streaming;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;
import iu.pti.hbaseapp.truthy.streaming.MemeClusteringTester.DiffusionIndices;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class FileProtomemeSpout extends BaseRichSpout implements MessageListener {
	protected static final Log LOG = LogFactory.getLog(FileProtomemeSpout.class);

	private static final long serialVersionUID = 9030175615674742538L;
	
	/** Path to the	.json.gz file containing tweets. Should be received from the topology configuration. */
	protected String jsonGzPath;
	
	/** Input .json.gz file reader */
	protected BufferedReader brInput;
	
	/** Length of a time step in milliseconds */
	protected long timeStepInMilli;
	
	/** Length of the siding window in number of time steps */
	protected int slidingWinLenInSteps;
	
	/** The start time (in milliseonds) of 'current' time step */ 
	protected long curStepStartMilli;
	
	/** The end time (in milliseonds) of 'current' time step */
	protected long curStepEndMilli;
	
	/** Useful for conversion between time and string */
	protected Calendar calTmp;
	
	/** The protomeme queue */
	protected LinkedList<ProtoMeme> pmQueue;
	
	/** Some functions in this helper will be useful for generating protomemes */
	protected MemeClusteringTester helper;
	
	/** Set of ground truth hashtags to remove when generating protomemes. */
	protected Set<String> gtHashtags = null;
	
	/** For serializing a protomeme */
	protected Gson gson;
	
	/** tells whether the "end of stream" message has been sent */
	protected boolean endOfStreamSent = false;
	
	/** ActiveMQ connection */
	protected Connection amqConnection;
	
	/** ActiveMQ session */
	protected Session amqSession;
	
	/** ActiveMQ receiver for report messages */
	protected MessageConsumer amqReceiver;
	
	protected SpoutOutputCollector collector;
	protected Long msgId;
	protected long ackMsgCount;
	protected long sentMsgCount;
	protected long maxUnAckMsgCount;

	@Override
	public void nextTuple() {
		if (sentMsgCount - ackMsgCount > maxUnAckMsgCount) {
			return;
		}
		try {
			if (pmQueue.size() == 0) {
				if (brInput == null) {
					if (!endOfStreamSent) {
						List<Object> tuple = new ArrayList<Object>(1);
						tuple.add(StreamClusteringTopology.PMGEN_MSG_ENDSTREAM);
						collector.emit(StreamClusteringTopology.CONTROL_STREAM_NAME, tuple, msgId);
						LOG.info("No more input to process. Total number of protomemes sent: " + sentMsgCount);
						msgId++;
						endOfStreamSent = true;						
					} else {						
						Utils.sleep(60000);
					}
					return;
				} else {
					Map<String, TweetFeatures> tidFeaturesMap = new HashMap<String, TweetFeatures>();
					DiffusionIndices diffIndices = new DiffusionIndices();
					boolean endOfFile = false;
					while (tidFeaturesMap.size() <= 0 && !endOfFile) {
						endOfFile = helper.readTweetsByTimeWin(tidFeaturesMap, diffIndices, curStepStartMilli, curStepEndMilli, brInput);
						if (endOfFile) {
							brInput.close();
							brInput = null;
						}
						curStepStartMilli = curStepEndMilli + 1;
						curStepEndMilli += timeStepInMilli;
					}
					if (tidFeaturesMap.size() <= 0) {
						return;
					}
					Map<String, ProtoMeme> protomemeMap = new HashMap<String, ProtoMeme>();
					helper.getProtomemesByTweets(tidFeaturesMap, diffIndices, protomemeMap, null, gtHashtags);
					pmQueue.addAll(protomemeMap.values());
				}
			}
			ProtoMeme pm = pmQueue.remove();
			List<Object> tuple = new ArrayList<Object>(2);
			tuple.add(pm.mainMarker);
			tuple.add(gson.toJson(pm, ProtoMeme.class));
			collector.emit(StreamClusteringTopology.PM_STREAM_NAME, tuple);
			sentMsgCount++;
			if (sentMsgCount % 1000 == 0) {
				LOG.info("number of messages sent: " + sentMsgCount);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Exception in nextTuple(): " + e.getMessage());
		}		
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		msgId = 0L;
		ackMsgCount = 0;
		sentMsgCount = 0;
		maxUnAckMsgCount = Integer.parseInt((String)conf.get(StreamClusteringTopology.MAX_UNACK_CONFKEY));
		timeStepInMilli = Long.parseLong((String)conf.get(StreamClusteringTopology.TIME_STEP_SEC_CONFKEY)) * 1000;
		slidingWinLenInSteps = Integer.parseInt((String)conf.get(StreamClusteringTopology.SLIDE_WIN_LEN_CONFKEY));
		String startingTweetTime = (String)conf.get(StreamClusteringTopology.FIRST_TWEET_TIME_CONFKEY);
		calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		GeneralHelpers.setDateTimeByString(calTmp, startingTweetTime);
		curStepStartMilli = calTmp.getTimeInMillis();
		curStepEndMilli = curStepStartMilli + timeStepInMilli - 1;
		pmQueue = new LinkedList<ProtoMeme>();
		helper = new MemeClusteringTester();
		gson = new Gson();
		jsonGzPath = (String)conf.get(StreamClusteringTopology.JSONGZ_PATH_CONFKEY);
		try {
			brInput = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(jsonGzPath)), "UTF-8"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		String hashtags = (String)conf.get(StreamClusteringTopology.GT_HASHTAG_CONFKEY);
		if (hashtags != null && hashtags.length() > 0) {
			gtHashtags = helper.parseHashtags(hashtags);
		}
		try {
			String amqUri = (String) conf.get(StreamClusteringTopology.ACTIVEMQ_URI_CONFKEY);
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(amqUri);
			amqConnection = connectionFactory.createConnection();
			amqConnection.start();
			amqSession = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Topic topic = amqSession.createTopic((String) conf.get(StreamClusteringTopology.REPORT_TOPIC_CONFKEY));
			amqReceiver = amqSession.createConsumer(topic);
			amqReceiver.setMessageListener(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(StreamClusteringTopology.PM_STREAM_NAME, 
				new Fields(StreamClusteringTopology.FIELD_MARKER, StreamClusteringTopology.FIELD_PM));
		declarer.declareStream(StreamClusteringTopology.CONTROL_STREAM_NAME, new Fields(StreamClusteringTopology.FIELD_MSG_TYPE));
	}
	
	/**
	 * Process report message received from ActiveMQ.
	 */
	@Override
	public void onMessage(Message amqMessage) {
		try {
			// read deltas from the synchronization message and update the clusters
			TextMessage textMessage = (TextMessage)amqMessage;
			ackMsgCount = Integer.parseInt(textMessage.getText());
			if (endOfStreamSent && ackMsgCount == sentMsgCount) {
				if (amqReceiver != null) {
					amqReceiver.close();
				}
				if (amqSession != null) {
					amqSession.close();
				}
				if (amqConnection != null) {
					amqConnection.close();
				}
			}
			LOG.info("number of processed messages reported by sync coordinator: " + ackMsgCount);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Exception when processing received ActiveMQ message: " + e.getMessage());
		}
	}
}