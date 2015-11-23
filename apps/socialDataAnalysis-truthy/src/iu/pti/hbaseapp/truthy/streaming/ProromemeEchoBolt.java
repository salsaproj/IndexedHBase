package iu.pti.hbaseapp.truthy.streaming;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ProromemeEchoBolt extends BaseRichBolt {
	private static final long serialVersionUID = -5728982573517512909L;

	protected static final Log LOG = LogFactory.getLog(ProromemeEchoBolt.class);
	
	/** Used to parse the JSON string of a protomeme */
	protected Gson gson;
	
	/** number of protomemes received so far */
	protected int pmCount = 0;
	
	/** Storm task ID */
	protected int taskID;
	
	/** Output collector for this bolt task */
	protected OutputCollector collector;

	@Override
	public void execute(Tuple touple) {
		String pmJson = touple.getStringByField("pm");
		ProtoMeme pm = gson.fromJson(pmJson, ProtoMeme.class);
		collector.ack(touple);
		pmCount++;
		if (pmCount % 10000 == 0) {
			LOG.info("Task " + taskID + "---Received " + pmCount + " protomemes. Last one:"  + pm.mainMarker +
					" number of tweet IDs: " + pm.tweetIds.size());
		}		
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		gson = new Gson();
		taskID = context.getThisTaskId();
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}
	
	@Override
	public void cleanup() {
		LOG.info("Task " + taskID + "---Total number of protomemes received: " + pmCount);
	}
}
