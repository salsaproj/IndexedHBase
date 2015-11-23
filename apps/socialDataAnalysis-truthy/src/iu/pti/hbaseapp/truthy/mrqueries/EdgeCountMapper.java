package iu.pti.hbaseapp.truthy.mrqueries;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import iu.pti.hbaseapp.truthy.ConstantsTruthy;
import iu.pti.hbaseapp.truthy.TruthyHelpers;
import iu.pti.hbaseapp.truthy.TweetSubsetProcessor.TwitterIdProcessMapper;

/**
 * Mapper class for the "get-retweet-edges" and "get-mention-edges" queries.
 * 
 * @author gaoxm
 */
public class EdgeCountMapper extends TwitterIdProcessMapper<Text, LongWritable> {
    
    private static final Log LOG = LogFactory.getLog(EdgeCountMapper.class);
    private long HBaseIOTime = 0L;
    private long UDFStartTime = 0L;
    
	enum EdgeType {
		MENTION, RETWEET
	}
	
	enum EdgeDirection {
		IN, OUT
	}
	
	EdgeType edgeType = EdgeType.MENTION;
	EdgeDirection direction = EdgeDirection.IN;
	HTable tweetTable = null;
	HashMap<String, Long> edgeCountMap = null;
	boolean useBigInt = false;
	
	@Override
	protected void map(LongWritable rowNum, Text txtTweetId, Context context) throws IOException, InterruptedException {
	    byte[] tweetId = null;
	    if (this.useBigInt) {
	        tweetId = TruthyHelpers.getTweetIdBigIntBytes(txtTweetId.toString());
		} else {
		    tweetId = TruthyHelpers.getTweetIdBytes(txtTweetId.toString());
		}
	    
	    long startTime = System.currentTimeMillis();
		if (edgeType == EdgeType.MENTION) {		    
			Get get = new Get(tweetId);
			get.addColumn(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_USER_ID_BYTES);
			get.addColumn(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_ENTITIES_BYTES);
			Result r = tweetTable.get(get);
			this.HBaseIOTime += System.currentTimeMillis() - startTime;
			if (r == null) {
				context.setStatus("Can't find tweet ID " + txtTweetId.toString());
				return;
			}
			byte[] userIdBytes = r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_USER_ID_BYTES);
			String userId = TruthyHelpers.getUserIDStrFromBytes(userIdBytes);
			String entities = Bytes.toString(r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_ENTITIES_BYTES));
			JsonObject joEntities = ConstantsTruthy.jsonParser.parse(entities).getAsJsonObject();
			JsonArray jaUserMentions = joEntities.get("user_mentions").getAsJsonArray();
			if (!jaUserMentions.isJsonNull() && jaUserMentions.size() > 0) {
				Iterator<JsonElement> ium = jaUserMentions.iterator();
				while (ium.hasNext()) {
					JsonElement jeId = ium.next().getAsJsonObject().get("id");
					if (jeId != null && !jeId.isJsonNull()) {
						String mentionedUid = jeId.getAsString();
						String edgeInfo = null;
						if (direction == EdgeDirection.IN) {
							edgeInfo = mentionedUid + " " + userId;
						} else {
							edgeInfo = userId + " " + mentionedUid;
						}
						Long count = edgeCountMap.get(edgeInfo);
						if (count != null) {
							edgeCountMap.put(edgeInfo, count + 1);
						} else {
							edgeCountMap.put(edgeInfo, 1l);
						}
					}
				}
			}
		} else {
			Get get = new Get(tweetId);
			get.addColumn(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_USER_ID_BYTES);
			get.addColumn(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_RETWEET_UID_BYTES);
			Result r = tweetTable.get(get);
			this.HBaseIOTime += System.currentTimeMillis() - startTime;
			if (r == null) {
				context.setStatus("Can't find tweet ID " + txtTweetId.toString());
				return;
			}
			byte[] userIdBytes = r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_USER_ID_BYTES);
			byte[] retweetUidBytes = r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_RETWEET_UID_BYTES);
			if (userIdBytes != null && retweetUidBytes != null) {
				String userId = TruthyHelpers.getUserIDStrFromBytes(userIdBytes);
				String retweetUid = TruthyHelpers.getUserIDStrFromBytes(retweetUidBytes);
				String edgeInfo = null;
				if (direction == EdgeDirection.IN) {
					edgeInfo = retweetUid + " " + userId;
				} else {
					edgeInfo = userId + " " + retweetUid;
				}
				Long count = edgeCountMap.get(edgeInfo);
				if (count != null) {
					edgeCountMap.put(edgeInfo, count + 1);
				} else {
					edgeCountMap.put(edgeInfo, 1l);
				}
			}			
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
        UDFStartTime = System.currentTimeMillis();
        LOG.info("UDF Start Time = " + UDFStartTime);
        LOG.info("Mapper setup start TimeStamp = " + UDFStartTime);
        
		Configuration conf = context.getConfiguration();
		// input file name is like "2012-03_memeTweets_0.txt"
		String inputFileName = ((FileSplit)context.getInputSplit()).getPath().getName();
		int idx = inputFileName.indexOf('_');
		String month = inputFileName.substring(0, idx);
		tweetTable = new HTable(conf, ConstantsTruthy.TWEET_TABLE_NAME + "-" + month);
		edgeCountMap = new HashMap<String, Long>(ConstantsTruthy.TWEETS_PER_MAPPER);
		String[] args = conf.get("additional.arguments").split("\\n");
		if (args[0].equalsIgnoreCase("mention")) {
			edgeType = EdgeType.MENTION;
		} else {
			edgeType = EdgeType.RETWEET;
		}
		if (args[1].equalsIgnoreCase("in")) {
			direction = EdgeDirection.IN;
		} else {
			direction = EdgeDirection.OUT;
		}
		this.useBigInt = TruthyHelpers.checkIfb4June2015(month);
        long endTime = System.currentTimeMillis();
        LOG.info("Mapper setup endTimeStamp = " + endTime);
        LOG.info("Mapper setup time takes = " + (endTime - UDFStartTime) + " ms");
	}
	
	@Override
	protected void cleanup(Context context) {
	    long startTime = System.currentTimeMillis();
		try {
			if (tweetTable != null) {
				tweetTable.close();
			}
			if (edgeCountMap != null) {
				for (Map.Entry<String, Long> e : edgeCountMap.entrySet()) {
					context.write(new Text(e.getKey()), new LongWritable(e.getValue()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
        long UDFEndTime = System.currentTimeMillis();
        LOG.info("UDF End Time = " + UDFEndTime);
        LOG.info("HBaseIO Time = " + this.HBaseIOTime + " ms");
        long UDFOverallTime = (UDFEndTime - this.UDFStartTime);
        LOG.info("UDF Computation Time = "
                + (UDFOverallTime - this.HBaseIOTime) + " ms");
        LOG.info("UDF finish Time = " + UDFOverallTime + " ms");
        long endTime = System.currentTimeMillis();
        LOG.info("Mapper cleanup End TimeStamp = " + endTime);
        LOG.info("Mapper cleanup time takes = " + (endTime - startTime) + " ms");
	}
}
