package iu.pti.hbaseapp.truthy.mrqueries;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import iu.pti.hbaseapp.truthy.ConstantsTruthy;
import iu.pti.hbaseapp.truthy.TweetData;
import iu.pti.hbaseapp.truthy.TweetTableClient;
import iu.pti.hbaseapp.truthy.TweetSubsetProcessor.TwitterIdProcessMapper;

/**
 * Mapper class for getting undirected retweet edges. I.e. if user1 and user2 have both retweeted statuses from each other,
 * then there will be only one pair [user1, user2] contained in the results. The user ID that is alphabetically smaller is put first. 
 * 
 * @author gaoxm
 */
public class RetweetEdgeListMapper extends TwitterIdProcessMapper<Text, LongWritable> {
	TweetTableClient tweetGetter = null;
	HashMap<String, Long> edgeCountMap = null;
	private static final Log LOG = LogFactory.getLog(GetTweetsMapper.class);
    private long HBaseIOTime = 0L;
    private long UDFStartTime = 0L;
	
	@Override
	protected void map(LongWritable rowNum, Text txtTweetId, Context context) throws IOException, InterruptedException {
		try {
		    long startTime = System.currentTimeMillis();
			TweetData tweet = tweetGetter.getTweetData(txtTweetId.toString(), false, false);
			this.HBaseIOTime += System.currentTimeMillis() - startTime;
			String userId = tweet.user_id_str;
			String retweetUid = tweet.retweeted_status_user_id_str;
			if (userId != null && retweetUid != null) {
				String edgeInfo = null;
				if (userId.compareTo(retweetUid) < 0) {
					edgeInfo = userId + " " + retweetUid;
				} else {
					edgeInfo = retweetUid + " " + userId;
				}
				Long count = edgeCountMap.get(edgeInfo);
				if (count != null) {
					edgeCountMap.put(edgeInfo, count + 1);
				} else {
					edgeCountMap.put(edgeInfo, 1l);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			context.setStatus("Exception when analyzing tweet " + txtTweetId.toString());
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	      UDFStartTime = System.currentTimeMillis();
	      LOG.info("UDF Start Time = " + UDFStartTime);
	      
		try {
			tweetGetter = new TweetTableClient(context);
			edgeCountMap = new HashMap<String, Long>(ConstantsTruthy.TWEETS_PER_MAPPER);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
	
	@Override
	protected void cleanup(Context context) {
		try {
			if (tweetGetter != null) {
				tweetGetter.close();
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
	}

}
