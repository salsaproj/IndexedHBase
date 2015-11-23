package iu.pti.hbaseapp.truthy.mrqueries;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import iu.pti.hbaseapp.truthy.ConstantsTruthy;
import iu.pti.hbaseapp.truthy.TruthyHelpers;
import iu.pti.hbaseapp.truthy.TweetSubsetProcessor.TwitterIdProcessMapper;

/**
 * Mapper class for executing the "user-post-count" query.
 * 
 * @author gaoxm
 */
public class UserPostCountMapper extends TwitterIdProcessMapper<Text, LongWritable> {
	HTable tweetTable = null;
	HashMap<String, Long> userPostCountMap = null;
	String month = "";
    private boolean useBigInt;
	
	@Override
	protected void map(LongWritable rowNum, Text txtTweetId, Context context) throws IOException, InterruptedException {
        byte[] tweetId = null;
        if (this.useBigInt) {
            tweetId = TruthyHelpers.getTweetIdBigIntBytes(txtTweetId.toString());
        } else {
            tweetId = TruthyHelpers.getTweetIdBytes(txtTweetId.toString());
        }
		Get get = new Get(tweetId);
		get.addColumn(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_USER_ID_BYTES);
		get.addColumn(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_USER_SNAME_BYTES);
		Result r = tweetTable.get(get);
		if (r == null) {
			context.setStatus("Can't find tweet ID " + txtTweetId.toString());
			return;
		}
		byte[] userIdBytes = r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_USER_ID_BYTES);
		String userId = TruthyHelpers.getUserIDStrFromBytes(userIdBytes);
		String userSname = Bytes.toString(r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_USER_SNAME_BYTES));
		String userInfo = userSname + "(" + userId + ")";
		
		Long count = userPostCountMap.get(userInfo);
		if (count != null) {
			userPostCountMap.put(userInfo, count + 1);
		} else {
			userPostCountMap.put(userInfo, 1l);
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		// input file name is like "2012-03_memeTweets_0.txt"
		String inputFileName = ((FileSplit)context.getInputSplit()).getPath().getName();
		int idx = inputFileName.indexOf('_');
		String month = inputFileName.substring(0, idx);
		this.month = month;
		tweetTable = new HTable(conf, ConstantsTruthy.TWEET_TABLE_NAME + "-" + month);
		userPostCountMap = new HashMap<String, Long>(ConstantsTruthy.TWEETS_PER_MAPPER);
		this.useBigInt = TruthyHelpers.checkIfb4June2015(month);
	}
	
	@Override
	protected void cleanup(Context context) {
		try {
			context.setStatus("userPostCountMap size: " + userPostCountMap.size()); 
			if (userPostCountMap != null) {
				for (Map.Entry<String, Long> e : userPostCountMap.entrySet()) {
					context.write(new Text(e.getKey()), new LongWritable(e.getValue()));
				}
				userPostCountMap.clear();
			}
			if (tweetTable != null) {
				tweetTable.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
