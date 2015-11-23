package iu.pti.hbaseapp.truthy.mrqueries;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.google.gson.Gson;

import iu.pti.hbaseapp.truthy.TweetData;
import iu.pti.hbaseapp.truthy.TweetSubsetProcessor.TwitterIdProcessMapper;
import iu.pti.hbaseapp.truthy.TweetTableClient;

/**
 * Mapper class for getting the retweeted status (in JSON format) contained in the tweets corresponding to a subset of tweet IDs. 
 * 
 * @author gaoxm
 */
public class GetRetweetsMapper extends TwitterIdProcessMapper<Text, NullWritable> {
	TweetTableClient tweetGetter = null;
	Gson gson = null;
	
	@Override
	protected void map(LongWritable rowNum, Text txtTweetId, Context context) throws IOException, InterruptedException {
		try {
			String tweetId = txtTweetId.toString();
			TweetData td = tweetGetter.getTweetData(tweetId, false, true);
			if (td.retweeted_status_id_str != null) {
				String tweetJson = gson.toJson(td, TweetData.class);
				context.write(new Text(tweetJson), NullWritable.get());
			}
		} catch (Exception e) {
			e.printStackTrace();
			context.setStatus("Exception when analyzing tweet " + txtTweetId.toString());
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		try {
			tweetGetter = new TweetTableClient(context);
			gson = new Gson();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e.getMessage());
		}
	}
	
	@Override
	protected void cleanup(Context context) {
		try {
			if (tweetGetter != null) {
				tweetGetter.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
