package iu.pti.hbaseapp.truthy.mrqueries;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import iu.pti.hbaseapp.truthy.TweetData;
import iu.pti.hbaseapp.truthy.TweetTableClient;
import iu.pti.hbaseapp.truthy.TweetSubsetProcessor.TwitterIdProcessMapper;

/**
 * Mapper class for creating timeseries, outputting date strings suitable for
 * use with a wordcount reducer. This is useful (albeit slow) in conjuction
 * with get-tweets-with-phrase.
 * 
 * Examples
 *
 * Defaults to daily resolution (YYYY-MM-dd):
 *
 * truthy-cmd get-tweets-and-analyze get-tweets-with-phrase "robert aderholt"
 * 2014-12-01T00:00:00 2014-12-31T12:59:00
 * iu.pti.hbaseapp.truthy.mrqueries.TimeSeriesMapper
 * iu.pti.hbaseapp.truthy.mrqueries.TextCountReducer nocompress
 * robert_aderholt_time_series -t
 *
 * One can specify a different resolution with a SimpleDateFormat string; this
 * gives hourly resolution:
 *
 * truthy-cmd get-tweets-and-analyze get-tweets-with-phrase "robert aderholt"
 * 2014-12-01T00:00:00 2014-12-31T12:59:00
 * iu.pti.hbaseapp.truthy.mrqueries.TimeSeriesMapper
 * iu.pti.hbaseapp.truthy.mrqueries.TextCountReducer nocompress
 * robert_aderholt_time_series -t "YYYY-MM-dd'T'HH"
 * 
 * @author claydavi
 */
public class TimeSeriesMapper extends TwitterIdProcessMapper<Text, LongWritable> {
	private TweetTableClient tweetGetter = null;
	private HashMap<String, Long> timeSeriesMap = null; 
	private final String twitterDateFormatString = "EEE MMM dd HH:mm:ss Z yyyy";
	private SimpleDateFormat twitterDateFormat = new SimpleDateFormat(twitterDateFormatString);
	//twitterDateFormat.setLenient(true);
	private String outputDateFormatString = "YYYY-MM-dd";
	private SimpleDateFormat outputDateFormat = new SimpleDateFormat(outputDateFormatString);

	
	@Override
	protected void map(LongWritable rowNum, Text txtTweetId, Context context) throws IOException, InterruptedException{
		try {
			String tweetId = txtTweetId.toString();
			TweetData td = tweetGetter.getTweetData(tweetId, false, false);
			// perform wordcount when tweet != nul
			if (td != null && td.created_at != null) {
				Date createdAt = this.twitterDateFormat.parse(td.created_at);
				this.updateTimeSeriesMap(createdAt);
			}
		} catch (Exception e) {
			e.printStackTrace();
			context.setStatus("Exception when analyzing tweet " + txtTweetId.toString());
		}
	}

	private void updateTimeSeriesMap(Date date){
		String key = outputDateFormat.format(date);
		Long count = this.timeSeriesMap.get(key);
		if (count != null) {
			this.timeSeriesMap.put(key, count + 1);
		} else {
			this.timeSeriesMap.put(key, 1l);
		}
	}

	private void testDateFormatString(SimpleDateFormat format) throws ParseException{
		String createdAtString = "Thu Dec 23 18:26:07 +0000 2010";
		Date createdAt = twitterDateFormat.parse(createdAtString);
		String outputString = format.format(createdAt);
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		try {
			this.tweetGetter = new TweetTableClient(context);
			this.timeSeriesMap = new HashMap<String, Long>();
			Configuration conf = context.getConfiguration();
			if (conf.get("additional.arguments") != null && !conf.get("additional.arguments").isEmpty()) {
				String[] args = conf.get("additional.arguments").split("\\n");
				if (args.length > 0)					
					this.outputDateFormatString = args[0];
					this.outputDateFormat = new SimpleDateFormat(outputDateFormatString);
					this.testDateFormatString(outputDateFormat);
				}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e.getMessage());
		}
	}
	
	@Override
	protected void cleanup(Context context) {
		try {
			if (this.tweetGetter != null) {
				this.tweetGetter.close();
			}
			// write the cached word count to HDFS
			if (this.timeSeriesMap != null) {
				for (Map.Entry<String, Long> e : this.timeSeriesMap.entrySet()) {
					//TODO if there is any out of memory issue, 
					//	   we have move this line back into map function
					context.write(new Text(e.getKey()), new LongWritable(e.getValue()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
