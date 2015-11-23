package iu.pti.hbaseapp.truthy.mrqueries;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import iu.pti.hbaseapp.truthy.ConstantsTruthy;
import iu.pti.hbaseapp.truthy.TruthyHelpers;
import iu.pti.hbaseapp.truthy.TweetData;
import iu.pti.hbaseapp.truthy.TweetTableClient;
import iu.pti.hbaseapp.truthy.TweetSubsetProcessor.TwitterIdProcessMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.google.gson.Gson;

/**
 * This Mapper class takes a list of user IDs as input; and for each user ID, it gets the tweets posted by this user,
 * retweets of this user's tweets, and tweets that mention this user in their text. 
 * 
 * @author gaoxm
 */
public class UserRelatedTweetsMapper extends TwitterIdProcessMapper<Text, NullWritable> {
	Gson gson = null;
	boolean needUser = true;
	boolean needRetweeted = true;
	Map<String, long[]> twIntervals = null;
	Map<String, TweetTableClient> tweetGetterMap = null;
	Map<String, HTable> userTweetsIdxTableMap = null;
	Map<String, HTable> retweetIdxTableMap = null;
	Map<String, HTable> memeIdxTableMap = null;
	int userCount = 0;
	int tweetCount = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		try {
			gson = new Gson();
			Configuration conf = context.getConfiguration();
			String timeWinStart = null;
			String timeWinEnd = null;
			if (conf.get("additional.arguments") != null) {
				String[] args = conf.get("additional.arguments").split("\\n");
				if (args.length > 1) {
					timeWinStart = args[0];
					timeWinEnd = args[1];
				}
			}
			if (timeWinStart == null || timeWinEnd == null) {
				throw new IOException("No arguments are given for the time window.");
			}
			
			// create tables for all months in the time window. Skip the tables that doesn't exist.
			tweetGetterMap = new HashMap<String, TweetTableClient>();
			userTweetsIdxTableMap = new HashMap<String, HTable>();
			retweetIdxTableMap = new HashMap<String, HTable>();
			memeIdxTableMap = new HashMap<String, HTable>();	
			twIntervals = TruthyHelpers.splitTimeWindowToMonths(timeWinStart, timeWinEnd);
			for (String month : twIntervals.keySet()) {
				try {
					TweetTableClient tweetGetter = new TweetTableClient(ConstantsTruthy.TWEET_TABLE_NAME + "-" + month);
					HTable userTweetsIdxTable = new HTable(conf, ConstantsTruthy.USER_TWEETS_TABLE_NAME + "-" + month);
					HTable retweetIdxTable = new HTable(conf, ConstantsTruthy.RETWEET_INDEX_TABLE_NAME + "-" + month);
					HTable memeIdxTable = new HTable(conf, ConstantsTruthy.MEME_INDEX_TABLE_NAME + "-" + month);
					userTweetsIdxTableMap.put(month, userTweetsIdxTable);
					tweetGetterMap.put(month, tweetGetter);
					retweetIdxTableMap.put(month, retweetIdxTable);
					memeIdxTableMap.put(month, memeIdxTable);					
				} catch (Exception et) {
					et.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e.getMessage());
		}
	}
	
	@Override
	protected void map(LongWritable rowNum, Text txtUserId, Context context) throws IOException, InterruptedException {
		try {
			String userId = txtUserId.toString();
			byte[] uidBytes = TruthyHelpers.getUserIdBytes(userId);
			
			// Get needed tweets from tables in each month 
			for (String month : userTweetsIdxTableMap.keySet()) {				
				// first, get all tweets this user posted, and all screen names used in these tweets
				HashSet<BytesWritable> tidSet = new HashSet<BytesWritable>();
				HashSet<String> snames = new HashSet<String>();
				tweetCount += getTweetsPostedByUser(uidBytes, month, tidSet, snames, context);
				
				// second, get all retweets of this user's tweets
				tweetCount += getRetweetsBySourceTids(month, tidSet, context);
				
				// finally, get all tweets mentioning this user
				tweetCount += getTweetsByUserMention(month, userId, snames, context);
			}
			
			userCount++;
			if (userCount % 20 == 0) {
				System.out.println("Processed " + userCount + " users. Number of tweets got: " + tweetCount);
			}
		} catch (Exception e) {
			e.printStackTrace();
			context.setStatus("Exception when analyzing tweet " + txtUserId.toString());
		}
	}
	
	/**
	 * Get all tweets posted by <b>uidBytes</b> in <b>month</b>, output tweet content to <b>context</b>, put tweet IDs to <b>tidSet</b>, and
	 * screen names to <b>snames</b>.
	 * 
	 * @param uidBytes
	 *  Byte array for user ID.
	 * @param month
	 *  Month to get data from.
	 * @param tidSet
	 *  Container for the tweet IDs found.
	 * @param snames
	 *  Container for the screen names used by this user.
	 * @param context
	 *  Map context to output to.
	 *  
	 * @return
	 *  Total number of tweets output to context.
	 */
	protected int getTweetsPostedByUser(byte[] uidBytes, String month, Set<BytesWritable> tidSet, Set<String> snames, Context context) {
		int count = 0;
		try {			
			// Get tweet IDs from the user-tweets index table
			long[] times = twIntervals.get(month);
			long tsStart = times[0];
			long tsEnd = times[1];
			HTable userTweetsIdxTable = userTweetsIdxTableMap.get(month);
			Scan scan = new Scan();
			scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
			if (tsEnd < Long.MAX_VALUE) {
				scan.setTimeRange(tsStart, tsEnd + 1);
			} else {
				scan.setTimeRange(tsStart, tsEnd);
			}
			scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
			scan.setStartRow(uidBytes);
			scan.setStopRow(uidBytes);

			ResultScanner rs = userTweetsIdxTable.getScanner(scan);
			Result r = rs.next();
			while (r != null) {
				for (KeyValue kv : r.list()) {
					BytesWritable bwTid = new BytesWritable(kv.getQualifier());
					tidSet.add(bwTid);
				}
				r = rs.next();
			}
			rs.close();
			
			// Get tweet content and user screen names
			TweetTableClient tweetGetter = tweetGetterMap.get(month);
			for (BytesWritable bwTid : tidSet) {
			    String tweetId = "";
			    if (tweetGetter.isUseBigInt()) {
			        tweetId = TruthyHelpers.getTweetIDStrFromBigIntBytes(bwTid.getBytes());
			    } else {
			        tweetId = TruthyHelpers.getTweetIDStrFromBytes(bwTid.getBytes());
			    }			    
				TweetData td = tweetGetter.getTweetData(tweetId, needUser, needRetweeted);
				snames.add(td.user_screen_name.toLowerCase());
				String tweetJson = gson.toJson(td, TweetData.class);
				context.write(new Text(tweetJson), NullWritable.get());
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return count;
	}
	
	/**
	 * Get all retweets of the tweet IDs contained in <b>tidSet</b>, and output the content of the retweets to context.
	 * @param month
	 *  Month to get data from.
	 * @param srcTidSet
	 *  Set of source tweet IDs.
	 * @param context
	 *  Map context to output to.
	 *  
	 * @return
	 *  Total number of tweets output to context.
	 */
	protected int getRetweetsBySourceTids(String month, Set<BytesWritable> srcTidSet, Context context) {
		int count = 0;
		try {
			long[] times = twIntervals.get(month);
			long tsStart = times[0];
			long tsEnd = times[1];
			HTable retweetIdxTable = retweetIdxTableMap.get(month);

			// get tweet IDs from retweet index table
			List<byte[]> tmpTids = new LinkedList<byte[]>();
			Scan scan = new Scan();
			scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
			if (tsEnd < Long.MAX_VALUE) {
				scan.setTimeRange(tsStart, tsEnd + 1);
			} else {
				scan.setTimeRange(tsStart, tsEnd);
			}
			scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
			for (BytesWritable bwTid : srcTidSet) {
				if (!retweetIdxTable.exists(new Get(bwTid.getBytes()))) {
					continue;
				}				
				scan.setStartRow(bwTid.getBytes());
				scan.setStopRow(bwTid.getBytes());
				ResultScanner rs = retweetIdxTable.getScanner(scan);
				Result r = rs.next();
				while (r != null) {
					for (KeyValue kv : r.list()) {
						tmpTids.add(kv.getQualifier());						
					}
					r = rs.next();
				}
				rs.close();
			}
			
			// Get content of retweets
			TweetTableClient tweetGetter = tweetGetterMap.get(month);
			for (byte[] tid : tmpTids) {
                String tweetId = "";
                if (tweetGetter.isUseBigInt()) {
                    tweetId = TruthyHelpers.getTweetIDStrFromBigIntBytes(tid);
                } else {
                    tweetId = TruthyHelpers.getTweetIDStrFromBytes(tid);
                }
				TweetData td = tweetGetter.getTweetData(tweetId, needUser, needRetweeted);
				String tweetJson = gson.toJson(td, TweetData.class);
				context.write(new Text(tweetJson), NullWritable.get());
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return count;
	}
	
	/**
	 * Get all tweets that have mentioned the user whose ID is <b>userId</b>, and 
	 * @param month
	 *  Month to get data from.
	 * @param userId
	 *  ID of the user to search about.
	 * @param snames
	 *  All screen names used by this user.
	 * @param context
	 *  Map context to output to.
	 *  
	 * @return
	 *  Total number of tweets output to context.
	 */
	protected int getTweetsByUserMention(String month, String userId, Set<String> snames, Context context) {
		int count = 0;
		try {
			long[] times = twIntervals.get(month);
			long tsStart = times[0];
			long tsEnd = times[1];
			HTable memeIdxTable = memeIdxTableMap.get(month);
			boolean useBigInt = TruthyHelpers.checkIfb4June2015(month);

			// get tweet IDs from meme index table
			Set<String> tmpTids = new HashSet<String>();
			Scan scan = new Scan();
			scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
			if (tsEnd < Long.MAX_VALUE) {
				scan.setTimeRange(tsStart, tsEnd + 1);
			} else {
				scan.setTimeRange(tsStart, tsEnd);
			}
			scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
			for (String sname : snames) {
				String mention = "@" + sname + "(" + userId + ")";
				byte[] mb = Bytes.toBytes(mention);
				if (!memeIdxTable.exists(new Get(mb))) {
					continue;
				}				
				scan.setStartRow(mb);
				scan.setStopRow(mb);
				ResultScanner rs = memeIdxTable.getScanner(scan);
				Result r = rs.next();
				while (r != null) {
					for (KeyValue kv : r.list()) {
					    if (useBigInt) {
					        tmpTids.add(TruthyHelpers.getTweetIDStrFromBigIntBytes(kv.getQualifier()));  
					    } else {
					        tmpTids.add(TruthyHelpers.getTweetIDStrFromBytes(kv.getQualifier()));  
					    }										
					}
					r = rs.next();
				}
				rs.close();
			}
			
			// Get content of tweets
			TweetTableClient tweetGetter = tweetGetterMap.get(month);
			for (String tid : tmpTids) {
				TweetData td = tweetGetter.getTweetData(tid, needUser, needRetweeted);
				String tweetJson = gson.toJson(td, TweetData.class);
				context.write(new Text(tweetJson), NullWritable.get());
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return count;
	}
		
	@Override
	protected void cleanup(Context context) {
		try {
			for (TweetTableClient ttc : tweetGetterMap.values()) {
				ttc.close();
			}
			for (HTable rit : retweetIdxTableMap.values()) {
				rit.close();
			}
			for (HTable mit : memeIdxTableMap.values()) {
				mit.close();
			}
			for (HTable uit: userTweetsIdxTableMap.values()) {
				uit.close();
			}
			System.out.println("Processed " + userCount + " users in total. Number of tweets got: " + tweetCount);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
