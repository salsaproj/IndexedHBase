package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;
import iu.pti.hbaseapp.MultiFileFolderWriter;
import iu.pti.hbaseapp.truthy.mrqueries.TextCountReducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * A simple wrapper class for testing new functionality and building blocks. Right now four functions are included:
 * <p/>
 * (1) Scan row keys of an index table with a prefix and output all matching row keys. <br/>
 * (2) Get meme life distribution by scanning a meme index table. <br/>
 * (3) Get meme popularity distribution by scanning a meme index table. <br/>
 * (4) Get user-post-count by using a specially designed meme index table that contains user IDs as cell values. <br/>
 * (5) Get the distribution of number of tweets among all users by scanning a user-tweets index table. <br/>
 * (6) Compute the entropy of posted domain names for every user by scanning meme index tables containing user IDs as cell values.
 * <p/>
 * Usage: java iu.pti.hbaseapp.truthy.TruthyGroupCounter [command] [arguments] <br/>
 * [command] [arguments] could be one of the following combinations: <br/>
 * <b>user-post-count-by-index</b> [memes] [start time] [end time] [output directory] <br/>
 * <b>scan-rowkey-range</b> [table name] [prefix] [hdfs working directory] <br/>
 * <b>meme-lifetime-dist</b> [table name] [lifetime unit] [hdfs working directory] <br/>
 * <b>meme-popularity-dist</b> [table name] [hdfs working directory] <br/>
 * <b>num-tweets-dist</b> [table name] [start time] [end time] [hdfs working directory] <br/>
 * <b>domain-entropy-by-user</b> [table name] [start time] [end time] [hdfs working directory]
 * 
 * @author gaoxm
 */
public class TruthyGroupCounter {
	protected static final Log LOG = LogFactory.getLog(TruthyGroupCounter.class);
	
	/**
	 * For scanning the row keys with a given prefix.
	 * @author gaoxm
	 *
	 */
	public static class RowKeyRangeMapper extends TableMapper<Text, LongWritable> {
		String prefix = null;
		HashMap<String, Long> rowkeyCountMap = null;
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			String rowStr = Bytes.toString(rowKey.get());
			if (!rowStr.startsWith(prefix)) {
				return;
			}
			Long count = rowkeyCountMap.get(rowStr);
			if (count != null) {
				rowkeyCountMap.put(rowStr, count + 1);
			} else {
				rowkeyCountMap.put(rowStr, 1l);
			}
		}
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			prefix = conf.get("prefix.to.match");
			rowkeyCountMap = new HashMap<String, Long>();
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (rowkeyCountMap != null) {
				for (Map.Entry<String, Long> e : rowkeyCountMap.entrySet()) {
					context.write(new Text(e.getKey()), new LongWritable(e.getValue()));
				}
			}
		}
	}
	
	/**
	 * For scanning the meme index table to get meme life time distribution.
	 * @author gaoxm
	 *
	 */
	public static class MemeLifetimeMapper extends TableMapper<IntWritable, LongWritable> {
		String lastMeme = null;
		long ltStart = -1;
		long ltEnd = -1;
		HashMap<Integer, Long> lifetimeMap = null;
		double lfUnit = 3600000.0;
		long count = 0;
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT)); 
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			String rowStr = Bytes.toString(rowKey.get());
			
			if (lastMeme != null && !rowStr.equals(lastMeme)) {
				// New meme encountered. Put life time of last meme to lifetimeMap.
				double lifetime = (ltEnd - ltStart) / lfUnit;
				int lfOutKey = 1;
				if (lifetime > 1) {
					lfOutKey = (int)lifetime + 1;
				}
				
				Long count = lifetimeMap.get(lfOutKey);
				if (count == null) {
					lifetimeMap.put(lfOutKey, 1l);
				} else {
					lifetimeMap.put(lfOutKey, count + 1);
				}
				
				if (lfOutKey >= 10000) {
					calTmp.setTimeInMillis(ltStart);
					String start = GeneralHelpers.getDateTimeString(calTmp);
					calTmp.setTimeInMillis(ltEnd);
					String end = GeneralHelpers.getDateTimeString(calTmp);
					System.out.println("meme: " + lastMeme + ", ltStart: " + ltStart + " (" + start + "), ltEnd: " + ltEnd 
							+ " (" + end + "), lf: " + lfOutKey);
				}
				
				lastMeme = null;
				ltStart = -1;
				ltEnd = -1;
			}
			
			lastMeme = rowStr;
			List<KeyValue> kvs = result.list();
			for (KeyValue kv : kvs) {
				long time = kv.getTimestamp();
				if (ltStart == -1 || time < ltStart) {
					ltStart = time;
				}
				
				if (ltEnd == -1 || time > ltEnd) {
					ltEnd = time;
				}
			}
			
			if (count % 100000 == 0) {
				context.setStatus("Procesed " + count + " rows");
			}
			count++;
		}
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String unit = conf.get("lifetime.unit");
			if (unit.equals("minute")) {
				lfUnit = 60000.0;
			} else {
				lfUnit = 3600000.0;
			}
			lifetimeMap = new HashMap<Integer, Long>();
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (lifetimeMap != null) {
				for (Map.Entry<Integer, Long> e : lifetimeMap.entrySet()) {
					context.write(new IntWritable(e.getKey()), new LongWritable(e.getValue()));
				}
			}
		}
	}
	
	/**
	 * For scanning the meme user index table to get meme popularity distribution.
	 * @author gaoxm
	 *
	 */
	public static class MemePopularityMapper extends TableMapper<IntWritable, LongWritable> {
		String lastMeme = null;
		HashSet<String> lastUserIds = null;
		HashMap<Integer, Long> popularityMap = null;
		long count = 0;
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			String rowStr = Bytes.toString(rowKey.get());
			if (rowStr.charAt(0) != '#') {
				return;
			}
			
			try {
				if (lastMeme == null) {
					lastMeme = rowStr;
					lastUserIds = new HashSet<String>();
					for (KeyValue kv : result.list()) {
						String userId = TruthyHelpers.getUserIDStrFromBytes(kv.getValue());
						lastUserIds.add(userId);
					}
				} else {
					if (rowStr.equals(lastMeme)) {
						for (KeyValue kv : result.list()) {
							String userId = TruthyHelpers.getUserIDStrFromBytes(kv.getValue());
							lastUserIds.add(userId);
						}
					} else {
						int popularity = lastUserIds.size();
						Long count = popularityMap.get(popularity);
						if (count == null) {
							popularityMap.put(popularity, 1l);
						} else {
							popularityMap.put(popularity, count + 1);
						}

						lastMeme = rowStr;
						lastUserIds.clear();
						for (KeyValue kv : result.list()) {
							String userId = TruthyHelpers.getUserIDStrFromBytes(kv.getValue());
							lastUserIds.add(userId);
						}
					}
				}

				if (count % 100000 == 0) {
					context.setStatus("Procesed " + count + " rows");
				}
				count++;
			} catch (Exception e) {
				e.printStackTrace();
				context.setStatus("Exception when processing meme " + rowStr + ": " + e.getMessage());
			}
		}
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			popularityMap = new HashMap<Integer, Long>();
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (popularityMap != null) {
				if (lastUserIds != null) {
					int popularity = lastUserIds.size();
					Long count = popularityMap.get(popularity);
					if (count == null) {
						popularityMap.put(popularity, 1l);
					} else {
						popularityMap.put(popularity, count + 1);
					}
				}
				
				for (Map.Entry<Integer, Long> e : popularityMap.entrySet()) {
					context.write(new IntWritable(e.getKey()), new LongWritable(e.getValue()));
				}
			}
		}
	}
	
	/**
	 * General reducer useful for counting numbers.
	 * @author gaoxm
	 *
	 */
	public static class NumberCountReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
		@Override
		protected void reduce(IntWritable key, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException {
			long totalCount = 0;
			for (LongWritable count : counts) {
				totalCount += count.get();
			}
			context.write(key, new LongWritable(totalCount));
		}
	}
	
	/**
	 * For scanning the meme index table to get meme daily frequency list.
	 * @author gaoxm
	 *
	 */
	public static class MemeDailyFreqMapper extends TableMapper<Text, Text> {
		String lastMeme = null;
		TreeMap<Long, Integer> dateFreqMap = null;
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		long dailyMilli = 86400000L;
		long startTime = -1;
		long endTime = -1;
		long count;
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			String rowStr = Bytes.toString(rowKey.get());
			if (lastMeme != null && !rowStr.equals(lastMeme)) {
				// New meme encountered. Output daily freqeuncy of last meme.
				for (Map.Entry<Long, Integer> e : dateFreqMap.entrySet()) {
					calTmp.setTimeInMillis(e.getKey());
					String dailyFreq = GeneralHelpers.getDateString(calTmp) + "|" + e.getValue();
					context.write(new Text(lastMeme), new Text(dailyFreq));
				}
				dateFreqMap.clear();
				count++;
			}
			
			if (lastMeme == null) {
				count++;
			}
			
			lastMeme = rowStr;
			List<KeyValue> kvs = result.list();
			for (KeyValue kv : kvs) {
				long time = kv.getTimestamp();
				if (time < startTime && time > endTime) {
					continue;
				}
				time = time / dailyMilli * dailyMilli;
				Integer freq = dateFreqMap.get(time);
				if (freq == null) {
					dateFreqMap.put(time, 1);
				} else {
					dateFreqMap.put(time, freq + 1);
				}
			}
			
			if (count % 10000 == 0) {
				context.setStatus("Procesed " + count + " memes");
				LOG.info("Procesed " + count + " memes");
			}
		}
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			String startDate = context.getConfiguration().get("start.date");
			String endDate = context.getConfiguration().get("end.date");
			Calendar calStart = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
			Calendar calEnd = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
			TruthyHelpers.setStartAndEndCalendar(calStart, calEnd, startDate, endDate);
			startTime = calStart.getTimeInMillis();
			endTime = calEnd.getTimeInMillis();			
			dateFreqMap = new TreeMap<Long, Integer>();
			count = 0;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (lastMeme != null) {
				for (Map.Entry<Long, Integer> e : dateFreqMap.entrySet()) {
					calTmp.setTimeInMillis(e.getKey());
					String dailyFreq = GeneralHelpers.getDateString(calTmp) + "|" + e.getValue();
					context.write(new Text(lastMeme), new Text(dailyFreq));
				}
			}
			LOG.info("Procesed " + count + " memes");
		}
	}
	
	/**
	 * HeapEntry class used for getting memes with top frequencies.
	 * @author gaoxm
	 */
	public static class MemeFreqHeapEntry implements Comparable<MemeFreqHeapEntry> {
		public String meme = null;
		public int freq = -1;
		
		public MemeFreqHeapEntry(String meme, int freq) throws IllegalArgumentException {
			if (meme == null) {
				throw new IllegalArgumentException("Twitter ID is null!");
			}
			this.meme = meme;
			this.freq = freq;
		}
		
		public int compareTo(MemeFreqHeapEntry that) {
			return this.freq - that.freq;
		}
	}
	
	/**
	 * For scanning the user-tweets index table to get the distribution of number of posted tweets
	 * among all users.
	 * @author gaoxm
	 *
	 */
	public static class NumTweetsDistMapper extends TableMapper<IntWritable, LongWritable> {
		byte[] lastUidBytes = null;
		int lastUidTweetCount = 0;
		HashMap<Integer, Long> tweetsCountDist = new HashMap<Integer, Long>();
		long count = 0;
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			byte[] uidBytes = rowKey.get();
			if (lastUidBytes == null) {
				lastUidBytes = uidBytes;
				lastUidTweetCount = result.size();
			} else if (Bytes.equals(lastUidBytes, uidBytes)) {
				lastUidTweetCount += result.size();
			} else {
				Long userCount = tweetsCountDist.get(lastUidTweetCount);
				if (userCount == null) {
					tweetsCountDist.put(lastUidTweetCount, 1L);
				} else {
					tweetsCountDist.put(lastUidTweetCount, 1L + userCount);
				}
				lastUidBytes = uidBytes;
				lastUidTweetCount = result.size();
			}
			
			count++;
			if (count % 200000 == 0) {
				context.setStatus("Procesed " + count + " memes");
				LOG.info("Procesed " + count + " memes");
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (lastUidBytes != null) {
				Long userCount = tweetsCountDist.get(lastUidTweetCount);
				if (userCount == null) {
					tweetsCountDist.put(lastUidTweetCount, 1L);
				} else {
					tweetsCountDist.put(lastUidTweetCount, 1L + userCount);
				}
			}
			for (Map.Entry<Integer, Long> e : tweetsCountDist.entrySet()) {
				context.write(new IntWritable(e.getKey()), new LongWritable(e.getValue()));
			}
			LOG.info("Procesed " + count + " memes");
		}
	}
	
	/**
	 * For scanning the meme index table to get the domain names posted by each user.
	 * @author gaoxm
	 */
	public static class UserDomainNameMapper extends TableMapper<Text, Text> {
		long count = 0;
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			String rowStr = Bytes.toString(rowKey.get());
			if (rowStr.startsWith("@") || rowStr.startsWith("#")) {
				return;
			}
			try {
				URI uri = new URI(rowStr);
				String domain = uri.getHost();
				List<KeyValue> kvs = result.list();
				for (KeyValue kv : kvs) {
					byte[] uidBytes = kv.getValue();
					String uid = TruthyHelpers.getUserIDStrFromBytes(uidBytes);
					context.write(new Text(uid), new Text(domain));
				}
			} catch (URISyntaxException e) {
				LOG.error("URISyntaxException for meme row string " + rowStr); 
			}
			
			count++;
			if (count % 200000 == 0) {
				context.setStatus("Procesed " + count + " memes");
				LOG.info("Procesed " + count + " memes");
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			LOG.info("Procesed " + count + " memes");
		}
	}
	
	/**
	 * Reducer class for computing the entropy based on the distribution of posted domain names for each user.  
	 * @author gaoxm
	 *
	 */
	public static class UserDomainNameEntropyReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		String lastUid = null;
		HashMap<String, Integer> lastDomainFreqs = null;
		
		@Override
		protected void reduce(Text uid, Iterable<Text> domains, Context context) throws IOException, InterruptedException {
			String uidStr = uid.toString();
			if (lastUid == null) {
				lastUid = uidStr;
				lastDomainFreqs = new HashMap<String, Integer>();
				updateDomainFreqs(domains, lastDomainFreqs);
			} else if (lastUid.equals(uidStr)) {
				updateDomainFreqs(domains, lastDomainFreqs);
			} else {
				double entropy = computeEntropy(lastDomainFreqs);
				context.write(new Text(lastUid), new DoubleWritable(entropy));
				lastUid = uidStr;
				lastDomainFreqs.clear();
				updateDomainFreqs(domains, lastDomainFreqs);
			}
		}

		/**
		 * Update the domain counts in <b>domainFreqs</b> using the domain names in <b>domains</b>.
		 * @param domains
		 *  An iterable list of domain names.
		 * @param domainFreqs
		 *  A map from domain names to their counts.
		 */
		protected void updateDomainFreqs(Iterable<Text> domains, Map<String, Integer> domainFreqs) {
			for (Text d : domains) {
				String domain = d.toString();
				Integer count = domainFreqs.get(domain);
				if (count == null) {
					domainFreqs.put(domain, 1);
				} else {
					domainFreqs.put(domain, count + 1);
				}
			}
		}
		
		/**
		 * Compute the entropy using the distribution of domain names in <b>domainFreqs</b>.
		 * @param domainFreqs
		 *  A map from domain names to their counts.
		 * @return
		 */
		protected double computeEntropy(Map<String, Integer> domainFreqs) {
			double entropy = 0;
			int totalFreq = 0;
			for (Map.Entry<String, Integer> e : domainFreqs.entrySet()) {
				totalFreq += e.getValue();
			}			
			for (Map.Entry<String, Integer> e : domainFreqs.entrySet()) {
				double p = e.getValue() * 1.0 / totalFreq;
				entropy -= p * Math.log(p);
			}
			return entropy;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (lastUid != null) {
				double entropy = computeEntropy(lastDomainFreqs);
				context.write(new Text(lastUid), new DoubleWritable(entropy));
			}
		}
	}

	/**
	 * Get user-post-count by using the advanced memeUserIndexTable.
	 * @param memes
	 *  An array containing hashtags, user-mentions, or URLs.
	 * @param startTime
	 *  Time window start point.
	 * @param endTime
	 *  Time window end point.
	 * @return
	 *  A map from user IDs to number of tweets posted about the given <b>memes</b>.
	 * @throws Exception
	 */
	public Map<String, Long> userPostCountByIndex(String[] memes, String startTime, String endTime) throws Exception {
		HashMap<String, Long> result = new HashMap<String, Long>();
		Configuration hbaseConfig = HBaseConfiguration.create();
		
		Map<String, long[]> intervals = TruthyHelpers.splitTimeWindowToMonths(startTime, endTime);
		for (Map.Entry<String, long[]> e : intervals.entrySet()) {
			String month = e.getKey();
			HTable memeUserIndexTable = new HTable(hbaseConfig, ConstantsTruthy.MEME_USER_INDEX_TABLE_NAME + "-" + month);
			for (String meme : memes) {
				byte[] rowKey = Bytes.toBytes(meme);
				Scan scan = new Scan();
				scan.setStartRow(rowKey);
				scan.setStopRow(rowKey);
				scan.setTimeRange(e.getValue()[0], e.getValue()[1]);
				scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
				scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
				ResultScanner rs = memeUserIndexTable.getScanner(scan);
				Result r = rs.next();
				while (r != null) {
					for (KeyValue kv : r.list()) {
						byte[] userIdBytes = kv.getValue();
						String userId = TruthyHelpers.getUserIDStrFromBytes(userIdBytes);
						Long count = result.get(userId);
						if (count != null) {
							result.put(userId, count + 1);
						} else {
							result.put(userId, 1l);
						}
					}
					r = rs.next();
				}
				rs.close();
			}
			memeUserIndexTable.close();
		}		
		return result;
	}
	
	/**
	 * Get user-post-count by using the advanced memeUserIndexTable, and write the results into <b>outputDir</b>.
	 * @param memes
	 *  An array containing hashtags, user-mentions, or URLs.
	 * @param startTime
	 *  Time window start point.
	 * @param endTime
	 *  Time window end point.
	 * @param outputDir
	 *  Results output directory.
	 * @throws Exception
	 */
	public void getUpcByAdvIdx(String[] memes, String startTime, String endTime, String outputDir) throws Exception {
		long begin = System.currentTimeMillis();
		Map<String, Long> counts = userPostCountByIndex(memes, startTime, endTime);
		MultiFileFolderWriter writer = new MultiFileFolderWriter(outputDir, "userPostCount", true, ConstantsTruthy.TWEETS_PER_MAPPER);
		for (Map.Entry<String, Long> e : counts.entrySet()) {
			writer.writeln(e.getKey() + "\t" + e.getValue());
		}
		writer.close();
		
		long end = System.currentTimeMillis();
		System.out.println("Done. Number of results: " + counts.size() + "; Time used in secs: " + (end - begin) / 1000.0);
	}
	
	
	/**
	 * Given a row key <b>prefix</b>, scan the HBase table whose name is <b>tableName</b> with a MapReduce job, and output all the row keys
	 * matching <b>prefix</b> to <b>outputDir</b>.
	 * @param tableName
	 * @param prefix
	 * @param outputDir
	 * @throws Exception
	 */
	public void scanRowKeyRangeByMR(String tableName, String prefix, String outputDir) throws Exception {
		if (!MultiFileFolderWriter.deleteIfExist(outputDir)) {
			System.err.println("Error: cannot delete old MapReduce output directory " + outputDir);
			System.exit(1);
		}
		Scan scan = new Scan();
		scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
		byte[] startRow = Bytes.toBytes(prefix);
		scan.setStartRow(startRow);
		boolean stopRowSet = false;
		byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
		for (int i = stopRow.length - 1; i >= 0; i--) {
			if (stopRow[i] != (byte) 255) {
				stopRow[i] += 1;
				stopRowSet = true;
				break;
			} else {
				stopRow[i] = 0;
			}
		}
		if (stopRowSet) {
			scan.setStopRow(stopRow);
		}

		Configuration conf = HBaseConfiguration.create();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		conf.set("prefix.to.match", prefix);
		Job job = Job.getInstance(conf, "Rowkey range scanner");
		job.setJarByClass(TruthyGroupCounter.class);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, RowKeyRangeMapper.class, Text.class, LongWritable.class, job, true);
		job.setReducerClass(TextCountReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		boolean succ = job.waitForCompletion(true);
		if (succ) {
			System.out.println("Please check out results in " + outputDir);
		} else {
			System.out.println("Error occurred when running the MapReduce job.");
		}
	}
	
	/**
	 * Scan the meme index table as specified by <b>tableName</b> with a MapReduce program, and output the distribution of all memes' lifetime
	 * lengh to <b>outputDir</b>. A meme's lifetime length is calculated by
	 * <br/> ( [time point of last appearance of meme] - [time point of first appearance of meme] ) / [lifetime <b>unit</b> length] 
	 * @param tableName
	 *  The name of a meme index table.
	 * @param unit
	 *  'minute' or 'hour'.
	 * @param outputDir
	 *  Results output directory.
	 * @throws Exception
	 */
	public void getMemeLifetimeDistribution(String tableName, String unit, String outputDir) throws Exception {
		if (!MultiFileFolderWriter.deleteIfExist(outputDir)) {
			System.err.println("Error: cannot delete old MapReduce output directory " + outputDir);
			System.exit(1);
		}
		Scan scan = new Scan();
		scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
		scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);

		Configuration conf = HBaseConfiguration.create();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		conf.set("lifetime.unit", unit);
		Job job = Job.getInstance(conf, "Meme Lifetime Distribution");
		job.setJarByClass(TruthyGroupCounter.class);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, MemeLifetimeMapper.class, IntWritable.class, LongWritable.class, job, true);
		job.setReducerClass(NumberCountReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		boolean succ = job.waitForCompletion(true);
		if (succ) {
			System.out.println("Please check out results in " + outputDir);
		} else {
			System.out.println("Error occurred when running the MapReduce job.");
		}
	}
	
	/**
	 * Given a meme user index table as specified by <b>tableName</b>, scan this table with MapReduce and generate the popularity distribution
	 * of all hashtags contained in the table, and save the results in <b>outputDir</b>. A meme's popularity is measured by how many different
	 * users have tweeted about it in the given index table.
	 * @param tableName
	 *  The name of a user index table.
	 * @param outputDir
	 *  Results output directory.
	 * @throws Exception
	 */
	public void getMemePopularityDistribution(String tableName, String outputDir) throws Exception {
		if (!MultiFileFolderWriter.deleteIfExist(outputDir)) {
			System.err.println("Error: cannot delete old MapReduce output directory " + outputDir);
			System.exit(1);
		}
		Scan scan = new Scan();
		scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
		scan.setStartRow(Bytes.toBytes("#"));
		scan.setStopRow(Bytes.toBytes("@"));
		scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf, "Meme Popularity Distribution");
		job.setJarByClass(TruthyGroupCounter.class);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, MemePopularityMapper.class, IntWritable.class, LongWritable.class,
				job, true);
		job.setReducerClass(NumberCountReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		boolean succ = job.waitForCompletion(true);
		if (succ) {
			System.out.println("Please check out results in " + outputDir);
		} else {
			System.out.println("Error occurred when running the MapReduce job.");
		}
	}

	/**
	 * Given a <b>startDate</b> and an <b>endDate</b>, generate the daily frequencies of all memes by scanning the necessary 
	 * meme index tables within the time window, and output the results in <b>outputDir</b>. <b>byWhat</b> specifies what 'type' of
	 * memes to get the daily frequencies for.
	 * @param byWhat
	 *  'hashtag', 'userMention', or 'URL'.
	 * @param startDate
	 *  Time window start point.
	 * @param endDate
	 *  Time window end point.
	 * @param outputDir
	 *  Results output directory.
	 * @throws Exception
	 */
	public void getMemeDailyFreqs(String byWhat, String startDate, String endDate, String outputDir) throws Exception {
		if (!MultiFileFolderWriter.deleteIfExist(outputDir)) {
			System.err.println("Error: cannot delete old MapReduce output directory " + outputDir);
			System.exit(1);
		}

		byte[] startRow = null;
		byte[] stopRow = null;
		if (byWhat.equalsIgnoreCase("hashtag")) {
			startRow = Bytes.toBytes("#");
			stopRow = Bytes.toBytes("$");
		} else if (byWhat.equalsIgnoreCase("userMention")) {
			startRow = Bytes.toBytes("@");
			stopRow = Bytes.toBytes("A");
		} else if (byWhat.equalsIgnoreCase("URL")) {
			startRow = Bytes.toBytes("http");
			stopRow = Bytes.toBytes("httq");
		}

		Map<String, long[]> monthMap = TruthyHelpers.splitTimeWindowToMonths(startDate, endDate);
		List<Scan> scans = new LinkedList<Scan>();
		for (Map.Entry<String, long[]> e : monthMap.entrySet()) {
			Scan scan = new Scan();
			scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
			scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
			scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(ConstantsTruthy.MEME_INDEX_TABLE_NAME + "-" + e.getKey()));
			scan.setStartRow(startRow);
			scan.setStopRow(stopRow);
			scans.add(scan);
		}

		Configuration conf = HBaseConfiguration.create();
		conf.set("start.date", startDate);
		conf.set("end.date", endDate);
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		conf.set("mapred.output.compress", "true");
		conf.set("mapred.output.compression.type", "BLOCK");
		conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		Job job = Job.getInstance(conf, "Meme Daily Frequency Generator");
		job.setJarByClass(TruthyGroupCounter.class);
		TableMapReduceUtil.initTableMapperJob(scans, MemeDailyFreqMapper.class, Text.class, Text.class, job, true);
		job.setReducerClass(DailyMemeFreqProcessor.DmfpReducer.class);
		if (monthMap.size() / 2 < 1) {
			job.setNumReduceTasks(1);
		} else {
			job.setNumReduceTasks(monthMap.size() / 2);
		}
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		boolean succ = job.waitForCompletion(true);
		if (succ) {
			System.out.println("Please check out results in " + outputDir);
		} else {
			System.out.println("Error occurred when running the MapReduce job.");
		}
	}
	
	/**
	 * Assuming .gz files in <b>inputDir</b> contains lines starting with memes' lifetime followed by other meme info, this function 
	 * create new files under <b>outputDir</b>, each containing info about all memes with the same lifetime length. 
	 * @param inputDir
	 * @param outputDir
	 * @throws Exception
	 */
	public void separateMemeByLifetime(String inputDir, String outputDir) throws Exception {
		File fInputDir = new File(inputDir);
		if (!fInputDir.exists() || fInputDir.isFile()) {
			throw new Exception("Input directory " + inputDir + " is invalid.");
		}		
		File fOutputDir = new File(outputDir);
		if (!fOutputDir.exists() && !fOutputDir.mkdirs()) {
			throw new Exception("Can't create output directory " + outputDir);
		}
		if (fOutputDir.isFile()) {
			throw new Exception("Output direcotry path " + outputDir + " already exists as a file.");
		}
		
		Map<Integer, PrintWriter> outWriterMap = new HashMap<Integer, PrintWriter>();
		Map<Integer, Integer> lifetimeCounts = new TreeMap<Integer, Integer>();
		int idx = -1;
		int lifetime = -1;
		int inputCount = 0;
		for (File f : fInputDir.listFiles()) {
			if (!f.getName().endsWith(".gz")) {
				continue;
			}
			GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(f));
			BufferedReader brInput = new BufferedReader(new InputStreamReader(gzInputStream, "UTF-8"));
			String line = brInput.readLine();
			while (line != null) {
				line = line.trim();
				if (line.length() > 0) {	
					idx = line.indexOf('\t');
					lifetime = Integer.valueOf(line.substring(0, idx));					
					PrintWriter pwOut = outWriterMap.get(lifetime);
					if (pwOut != null) {
						pwOut.println(line);
					} else {
						FileOutputStream fos = new FileOutputStream(outputDir + File.separator + "lifetime_" + lifetime + ".txt.gz");
						pwOut = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(fos), "UTF-8"));
						pwOut.println(line);
						outWriterMap.put(lifetime, pwOut);
					}
					
					Integer ltCount = lifetimeCounts.get(lifetime);
					if (ltCount == null) {
						lifetimeCounts.put(lifetime, 1);
					} else {
						lifetimeCounts.put(lifetime, ltCount + 1);
					}
					
					inputCount++;
					if (inputCount % 100000 == 0) {
						System.out.println("Processed " + inputCount + " input lines.");
					}
				}
				line = brInput.readLine();
			}
			brInput.close();
		}
		
		for (Map.Entry<Integer, PrintWriter> e : outWriterMap.entrySet()) {
			e.getValue().close();
		}
		System.out.println("Done! Total number of input lines processed: " + inputCount);
		for (Map.Entry<Integer, Integer> e : lifetimeCounts.entrySet()) {
			System.out.println("Number of memes with lifetime " + e.getKey() + ": " + e.getValue());
		}
	}
	
	/**
	 * Extract all unique memes from a daily meme frequency file and write them to a separate file.
	 * @param dmfPath
	 *  Path to the daily meme frequency file.
	 * @param memePath
	 *  Path to the output file containing all unique memes.
	 * @throws Exception
	 */
	public void getMemesFromDailyMemeFreqFile(String dmfPath, String memePath) throws Exception {
		Set<String> memes = new HashSet<String>();
		
		System.out.println("About to read in all the memes. Press 'Enter' to continue.");
		System.in.read();
		long count = 0;
		GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(dmfPath));
		BufferedReader brInput = new BufferedReader(new InputStreamReader(gzInputStream, "UTF-8"));
		String line = brInput.readLine();
		int idx1 = -1;
		int idx2 = -1;
		while (line != null) {
			line = line.trim();
			idx1 = line.indexOf('|');
			idx2 = line.lastIndexOf('|');
			if (idx1 >= 0 && idx2 >= 0 || idx1 != idx2) {
				String meme = line.substring(idx1 + 1, idx2);
				memes.add(meme);
			}
			
			count++;
			if (count % 1000000 == 0) {
				System.out.println(count + " lines processed.");
			}
			line = brInput.readLine();
		}
		System.out.println(count + " lines processed.");
		brInput.close();		
		
		System.out.println(memes.size() + " unique memes read. Press 'Enter' to continue writing all memes to the output file.");
		System.in.read();
		FileOutputStream fos = new FileOutputStream(memePath);
		PrintWriter pwOut = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(fos), "UTF-8"));
		count = 0;
		for (String meme : memes) {
			pwOut.println(meme);
			count++;
			if (count % 1000000 == 0) {
				System.out.println(count + " memes written.");
			}
		}
		pwOut.close();
		System.out.println("Done.");		
	}
	
	/**
	 * Assuming <b>memeFreqDir</b> contains files about memes and their frequencies, find <b>nMemesToFind</b> memes with
	 * the highest frequencies, and output them to stdout.
	 *  
	 * @param memeFreqDir
	 *  Path to the directory containing files about memes and their frequencies. 
	 * @param nMemesToFind
	 *  Number of top memes to find.
	 * @throws Exception
	 */
	public void findMemesWithTopFreq(String memeFreqDir, int nMemesToFind) throws Exception {
		// First, read all memes and freqs into a Map. Consider memes as case-insensitive.
		Map<String, Integer> memeFreqs = new HashMap<String, Integer>();
		File fMemeDreqDir = new File(memeFreqDir);
		for (File f : fMemeDreqDir.listFiles()) {
			if (f.isFile() && f.length() > 0) {
				BufferedReader brInput = new BufferedReader(new FileReader(f));
				String line = brInput.readLine();
				int idx = -1;
				String meme = null;
				int freq = -1;
				while (line != null) {
					line = line.trim();
					idx = line.indexOf('\t');
					if (idx >= 0) {
						meme = line.substring(0, idx).toLowerCase();
						freq = Integer.valueOf(line.substring(idx + 1));
						Integer oldFreq = memeFreqs.get(meme);
						if (oldFreq == null) {
							memeFreqs.put(meme, freq);
						} else {
							memeFreqs.put(meme, oldFreq + freq);
						}
					}
					line = brInput.readLine();
				}
				brInput.close();
			}
		}
		
		// Use a heap to find the top nMemesToFind memes.
		PriorityQueue<MemeFreqHeapEntry> memeFreqHeap = new PriorityQueue<MemeFreqHeapEntry>(nMemesToFind);
		for (Map.Entry<String, Integer> e : memeFreqs.entrySet()) {
			String meme = e.getKey();
			int freq = e.getValue();
			MemeFreqHeapEntry he = new MemeFreqHeapEntry(meme, freq);
			if (memeFreqHeap.size() < nMemesToFind) {
				memeFreqHeap.offer(he);
			} else {
				MemeFreqHeapEntry curTop = memeFreqHeap.poll();
				if (curTop.compareTo(he) < 0) {
					memeFreqHeap.remove();
					memeFreqHeap.offer(he);
				}
			}
		}
		
		StringBuilder sb = new StringBuilder();
		for (MemeFreqHeapEntry he : memeFreqHeap) {
			System.out.println(he.meme + "\t" + he.freq);
			sb.append(he.meme).append(',');
		}
		System.out.println("Done!");
		sb.deleteCharAt(sb.length() - 1);
		System.out.println(sb.toString());
	}
	
	/**
	 * Get the distribution of number of tweets among all users by scanning a user-tweets index table.
	 * @param tableName
	 *  Must be a 'userTweetsIndexTable'.
	 * @param startTime
	 *  Start position of a time window for counting tweets with valid timestamps.
	 *  Should be in the format of '2012-01-01T00:00:00'.
	 * @param endTime
	 *  End position of a time window for counting tweets with valid timestamps.
	 *  Should be in the format of '2012-04-01T00:00:55'.
	 * @param hdfsWorkDir
	 *  Output directory on HDFS.
	 * @throws Exception
	 */
	public void getNumTweetsDist(String tableName, String startTime, String endTime, String hdfsWorkDir) throws Exception {
		if (!MultiFileFolderWriter.deleteIfExist(hdfsWorkDir)) {
			System.err.println("Error: cannot delete old MapReduce output directory " + hdfsWorkDir);
			System.exit(1);
		}
		if (!tableName.startsWith(ConstantsTruthy.USER_TWEETS_TABLE_NAME)) {
			System.err.println("Error: table name is not a " + ConstantsTruthy.USER_TWEETS_TABLE_NAME);
			System.exit(1);
		}
		Scan scan = new Scan();
		scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
		scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		GeneralHelpers.setDateTimeByString(calTmp, startTime);
		long startMilli = calTmp.getTimeInMillis();
		GeneralHelpers.setDateTimeByString(calTmp, endTime);
		long endMilli = calTmp.getTimeInMillis();
		scan.setTimeRange(startMilli, endMilli);

		Configuration conf = HBaseConfiguration.create();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf, "Number of posted tweets distribution");
		job.setJarByClass(TruthyGroupCounter.class);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, NumTweetsDistMapper.class, IntWritable.class, LongWritable.class, job, true);
		job.setReducerClass(NumberCountReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(hdfsWorkDir));
		boolean succ = job.waitForCompletion(true);
		if (succ) {
			System.out.println("Please check out results in " + hdfsWorkDir);
		} else {
			System.out.println("Error occurred when running the MapReduce job.");
		}
	}
	
	/**
	 * For a given time window as specified by <b>startTime</b> and <b>endTime</b>, find the domain names posted
	 * by each user, and compute the entropy of the domain name distribution.
	 * @param startTime
	 *  Start position of a given time window. Should be in the format of '2012-01-01T00:00:00'.
	 * @param endTime
	 *  Start position of a given time window. Should be in the format of '2012-04-01T00:00:55'.
	 * @param hdfsWorkDir
	 *  HDFS output directory.
	 * @throws Exception
	 */
	public void getUserDomainEntropy(String startTime, String endTime, String hdfsWorkDir) throws Exception {
		if (!MultiFileFolderWriter.deleteIfExist(hdfsWorkDir)) {
			System.err.println("Error: cannot delete old MapReduce output directory " + hdfsWorkDir);
			System.exit(1);
		}
		
		Map<String, long[]> monthMap = TruthyHelpers.splitTimeWindowToMonths(startTime, endTime);
		List<Scan> scans = new LinkedList<Scan>();
		// A - z should encapsulate alll kinds of protocols in URLs
		byte[] startRow = Bytes.toBytes("A");
		byte[] stopRow = Bytes.toBytes("(");
		for (Map.Entry<String, long[]> e : monthMap.entrySet()) {
			Scan scan = new Scan();
			scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
			scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
			scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(ConstantsTruthy.MEME_INDEX_TABLE_NAME + "-" + e.getKey()));
			scan.setStartRow(startRow);
			scan.setStopRow(stopRow);
			scan.setTimeRange(e.getValue()[0], e.getValue()[1] + 1);
			scans.add(scan);
		}

		Configuration conf = HBaseConfiguration.create();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		Job job = Job.getInstance(conf, "User Domain Name Entropy Calculator");
		job.setJarByClass(TruthyGroupCounter.class);
		TableMapReduceUtil.initTableMapperJob(scans, UserDomainNameMapper.class, Text.class, Text.class, job, true);
		job.setReducerClass(UserDomainNameEntropyReducer.class);
		if (monthMap.size() / 2 < 1) {
			job.setNumReduceTasks(1);
		} else {
			job.setNumReduceTasks(monthMap.size() / 2);
		}
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(hdfsWorkDir));
		boolean succ = job.waitForCompletion(true);
		if (succ) {
			System.out.println("Please check out results in " + hdfsWorkDir);
		} else {
			System.out.println("Error occurred when running the MapReduce job.");
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			String command = args[0];
			if (command.equals("user-post-count-by-index")) {
				String[] memes = args[1].toLowerCase().split(",");
				String startTime = args[2];
				String endTime = args[3];
				String outputDir = args[4];
			
				TruthyGroupCounter tgc = new TruthyGroupCounter();
				tgc.getUpcByAdvIdx(memes, startTime, endTime, outputDir);
			} else if (command.equals("scan-rowkey-range")) {
				String tableName = args[1];
				String prefix = args[2];
				String outputDir = args[3];
				
				TruthyGroupCounter tgc = new TruthyGroupCounter();
				tgc.scanRowKeyRangeByMR(tableName, prefix, outputDir);				
			} else if (command.equals("meme-lifetime-dist")) {
				String tableName = args[1];
				String unit = args[2];
				String workingDir = args[3];
				
				TruthyGroupCounter tgc = new TruthyGroupCounter();
				tgc.getMemeLifetimeDistribution(tableName, unit, workingDir);
			} else if (command.equals("meme-daily-freq")) {
				String byWhat = args[1];
				String startDate = args[2];
				String endDate = args[3];
				String outputDir = args[4];
				
				TruthyGroupCounter tgc = new TruthyGroupCounter();
				tgc.getMemeDailyFreqs(byWhat, startDate, endDate, outputDir);				
			} else if (command.equals("meme-popularity-dist")) {
				String tableName = args[1];
				String workingDir = args[2];
				
				TruthyGroupCounter tgc = new TruthyGroupCounter();
				tgc.getMemePopularityDistribution(tableName, workingDir);				
			} else if (command.equals("separate-meme-by-lifetime")) {
				String inputDir = args[1];
				String outputDir = args[2];
				TruthyGroupCounter tgc = new TruthyGroupCounter();
				tgc.separateMemeByLifetime(inputDir, outputDir);
			} else if (command.equals("get-memes-from-dmf")) {
				String dmfPath = args[1];
				String memePath = args[2];
				TruthyGroupCounter tgc = new TruthyGroupCounter();
				tgc.getMemesFromDailyMemeFreqFile(dmfPath, memePath);
			} else if (command.equals("top-memes-by-freq")) {
				String mfdPath = args[1];
				int nMemesToFind = Integer.valueOf(args[2]);
				TruthyGroupCounter tgc = new TruthyGroupCounter();
				tgc.findMemesWithTopFreq(mfdPath, nMemesToFind);
			} else if (command.equals("num-tweets-dist")) {
				String tableName = args[1];
				String startTime = args[2];
				String endTime = args[3];
				String hdfsOutDir = args[4];
				TruthyGroupCounter tgc = new TruthyGroupCounter();
				tgc.getNumTweetsDist(tableName, startTime, endTime, hdfsOutDir);
			} else if (command.equals("domain-entropy-by-user")) {
				String startTime = args[1];
				String endTime = args[2];
				String hdfsOutDir = args[3];
				TruthyGroupCounter tgc = new TruthyGroupCounter();
				tgc.getUserDomainEntropy(startTime, endTime, hdfsOutDir);
			} else {
				System.out.println("Unsupported command : " + command);
				usage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			usage();
			System.exit(1);
		}
	}

	public static void usage(){
		System.out.println("Usage: java iu.pti.hbaseapp.truthy.TruthyGroupCounter <command> <arguments>");
		System.out.println("	<command> <arguments> could be one of the following combinations:");
		System.out.println("	user-post-count-by-index <memes> <start time> <end time> <output directory>");
		System.out.println("	scan-rowkey-range <tablename> <prefix> <hdfs working directory>");
		System.out.println("	meme-lifetime-dist <tablename> <lifetime unit> <hdfs working directory>");
		System.out.println("	meme-popularity-dist <tablename> <hdfs working directory>");
		System.out.println("	meme-daily-freq <start date> <end date> <hdfs output directory>");
		System.out.println("	separate-meme-by-lifetime <local input directory> <local output directory>");
		System.out.println("	get-memes-from-dmf <daily meme frequency file path> <meme file path>");
		System.out.println("	top-memes-by-freq <meme frequency directory path> <number of top memes to find>");
		System.out.println("	num-tweets-dist <table name> <start time> <end time> <hdfs working directory>");
		System.out.println("	domain-entropy-by-user <start time> <end time> <hdfs working directory>");
	}

}
