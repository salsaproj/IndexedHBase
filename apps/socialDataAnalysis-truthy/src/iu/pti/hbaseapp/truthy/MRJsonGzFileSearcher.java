package iu.pti.hbaseapp.truthy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;
import iu.pti.hbaseapp.MultiFileFolderWriter;
import iu.pti.hbaseapp.truthy.mrqueries.TextCountReducer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class MRJsonGzFileSearcher {
	protected static final Log LOG = LogFactory.getLog(MRJsonGzFileSearcher.class);
	
	/**
	 * "MrfsMapper" stands for "MapReduce .json.gz file search Mapper".
	 * @author gaoxm
	 */
	static class MrfsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		Set<String> memeSet;
		long startTime;
		long endTime;
		Calendar calTmp;
		JsonParser jsonParser;
		int tweetCount;
		int matchCount;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String[] memes = conf.get("memes").split(",+");
			memeSet = new HashSet<String>();
			for (String meme : memes) {
				memeSet.add(meme);
			}
			calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
			String sStart = conf.get("start.time");
			String sEnd = conf.get("end.time");
			GeneralHelpers.setDateTimeByString(calTmp, sStart);
			startTime = calTmp.getTimeInMillis();
			GeneralHelpers.setDateTimeByString(calTmp, sEnd);
			endTime = calTmp.getTimeInMillis();
			jsonParser = new JsonParser();
			tweetCount = 0;
			matchCount = 0;
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException {
			String jsonStr = value.toString().trim();
			try {				
				JsonObject joTweet = jsonParser.parse(jsonStr).getAsJsonObject();
				JsonElement jeTime = joTweet.get("created_at");
				if (jeTime == null || jeTime.isJsonNull()) {
					return;
				}
				long createTime = ConstantsTruthy.dateFormat.parse(jeTime.getAsString()).getTime();
				if (createTime < startTime || createTime > endTime) {
					return;
				}
				
				JsonElement jeEntities = joTweet.get("entities");
				if (jeEntities == null || jeEntities.isJsonNull()) {
					return;
				}

				JsonArray jaHashTags = jeEntities.getAsJsonObject().get("hashtags").getAsJsonArray();
				if (!jaHashTags.isJsonNull() && jaHashTags.size() > 0) {
					Iterator<JsonElement> iht = jaHashTags.iterator();
					while (iht.hasNext()) {
						String hashtag = "#" + iht.next().getAsJsonObject().get("text").getAsString().toLowerCase();
						if (memeSet.contains(hashtag)) {
							matchCount++;
							context.write(new Text(jsonStr), NullWritable.get());
							break;
						}
					}
				}
			} catch (Exception e) {
				context.setStatus("Exception when processing tweet: + " + e.getMessage() + "\n" + jsonStr);
			} finally {
				tweetCount++;
				if (tweetCount % 500000 == 0) {
					LOG.info("Processed " + tweetCount + " tweets. Match found: " + matchCount);
				}
			}
		}
	}
	
	/**
	 * "FsmcMappper" stands for "File search meme cooccurrance Mapper".
	 * @author gaoxm
	 */
	static class FsmcMapper extends Mapper<LongWritable, Text, Text, Text> {
		String seedMeme;
		long startTime;
		long endTime;
		Calendar calTmp;
		JsonParser jsonParser;
		int tweetCount;
		int matchCount;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			seedMeme = conf.get("seed.meme");
			calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
			String sStart = conf.get("start.time");
			String sEnd = conf.get("end.time");
			GeneralHelpers.setDateTimeByString(calTmp, sStart);
			startTime = calTmp.getTimeInMillis();
			GeneralHelpers.setDateTimeByString(calTmp, sEnd);
			endTime = calTmp.getTimeInMillis();
			jsonParser = new JsonParser();
			tweetCount = 0;
			matchCount = 0;
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException {
			String jsonStr = value.toString().trim();
			try {				
				JsonObject joTweet = jsonParser.parse(jsonStr).getAsJsonObject();
				JsonElement jeTime = joTweet.get("created_at");
				if (jeTime == null || jeTime.isJsonNull()) {
					return;
				}
				long createTime = ConstantsTruthy.dateFormat.parse(jeTime.getAsString()).getTime();
				if (createTime < startTime || createTime > endTime) {
					return;
				}
				
				JsonElement jeEntities = joTweet.get("entities");
				if (jeEntities == null || jeEntities.isJsonNull()) {
					return;
				}

				JsonArray jaHashTags = jeEntities.getAsJsonObject().get("hashtags").getAsJsonArray();
				if (!jaHashTags.isJsonNull() && jaHashTags.size() > 0) {
					LinkedList<String> hashtags = new LinkedList<String>();
					boolean hasSeed = false;
					Iterator<JsonElement> iht = jaHashTags.iterator();
					while (iht.hasNext()) {
						String hashtag = "#" + iht.next().getAsJsonObject().get("text").getAsString().toLowerCase();
						hashtags.add(hashtag);
						if (hashtag.equals(seedMeme)) {
							hasSeed = true;
						}
					}
					if (hasSeed) {
						matchCount++;
						String tweetId = joTweet.get("id").getAsString();
						for (String ht : hashtags) {
							context.write(new Text(ht), new Text(tweetId));
						}
					}
				}
			} catch (Exception e) {
				context.setStatus("Exception when processing tweet: + " + e.getMessage() + "\n" + jsonStr);
			} finally {
				tweetCount++;
				if (tweetCount % 500000 == 0) {
					LOG.info("Processed " + tweetCount + " tweets. Match found:" + matchCount); 
				}
			}
		}
	}
	
	/**
	 * "FsmcReducer" stands for "file search meme cooccurrance reducer".
	 * 
	 * @author gaoxm
	 */
	public static class FsmcReducer extends Reducer<Text, Text, Text, NullWritable> {
		String seedMeme;
		String seedMemeTidDir;
		String lastMeme;
		Set<String> tweetIdSet;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			seedMeme = conf.get("seed.meme");
			seedMemeTidDir = conf.get("seed.meme.tid.dir");
			lastMeme = null;
			tweetIdSet = new HashSet<String>();
		}
		
		@Override
		protected void reduce(Text memeText, Iterable<Text> tweetIds, Context context) throws IOException, InterruptedException {
			String meme = memeText.toString();
			if (lastMeme != null && !lastMeme.equals(meme)) {
				if (lastMeme.equals(seedMeme)) {
					// To prepare for related hashtag mining, the tweet IDs for the seed meme is written to a separate directory.
					try {
						MultiFileFolderWriter writer = new MultiFileFolderWriter(seedMemeTidDir, "tweetIds_", true, -1);
						writer.writeln(lastMeme);
						for (String tid : tweetIdSet) {
							writer.writeln(tid);
						}
						writer.close();
					} catch (Exception e) {
						throw new IOException(e);
					}
				} else {
					context.write(new Text(lastMeme), NullWritable.get());
					for (String tid : tweetIdSet) {
						context.write(new Text(tid), NullWritable.get());
					}
				}
				tweetIdSet.clear();				
			}
			lastMeme = meme;
			for (Text tid : tweetIds) {
				tweetIdSet.add(tid.toString());
			}			
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			if (lastMeme != null) {
				if (lastMeme.equals(seedMeme)) {
					try {
						MultiFileFolderWriter writer = new MultiFileFolderWriter(seedMemeTidDir, "tweetIds_", true, -1);
						writer.writeln(lastMeme);
						for (String tid : tweetIdSet) {
							writer.writeln(tid);
						}
						writer.close();
					} catch (Exception e) {
						throw new IOException(e);
					}
				} else {
					context.write(new Text(lastMeme), NullWritable.get());
					for (String tid : tweetIdSet) {
						context.write(new Text(tid), NullWritable.get());
					}
				}	
			}
		}
	}
	
	/**
	 * Mapper for mining related hashtags by using the Jaccard coefficient, taking a directory containing tweet IDs of
	 * coexisting hashtags for a seed hashtag.
	 * @author gaoxm
	 */
	static class RelatedHashtagMiningMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		String seedMemeTidDir;
		double threshold;
		Set<String> seedMemeTids;
		Set<String> lastMemeTids;
		String lastMeme;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			try {
				Configuration conf = context.getConfiguration();
				seedMemeTidDir = conf.get("seed.meme.tid.dir");
				seedMemeTids = new HashSet<String>();
				FileSystem fs = FileSystem.get(new URI(seedMemeTidDir), conf);
				Path folderPath = new Path(seedMemeTidDir);
				for (FileStatus fstat : fs.listStatus(folderPath)) {
					if (!fs.isDirectory(fstat.getPath())) {
						BufferedReader brTid = new BufferedReader(new InputStreamReader(fs.open(fstat.getPath())));
						String line = brTid.readLine();
						while (line != null) {
							line = line.trim();
							if (line.length() > 0) {
								seedMemeTids.add(line);
							}
							line = brTid.readLine();
						}
						brTid.close();
					}
				}

				threshold = conf.getDouble("threshold", 0.005);
				lastMeme = null;
				lastMemeTids = new HashSet<String>();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if (line.charAt(0) == '#') {
				if (lastMeme == null) {
					lastMeme = line;
				} else {
					// compute the jaccard coefficient for lastMeme
					double coeff = getJaccardCoeff(seedMemeTids, lastMemeTids);
					if (coeff >= threshold) {
						context.write(new Text(lastMeme + "\t" + coeff), NullWritable.get());
					}
					lastMeme = line;
					lastMemeTids.clear();
				}
			} else {
				lastMemeTids.add(line);
			}
		}
		
		protected double getJaccardCoeff(Set<String> seedMemeTids, Set<String> targetMemeTids) {
			Set<String> s1 = null;
			Set<String> s2 = null;
			if (seedMemeTids.size() < targetMemeTids.size()) {
				s1 = seedMemeTids;
				s2 = targetMemeTids;
			} else {
				s1 = targetMemeTids;
				s2 = seedMemeTids;
			}
			
			int intersectionSize = 0;
			int unioSize = s2.size();
			for (String tid : s1) {
				if (s2.contains(tid)) {
					intersectionSize++;
				} else {
					unioSize++;
				}
			}
			
			return intersectionSize * 1.0 / unioSize;
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			if (lastMeme != null) {
				// compute the jaccard coefficient for lastMeme
				double coeff = getJaccardCoeff(seedMemeTids, lastMemeTids);
				if (coeff >= threshold) {
					context.write(new Text(lastMeme + "\t" + coeff), NullWritable.get());
				}
			}
		}
	}
	
	/**
	 * "FsreMapper" stands for "File Search Retweet Edge generation mapper".
	 * @author gaoxm
	 */
	static class FsmeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		Set<String> memeSet;
		long startTime;
		long endTime;
		Calendar calTmp;
		JsonParser jsonParser;
		int tweetCount;
		int matchCount;
		Map<String, Long> mentionEdgeCount;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String[] memes = conf.get("memes").split(",+");
			memeSet = new HashSet<String>();
			for (String meme : memes) {
				memeSet.add(meme);
			}
			calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
			String sStart = conf.get("start.time");
			String sEnd = conf.get("end.time");
			GeneralHelpers.setDateTimeByString(calTmp, sStart);
			startTime = calTmp.getTimeInMillis();
			GeneralHelpers.setDateTimeByString(calTmp, sEnd);
			endTime = calTmp.getTimeInMillis();
			jsonParser = new JsonParser();
			tweetCount = 0;
			matchCount = 0;
			mentionEdgeCount = new HashMap<String, Long>();
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException {
			String jsonStr = value.toString().trim();
			try {				
				JsonObject joTweet = jsonParser.parse(jsonStr).getAsJsonObject();
				JsonElement jeTime = joTweet.get("created_at");
				if (jeTime == null || jeTime.isJsonNull()) {
					return;
				}
				long createTime = ConstantsTruthy.dateFormat.parse(jeTime.getAsString()).getTime();
				if (createTime < startTime || createTime > endTime) {
					return;
				}		
				
				JsonElement jeEntities = joTweet.get("entities");
				if (jeEntities == null || jeEntities.isJsonNull()) {
					return;
				}
				
				JsonArray jaMentions = jeEntities.getAsJsonObject().get("user_mentions").getAsJsonArray();
				if (jaMentions.isJsonNull() || jaMentions.size() <= 0) {
					return;
				}

				JsonArray jaHashTags = jeEntities.getAsJsonObject().get("hashtags").getAsJsonArray();
				if (!jaHashTags.isJsonNull() && jaHashTags.size() > 0) {
					Iterator<JsonElement> iht = jaHashTags.iterator();
					while (iht.hasNext()) {
						String hashtag = "#" + iht.next().getAsJsonObject().get("text").getAsString().toLowerCase();
						if (memeSet.contains(hashtag)) {
							matchCount++;
							JsonObject joUser = joTweet.get("user").getAsJsonObject();
							String uid = joUser.get("id").getAsString();
							
							Iterator<JsonElement> ium = jaMentions.iterator();
							while (ium.hasNext()) {
								JsonObject jomu = ium.next().getAsJsonObject();
								String mentionedUid = jomu.get("id").getAsString();
								String edgeInfo = uid + " " + mentionedUid;
								Long count = mentionEdgeCount.get(edgeInfo);
								if (count == null) {
									mentionEdgeCount.put(edgeInfo, 1L);
								} else {
									mentionEdgeCount.put(edgeInfo, count + 1L);
								}								
							}							
							break;
						}
					}
				}
			} catch (Exception e) {
				context.setStatus("Exception when processing tweet: + " + e.getMessage() + "\n" + jsonStr);
			} finally {
				tweetCount++;
				if (tweetCount % 500000 == 0) {
					LOG.info("Processed " + tweetCount + " tweets. Match found:" + matchCount); 
				}
			}
		}	
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Map.Entry<String, Long> e : mentionEdgeCount.entrySet()) {
				context.write(new Text(e.getKey()), new LongWritable(e.getValue()));
			}
			super.cleanup(context);
		}
	}
	
	/**
	 * Mapper for getting a meme daily frequency list.
	 * @author gaoxm
	 */
	static class MemeDailyFreqMapper extends Mapper<LongWritable, Text, Text, Text> {
		Map<String, Map<Long, Integer>> memeFreqs;
		Calendar calTmp;
		JsonParser jsonParser;
		int tweetCount;
		long dailyMilli = 86400000L;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			memeFreqs = new HashMap<String, Map<Long, Integer>>();
			calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
			jsonParser = new JsonParser();
			tweetCount = 0;
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException {
			String jsonStr = value.toString().trim();
			try {				
				JsonObject joTweet = jsonParser.parse(jsonStr).getAsJsonObject();
				JsonElement jeTime = joTweet.get("created_at");
				if (jeTime == null || jeTime.isJsonNull()) {
					return;
				}
				long createTime = ConstantsTruthy.dateFormat.parse(jeTime.getAsString()).getTime();
				createTime = createTime / dailyMilli * dailyMilli;
								
				JsonElement jeEntities = joTweet.get("entities");
				if (jeEntities == null || jeEntities.isJsonNull()) {
					return;
				}
				JsonElement jeRetweet = joTweet.get("retweeted_status");
				long createTime2 = -1;
				if (jeRetweet != null && !jeRetweet.isJsonNull()) {
					createTime2 = ConstantsTruthy.dateFormat.parse(jeRetweet.getAsJsonObject().get("created_at").getAsString()).getTime();
					createTime2 = createTime2 / dailyMilli * dailyMilli;
				}
				
				JsonArray jaHashTags = jeEntities.getAsJsonObject().get("hashtags").getAsJsonArray();
				if (!jaHashTags.isJsonNull() && jaHashTags.size() > 0) {
					Iterator<JsonElement> iht = jaHashTags.iterator();
					while (iht.hasNext()) {
						String hashtag = "#" + iht.next().getAsJsonObject().get("text").getAsString().toLowerCase();
						Map<Long, Integer> dateFreqs = memeFreqs.get(hashtag);
						if (dateFreqs == null) {
							dateFreqs = new HashMap<Long, Integer>();
							memeFreqs.put(hashtag, dateFreqs);
						}
						Integer freq = dateFreqs.get(createTime);
						if (freq == null) {
							dateFreqs.put(createTime, 1);
						} else {
							dateFreqs.put(createTime, freq + 1);
						}
						if (createTime2 > 0) {
							freq = dateFreqs.get(createTime2);
							if (freq == null) {
								dateFreqs.put(createTime2, 1);
							} else {
								dateFreqs.put(createTime2, freq + 1);
							}
						}						
					}
				}
			} catch (Exception e) {
				context.setStatus("Exception when processing tweet: + " + e.getMessage() + "\n" + jsonStr);
			} finally {
				tweetCount++;
				if (tweetCount % 500000 == 0) {
					LOG.info("Processed " + tweetCount + " tweets. Match found:"); 
				}
			}
		}	
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Map.Entry<String, Map<Long, Integer>> e : memeFreqs.entrySet()) {
				String meme = e.getKey();
				Map<Long, Integer> dateFreqs = e.getValue();
				for (Map.Entry<Long, Integer> edf : dateFreqs.entrySet()) {
					long time = edf.getKey();
					int freq = edf.getValue();
					calTmp.setTimeInMillis(time);
					String dateFreqStr = GeneralHelpers.getDateString(calTmp) + "|" + freq;
					context.write(new Text(meme), new Text(dateFreqStr));
				}
			}
			super.cleanup(context);
		}
	}
	
	/**
	 * Reducer for getting a meme daily frequency list.
	 * 
	 * @author gaoxm
	 */
	public static class MemeDailyFreqReducer extends Reducer<Text, Text, Text, NullWritable> {		
		@Override
		protected void reduce(Text memeText, Iterable<Text> dateFreqs, Context context) throws IOException, InterruptedException {
			String meme = memeText.toString();
			TreeMap<String, Integer> dateFreqMap = new TreeMap<String, Integer>();
			for (Text df : dateFreqs) {
				String dateFreqStr = df.toString();
				int idx = dateFreqStr.indexOf('|');
				String date = dateFreqStr.substring(0, idx);
				int freq = Integer.valueOf(dateFreqStr.substring(idx + 1));
				Integer curFreq = dateFreqMap.get(date);
				if (curFreq == null) {
					dateFreqMap.put(date, freq);
				} else {
					dateFreqMap.put(date, curFreq + freq);
				}
			}
			
			StringBuilder sb = new StringBuilder();
			sb.append(meme).append('\t');
			for (Map.Entry<String, Integer> e : dateFreqMap.entrySet()) {
				sb.append(e.getKey()).append('|').append(e.getValue()).append(' ');
			}
			context.write(new Text(sb.toString()), NullWritable.get());
		}
	}
	
	/**
	 * Use the MrfsMapper to scan files in inputDir and find tweets containing the given memes.
	 * 
	 * @param inputDir
	 * 	HDFS directory containing .json.gz files.
	 * @param memes
	 * 	A comma separated string containing hashtags to search for.
	 * @param startTime
	 * 	Start point of a given time window.
	 * @param endTime
	 * 	End point of a given time window.
	 * @param outputDir
	 * 	HDFS directory for the output data.
	 */
	public void getTweetsByMeme(String inputDir, String memes, String startTime, String endTime, String outputDir) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("memes", memes);
		conf.set("start.time", startTime);
		conf.set("end.time", endTime);
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf, "Truthy .json.gz file searcher");
		job.setJarByClass(MrfsMapper.class);
		FileInputFormat.setInputPaths(job, new Path(inputDir));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MrfsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		job.setNumReduceTasks(0);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	/**
	 * Use the FsreMapper to scan files in inputDir and get the retweet network related to the given memes.
	 * 
	 * @param inputDir
	 * 	HDFS directory containing .json.gz files.
	 * @param memes
	 * 	A comma separated string containing hashtags to search for.
	 * @param startTime
	 * 	Start point of a given time window.
	 * @param endTime
	 * 	End point of a given time window.
	 * @param outputDir
	 * 	HDFS directory for the output data.
	 * @param nReducers
	 * 	Number of reducers to use.
	 */
	public void getMentionNetwork(String inputDir, String memes, String startTime, String endTime, String outputDir, int nReducers) 
			throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("memes", memes);
		conf.set("start.time", startTime);
		conf.set("end.time", endTime);
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf, "Truthy retweet network generator by .json.gz file scanning");
		job.setJarByClass(FsmeMapper.class);
		FileInputFormat.setInputPaths(job, new Path(inputDir));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(FsmeMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(TextCountReducer.class);
		job.setNumReduceTasks(nReducers);
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	/**
	 * Use the FsmcMapper, FsmcReducer, and RelatedHashtagMiningMapper to mine related hashtags about .
	 * 
	 * @param inputDir
	 * 	HDFS directory containing .json.gz files.
	 * @param seedMeme
	 * 	The seed hashtag.
	 * @param startTime
	 * 	Start point of a given time window.
	 * @param endTime
	 * 	End point of a given time window.
	 * @param outputDir
	 * 	HDFS directory for the output data.
	 * @param nReducers
	 * 	Number of reducers to use when getting co-occurred hashtags.
	 * @param threshold
	 * 	Jaccard coefficient threshold.
	 */
	public void mineRelatedHashtag(String inputDir, String seedMeme, String startTime, String endTime, String outputDir, int nReducers, 
			double threshold) throws Exception {
		// first MR job for finding the tweet IDs containing each hashtag that co-occurred with seedMeme 
		System.out.println("Finding tweet IDs for all co-coccured hashtags...");
		Configuration conf = HBaseConfiguration.create();
		int idx = outputDir.lastIndexOf('/', outputDir.length() - 2);
		long time = System.currentTimeMillis();
		String seedMemeTidDir = outputDir.substring(0, idx + 1) + "seedMemeTid_" + time;
		String candMemeTidDir = outputDir.substring(0, idx + 1) + "candMemeTid_" + time;
		conf.set("seed.meme", seedMeme);
		conf.set("start.time", startTime);
		conf.set("end.time", endTime);
		conf.set("seed.meme.tid.dir", seedMemeTidDir);
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf, "Finding tweet IDs for co-occurred hashtags related to the seed hashtag");
		job.setJarByClass(FsmcMapper.class);
		FileInputFormat.setInputPaths(job, new Path(inputDir));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(FsmcMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(FsmcReducer.class);
		job.setNumReduceTasks(nReducers);
		FileOutputFormat.setOutputPath(job, new Path(candMemeTidDir));
		boolean succ = job.waitForCompletion(true);
		if (!succ) {
			System.out.println("Error when running the last MR job. Exiting...");
			System.exit(1);
		}
		
		// Second MR job to process the output of the first job to filter co-occurred hashtags based on Jaccard coefficient
		System.out.println("Generating related hashtags using Jaccard coefficient...");
		conf = HBaseConfiguration.create();
		conf.set("seed.meme.tid.dir", seedMemeTidDir);
		conf.setDouble("threshold", threshold);
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		job = Job.getInstance(conf, "Related hashtag mining job using Jaccard coefficient");
		job.setJarByClass(RelatedHashtagMiningMapper.class);
		FileInputFormat.setInputPaths(job, new Path(candMemeTidDir));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(RelatedHashtagMiningMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		succ = job.waitForCompletion(true);
		if (!succ) {
			System.out.println("Error when computing related hashtags using Jaccard coefficient.");
			System.exit(1);
		}
	}
	
	/**
	 * Use the MemeLifeTimeMapper and MemeLifeTimeReducer get the meme lifetime distribution from the .json.gz files in inputDir.
	 * 
	 * @param inputDir
	 * 	HDFS directory containing .json.gz files.
	 * @param outputDir
	 * 	HDFS directory for the output data.
	 * @param nReducers
	 * 	Number of reducers to use.
	 */
	public void getMemeDailyFreq(String inputDir, String outputDir, int nReducers) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf, "Meme lifetime distribution generator");
		job.setJarByClass(MemeDailyFreqMapper.class);
		FileInputFormat.setInputPaths(job, new Path(inputDir));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MemeDailyFreqMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MemeDailyFreqReducer.class);
		job.setNumReduceTasks(nReducers);
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void usage() {
		System.err.println("Usage: MRJsonGzFileSearcher <command> <arguments>");
		System.err.println("	Where '<command> <arguments>' could be one of the following:");
		System.err.println("	get-tweets-with-meme <.json.gz file directory> <memes> <start time> <end time> <output directory>");
		System.err.println("	get-mention-network-with-meme <.json.gz file directory> <memes> <start time> <end time>"
				+ " <output directory> <number of reducers>");
		System.err.println("	mine-related-hashtag <.json.gz file directory> <seed meme> <start time> <end time>"
				+ " <output directory> <number of reducers> <Jaccard coefficient threshold>");
		System.err.println("	get-meme-daily-freq <.json.gz file directory> <output directory> <number of reducers>");
	}

	/**
	 * Main entry point.
	 * @param args The command line parameters.
	 * @throws Exception When running the job fails.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 1) {
			usage();
			System.exit(-1);
		}
		
		String command = otherArgs[0];
		if (command.equals("get-tweets-with-meme")) {
			if (otherArgs.length < 6) {
				usage();
				System.exit(-1);
			}
			String inputDir = otherArgs[1];
			String memes = otherArgs[2];
			String startTime = otherArgs[3];
			String endTime = otherArgs[4];
			String outputDir = otherArgs[5];
			MRJsonGzFileSearcher searcher =  new MRJsonGzFileSearcher();
			searcher.getTweetsByMeme(inputDir, memes, startTime, endTime, outputDir);			
		} else if (command.equals("get-mention-network-with-meme")) {
			if (otherArgs.length < 7) {
				usage();
				System.exit(-1);
			}
			String inputDir = otherArgs[1];
			String memes = otherArgs[2];
			String startTime = otherArgs[3];
			String endTime = otherArgs[4];
			String outputDir = otherArgs[5];
			int nReducers = Integer.valueOf(otherArgs[6]);
			MRJsonGzFileSearcher searcher =  new MRJsonGzFileSearcher();
			searcher.getMentionNetwork(inputDir, memes, startTime, endTime, outputDir, nReducers);			
		} else if (command.equals("mine-related-hashtag")) {
			if (otherArgs.length < 8) {
				usage();
				System.exit(-1);
			}
			String inputDir = otherArgs[1];
			String seedMeme = otherArgs[2];
			String startTime = otherArgs[3];
			String endTime = otherArgs[4];
			String outputDir = otherArgs[5];
			int nReducers = Integer.valueOf(otherArgs[6]);
			double threshold = Double.valueOf(otherArgs[7]);
			MRJsonGzFileSearcher searcher =  new MRJsonGzFileSearcher();
			searcher.mineRelatedHashtag(inputDir, seedMeme, startTime, endTime, outputDir, nReducers, threshold);			
		} else if (command.equals("get-meme-daily-freq")) {
			if (otherArgs.length < 4) {
				usage();
				System.exit(-1);
			}
			String inputDir = otherArgs[1];
			String outputDir = otherArgs[2];
			int nReducers = Integer.valueOf(otherArgs[3]);
			MRJsonGzFileSearcher searcher =  new MRJsonGzFileSearcher();
			searcher.getMemeDailyFreq(inputDir, outputDir, nReducers);			
		} else {
			usage();
			System.exit(-1);
		}		
	}
}
