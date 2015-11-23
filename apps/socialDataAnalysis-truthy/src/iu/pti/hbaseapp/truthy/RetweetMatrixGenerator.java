package iu.pti.hbaseapp.truthy;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce program for generating the adjacency matrix of the retweet network from tweet IDs found by
 * using index table operators.
 * <p/>
 * Usage: java iu.pti.hbaseapp.truthy.RetweetMatrixGenerator [input directory] [output directory] [number of reducers]
 * 
 * @author gaoxm
 */
public class RetweetMatrixGenerator {
	
	/**
	 * Retweet matrix generation mapper.
	 */
	public static class RmgMapper extends Mapper<LongWritable, Text, Text, Text> {
		HTable tweetTable = null;
		HashMap<String, Set<String>> adjacencyMap = null;
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
			get.addColumn(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_RETWEET_UID_BYTES);
			Result r = tweetTable.get(get);
			if (r == null) {
				context.setStatus("Can't find tweet ID " + txtTweetId.toString());
				return;
			}
			byte[] userIdBytes = r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_USER_ID_BYTES);
			byte[] retweetUidBytes = r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_RETWEET_UID_BYTES);
			if (userIdBytes != null && retweetUidBytes != null) {
				String userId = TruthyHelpers.getUserIDStrFromBytes(userIdBytes);
				String retweetUid = TruthyHelpers.getUserIDStrFromBytes(retweetUidBytes);
				Set<String> uidNeighbours = adjacencyMap.get(userId);
				if (uidNeighbours != null) {
					uidNeighbours.add(retweetUid);
				} else {
					uidNeighbours = new HashSet<String>();
					uidNeighbours.add(retweetUid);
					adjacencyMap.put(userId, uidNeighbours);
				}
				
				Set<String> ruidNeighbours = adjacencyMap.get(retweetUid);
				if (ruidNeighbours != null) {
					ruidNeighbours.add(userId);
				} else {
					ruidNeighbours = new HashSet<String>();
					ruidNeighbours.add(userId);
					adjacencyMap.put(retweetUid, ruidNeighbours);
				}
			}
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			// input file name is like "2012-03_memeTweets_0.txt"
			String inputFileName = ((FileSplit)context.getInputSplit()).getPath().getName();
			int idx = inputFileName.indexOf('_');
			String month = inputFileName.substring(0, idx);
			tweetTable = new HTable(conf, ConstantsTruthy.TWEET_TABLE_NAME + "-" + month);
			adjacencyMap = new HashMap<String, Set<String>>(ConstantsTruthy.TWEETS_PER_MAPPER);
			this.useBigInt = TruthyHelpers.checkIfb4June2015(month);
		}
		
		@Override
		protected void cleanup(Context context) {
			try {
				if (tweetTable != null) {
					tweetTable.close();
				}
				if (adjacencyMap != null) {
					for (Map.Entry<String, Set<String>> e : adjacencyMap.entrySet()) {
						String id = e.getKey();
						Set<String> neighbours = e.getValue();
						StringBuilder sb = new StringBuilder();
						for (String nbr : neighbours) {
							sb.append(nbr).append('\t');
						}
						sb.deleteCharAt(sb.length() - 1);
						context.write(new Text(id), new Text(sb.toString()));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * General reducer useful for collecting neighbours.
	 * @author gaoxm
	 *
	 */
	public static class NeighbourCollectReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> neighbourSets, Context context) throws IOException, InterruptedException {
			Set<String> allNbrs = new HashSet<String>();
			for (Text neighbourStr : neighbourSets) {
				String[] nbrs = neighbourStr.toString().split("\\t");
				for (String nbr: nbrs) {
					if (nbr.length() > 0) {
						allNbrs.add(nbr);
					}
				}
			}
			StringBuilder sb = new StringBuilder();
			for (String nbr : allNbrs) {
				sb.append(nbr).append('\t');
			}
			sb.deleteCharAt(sb.length() - 1);
			context.write(key, new Text(sb.toString()));
		}
	}
	
	protected static void usage() {
		System.out.println("Usage: java iu.pti.hbaseapp.truthy.RetweetMatrixGenerator <input directory> <output directory> <number of reducers>");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			if (args.length < 3) {
				usage();
				System.exit(1);
			}
			String mrInput = args[0];
			String mrOutput = args[1];
			int nReducer = Integer.valueOf(args[2]);

			long begin = System.currentTimeMillis();
			Configuration conf = HBaseConfiguration.create();
			conf.set("mapred.map.tasks.speculative.execution", "false");
			conf.set("mapred.reduce.tasks.speculative.execution", "false");
			Job job = Job.getInstance(conf, "retweet matrix generator");
			job.setJarByClass(RetweetMatrixGenerator.class);
			FileInputFormat.setInputPaths(job, new Path(mrInput));
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(RmgMapper.class);
			job.setReducerClass(NeighbourCollectReducer.class);
			job.setNumReduceTasks(nReducer);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job, new Path(mrOutput));
			TableMapReduceUtil.addDependencyJars(job);
			boolean succ = job.waitForCompletion(true);
			if (succ) {
				System.out.println("Please check out results in " + mrOutput);
			} else {
				System.out.println("Error occurred when running the MapReduce job.");
			}
			long end = System.currentTimeMillis();
			System.out.println("Done. Time used in secs: " + (end - begin) / 1000.0);
		} catch (Exception e) {
			usage();
			e.printStackTrace();
		}
	}

}
