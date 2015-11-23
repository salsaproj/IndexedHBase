package iu.pti.hbaseapp.truthy;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This MapReduce application processes the daily meme frequency files from Diego, in the format of 
 * <br/>
 * [date, (hashtag | frequency)],
 * <br/>
 * and generates output files in the format of 
 * <br/>
 * [hashtag, a list of (date : frequency)].
 * <p/>
 * Usage: java iu.pti.hbaseapp.truthy.DailyMemeFreqProcessor [input directory] [output directory] [first valid date] [last valid date] 
 * [number of reducers]
 * 
 * @author gaoxm
 */
public class DailyMemeFreqProcessor {
	
	/**
	 * Mapper class used by {@link #DailyMemeFreqProcessor}. 
	 * 
	 * @author gaoxm
	 */
	public static class DmfpMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException {
			try {
				String line = value.toString().trim();
				int idx1 = line.indexOf('|');
				int idx2 = line.lastIndexOf('|');
				if (idx1 < 0 || idx2 < 0 || idx1 == idx2) {
					System.err.println("Invalid line: " + line);
					return;
				}
				
				String date = line.substring(0, idx1);
				date = date.replace('_', '-');
				String meme = line.substring(idx1 + 1, idx2);
				String freq = line.substring(idx2 + 1);
				context.write(new Text(meme), new Text(date + "|" + freq));
			} catch (Exception e) {
				e.printStackTrace();
				throw new IOException(e);
			}
		}
	}
	
	/**
	 * Reducer class used by {@link #DailyMemeFreqProcessor}. 
	 * 
	 * @author gaoxm
	 */
	public static class DmfpReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text memeText, Iterable<Text> dailyFreqs, Context context) throws IOException, InterruptedException {
			try {				
				TreeMap<String, Integer> dateFreqMap = new TreeMap<String, Integer>();
				for (Text dailyFreq : dailyFreqs) {
					String dailyInfo = dailyFreq.toString();
					int idx = dailyInfo.indexOf('|');
					if (idx < 0) {
						System.err.println("Invalid daily frequency: " + dailyInfo);
					}
					String date = dailyInfo.substring(0, idx);
					int freq = Integer.parseInt(dailyInfo.substring(idx + 1));
					
					Integer oldFreq = dateFreqMap.get(date);
					if (oldFreq == null) {
						dateFreqMap.put(date, freq);
					} else {
						dateFreqMap.put(date, oldFreq + freq);
					}
				}
				
				StringBuilder sb = new StringBuilder();
				for (Map.Entry<String, Integer> e : dateFreqMap.entrySet()) {
					sb.append(e.getKey()).append('|').append(e.getValue()).append(' ');
				}
				if (sb.length() > 0) {
					sb.deleteCharAt(sb.length() - 1);
				}
				context.write(memeText, new Text(sb.toString()));
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
	}
	
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		int nReducers = Integer.parseInt(args[2]);
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		conf.set("mapred.output.compress", "true");
		conf.set("mapred.output.compression.type", "BLOCK");
		conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		Job job = Job.getInstance(conf, "Truthy daily meme frequency file processer");
		job.setJarByClass(DmfpMapper.class);
		FileInputFormat.setInputPaths(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(DmfpMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(DmfpReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(nReducers);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Wrong number of arguments: " + otherArgs.length);
			System.err.println("Usage: java iu.pti.hbaseapp.truthy.DailyMemeFreqProcessor <input directory> <output directory> "
					+ "<number of reducers>");
			System.exit(-1);
		}
		
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
