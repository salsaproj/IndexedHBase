package iu.pti.hbaseapp.clueweb09;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DocIdPortionCounter {
	/**
	 * Internal Mapper to be run by Hadoop.
	 */
	public static class DpcMapper extends TableMapper<IntWritable, IntWritable> {
		int sectionCount = 0;
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			int docId = Bytes.toInt(rowKey.get());
			context.write(new IntWritable(docId % sectionCount), new IntWritable(1));
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();
			sectionCount = Integer.valueOf(configuration.get("section.count"));
		}
	}
	
	public static class DpcReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
		protected void reduce(IntWritable secNum, Iterable<IntWritable> counts, Context context) 
				throws IOException, InterruptedException {
			int total = 0;
			for (IntWritable c : counts) {
				total += c.get();
			}
			context.write(secNum, new IntWritable(total));
		}
	}
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		int secCount = Integer.valueOf(args[0]);
		String outputPath = args[1];
		conf.set("section.count", args[0]);
		Scan scan = new Scan();
		scan.addColumn(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES);

		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf,	"Count the number of document within each document section");
		job.setJarByClass(DocIdPortionCounter.class);
		TableMapReduceUtil.initTableMapperJob(Cw09Constants.CLUEWEB09_DATA_TABLE_NAME, scan, DpcMapper.class, IntWritable.class, 
												IntWritable.class, job, true, CustomizedSplitTableInputFormat.class);
		job.setCombinerClass(DpcReducer.class);
		job.setReducerClass(DpcReducer.class);
		job.setNumReduceTasks(secCount);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, minimum required: 2");
			System.err.println("Usage: DocIdPortionCounter <section count> <HDFS output path>");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
