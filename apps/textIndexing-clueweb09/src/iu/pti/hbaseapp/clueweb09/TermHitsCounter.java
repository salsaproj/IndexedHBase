package iu.pti.hbaseapp.clueweb09;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class TermHitsCounter {
	/**
	 * Internal Mapper to be run by Hadoop.
	 */
	public static class ThcMapper extends TableMapper<Text, LongWritable> {
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			List<KeyValue> kvList = result.list();
			//boolean isTextIndex = tableName.equals(Constants.TEXTS_INDEX_TABLE_NAME);
			long totalFreq = 0;
			Iterator<KeyValue> iter = kvList.iterator();
			while (iter.hasNext()) {
				byte[] value = iter.next().getValue();
				totalFreq += Bytes.toInt(value);
			}
			
			Text term = new Text(Bytes.toString(rowKey.get()));
			context.write(term, new LongWritable(totalFreq));
		}
	}
	
	public static class ThcCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text term, Iterable<LongWritable> freqs, Context context) 
				throws IOException, InterruptedException {
			long totalFreq = 0;
			for (LongWritable freq : freqs) {
				totalFreq += freq.get();
			}
			
			if (totalFreq > 1) {
				context.write(term, new LongWritable(totalFreq));
			}
		}
	}
	
	public static class ThcReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
		@Override
		protected void reduce(Text term, Iterable<LongWritable> freqs, Context context) 
				throws IOException, InterruptedException {
			long totalFreq = 0;
			for (LongWritable freq : freqs) {
				totalFreq += freq.get();
			}
			
			if (totalFreq > 1) {
				byte[] rowKey = Bytes.toBytes(term.toString());
				Put put = new Put(rowKey);
				put.add(Cw09Constants.CF_FREQUENCIES_BYTES, Cw09Constants.QUAL_COUNT_BYTES, Bytes.toBytes(totalFreq));
				context.write(new ImmutableBytesWritable(rowKey), put);
			}
		}
	}
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		Scan scan = new Scan();
		scan.addFamily(Cw09Constants.CF_FREQUENCIES_BYTES);
		scan.setBatch(Cw09Constants.CW09_INDEX_SCAN_BATCH);

		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf,	"Count the total frequency of each term in the index table");
		job.setJarByClass(TermHitsCounter.class);
		//TableMapReduceUtil.initTableMapperJob(Constants.CLUEWEB09_INDEX_TABLE_NAME, scan, 
		//		ThcMapper.class, Text.class, LongWritable.class, job);
		TableMapReduceUtil.initTableMapperJob(Cw09Constants.CLUEWEB09_INDEX_TABLE_NAME, scan, ThcMapper.class, Text.class, 
												LongWritable.class, job, true, CustomizedSplitTableInputFormat.class);
		job.setCombinerClass(ThcCombiner.class);
		TableMapReduceUtil.initTableReducerJob(Cw09Constants.CLUEWEB09_TERM_COUNT_TABLE_NAME, ThcReducer.class, job);
		job.setNumReduceTasks(40);
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
