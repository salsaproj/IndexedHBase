package iu.pti.hbaseapp.clueweb09;

import java.io.IOException;
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class HBaseOverheadTester {
	/**
	 * Internal Mapper to be run by Hadoop.
	 */
	public static class HotMapper extends TableMapper<ImmutableBytesWritable, Put> {
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			List<KeyValue> kvList = result.list();
			for (KeyValue kv : kvList) {
				Put put = new Put(rowKey.get());
				put.add(kv);
				context.write(rowKey, put);
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
		Job job = Job.getInstance(conf,	"Copy data from index table to duplicated index table to test the overhead of HBase");
		job.setJarByClass(HBaseOverheadTester.class);
		TableMapReduceUtil.initTableMapperJob(Cw09Constants.CLUEWEB09_INDEX_TABLE_NAME, scan, HotMapper.class, ImmutableBytesWritable.class,
												Writable.class, job, true, CustomizedSplitTableInputFormat.class);
		TableMapReduceUtil.initTableReducerJob(Cw09Constants.CLUEWEB09_DUP_INDEX_TABLE_NAME, null, job);
		job.setNumReduceTasks(0);
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
