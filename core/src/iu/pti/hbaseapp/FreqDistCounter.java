package iu.pti.hbaseapp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * Given an inverted index table containing document IDs as qualifiers and words' frequencies as cell values,
 * this MapReduce application takes such a table as input, and generate text files containing multiple lines.
 * Each line is in the form of <br/> 
 * [word] [document count of this word] [total frequency of this word] [index table record size of this word]
 * <p/>
 * These output files can be further processed by {@link #DistributionSeparator} to generate distributions
 * of document count, total frequency, and index table record size among all the words.  
 * 
 * @author gaoxm
 */
public class FreqDistCounter {
	/**
	 * Mapper class used by {@link #FreqDistCounter}.
	 */
	public static class FdcMapper extends TableMapper<ImmutableBytesWritable, Text> {
		protected static byte[] columnFamilyBytes = null;
		protected static String tableName = null;
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			long recordSize = GeneralHelpers.getHBaseResultKVSize(result);
			List<KeyValue> cellList = result.list();
			long docCount = cellList.size();
			//boolean isTextIndex = tableName.equals(Constants.TEXTS_INDEX_TABLE_NAME);
			long totalFreq = 0;
			for (KeyValue cell : cellList) {
				byte[] value = cell.getValue();
				totalFreq += Bytes.toInt(value);
			}
			
			StringBuffer sb = new StringBuffer();
			sb.append(docCount).append('\t');
			sb.append(totalFreq).append('\t');
			sb.append(recordSize);
			
			context.write(rowKey, new Text(sb.toString()));
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();
			if (columnFamilyBytes == null) {
				columnFamilyBytes = Bytes.toBytes(configuration.get("column.family.name"));
			}
			if (tableName == null) {
				tableName = configuration.get("table.name");
			}
		}
	}
	
	/**
	 * Combiner class used by {@link #FreqDistCounter}.
	 */
	public static class FdcCombiner extends Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text> {
		@Override
		protected void reduce(ImmutableBytesWritable term, Iterable<Text> statLines, Context context) 
				throws IOException, InterruptedException {
			long docCount = 0;
			long totalFreq = 0;
			long recordSize = 0;
			for (Text line : statLines) {
				String[] nums = line.toString().split("\\t");
				docCount += Long.valueOf(nums[0]);
				totalFreq += Long.valueOf(nums[1]);
				recordSize += Long.valueOf(nums[2]);
			}
			
			StringBuffer sb = new StringBuffer();
			sb.append(docCount).append('\t');
			sb.append(totalFreq).append('\t');
			sb.append(recordSize);
			context.write(term, new Text(sb.toString()));
		}
	}
	
	/**
	 * Reducer class used by FreqDistCounter.
	 */
	public static class FdcReducer extends Reducer<ImmutableBytesWritable, Text, Text, Text> {
		@Override
		protected void reduce(ImmutableBytesWritable term, Iterable<Text> statLines, Context context) throws IOException, InterruptedException {
			long docCount = 0;
			long totalFreq = 0;
			long recordSize = 0;
			for (Text line : statLines) {
				String[] nums = line.toString().split("\\t");
				docCount += Long.valueOf(nums[0]);
				totalFreq += Long.valueOf(nums[1]);
				recordSize += Long.valueOf(nums[2]);
			}

			StringBuffer sb = new StringBuffer();
			sb.append(docCount).append('\t');
			sb.append(totalFreq).append('\t');
			sb.append(recordSize);
			context.write(new Text(Bytes.toString(term.get())), new Text(sb.toString()));
		}
	}
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		String tableName = args[0];
		String columnFamily = args[1];
		String outputPath = args[2];
	    conf.set("column.family.name", columnFamily);
	    conf.set("table.name", tableName);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(columnFamily));
		scan.setBatch(Constants.TABLE_SCAN_BATCH);

		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf,	"Count the docCount, freqCount, and indexRecordSize for each term in " + tableName);
		job.setJarByClass(FreqDistCounter.class);
		//TableMapReduceUtil.initTableMapperJob(tableName, scan, Mapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, FdcMapper.class, ImmutableBytesWritable.class, Text.class,
												job, true);
		job.setCombinerClass(FdcCombiner.class);
		job.setReducerClass(FdcReducer.class);
		job.setNumReduceTasks(40);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}
	
	/**
	 * Usage: iu.pti.hbaseapp.FreqDistCounter <table name> <column family> <output path>
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, required: 3");
			System.err.println("Usage: FreqDistCounter <table name> <column family> <output path>");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
