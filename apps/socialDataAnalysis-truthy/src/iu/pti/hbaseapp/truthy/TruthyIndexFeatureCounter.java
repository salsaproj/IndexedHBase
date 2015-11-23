package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.GeneralHelpers;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * MapReduce application for counting two features of a given Truthy index table: for each row in the index table, the application
 * outputs the row key, the total number of columns in this row (in most cases, this is the total number of tweets containing the
 * row key in some field), and the total record size of this row (after decompressed in memory).
 * <p/>
 * Usage: java iu.pti.hbaseapp.truthy.TruthyIndexFeatureCounter [index table name] [column family] [output path] [row key type]
 * 
 * @author gaoxm
 */
public class TruthyIndexFeatureCounter {
	
	/**
	 * Mapper class used by {@link #TruthyIndexFeatureCounter}.
	 */
	public static class TfcMapper extends TableMapper<Text, Text> {
		enum RowKeyType {
			STRING, TWEET_ID, USER_ID
		}
		
		RowKeyType rowKeyType = null;
		String lastRowKeyStr = null;
		long totalColCount = 0;
		long totalRecordSize = 0;
		boolean useBigInt = false;
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			long recordSize = GeneralHelpers.getHBaseResultKVSize(result);
			long colCount = result.size();
			String rowKeyStr = null;
			if (rowKeyType == RowKeyType.STRING) {
				rowKeyStr = Bytes.toString(rowKey.get());
			} else if (rowKeyType == RowKeyType.USER_ID) {
				rowKeyStr = TruthyHelpers.getUserIDStrFromBytes(rowKey.get());
			} else {
			    if (this.useBigInt) {
			        rowKeyStr = TruthyHelpers.getTweetIDStrFromBigIntBytes(rowKey.get());
			    } else {
			        rowKeyStr = TruthyHelpers.getTweetIDStrFromBytes(rowKey.get());
			    }
				
			}
			
			
			if (lastRowKeyStr == null) {
				// start counting for the first row key
				lastRowKeyStr = rowKeyStr;
				totalColCount = colCount;
				totalRecordSize = recordSize;
			} else {
				if (lastRowKeyStr.equals(rowKeyStr)) {
					// keep counting for the same row key
					totalColCount += colCount;
					totalRecordSize += recordSize;
				} else {
					// write information about last row key
					StringBuffer sb = new StringBuffer();
					sb.append(totalColCount).append('\t');
					sb.append(totalRecordSize);
					context.write(new Text(lastRowKeyStr), new Text(sb.toString()));
					
					// start counting for current row key
					lastRowKeyStr = rowKeyStr;
					totalColCount = colCount;
					totalRecordSize = recordSize;
				}
			}
		}
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			rowKeyType = RowKeyType.STRING;
			String type = conf.get("row.key.type");
			String tableName = conf.get("table.name");
			int idx = tableName.indexOf("-") + 1; 
			String month = tableName.substring(idx);
			this.useBigInt = TruthyHelpers.checkIfb4June2015(month);
			if (type != null) {
				if (type.equalsIgnoreCase("tweetId")) {
					rowKeyType = RowKeyType.TWEET_ID;
				} else if (type.equalsIgnoreCase("userId")) {
					rowKeyType = RowKeyType.USER_ID;
				}
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			if (lastRowKeyStr != null) {
				// write information about last row key
				StringBuffer sb = new StringBuffer();
				sb.append(totalColCount).append('\t');
				sb.append(totalRecordSize);
				context.write(new Text(lastRowKeyStr), new Text(sb.toString()));
			}
		}
	}
			
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		String tableName = args[0];
		String columnFamily = args[1];
		String outputPath = args[2];
		String rowKeyType = args[3];
	    conf.set("row.key.type", rowKeyType);
	    conf.set("table.name", tableName);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(columnFamily));
		scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);

		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf,	"Count the column count and indexRecordSize for each row in " + tableName);
		job.setJarByClass(TruthyIndexFeatureCounter.class);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, TfcMapper.class, Text.class, Text.class, job, true);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		TableMapReduceUtil.addDependencyJars(job);
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 4) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, required: 4");
			System.err.println("Usage: TruthyIndexFeatureCounter <table name> <column family> <output path> <row key type>");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
