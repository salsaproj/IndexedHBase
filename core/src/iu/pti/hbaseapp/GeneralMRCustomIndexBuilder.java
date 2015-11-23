package iu.pti.hbaseapp;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * MapReduce application for building index tables using an existing source table, following the rules given in a
 * custom index configuration file.
 * <p/>
 * Usage (arguments in '{}' are optional): <br/>
 * java iu.pti.hbaseapp.GeneralMRCustomIndexBuilder [path to XML config] [source table name] {[index table name]}
 * 
 * @author gaoxm
 */
public class GeneralMRCustomIndexBuilder {
	/**
	 * Mapper class used by {@link #GeneralMRCustomIndexBuilder}.
	 */
	public static class GcibMapper extends TableMapper<ImmutableBytesWritable, Put> {
		protected String indexConfigPath = null;
		protected String sourceTableName = null;
		protected String indexTableName = null;
		GeneralCustomIndexer gci = null;
		boolean firstPut = true;
		long rowCount = 0;
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			try {
				byte[] rowBytes = rowKey.get();
				Put sourcePut = new Put(rowBytes);
				KeyValue[] kvs = result.raw();
				for (KeyValue kv : kvs) {
					sourcePut.add(kv);
				}

				if (indexTableName == null) {
					gci.index(sourceTableName, sourcePut, context);
				} else {
					Map<byte[], List<Put>> indexPutsMap = gci.index(sourceTableName, sourcePut);
					for (Map.Entry<byte[], List<Put>> e : indexPutsMap.entrySet()) {
						List<Put> indexPuts = e.getValue();
						for (Put p : indexPuts) {
							context.write(new ImmutableBytesWritable(p.getRow()), p);
						}
					}
				}
				if (rowCount % 10000 == 0) {
					context.setStatus("processed " + rowCount + " rows in the source table");
				}
				rowCount++;
			} catch (Exception e) {
				e.printStackTrace();
				context.setStatus("Exception when indexing row key " + GeneralHelpers.makeStringByEachByte(rowKey.get()) + ": " + e.getMessage());
			}
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			try {
				Configuration conf = context.getConfiguration();
				indexConfigPath = conf.get("index.config.path");
				indexTableName = conf.get("index.table.name");
				sourceTableName = conf.get("source.table.name");
				gci = new GeneralCustomIndexer(indexConfigPath, indexTableName);
			} catch (Exception e) {
				e.printStackTrace();
				throw new InterruptedException(e.getMessage());
			}
		}
	}

	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		String indexConfigPath = args[0];
		String sourceTableName = args[1];
		String indexTableName = null;
		if (args.length > 2) {
			indexTableName = args[2];
		}
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		conf.set("index.config.path", indexConfigPath);
		conf.set("source.table.name", sourceTableName);
		if (indexTableName != null) {
			conf.set("index.table.name", indexTableName);
		}
	    Scan scan = new Scan();
	    
		Job job = Job.getInstance(conf,	"Building index from " + sourceTableName);
		job.setJarByClass(GeneralMRCustomIndexBuilder.class);
		
		TableMapReduceUtil.initTableMapperJob(sourceTableName, scan, GcibMapper.class, ImmutableBytesWritable.class, 
												Writable.class, job, true);
		if (indexTableName == null) {
			job.setOutputFormatClass(MultiTableOutputFormat.class);
		} else {
			TableMapReduceUtil.initTableReducerJob(indexTableName, null, job);
		}
		job.setNumReduceTasks(0);
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, minimum required: 2");
			System.err.println("Usage: java iu.pti.hbaseapp.GeneralMRCustomIndexBuilder <path to XML config> <source table name> "
					+ "[<index table name>]");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
