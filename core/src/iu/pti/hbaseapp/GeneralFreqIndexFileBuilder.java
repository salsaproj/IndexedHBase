package iu.pti.hbaseapp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Given a source table containing text data in some column, this application launches a MapReduce program to
 * parallelly process the data table, and generate HFiles for a target inverted index table that uses indexed
 * words as row keys, column name as document IDs, and cell values as words' frequencies in corresponding
 * documents. The HFiles can then be bulk-loaded to the target index table. Generating HFiles directly is more
 * efficient than inserting puts to HBase tables.
 * <p/>
 * Usage: java iu.pti.hbaseapp.GeneralFreqIndexFileBuilder [text table name] [text table column family] [text table qualifier]
 * [index table name] [index column family] [HDFS output path] [intermediate compression type]
 * 
 * @author gaoxm
 */
public class GeneralFreqIndexFileBuilder {
	
	
	
	/**
	 * Mapper class used by by {@link #GeneralFreqIndexFileBuilder}.
	 */
	public static class GfifbMapper extends TableMapper<KeyValueComparable, NullWritable> {
		static byte[] textTableCfBytes = null;
		static byte[] textTableQualBytes = null;
		static byte[] indexTableCfBytes = null;
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			byte[] contentBytes = result.getValue(textTableCfBytes, textTableQualBytes);
			String content = Bytes.toString(contentBytes);
			
			HashMap<String, Integer> freqs = GeneralHelpers.getTermFreqsByLuceneAnalyzer(Constants.getLuceneAnalyzer(), content, Constants.DUMMY_FIELD_NAME);
			for (Map.Entry<String, Integer> e : freqs.entrySet()) {
				String term = e.getKey();
				KeyValueComparable kv = new KeyValueComparable(Bytes.toBytes(term), indexTableCfBytes, rowKey.get(), 
																Bytes.toBytes(e.getValue()));
				context.write(kv, NullWritable.get());
			}
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			if (textTableCfBytes == null) {
				Configuration conf = context.getConfiguration();
				textTableCfBytes = Bytes.toBytes(conf.get("text.table.column.family"));
				textTableQualBytes = Bytes.toBytes(conf.get("text.table.qualifier"));
				indexTableCfBytes = Bytes.toBytes(conf.get("index.table.column.family"));
			}
		}
	}
	
	/**
	 * Reducer class used by by {@link #GeneralFreqIndexFileBuilder}.
	 */
	public static class GfifbReducer extends Reducer<KeyValueComparable, NullWritable, ImmutableBytesWritable, KeyValue> {
		@Override
		protected void reduce(KeyValueComparable kv, Iterable<NullWritable> nulls, Context context) 
				throws IOException, InterruptedException {
			context.write(new ImmutableBytesWritable(kv.getKeyValue().getRow()), kv.getKeyValue());
		}
	}
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws Exception {
		String textTableName = args[0];
		String textTableCf = args[1];
		String textTableQual = args[2];
		String indexTableName = args[3];
		String indexTableCf = args[4];
		String outputDir = args[5];
		String compression = args[6];
		
		conf.set("text.table.column.family", textTableCf);
		conf.set("text.table.qualifier", textTableQual);
		conf.set("index.table.column.family", indexTableCf);
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		if (!compression.equals("none")) {
			conf.set("mapred.compress.map.output", "true");
			conf.set("mapred.map.output.compression.type", "BLOCK");
			if (compression.equals("gzip")) {
				conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
			} else if (compression.equals("snappy")) {
				conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
			} else if (compression.equals("bzip2")) {
				conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
			} 
		}
	    Scan scan = new Scan();
	    scan.addColumn(Bytes.toBytes(textTableCf), Bytes.toBytes(textTableQual));
		Job job = Job.getInstance(conf,	"Building freq_index from " + textTableName);
		conf = job.getConfiguration();
		job.setJarByClass(GfifbMapper.class);
		TableMapReduceUtil.initTableMapperJob(textTableName, scan, GfifbMapper.class, KeyValueComparable.class, 
												NullWritable.class, job, true);
		job.setReducerClass(GfifbReducer.class);
		Path outputPath = new Path(outputDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		Class<? extends Partitioner<?, ?>> topClass = null;
	    try {
	    	topClass = (Class<? extends Partitioner<?, ?>>)Class.forName("org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner");
	    } catch (ClassNotFoundException e) {
	    	topClass = (Class<? extends Partitioner<?, ?>>)Class.forName("org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner");
	    }
		job.setPartitionerClass(topClass);		
		job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(KeyValue.class);
	    job.setOutputFormatClass(HFileOutputFormat.class);
	    job.setReducerClass(GfifbReducer.class);
	    
	    // write start keys for reducers to distributed cache, which will be used by TotalOrderPartitioner
		HTable table = new HTable(conf, indexTableName);
		byte[][] byteKeys = table.getStartKeys();
		ArrayList<ImmutableBytesWritable> startKeys = new ArrayList<ImmutableBytesWritable>(byteKeys.length);
		for (byte[] byteKey : byteKeys) {
			startKeys.add(new ImmutableBytesWritable(byteKey));
		}
		job.setNumReduceTasks(startKeys.size());
		Path partitionsPath = new Path(job.getWorkingDirectory(), "partitions_" + UUID.randomUUID());
		FileSystem fs = partitionsPath.getFileSystem(conf);
		TreeSet<ImmutableBytesWritable> sorted = new TreeSet<ImmutableBytesWritable>(startKeys);
		ImmutableBytesWritable first = sorted.first();
		if (!first.equals(HConstants.EMPTY_BYTE_ARRAY)) {
			table.close();
			throw new IllegalArgumentException("First region of table should have empty start key. Instead has: " 
												+ Bytes.toStringBinary(first.get()));
		}
		sorted.remove(first);
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(partitionsPath), Writer.keyClass(KeyValueComparable.class),
				Writer.valueClass(NullWritable.class));
		try {
			for (ImmutableBytesWritable startKey : sorted) {
				writer.append(new KeyValueComparable(startKey.get(), null, null, null), NullWritable.get());
			}
		} finally {
			writer.close();
		}		
		partitionsPath = fs.makeQualified(partitionsPath);

		URI cacheUri;
		try {
			cacheUri = new URI(partitionsPath.toString() + "#" + TotalOrderPartitioner.DEFAULT_PATH);
		} catch (URISyntaxException e) {
			table.close();
			throw new IOException(e);
		}
	    DistributedCache.addCacheFile(cacheUri, conf);
	    DistributedCache.createSymlink(conf);
		
	    // configure compression
	    StringBuilder compressionConfigValue = new StringBuilder();
	    HTableDescriptor tableDescriptor = table.getTableDescriptor();
	    Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
	    int i = 0;
		for (HColumnDescriptor familyDescriptor : families) {
			if (i++ > 0) {
				compressionConfigValue.append('&');
			}
			compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString(), "UTF-8"));
			compressionConfigValue.append('=');
			compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getCompression().getName(), "UTF-8"));
		}
	    conf.set("hbase.hfileoutputformat.families.compression", compressionConfigValue.toString());
	    TableMapReduceUtil.addDependencyJars(job);
	    
		System.out.println("num of reducers for GeneralFreqIndexFileBuilder job: " + job.getNumReduceTasks() + " compression type: "
							+ job.getConfiguration().get("mapred.map.output.compression.codec"));
		
		table.close();
		return job;
	}
	
	/**
	 * Usage: GeneralFreqIndexFileBuilder [text table name] [text table column family] [text table qualifier]
	 * [index table name] [index column family] [HDFS output path] [intermediate compression type]
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 7) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, minimum required: 7");
			System.err.println("Usage: GeneralFreqIndexFileBuilder <text table name> <text table column family> <text table qualifier>"
								+ " <index table name> <index column family> <HDFS output path> <intermediate compression type>");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
