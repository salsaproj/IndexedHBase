package iu.pti.hbaseapp.clueweb09;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;
import iu.pti.hbaseapp.KeyValueComparable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * An improved index file builder that generates HFiles for index tables. It relies on the Hadoop MapReduce framework to 
 * sort KeyValue objects and uses gzip to compress the intermediate data. 
 * @author gaoxm
 *
 */
public class FreqIndexFileBuilder2Cw09 {
	
	/**
	 * Internal Mapper to be run by Hadoop.
	 */
	public static class FibfsMapper extends TableMapper<KeyValueComparable, NullWritable> {	
		int sectionNumber = 0;
		int sectionCount = 0;
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			int docNum = Bytes.toInt(rowKey.get());
			if ((docNum % sectionCount) != sectionNumber) {
				return;
			}
			
			byte[] contentBytes = result.getValue(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES);
			String content = Bytes.toString(contentBytes);
			if (context.getConfiguration().get("content.type").toLowerCase(Locale.ENGLISH).equals("html")) {
				content = Cw09Constants.txtExtractor.htmltoText(content);
			}
			
			HashMap<String, Integer> freqs = GeneralHelpers.getTermFreqsByLuceneAnalyzer(Constants.getLuceneAnalyzer(), 
					content, Cw09Constants.INDEX_OPTION_TEXT);
			for (Map.Entry<String, Integer> e : freqs.entrySet()) {
				String term = e.getKey();
				KeyValueComparable kv = new KeyValueComparable(Bytes.toBytes(term), Cw09Constants.CF_FREQUENCIES_BYTES, rowKey.get(), 
																Bytes.toBytes(e.getValue()));
				context.write(kv, NullWritable.get());
			}
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			sectionNumber = Integer.valueOf(conf.get("section.number"));
			sectionCount = Integer.valueOf(conf.get("section.count"));
			Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
			for (Path p : localFiles) {
				System.out.println(p.toUri());
			}
		}
	}
	
	public static class FibfsReducer extends Reducer<KeyValueComparable, NullWritable, ImmutableBytesWritable, KeyValue> {
		@Override
		protected void reduce(KeyValueComparable kv, Iterable<NullWritable> nulls, Context context) 
				throws IOException, InterruptedException {
			context.write(new ImmutableBytesWritable(kv.getKeyValue().getValue()), kv.getKeyValue());
		}
	}
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws Exception {
		conf.set("content.type", args[0]);
		String outputDir = args[1];
		conf.set("section.number", args[2]);
		conf.set("section.count", args[3]);
		String compression = args[4].toLowerCase(Locale.ENGLISH);
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
	    scan.addColumn(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES);
		Job job = Job.getInstance(conf,	"Building freq_index from " + Cw09Constants.CLUEWEB09_DATA_TABLE_NAME);
		conf = job.getConfiguration();
		job.setJarByClass(FibfsMapper.class);
		TableMapReduceUtil.initTableMapperJob(Cw09Constants.CLUEWEB09_DATA_TABLE_NAME, scan, FibfsMapper.class, KeyValueComparable.class, 
												NullWritable.class, job, true, CustomizedSplitTableInputFormat.class);
		job.setReducerClass(FibfsReducer.class);
		Path outputPath = new Path(outputDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		Class<? extends Partitioner> topClass = null;
	    try {
	    	topClass = (Class<? extends Partitioner>) Class.forName("org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner");
	    } catch (ClassNotFoundException e) {
	    	topClass = (Class<? extends Partitioner>) Class.forName("org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner");
	    }
		job.setPartitionerClass(topClass);		
		job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(KeyValue.class);
	    job.setOutputFormatClass(HFileOutputFormat.class);
	    job.setReducerClass(FibfsReducer.class);
	    
	    // write start keys for reducers to distributed cache, which will be used by TotalOrderPartitioner
		HTable table = new HTable(conf, Cw09Constants.CLUEWEB09_INDEX_TABLE_NAME);
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
	    
		System.out.println("num of reducers for FreqIndexFileBuilder2 job: " + job.getNumReduceTasks() + ", section number: " + args[2]
							+ " section count: " + args[3] + " compression type: " + job.getConfiguration().get("mapred.map.output.compression.codec"));
		table.close();
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 5) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, minimum required: 2");
			System.err.println("Usage: FreqIndexFileBuilder2Cw09 <text or html content in the data table> <HDFS output path>"
								+ " <section number> <section count> <compression type>");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
