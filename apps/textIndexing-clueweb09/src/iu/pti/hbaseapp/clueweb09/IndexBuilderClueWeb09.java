package iu.pti.hbaseapp.clueweb09;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class IndexBuilderClueWeb09 {
	/**
	 * Internal Mapper to be run by Hadoop.
	 */
	public static class IbMapper extends TableMapper<ImmutableBytesWritable, Put> {		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			byte[] contentBytes = result.getValue(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES);
			String content = Bytes.toString(contentBytes);
			if (context.getConfiguration().get("content.type").toLowerCase(Locale.ENGLISH).equals("html")) {
				content = Cw09Constants.txtExtractor.htmltoText(content);
			}
			HashMap<String, Integer> freqs = new HashMap<String, Integer>();
			HashMap<String, ByteArrayOutputStream> termPoses = GeneralHelpers.getTermFreqPosesByLuceneAnalyzer(Constants.getLuceneAnalyzer(),
					content, Cw09Constants.INDEX_OPTION_TEXT, freqs, false);
			Iterator<String> iterTerms = termPoses.keySet().iterator();
			while (iterTerms.hasNext()) {
				String term = iterTerms.next();
				
				Put put = new Put(Bytes.toBytes(term));
				put.add(Bytes.toBytes(Cw09Constants.CF_POSITIONS), rowKey.get(), termPoses.get(term).toByteArray());
				context.write(new ImmutableBytesWritable(Cw09Constants.CW09_POSVEC_TABLE_BYTES), put);
				
				Put put2 = new Put(Bytes.toBytes(term));
				put2.add(Bytes.toBytes(Cw09Constants.CF_FREQUENCIES), rowKey.get(), Bytes.toBytes(freqs.get(term)));
				context.write(new ImmutableBytesWritable(Cw09Constants.CW09_INDEX_TABLE_BYTES), put2);
			}
		}		
	}
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		conf.set("content.type", args[0]);
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
	    Scan scan = new Scan();
	    scan.addColumn(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES);
		Job job = Job.getInstance(conf,	"Building index from " + Cw09Constants.CLUEWEB09_DATA_TABLE_NAME);
		job.setJarByClass(IbMapper.class);
		TableMapReduceUtil.initTableMapperJob(Cw09Constants.CLUEWEB09_DATA_TABLE_NAME, scan, IbMapper.class, ImmutableBytesWritable.class, 
												Writable.class, job, true, CustomizedSplitTableInputFormat.class);
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		job.setNumReduceTasks(0);
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 1) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, minimum required: 1");
			System.err.println("Usage: IndexBuilderClueWeb09 <text or html content in the data table>");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
