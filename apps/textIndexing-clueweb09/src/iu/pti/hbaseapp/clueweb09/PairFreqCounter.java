package iu.pti.hbaseapp.clueweb09;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class PairFreqCounter {	
	/**
	 * Internal Mapper to be run by Hadoop.
	 */
	public static class PfcMapper extends TableMapper<Text, IntWritable> {
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			byte[] contentBytes = result.getValue(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES);
			String content = Bytes.toString(contentBytes);
			if (context.getConfiguration().get("content.type").toLowerCase(Locale.ENGLISH).equals("html")) {
				content = Cw09Constants.txtExtractor.htmltoText(content);
			}
						
			HashMap<String, Integer> freqs = getTermPairFreqsByLuceneAnalyzer(Constants.getLuceneAnalyzer(), content,
					Cw09Constants.INDEX_OPTION_TEXT);
			Iterator<String> iterCombs = freqs.keySet().iterator();
			int idx = -1;
			String term1 = null;
			String term2 = null;
			while (iterCombs.hasNext()) {
				String combination = iterCombs.next();
				int freq = freqs.get(combination);
				
				idx = combination.indexOf(' ');
				if (idx >= 0) {
					term1 = combination.substring(0, idx);
					term2 = combination.substring(idx+1);
					// negative numbers represent "backward co-appearances"; e.g., "bbb" appearing before "aaa"
					if (term1.compareTo(term2) > 0) {
						combination = term2 + " " + term1;
						freq = -1 * freq;
					}
				}
				context.write(new Text(combination), new IntWritable(freq));
			}
		}		
	}
	
	public static class PfcCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text wordComb, Iterable<IntWritable> freqs, Context context) 
				throws IOException, InterruptedException {
			int posFreq = 0;
			int negFreq = 0;
			for (IntWritable freq : freqs) {
				int f = freq.get();
				if (f > 0) {
					posFreq += f;
				} else {
					negFreq += f;
				}
			}
			context.write(wordComb, new IntWritable(posFreq));
			context.write(wordComb, new IntWritable(negFreq));
		}
	}
	
	public static class PfcReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
		@Override
		protected void reduce(Text wordComb, Iterable<IntWritable> freqs, Context context) 
				throws IOException, InterruptedException {
			int posFreq = 0;
			int negFreq = 0;
			for (IntWritable freq : freqs) {
				int f = freq.get();
				if (f > 0) {
					posFreq += f;
				} else {
					negFreq += f;
				}
			}
			if (posFreq != 0 && negFreq != 0) {
				byte[] combBytes = Bytes.toBytes(wordComb.toString());
				Put put = new Put(combBytes);
				put.add(Cw09Constants.CF_FREQUENCIES_BYTES, Cw09Constants.QUAL_FORWARD_BYTES, Bytes.toBytes(posFreq));
				put.add(Cw09Constants.CF_FREQUENCIES_BYTES, Cw09Constants.QUAL_BACKWARD_BYTES, Bytes.toBytes(negFreq));
				context.write(new ImmutableBytesWritable(combBytes), put);
			}
		}
	}
	
	/**
	 * get the term-pairs in a string and their frequencies by using a Lucene analyzer
	 * @param analyzer
	 * @param s
	 * @param fieldName
	 * @return
	 */
	public static HashMap<String, Integer> getTermPairFreqsByLuceneAnalyzer(Analyzer analyzer, String s, String fieldName) {
		HashMap<String, Integer> res = new HashMap<String, Integer>();
		try {
			TokenStream ts = analyzer.reusableTokenStream(fieldName, new StringReader(s));
			CharTermAttribute charTermAttr = ts.addAttribute(CharTermAttribute.class);
			
			String term1 = null;
			while (ts.incrementToken()) {
				String termVal = charTermAttr.toString();
				if (GeneralHelpers.isNumberString(termVal)) {
					continue;
				}
				
				if (term1 == null) {
					term1 = termVal;
					continue;
				}
				String combination = term1 + " " + termVal;
				if (res.containsKey(combination)) {
					res.put(combination, res.get(combination)+1);
				} else {
					res.put(combination, 1);
				}
				term1 = termVal;
			}
			ts.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}
	
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		conf.set("content.type", args[0]);
	    Scan scan = new Scan();
	    scan.addColumn(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES);
	    conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf,	"Counting word-pair frequencies from " + Cw09Constants.CLUEWEB09_DATA_TABLE_NAME);
		job.setJarByClass(PfcMapper.class);
		//TableMapReduceUtil.initTableMapperJob(Constants.CLUEWEB09_DATA_TABLE_NAME, scan, PfcMapper.class, Text.class, IntWritable.class, job);
		TableMapReduceUtil.initTableMapperJob(Cw09Constants.CLUEWEB09_DATA_TABLE_NAME, scan, PfcMapper.class, Text.class, IntWritable.class, 
												job, true, CustomizedSplitTableInputFormat.class);
		job.setCombinerClass(PfcCombiner.class);
		TableMapReduceUtil.initTableReducerJob(Cw09Constants.CLUEWEB09_PAIRFREQ_TABLE_NAME, PfcReducer.class, job);
		job.setNumReduceTasks(40);
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 1) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, minimum required: 1");
			System.err.println("Usage: PairFreqCounter <text or html content in the data table>");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
