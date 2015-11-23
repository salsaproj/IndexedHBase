package iu.pti.hbaseapp.clueweb09;

import java.io.IOException;
import java.util.Locale;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ScanSearcherClueWeb09 {
	/**
	 * Internal Mapper to be run by Hadoop.
	 */
	public static class SwsMapper extends TableMapper<IntWritable, Text> {
		String word = "";
		boolean getContent = false;
		boolean isTextContent = false;
		Text emptyText = new Text("");
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			byte[] contentBytes = result.getValue(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES);
			String content = Bytes.toString(contentBytes);
			if (!isTextContent) {
				content = Cw09Constants.txtExtractor.htmltoText(content);
			}
			content = content.toLowerCase(Locale.ENGLISH);
			
			String word1 = word;
			String word2 = "";
			int idx = word.indexOf(',');
			if (idx >= 0) {
				word1 = word.substring(0, idx);
				word2 = word.substring(idx + 1);
			}
			
			boolean word1Found = false;
			idx = content.indexOf(word1);
			while (idx >= 0) {
				int idxBefore = idx -1;
				int idxAfter = idx + word1.length();
				boolean letterBefore = idxBefore >= 0 && Character.isLetter(content.charAt(idxBefore));
				boolean letterAfter = idxAfter < content.length() && Character.isLetter(content.charAt(idxAfter));
				if (letterBefore || letterAfter) {
					if (idxAfter < content.length()) {
						idx = content.indexOf(word1, idxAfter);
					} else {
						break;
					}
				} else {
					word1Found = true;
					break;
				}
			}
			
			if (word1Found && content.contains(word2)) {
				IntWritable docId = new IntWritable(Bytes.toInt(rowKey.get()));
				// we only print doc ids since printing content to output will take too much disk space
				context.write(docId, emptyText);
			}
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();
			isTextContent = configuration.get("content.type").equals("text");
			word = configuration.get("word");
			getContent = configuration.getBoolean("get.content", false);
		}		
	}
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		conf.set("content.type", args[0]);
		conf.set("word", args[1]);
		conf.set("get.content", args[2]);
		String outputPath = args[3];
	    Scan scan = new Scan();
	    scan.addColumn(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES);
		Job job = Job.getInstance(conf,	"Searching " + args[1] + " from " + Cw09Constants.CLUEWEB09_DATA_TABLE_NAME);
		job.setJarByClass(ScanSearcherClueWeb09.class);
		TableMapReduceUtil.initTableMapperJob(Cw09Constants.CLUEWEB09_DATA_TABLE_NAME, scan, SwsMapper.class, IntWritable.class, 
												Text.class, job, true, CustomizedSplitTableInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 4) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, minimum required: 1");
			System.err.println("Usage: ScanSearcherClueWeb09 <text or html content in the data table> <word> " + 
								"<true or false for getting document contents> <output path>");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
