package iu.pti.hbaseapp.clueweb09;

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

public class SynonymScanner {
	
	public static class SsMapper extends TableMapper<Text, Text> {
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			String termComb = Bytes.toString(rowKey.get());
			double plusScore = Bytes.toDouble(result.getValue(Cw09Constants.CF_SCORES_BYTES, Cw09Constants.QUAL_PLUS_BYTES));
			double minScore = Bytes.toDouble(result.getValue(Cw09Constants.CF_SCORES_BYTES, Cw09Constants.QUAL_MIN_BYTES));
			int forwardCount = Bytes.toInt(result.getValue(Cw09Constants.CF_SCORES_BYTES, Cw09Constants.QUAL_FORWARD_BYTES));
			int backwardCount = Bytes.toInt(result.getValue(Cw09Constants.CF_SCORES_BYTES, Cw09Constants.QUAL_BACKWARD_BYTES));
			long term1Count = Bytes.toLong(result.getValue(Cw09Constants.CF_SCORES_BYTES, Cw09Constants.QUAL_TERM1COUNT_BYTES));
			long term2Count = Bytes.toLong(result.getValue(Cw09Constants.CF_SCORES_BYTES, Cw09Constants.QUAL_TERM2COUNT_BYTES));
						
			StringBuffer sb = new StringBuffer();
			sb.append(plusScore).append('\t');
			sb.append(minScore).append('\t');
			sb.append(forwardCount).append('\t');
			sb.append(backwardCount).append('\t');
			sb.append(term1Count).append('\t');
			sb.append(term2Count).append('\t');
			
			context.write(new Text(termComb), new Text(sb.toString()));
		}
	}
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		String outputPath = args[0];
		Scan scan = new Scan();
		scan.addFamily(Cw09Constants.CF_SCORES_BYTES);

		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf,	"Scan the synonym scores in " + Cw09Constants.CLUEWEB09_SYNONYM_TABLE_NAME + " to a text file.");
		job.setJarByClass(SynonymScanner.class);
		//TableMapReduceUtil.initTableMapperJob(Constants.CLUEWEB09_SYNONYM_TABLE_NAME, scan, SsMapper.class, ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
		TableMapReduceUtil.initTableMapperJob(Cw09Constants.CLUEWEB09_SYNONYM_TABLE_NAME, scan, SsMapper.class, ImmutableBytesWritable.class, 
												ImmutableBytesWritable.class, job, true, CustomizedSplitTableInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 1) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, required: 1");
			System.err.println("Usage: SynonymScanner <output path>");
			System.exit(1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
