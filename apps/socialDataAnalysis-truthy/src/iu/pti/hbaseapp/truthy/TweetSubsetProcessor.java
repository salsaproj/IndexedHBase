package iu.pti.hbaseapp.truthy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Application for processing a subset of tweet IDs with MapReduce.
 * <p/>
 * Usage (arguments in '{}' are optional): <br/>
 * java iu.pti.hbaseapp.truthy.TweetSubsetProcessor [input directory] [output directory] [number of reducers] [mapper class] [reducer class]
 * [map output key class] [map output value class] [compress or nocompress for output] {[additional parameters]}
 * 
 * @author gaoxm
 */
public class TweetSubsetProcessor {
	
	/**
	 * Extends the base <code>Mapper</code> class to process input files containing twitter IDs.
	 *
	 * @param <KEYOUT>  The type of the key.
	 * @param <VALUEOUT>  The type of the value.
	 * @see org.apache.hadoop.mapreduce.Mapper
	 */
	public static abstract class TwitterIdProcessMapper<KEYOUT, VALUEOUT> extends Mapper<LongWritable, Text, KEYOUT, VALUEOUT> {
	}

	protected static void usage() {
		System.out.println("Usage: java iu.pti.hbaseapp.truthy.TweetSubsetProcessor <input directory> <output directory> <number of reducers> "
				+ "<mapper class> <reducer class> <map output key class> <map output value class> <compress or nocompress for output> "
				+ "[<additonal parameters>]");
	}

	/**
	 * Create and run a MapReduce job with the given arguments to process the tweet ID files in mrInput.
	 * 
	 * @param mrInput
	 * @param mrOutput
	 * @param nReducer
	 * @param mapClassName
	 * @param reduceClassName
	 * @param mapoutKeyClassName
	 * @param mapoutValClassName
	 * @param compressOutput
	 * @param extraArgs
	 * @return
	 * @throws Exception
	 */
	public static boolean runMrProcessor(String mrInput, String mrOutput, int nReducer, String mapClassName, String reduceClassName,
			String mapoutKeyClassName, String mapoutValClassName, boolean compressOutput, String extraArgs) throws Exception {
	    System.out.println("Job2 start TimeStamp = " + System.currentTimeMillis());
		Configuration conf = HBaseConfiguration.create();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		if (compressOutput) {
			conf.set("mapred.output.compress", "true");
			conf.set("mapred.output.compression.type", "BLOCK");
			conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		}
		if (extraArgs.length() > 0) {
			conf.set("additional.arguments", extraArgs);
		}
		conf.set(ConstantsTruthy.MRJOB_INPUT_PATH, mrInput);
		conf.set(ConstantsTruthy.MRJOB_OUTPUT_PATH, mrOutput);
		conf.set(ConstantsTruthy.MRJOB_NUM_REDUCERS, Integer.toString(nReducer));
		
		Job job = Job.getInstance(conf, "tweet subset processer");
		job.setJarByClass(TweetSubsetProcessor.class);
		FileInputFormat.setInputPaths(job, new Path(mrInput));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Class.forName(mapClassName).asSubclass(TwitterIdProcessMapper.class));
		if (reduceClassName.equals("-nored")) {
			job.setNumReduceTasks(0);
		} else {
			job.setReducerClass(Class.forName(reduceClassName).asSubclass(Reducer.class));
			job.setNumReduceTasks(nReducer);
		}		
		job.setMapOutputKeyClass(Class.forName(mapoutKeyClassName));
		job.setMapOutputValueClass(Class.forName(mapoutValClassName));
		FileOutputFormat.setOutputPath(job, new Path(mrOutput));
		TableMapReduceUtil.addDependencyJars(job);
		return job.waitForCompletion(true);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			if (args.length < 7) {
				usage();
				System.exit(1);
			}
			String mrInput = args[0];
			String mrOutput = args[1];
			int nReducer = Integer.valueOf(args[2]);
			String mapClassName = args[3];
			String reduceClassName = args[4];
			String outKeyClassName = args[5];
			String outValClassName = args[6];
			boolean compressOutupt = args[7].equals("compress");
			
			StringBuilder sb = new StringBuilder();
			int i = 8;
			while (i < args.length) {
				sb.append(args[i++]).append('\n');
			}
			if (sb.length() > 0) {
				sb.deleteCharAt(sb.length() - 1);
			}
			String extraArgs = sb.toString();

			long begin = System.currentTimeMillis();
			boolean succ = runMrProcessor(mrInput, mrOutput, nReducer, mapClassName, reduceClassName, outKeyClassName, outValClassName, 
					compressOutupt, extraArgs);
			if (succ) {
				System.out.println("Please check out results in " + mrOutput);
			} else {
				System.out.println("Error occurred when running the MapReduce job.");
			}
			long end = System.currentTimeMillis();
			System.out.println("Done. Time used in secs: " + (end - begin) / 1000.0);
		} catch (Exception e) {
			usage();
			e.printStackTrace();
		}
	}
}
