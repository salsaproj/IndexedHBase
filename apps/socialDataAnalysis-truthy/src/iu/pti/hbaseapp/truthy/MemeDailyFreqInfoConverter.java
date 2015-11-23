package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;

import java.io.IOException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * A MapReduce application for converting the output files of {@link #DailyMemeFreqProcessor} by a number of different ways.
 * Give a line in the format of [hashtag, a list of (date : frequency)] as input, it converts it into one or more lines
 * in the format of [number, hashtag, a list of (date : frequency)]. The content of each line depends on the type of conversion.
 * <p/>
 * If the conversion is done by each hashtag's appearance in consecutive lifetime units defined in the number of days (e.g. a lifetime 
 * unit can be defined as 7 days to measure a hashtag's appearance in consecutive weeks),  then "number" is the number of consecutive
 * life time units when the hashtag's frequency is > 0, and "list of (date : frequency)" is the daily frequency of the hashtag
 * during those consecutive lifetime units.
 * <p/>
 * If the conversion is done by each hashtag's sum of the length of all consecutive lifetime units, then "number" is the sum of the length
 * of all consecutive lifetime units of a hashtag, and "list of (date : frequency)" is the whole list of daily frequency of the hashtag.
 * E.g. if "#abc" first appear in 4 consecutive weeks during March and April in 2012, and then in another 3 consecutive weeks during
 * June and July in 2012, then "number" will be 7.
 * <p/>
 * If the conversion is done by each hashtag's lifespan (the total time between its first appearance and last appearance) in lifetime units,
 * then "number" will be (lifespan in days) / (length of a lifetime unit in days).
 * <p/>
 * If the conversion is done by each hashtag's lifespan in days, then "number" will be its lifespan in days.
 * <p/>
 * If the conversion is done by each hashtag's total frequency, then "number" will be its total frequency.
 * 
 * <p/>
 * Usage: java iu.pti.hbaseapp.truthy.MemeWeekLifeSorter [input directory] [output directory] [by what and additional parameters]
 * 
 * @author gaoxm
 */
public class MemeDailyFreqInfoConverter {
	public static class MdfcMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		enum ConvertByWhat {
			CONS_LTUNITS, TOTAL_LTUNITS, LIFESPAN_LTUNITS, LIFESPAN_DAYS, TOTAL_FREQ
		}
		ConvertByWhat convertBy;
		Calendar calTmp;
		long ltUnitMilli = -1;
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException {
			try {
				String line = value.toString().trim();
				switch (convertBy) {
				case CONS_LTUNITS:
					processLineByConsLtUnits(line, context);
					break;
				default:
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new IOException(e);
			}
		}
		
		/**
		 * Process an input line and output key-value pairs in the form of [numConsLtUnits, hashtagInfo], where "hashtagInfo" is in the form of
		 * <br/> [hashtag] [date|frequency] [date|frequency] ...
		 * <br/> [hashtag] is separated from the rest with a '\t'. The list of [date|frequency] only covers the lifetime units counted towards
		 * this "numConsLtUnits". 
		 * 
		 * @param line
		 * @param context
		 * @throws Exception
		 */
		protected void processLineByConsLtUnits(String line, Context context) throws Exception {
			// a line is in the format of "#euro2012    2012-06-08|345 2012-06-09|559 2012-06-10|9987 ..."
			int idx = line.indexOf('\t');
			if (idx < 0) {
				return;
			}
			
			String hashtag = line.substring(0, idx);
			String[] dateFreqs = line.substring(idx + 1).split(" ");
			if (dateFreqs.length < 2) {
				context.write(new LongWritable(1L), new Text(line));
				return;
			}
			long consStartTime = -1;
			long lastLtUnitNum = -1;
			LinkedList<String> consLtUnitsDateFreqList = new LinkedList<String>();
			long thisDateTime = -1;
			long thisLtUnitNum = -1;
			StringBuilder sb = new StringBuilder();
			sb.append(hashtag).append('\t');
			for (String dateFreqStr : dateFreqs) {
				idx = dateFreqStr.indexOf('|');
				String date = dateFreqStr.substring(0, idx);
				GeneralHelpers.setDateByString(calTmp, date);
				thisDateTime = calTmp.getTimeInMillis();
				if (consStartTime < 0) {
					consStartTime = thisDateTime;
					lastLtUnitNum = 1;
					consLtUnitsDateFreqList.add(dateFreqStr);
				} else {
					thisLtUnitNum = (thisDateTime - consStartTime) / ltUnitMilli + 1;
					if (thisLtUnitNum <= lastLtUnitNum + 1) {
						// still in consecutive lifetime units
						lastLtUnitNum = thisLtUnitNum;
						consLtUnitsDateFreqList.add(dateFreqStr);
					} else {
						// not in a consecutive lifetime unit. We need to output the last consecutive lifetime unit's information, 
						// and restart counting
						if (lastLtUnitNum > 0) {
							sb.setLength(hashtag.length() + 1);
							for (String s : consLtUnitsDateFreqList) {
								sb.append(s).append(' ');
							}
							sb.deleteCharAt(sb.length() - 1);

							context.write(new LongWritable(lastLtUnitNum), new Text(sb.toString()));
						}
						
						consStartTime = thisDateTime;
						lastLtUnitNum = 1;
						consLtUnitsDateFreqList.clear();
						consLtUnitsDateFreqList.add(dateFreqStr);
					}
				}
			}
			
			if (lastLtUnitNum > 0) {
				sb.setLength(hashtag.length() + 1);
				for (String s : consLtUnitsDateFreqList) {
					sb.append(s).append(' ');
				}
				sb.deleteCharAt(sb.length() - 1);
				context.write(new LongWritable(lastLtUnitNum), new Text(sb.toString()));
			}
		}
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String convertOpt = conf.get("sort.by.what");
			if (convertOpt.equalsIgnoreCase("consecutiveLifetimeUnits")) {
				convertBy = ConvertByWhat.CONS_LTUNITS;
				ltUnitMilli = 86400000 * Integer.valueOf(conf.get("lifetime.unit.days"));
			} else if (convertOpt.equalsIgnoreCase("totalLifetimeUnits")) {
				convertBy = ConvertByWhat.TOTAL_LTUNITS;
				ltUnitMilli = 86400000 * Integer.valueOf(conf.get("lifetime.unit.days"));
			} else if (convertOpt.equalsIgnoreCase("lifespanInLifetimeUnits")) {
				convertBy = ConvertByWhat.LIFESPAN_LTUNITS;
				ltUnitMilli = 86400000 * Integer.valueOf(conf.get("lifetime.unit.days"));
			} else if (convertOpt.equalsIgnoreCase("lifespanInDays")) {
				convertBy = ConvertByWhat.LIFESPAN_DAYS;
			} else if (convertOpt.equalsIgnoreCase("totalFrequency")) {
				convertBy = ConvertByWhat.TOTAL_FREQ;
			} else {
				throw new IOException("Invalid sort option: " + convertOpt);
			}
			calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		}
	}
	
	public static Job configureJob(Configuration conf, String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		String sortByWhat = args[2];
		conf.set("sort.by.what", sortByWhat);
		if (sortByWhat.equalsIgnoreCase("consecutiveLifetimeUnits") || sortByWhat.equalsIgnoreCase("totalLifetimeUnits")
				|| sortByWhat.equalsIgnoreCase("lifespanInLifetimeUnits")) {
			conf.set("lifetime.unit.days", args[3]);
		}
		
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		conf.set("mapred.output.compress", "true");
		conf.set("mapred.output.compression.type", "BLOCK");
		conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		Job job = Job.getInstance(conf, "Truthy meme daily frequency list converter");
		job.setJarByClass(MdfcMapper.class);
		FileInputFormat.setInputPaths(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MdfcMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}
	
	public static void usage() {
		System.out.println("Usage: java iu.pti.hbaseapp.truthy.MemeDailyFreqInfoConverter <input directory> <output directory> "
				+ "<by what and additional parameters>");
		System.out.println("  Where <by what and additional parameters> can be one of the following:");
		System.out.println("  consecutiveLifetimeUnits <lifetime unit length in days>");
		System.out.println("  totalLifetimeUnits <lifetime unit length in days>");
		System.out.println("  lifespanInLifetimeUnits <lifetime unit length in days>");
		System.out.println("  lifespanInDays");
		System.out.println("  totalFrequency");
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.out.println("Wrong number of arguments: " + otherArgs.length);
			usage();
			System.exit(-1);
		}
		
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
