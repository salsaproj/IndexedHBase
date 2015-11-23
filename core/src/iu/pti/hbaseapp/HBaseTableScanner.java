package iu.pti.hbaseapp;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
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
 * This application launches a MapReduce program to scan a given HBase table, and write the content to output 
 * files in a human readable format. 4 data types are supported for the row key, column family, qualifier, and 
 * cell value: string, int, double, long.
 * <p/>
 * Usage (arguments in '{}' are optional): java iu.pti.hbaseapp.HBaseTableScanner [table name] [column family 
 * name] [row type] {[cf type] [qualifier type] [value type] [number of rows to read]}
 * 
 * @author gaoxm
 */
public class HBaseTableScanner {
	
	public static class TsMapper extends TableMapper<Text, Text> {
		protected static Constants.DataType rowDataType = Constants.DataType.UNKNOWN;
		protected static Constants.DataType cfDataType = Constants.DataType.UNKNOWN;
		protected static Constants.DataType qualDataType = Constants.DataType.UNKNOWN;
		protected static Constants.DataType valDataType = Constants.DataType.UNKNOWN;
		protected static byte[] cfBytes = null;
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			// don't do anything if row type is unknown
			if (rowDataType == Constants.DataType.UNKNOWN) {
				return;
			}
			
			String row = "";
			switch (rowDataType) {
			case INT:
				row = Integer.toString(Bytes.toInt(rowKey.get()));
				break;
			case STRING:
				row = Bytes.toString(rowKey.get());
				break;
			case DOUBLE:
				row = Double.toString(Bytes.toDouble(rowKey.get()));
			default:
			}
			
			if (cfBytes == null) {
				context.write(new Text(row), new Text(""));
			} else {
				StringBuffer sb = new StringBuffer();
				List<KeyValue> lkv = result.list();
				Iterator<KeyValue> iter = lkv.iterator();
				while (iter.hasNext()) {
					KeyValue kv = iter.next();
					switch (valDataType) {
					case INT:
						sb.append(Bytes.toInt(kv.getValue()));
						break;
					case STRING:
						sb.append(Bytes.toString(kv.getValue()));
						break;
					case DOUBLE:
						sb.append(Bytes.toDouble(kv.getValue()));
					default:
					}
					sb.append('\t');
				}
				context.write(new Text(row), new Text(sb.toString()));
			}
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			if (rowDataType != Constants.DataType.UNKNOWN) {
				return;
			}
			
			Configuration configuration = context.getConfiguration();
			String rowType = configuration.get("row.type");
			String cfName = configuration.get("column.family.name");
			String cfType = configuration.get("column.family.type");
			String qualType = configuration.get("qualifier.type");
			String valType = configuration.get("value.type");
			
			if (rowType != null) {
				if (rowType.equals("int")) {
					rowDataType = Constants.DataType.INT;
				} else if (rowType.equals("string")) {
					rowDataType = Constants.DataType.STRING;
				} else if (rowType.equals("double")) {
					rowDataType = Constants.DataType.DOUBLE;
				}
			}
			
			if (cfType != null) {
				if (cfType.equals("int")) {
					cfDataType = Constants.DataType.INT;
					cfBytes = Bytes.toBytes(Integer.valueOf(cfName));
				} else if (cfType.equals("string")) {
					cfDataType = Constants.DataType.STRING;
					cfBytes = Bytes.toBytes(cfName);
				} else if (cfType.equals("double")) {
					cfDataType = Constants.DataType.DOUBLE;
					cfBytes = Bytes.toBytes(Double.valueOf(cfName));
				}
			}
			
			if (qualType != null) {
				if (qualType.equals("int")) {
					qualDataType = Constants.DataType.INT;
				} else if (qualType.equals("string")) {
					qualDataType = Constants.DataType.STRING;
				} else if (qualType.equals("double")) {
					qualDataType = Constants.DataType.DOUBLE;
				}
			}
			
			if (valType != null) {
				if (valType.equals("int")) {
					valDataType = Constants.DataType.INT;
				} else if (valType.equals("string")) {
					valDataType = Constants.DataType.STRING;
				} else if (valType.equals("double")) {
					valDataType = Constants.DataType.DOUBLE;
				}
			}
		}
	}
	
	/**
	 * Job configuration.
	 */
	protected static Job configureJob(Configuration conf, String[] args) throws IOException {		
		String outputPath = args[0];
		String tableName = args[1];
		String rowType = args[2].toLowerCase();
		conf.set("row.type", rowType);
		String cfName = null;
		String cfType = null;
		String qualType = null;
		String valType = null;
		if (args.length >= 7) {
			cfName = args[3];
			cfType = args[4].toLowerCase();
			qualType = args[5].toLowerCase();
			valType = args[6].toLowerCase();
			conf.set("column.family.name", cfName);
			conf.set("column.family.type", cfType);
			conf.set("qualifier.type", qualType);
			conf.set("value.type", valType);
		}
		
		Scan scan = new Scan();
		if (cfType != null) {
			if (cfType.equals("int")) {
				scan.addFamily(Bytes.toBytes(Integer.valueOf(cfName)));
			} else if (cfType.equals("string")) {
				scan.addFamily(Bytes.toBytes(cfName));
			} else if (cfType.equals("double")) {
				scan.addFamily(Bytes.toBytes(Double.valueOf(cfName)));
			}
		}
		
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf,	"Scan the content of table " + tableName + " to a text file.");
		job.setJarByClass(HBaseTableScanner.class);
		//TableMapReduceUtil.initTableMapperJob(tableName, scan, TsMapper.class, ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, TsMapper.class, ImmutableBytesWritable.class, ImmutableBytesWritable.class,
												job, true);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}
		
	protected static void usage() {
		System.out.println("Usage: HBaseTableScanner <output path> <table name> <row type> [<column family name> <cf type> "
				+ "<qualifier type> <value type>]");
	}

	/**
	 * Usage (arguments in '{}' are optional): java iu.pti.hbaseapp.HBaseTableScanner [table name] [column family 
	 * name] [row type] {[cf type] [qualifier type] [value type] [number of rows to read]}
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			usage();
			System.exit(1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
