package iu.pti.hbaseapp.diglib;

import java.io.IOException;

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

/**
 * This application scans an index table with multiple mappers in parallel, and write the total number of document
 * count of each term to the "termDocCountTable".
 * <p/>
 * Usage: java iu.pti.hbaseapp.diglib.TermDocCounter [index table name] [column family name]
 * 
 * @author gaoxm
 *
 */
public class TermDocCounter {
	/**
	 * Mapper class used by {@link #TermDocCounter}.
	 */
	public static class TdcMapper extends TableMapper<ImmutableBytesWritable, Put> {
		protected static enum IndexTableTypes {
			TEXTS, KEYWORDS, AUTHORS, CATEGORY, TITLE, PUBLISHERS, LOCATION, ADDINFO, NONE
		}
		protected static IndexTableTypes indexTableType = IndexTableTypes.NONE;
		protected static byte[] columnFamilyBytes = null;
		protected static byte[] termDocCountTableBytes = Bytes.toBytes(DigLibConstants.TERM_DOC_COUNT_TABLE_NAME);
		protected static byte[] cfDocCounts = Bytes.toBytes(DigLibConstants.CF_DOC_COUNTS);
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			//rowKey is the bytes representation of term values
			Put put = new Put(rowKey.get());
			byte[] qualBytes = Bytes.toBytes("None");
			switch (indexTableType) {
			case TEXTS:
				qualBytes = Bytes.toBytes("Text");
				break;
			case KEYWORDS:
				qualBytes = Bytes.toBytes(DigLibConstants.QUALIFIER_KEYWORDS);
				break;
			case AUTHORS:
				qualBytes = Bytes.toBytes(DigLibConstants.QUALIFIER_AUTHORS);
				break;
			case CATEGORY:
				qualBytes = Bytes.toBytes(DigLibConstants.QUALIFIER_CATEGORY);
				break;
			case TITLE:
				qualBytes = Bytes.toBytes(DigLibConstants.QUALIFIER_TITLE);
				break;
			case PUBLISHERS:
				qualBytes = Bytes.toBytes(DigLibConstants.QUALIFIER_PUBLISHERS);
				break;
			case LOCATION:
				qualBytes = Bytes.toBytes(DigLibConstants.QUALIFIER_LOCATION);
				break;
			case ADDINFO:
				qualBytes = Bytes.toBytes(DigLibConstants.QUALIFIER_ADDITIONAL);
			default:
			}
			put.add(cfDocCounts, qualBytes, Bytes.toBytes(result.list().size()));
			context.write(new ImmutableBytesWritable(termDocCountTableBytes), put);
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();
			if (columnFamilyBytes == null) {
				columnFamilyBytes = Bytes.toBytes(configuration.get("column.family.name"));
			}
			if (indexTableType == IndexTableTypes.NONE) {
				String tableName = configuration.get("index.table.name");
				if (tableName.equals(DigLibConstants.TEXTS_INDEX_TABLE_NAME)) {
					indexTableType = IndexTableTypes.TEXTS;
				} else if (tableName.equals(DigLibConstants.KEYWORDS_INDEX_TABLE_NAME)) {
					indexTableType = IndexTableTypes.KEYWORDS;
				} else if (tableName.equals(DigLibConstants.AUTHORS_INDEX_TABLE_NAME)) {
					indexTableType = IndexTableTypes.AUTHORS;
				} else if (tableName.equals(DigLibConstants.CATEGORY_INDEX_TABLE_NAME)) {
					indexTableType = IndexTableTypes.CATEGORY;
				} else if (tableName.equals(DigLibConstants.TITLE_INDEX_TABLE_NAME)) {
					indexTableType = IndexTableTypes.TITLE;
				} else if (tableName.equals(DigLibConstants.PUBLISHERS_INDEX_TABLE_NAME)) {
					indexTableType = IndexTableTypes.PUBLISHERS;
				} else if (tableName.equals(DigLibConstants.LOCATION_INDEX_TABLE_NAME)) {
					indexTableType = IndexTableTypes.LOCATION;
				} else if (tableName.equals(DigLibConstants.ADDINFO_INDEX_TABLE_NAME)) {
					indexTableType = IndexTableTypes.ADDINFO;
				}
			}
		}
	}
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		String tableName = args[0];
		String columnFamily = args[1];
		conf.set("index.table.name", tableName);
		conf.set("column.family.name", columnFamily);

		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(columnFamily));
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf,	"Counting the number of documents containing each term.");
		job.setJarByClass(TdcMapper.class);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, TdcMapper.class, ImmutableBytesWritable.class, Writable.class, job, true);
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		job.setNumReduceTasks(0);
		
		return job;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, minimum required: 3");
			System.err.println("Usage: java iu.pti.hbaseapp.diglib.TermDocCounter <index table name> <column family name>");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}