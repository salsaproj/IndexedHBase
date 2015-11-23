package iu.pti.hbaseapp.diglib;

import java.io.File;
import java.io.FileInputStream;
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
 * MapReduce data loader for the digital library application. It uses a table containing metadata about the
 * books as input, and load the text and image data of the books to the text data table and image data table
 * for all books.
 * <p/>
 * Usage as a Java application:<br/> 
 * java iu.pti.hbaseapp.diglib.BookDataLoader [table name] [column family] [qualifier] [book database dir]
 * 
 * @author gaoxm
 *
 */
public class BookDataLoader {
	/**
	 * Internal Mapper to be run by Hadoop.
	 */
	public static class Map extends TableMapper<ImmutableBytesWritable, Put> {
		private byte[] columnFamily;
		private byte[] qualifier;
		private String bookDbDir;

		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			byte[] bookNumBytes = rowKey.get();
			byte[] bookRelDirBytes = result.getValue(columnFamily, qualifier);
			String bookRelDir = Bytes.toString(bookRelDirBytes);
			String bookDirPath = bookDbDir + File.separator + bookRelDir;
			File bookDirFile = new File(bookDirPath);
			File[] pageFiles = bookDirFile.listFiles();
			int txtFileCount = 0;
			int tifFileCount = 0;
			
			if (pageFiles == null) {
				System.out.println("Directory " + bookDirPath + " does not exist. File listing returns null.");
				return;
			}
			
			for (int i=0; i<pageFiles.length; i++) {
				String fileName = pageFiles[i].getName();
				int idx = fileName.lastIndexOf('.');
				String pageNumStr = fileName.substring(0, idx);
				String extension = fileName.substring(idx+1);
				try {
					if (extension.equals("txt") && isIntegerString(pageNumStr)) {
						int pageNum = Integer.valueOf(pageNumStr);
						FileInputStream fis = new FileInputStream(pageFiles[i]);
						byte[] pageTxt = new byte[(int) pageFiles[i].length()];
						fis.read(pageTxt);
						fis.close();
						Put put = new Put(bookNumBytes);
						put.add(Bytes.toBytes("pages"), Bytes.toBytes(pageNum),	pageTxt);
						context.write(new ImmutableBytesWritable(Bytes.toBytes("bookTextTable")), put);
						txtFileCount++;
					} else if (extension.equals("tif")) {
						int pageNum = Integer.valueOf(pageNumStr);
						FileInputStream fis = new FileInputStream(pageFiles[i]);
						byte[] pageImg = new byte[(int) pageFiles[i].length()];
						fis.read(pageImg);
						fis.close();
						byte[] pageNumBytes = Bytes.toBytes(pageNum);
						byte[] imageTableRow = new byte[bookNumBytes.length	+ pageNumBytes.length + 1];
						// image table row: <length of bookNumBytes><bookNumBytes><pageNumBytes>
						imageTableRow[0] = (byte) bookNumBytes.length;
						int pos = 1;
						for (int j = 0; j < bookNumBytes.length; j++) {
							imageTableRow[pos++] = bookNumBytes[j];
						}
						for (int j = 0; j < pageNumBytes.length; j++) {
							imageTableRow[pos++] = pageNumBytes[j];
						}

						Put put = new Put(imageTableRow);
						put.add(Bytes.toBytes("image"), Bytes.toBytes("image"),	pageImg);
						context.write(new ImmutableBytesWritable(Bytes.toBytes("bookImageTable")), put);
						tifFileCount++;
					}
				} catch (Exception e) {
					System.out.println("Exception when processing file " + pageFiles[i].getPath() + " : " + e.getMessage());
				}
			}
			
			System.out.println("loaded " + txtFileCount + " text pages and " + tifFileCount + " image pages for " + bookRelDir);
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();
			columnFamily = Bytes.toBytes(configuration.get("column.family.name"));
			qualifier = Bytes.toBytes(configuration.get("qualifier.name"));
			bookDbDir = configuration.get("book.database.dir");
		}
	}

	/**
	 * Job configuration.
	 */
	protected static Job configureJob(Configuration conf, String[] args) throws IOException {
		String tableName = args[0];
		String columnFamily = args[1];
		String qualifier = args[2];
		String bookDbDir = args[3];
	    conf.set("column.family.name", columnFamily);
	    conf.set("qualifier.name", qualifier);
	    conf.set("book.database.dir", bookDbDir);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));

		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf,	"Text and image table filling from " + tableName);
		job.setJarByClass(Map.class);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, Map.class, ImmutableBytesWritable.class, Writable.class, job, true);
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		job.setNumReduceTasks(0);
		
		return job;
	}

	/**
	 * Usage: java iu.pti.hbaseapp.BookDataLoader [table name] [column family] [qualifier] [book database dir]
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 4) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, required: 4");
			System.err.println("Usage: BookDataLoader <table name> <column family> <qualifier> <book database dir>");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	/**
	 * check if a string is a representation of an integer
	 * @param str
	 * @return
	 */
	public static boolean isIntegerString(String str) {
		if (str.isEmpty()) {
			return false;
		}
		char c = str.charAt(0);
		if (c == '+' || c == '-' || Character.isDigit(c)) {
			for (int i=1; i<str.length(); i++) {
				if (!Character.isDigit(str.charAt(i))) {
					return false;
				}
			}
			return true;
		} else {
			return false;
		}		
	}
}

