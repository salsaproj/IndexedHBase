package iu.pti.hbaseapp.clueweb09;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class IndexSearcherClueWeb09 {
	
	/**
	 * Internal Mapper to be run by Hadoop.
	 */
	public static class IwsMapper extends Mapper<LongWritable, Text, Text, Text> {
		String word = "";
		boolean getContent = false;
		boolean isTextContent = false;
		Text emptyText = new Text("");
		HTable dataTable = null;
		
		@Override
		protected void map(LongWritable rowNum, Text txtDocId, Context context) throws IOException, InterruptedException {
			int docId = Integer.valueOf(txtDocId.toString());
			Get gData = new Get(Bytes.toBytes(docId));
			Result rData = dataTable.get(gData);
			String sData = Bytes.toString(rData.getValue(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES));
			if (!isTextContent) {
				sData = Cw09Constants.txtExtractor.htmltoText(sData);
			}
			
			context.write(txtDocId, emptyText);
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();
			isTextContent = configuration.get("content.type").equals("text");
			word = configuration.get("word");
			dataTable = new HTable(configuration, Cw09Constants.CW09_DATA_TABLE_BYTES);			
		}
		
		@Override
		protected void cleanup(Context context) {
			try {
				if (dataTable != null) {
					dataTable.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws Exception {
		String contentType = args[0];
		String words = args[1];
		String inputDirPath = args[2];
		String outputPath = args[3];
		int docLimit = Integer.valueOf(args[4]);
		int mapperDocNum = Integer.valueOf(args[5]);
		
		// write the document IDs to multiple files under input directory
		System.out.print("IndexSearcherClueWeb09: writing document IDs to " + inputDirPath + "...");
		FileSystem fs = FileSystem.get(conf);
		int fileCount = 0;
		String inputFilePath = inputDirPath + File.separator + fileCount + ".txt";
		PrintWriter pwDocId = new PrintWriter(new OutputStreamWriter(fs.create(new Path(inputFilePath))));
		
		HTable freqTable = new HTable(conf, Cw09Constants.CW09_INDEX_TABLE_BYTES);
		words = words.toLowerCase();
		int idx = words.indexOf(',');
		if (idx >= 0) {
			String word1 = words.substring(0, idx);
			String word2 = words.substring(idx + 1);

			byte[] rowKey1 = Bytes.toBytes(word1);
			Scan scan1 = new Scan();
			scan1.setStartRow(rowKey1);
			scan1.setStopRow(rowKey1);
			scan1.setBatch(Cw09Constants.CW09_INDEX_SCAN_BATCH / 2);
			ResultScanner rs1 = freqTable.getScanner(scan1);

			byte[] rowKey2 = Bytes.toBytes(word2);
			Scan scan2 = new Scan();
			scan2.setStartRow(rowKey2);
			scan2.setStopRow(rowKey2);
			scan2.setBatch(Cw09Constants.CW09_INDEX_SCAN_BATCH / 2);
			ResultScanner rs2 = freqTable.getScanner(scan2);

			int docCount = 0;
			int lineCount = 0;
			Result r1 = rs1.next();
			Result r2 = rs2.next();
			Iterator<KeyValue> iter1 = null;
			Iterator<KeyValue> iter2 = null;
			scannerLoop: while (r1 != null && r2 != null) {
				if (iter1 == null) {
					List<KeyValue> kvs1 = r1.list();
					iter1 = kvs1.iterator();
				}
				if (iter2 == null) {
					List<KeyValue> kvs2 = r2.list();
					iter2 = kvs2.iterator();
				}

				KeyValue kv1 = null;
				KeyValue kv2 = null;
				while (iter1.hasNext() && iter2.hasNext()) {
					if (kv1 == null) {
						kv1 = iter1.next();
					}
					if (kv2 == null) {
						kv2 = iter2.next();
					}

					int docId1 = Bytes.toInt(kv1.getQualifier());
					int docId2 = Bytes.toInt(kv2.getQualifier());
					if (docId1 == docId2) {
						pwDocId.println(docId1);
						docCount++;
						if (docLimit >= 0 && docCount >= docLimit) {
							break scannerLoop;
						}
						lineCount++;
						if (lineCount >= mapperDocNum) {
							// start writing to a new file
							pwDocId.close();
							fileCount++;
							inputFilePath = inputDirPath + File.separator + fileCount + ".txt";
							pwDocId = new PrintWriter(new OutputStreamWriter(fs.create(new Path(inputFilePath))));
							lineCount = 0;
						}

						kv1 = null;
						kv2 = null;
					} else if (docId1 < docId2) {
						kv1 = null;
					} else {
						kv2 = null;
					}
				}

				if (!iter1.hasNext()) {
					iter1 = null;
					r1 = rs1.next();
				}
				if (!iter2.hasNext()) {
					iter2 = null;
					r2 = rs2.next();
				}
			}
			rs1.close();
			rs2.close();
			pwDocId.close();
			freqTable.close();
		} else {
			byte[] rowKey = Bytes.toBytes(words);
			Scan scan = new Scan();
			scan.setStartRow(rowKey);
			scan.setStopRow(rowKey);
			scan.setBatch(Cw09Constants.CW09_INDEX_SCAN_BATCH / 2);
			ResultScanner rs = freqTable.getScanner(scan);

			int docCount = 0;
			int lineCount = 0;
			Result r = rs.next();
			scannerLoop: while (r != null) {
				List<KeyValue> kvs = r.list();
				for (KeyValue kv : kvs) {
					byte[] docId = kv.getQualifier();
					pwDocId.println(Bytes.toInt(docId));
					docCount++;
					if (docLimit >= 0 && docCount >= docLimit) {
						break scannerLoop;
					}

					lineCount++;
					if (lineCount >= mapperDocNum) {
						// start writing to a new file
						pwDocId.close();
						fileCount++;
						inputFilePath = inputDirPath + File.separator + fileCount + ".txt";
						pwDocId = new PrintWriter(new OutputStreamWriter(fs.create(new Path(inputFilePath))));
						lineCount = 0;
					}
				}
				r = rs.next();
			}
			rs.close();
			pwDocId.close();
			freqTable.close();
		}
		fileCount++;
		System.out.println("done with " + fileCount + " doc ID files");
		
		// now configure the MR job with these input files
		conf.set("content.type", contentType);
		conf.set("word", words);
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf,	"Searching " + words + " from " + Cw09Constants.CLUEWEB09_DATA_TABLE_NAME + " using index");
		job.setJarByClass(IndexSearcherClueWeb09.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(IwsMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(job, new Path(inputDirPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setInputFormatClass(TextInputFormat.class);
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 6) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, minimum required: 1");
			System.err.println("Usage: IndexSearcherClueWeb09 <text or html content in the data table> <word> <path for document ID files>" + 
								" <output path> <document number limit> <number of document IDs for each mapper>");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
