package iu.pti.hbaseapp.clueweb09;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TestClientClueWeb09 {
	
	/**
	 * Test random access speed to a table
	 * @param propFilePath
	 * @return
	 * @throws Exception
	 */
	public int tableAccessTest(String propFilePath) throws Exception {
		Properties prop = new Properties();
		FileReader fr = new FileReader(propFilePath);
		prop.load(fr);
		fr.close();
		
		String tableName = prop.getProperty("table.name");
		String rowKeyDir = prop.getProperty("rowKey.dir");
		String numOfAccess = prop.getProperty("number.of.access");
		String rowKeyType = prop.getProperty("rowKey.type").toLowerCase();
		String rowKeyCount = prop.getProperty("rowKey.count");
		String expRowKeyFile = prop.getProperty("rowKey.exception.file");
		
		int nAccess = Integer.valueOf(numOfAccess);
		Constants.DataType rowDataType = Constants.DataType.UNKNOWN;
		if (rowKeyType.equals("int")) {
			rowDataType = Constants.DataType.INT;
		} else if (rowKeyType.equals("string")) {
			rowDataType = Constants.DataType.STRING;
		} else if (rowKeyType.equals("double")) {
			rowDataType = Constants.DataType.DOUBLE;
		}
		
		int rowCount = 2000;
		if (rowKeyCount != null) {
			rowCount = Integer.valueOf(rowKeyCount);
		}
		
		// read the rows into an ArrayList
		ArrayList<byte[]> rows = new ArrayList<byte[]>(rowCount);
		readRowKeysFromDir(rowKeyDir, rowDataType, rowCount, rows, expRowKeyFile);
		
		// random access to the table for nAccess times
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable table = new HTable(hbaseConfig, Bytes.toBytes(tableName));
		Scan scan = new Scan();
		scan.setBatch(Cw09Constants.CW09_INDEX_SCAN_BATCH);
		long totalAccess = nAccess;
		long totalSize = 0;
		long failCount = 0;
		long startTime = System.currentTimeMillis();
		while (nAccess > 0) {
			// since some rows could be very large (>2GB), we need to scan a part of each row, instead of getting
			// the whole row, to avoid memory issues.
			int rPos = (int)(Math.random() * rows.size());
			Get gIdx = new Get(rows.get(rPos));
			Result rIdx = table.get(gIdx);
			if (rIdx == null || rIdx.isEmpty()) {
				failCount++;
			} else {
				totalSize += GeneralHelpers.getHBaseResultKVSize(rIdx);
			}
			/*
			scan.setStartRow(rows.get(rPos));
			scan.setStopRow(rows.get(rPos));
			ResultScanner rs = table.getScanner(scan);
			Result r = rs.next();
			if (r != null) {
				totalSize += r.getWritableSize();
			} else {
				failCount++;
			}
			rs.close();
			*/
			nAccess--;			
		}
		table.close();
		
		long endTime = System.currentTimeMillis();
		Calendar cal1 = Calendar.getInstance();
		cal1.setTimeInMillis(startTime);
		System.out.println("start time: " + GeneralHelpers.getDateTimeString(cal1));
		long timeDiff = endTime - startTime;
		System.out.println(timeDiff + " ms used to randomly access " + totalAccess + " rows from table " + tableName + ".");
		double avgRows = totalAccess * 1000.0 / timeDiff;
		double avgBytes = (totalSize / 1024.0) * 1000.0 / timeDiff;
		System.out.println("Total size read: " + totalSize + "; avg rows/s: " + avgRows + "; avg KBs/s: " + avgBytes + "; fail count: " + failCount);
		cal1.setTimeInMillis(endTime);
		System.out.println("end time: " + GeneralHelpers.getDateTimeString(cal1));
		
		return (int)timeDiff;
	}
	
	/**
	 * Test scan speed to a table
	 * @param propFilePath
	 * @return
	 * @throws Exception
	 */
	public int tableScanTest(String propFilePath) throws Exception {
		Properties prop = new Properties();
		prop.load(new FileReader(propFilePath));
		
		String tableName = prop.getProperty("table.name");
		String rowKeyDir = prop.getProperty("rowKey.dir");
		String numOfAccess = prop.getProperty("number.of.access");
		String rowKeyType = prop.getProperty("rowKey.type").toLowerCase();
		String rowKeyCount = prop.getProperty("rowKey.count");
		
		Constants.DataType rowDataType = Constants.DataType.UNKNOWN;
		if (rowKeyType.equals("int")) {
			rowDataType = Constants.DataType.INT;
		} else if (rowKeyType.equals("string")) {
			rowDataType = Constants.DataType.STRING;
		} else if (rowKeyType.equals("double")) {
			rowDataType = Constants.DataType.DOUBLE;
		}
		
		int rowCount = 2000;
		if (rowKeyCount != null) {
			rowCount = Integer.valueOf(rowKeyCount);
		}
		
		// read the rows into an ArrayList. In the scanning test, the number of rows scanned should be numOfAccess
		int nAccess = Integer.valueOf(numOfAccess);
		ArrayList<byte[]> rows = new ArrayList<byte[]>(nAccess);
		readRowKeysFromDir(rowKeyDir, rowDataType, nAccess, rows, null);
		
		// scanning access to the table for nAccess times
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable table = new HTable(hbaseConfig, Bytes.toBytes(tableName));
		Scan scan = new Scan();
		scan.setStartRow(rows.get(0));
		scan.setStopRow(rows.get(rows.size() - 1));
		scan.setBatch(Cw09Constants.CW09_INDEX_SCAN_BATCH);
		
		long resultCount = 0;
		long totalSize = 0;
		int rowsRead = 0;
		byte[] lastRow = null;
		long startTime = System.currentTimeMillis();
		ResultScanner rs = table.getScanner(scan);
		Result r = rs.next();
		while (r != null) {
			r = rs.next();
			resultCount++;
			totalSize += GeneralHelpers.getHBaseResultKVSize(r);
			if (!Arrays.equals(r.getRow(), lastRow)) {
				rowsRead++;
				lastRow = r.getRow();
			}
		}
		table.close();
		
		long endTime = System.currentTimeMillis();
		long timeDiff = endTime - startTime;
		Calendar cal1 = Calendar.getInstance();
		cal1.setTimeInMillis(startTime);
		System.out.println("rowKey.count: " + rowCount);
		System.out.println("start time: " + GeneralHelpers.getDateTimeString(cal1));
		System.out.println(timeDiff + " ms used to scan " + resultCount + " result objects from table " + tableName + ".");
		double avgRows = rowsRead * 1000.0 / timeDiff;
		double avgBytes = (totalSize / 1024.0) * 1000.0 / timeDiff;
		System.out.println("Total size read: " + totalSize + "; avg rows/s: " + avgRows + "; avg KBs/s: " + avgBytes);
		cal1.setTimeInMillis(endTime);
		System.out.println("end time: " + GeneralHelpers.getDateTimeString(cal1));
		
		return (int)timeDiff;
	}

	/**
	 * starting from a random position, loop through the row key files under rowKeyDir to read
	 * rowCount row keys to rows
	 * @param rowKeyDir
	 * @param rowDataType
	 * @param rowCount
	 * @param rows
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected void readRowKeysFromDir(String rowKeyDir, Constants.DataType rowDataType, int rowCount, ArrayList<byte[]> rows, String expRowKeyFile)
			throws FileNotFoundException, IOException {
		HashSet<String> expRowKeySet = new HashSet<String>();
		if (expRowKeyFile != null) {
			System.out.println(expRowKeyFile);
			BufferedReader brRows = new BufferedReader(new FileReader(expRowKeyFile));
			String line = brRows.readLine();
			while (line != null) {
				int idx = line.indexOf('\t');
				if (idx >= 0) {
					line = line.substring(0, idx);
				}
				expRowKeySet.add(line);
				line = brRows.readLine();
			}
			brRows.close();
		}
		
		File dirFile = new File(rowKeyDir);
		File[] rowKeyFiles = dirFile.listFiles();
		int fileCount = 0;
		for (File f : rowKeyFiles) {
			if (f.isFile()) {
				fileCount++;
			}
		}
		int startPos = (int)(Math.random() * fileCount);
		System.out.println("starting rowkey file : " + rowKeyFiles[startPos].getName());
		int filesDone = 0;
		int rowsDone = 0;
		int largeRowCount = 0;
		while (rowsDone < rowCount && filesDone < fileCount) {
			File f = rowKeyFiles[startPos++ % rowKeyFiles.length];
			if (f.isDirectory()) {
				continue;
			}
			BufferedReader brRows = new BufferedReader(new FileReader(f));
			String line = brRows.readLine();
			while (line != null) {
				line = line.trim();
				if (!expRowKeySet.contains(line)) {
					switch (rowDataType) {
					case INT:
						rows.add(Bytes.toBytes(Integer.valueOf(line)));
						break;
					case STRING:
						rows.add(Bytes.toBytes(line));
						break;
					case DOUBLE:
						rows.add(Bytes.toBytes(Double.valueOf(line)));
					default:
					}
					rowsDone++;
					if (rowsDone >= rowCount) {
						break;
					}
				} else {
					largeRowCount++;
				}
				line = brRows.readLine();
			}
			brRows.close();
			filesDone++;
		}
		System.out.println("large rows avoided: " + largeRowCount);
	}
	
	/**
	 * starting from a random position, loop through the row key files under rowKeyDir to read
	 * rowCount row keys to rows
	 * @param rowKeyDir
	 * @param rowDataType
	 * @param rowCount
	 * @param rows
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected void readRowKeysFromDir2(String rowKeyDir, Constants.DataType rowDataType, int rowCount, ArrayList<String> rows)
			throws FileNotFoundException, IOException {
		File dirFile = new File(rowKeyDir);
		File[] rowKeyFiles = dirFile.listFiles();
		int fileCount = 0;
		for (File f : rowKeyFiles) {
			if (f.isFile()) {
				fileCount++;
			}
		}
		int startPos = (int)(Math.random() * fileCount);
		int filesDone = 0;
		int rowsDone = 0;
		while (rowsDone < rowCount && filesDone < fileCount) {
			File f = rowKeyFiles[startPos++ % rowKeyFiles.length];
			if (f.isDirectory()) {
				continue;
			}
			BufferedReader brRows = new BufferedReader(new FileReader(f));
			String line = brRows.readLine();
			while (line != null) {
				line = line.trim();
				rows.add(line);
				rowsDone++;
				if (rowsDone >= rowCount) {
					break;
				}
				line = brRows.readLine();
			}
			brRows.close();
			filesDone++;
		}
	}
	
	/**
	 * Test real-time document insertion performance.
	 * @param inputListPath path to a txt file containing .warc.gz file paths.
	 */
	public void realtimeInsertTest(String inputListPath) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable dataTable = new HTable(hbaseConfig, Cw09Constants.CW09_DATA_TABLE_BYTES);
		HTable freqTable = new HTable(hbaseConfig, Cw09Constants.CW09_INDEX_TABLE_BYTES);
		
		LinkedList<String> paths = new LinkedList<String>();
		GeneralHelpers.readFileToCollection(inputListPath, paths);
		HTMLTextParser txtExtractor = new HTMLTextParser();
		long totalFileSize = 0;
		int docCount = 0;
		int indexRecordCount = 0;
		long startTime = System.currentTimeMillis();
		for (String path : paths) {
			File warcFile = new File(path);
			totalFileSize += warcFile.length();
			GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(warcFile));
			DataInputStream inStream = new DataInputStream(gzInputStream);
			WarcRecord thisWarcRecord;
			while ((thisWarcRecord = WarcRecord.readNextWarcRecord(inStream)) != null) {
				// see if it's a response record
				if (thisWarcRecord.getHeaderRecordType().equals("response")) {
					// it is - create a WarcHTML record
					WarcHTMLResponseRecord htmlRecord = new WarcHTMLResponseRecord(thisWarcRecord);
					// get our TREC ID and target URI
					String thisTRECID = htmlRecord.getTargetTrecID();
					String thisTargetURI = htmlRecord.getTargetURI();
					String html = thisWarcRecord.getContentUTF8();
					int idx = html.indexOf("Content-Length");
					if (idx >= 0) {
						idx = html.indexOf('<', idx);
						if (idx < 0) {
							html = "";
						} else {
							html = html.substring(idx); 
						}
					}
					String content = txtExtractor.htmltoText(html);
					if (content == null) {
						continue;
					}
					byte[] docIdBytes = DataLoaderClueWeb09.getRowKeyFromTrecId(thisTRECID);
					byte[] uriBytes = Bytes.toBytes(thisTargetURI);
					byte[] contentBytes = Bytes.toBytes(content);
					Put put = new Put(docIdBytes);
					put.add(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_URI_BYTES, uriBytes);
					put.add(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES, contentBytes);
					dataTable.put(put);
					dataTable.flushCommits();
					docCount++;
					
					// add index records to index table
					HashMap<String, Integer> freqs = GeneralHelpers.getTermFreqsByLuceneAnalyzer(Constants.getLuceneAnalyzer(), content, 
							Cw09Constants.INDEX_OPTION_TEXT);
					LinkedList<Row> ops = new LinkedList<Row>();
					for (Map.Entry<String, Integer> e : freqs.entrySet()) {
						String term = e.getKey();
						Put putIdx = new Put(Bytes.toBytes(term));
						putIdx.add(Cw09Constants.CF_FREQUENCIES_BYTES, docIdBytes, Bytes.toBytes(e.getValue()));
						ops.add(putIdx);
						indexRecordCount++;
					}
					Object[] results = freqTable.batch(ops);
					int failCount = 0;
					for (Object res : results) {
						if (res == null) {
							failCount++;
						}
					}
					if (failCount > 0) {
						System.out.println(failCount + " failures when inserting index records for doc ID " + thisTRECID);
						indexRecordCount -= failCount;
					}
					freqTable.flushCommits();	
				}
			}
			inStream.close();
		}
		dataTable.close();
		freqTable.close();
		long endTime = System.currentTimeMillis();
		Calendar cal1 = Calendar.getInstance();
		cal1.setTimeInMillis(startTime);
		System.out.println("start time: " + GeneralHelpers.getDateTimeString(cal1));
		long timeDiff = endTime - startTime;
		double avgDocCount = docCount * 1000.0 / timeDiff;
		double avgSize = (totalFileSize / 1024.0) * 1000.0 / timeDiff;
		double avgIndexCount = indexRecordCount * 1000.0 / timeDiff;
		System.out.println(timeDiff + " ms used to process " + docCount + " documents. avg doc/s: " + avgDocCount 
							+ "; avg size: " + avgSize + " KB/s; avg index records/s: " + avgIndexCount);
		cal1.setTimeInMillis(endTime);
		System.out.println("end time: " + GeneralHelpers.getDateTimeString(cal1));
		
	}
	
	/**
	 * Test the performance for searching documents containing a single word
	 * @param word
	 * @param docLimit
	 * @param getContent
	 * @param outputPath
	 */
	public void singleWordSearchTest(String word, int docLimit, boolean getContent, String outputPath, boolean isContentText) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		long startTime = System.currentTimeMillis();
		
		HTable dataTable = new HTable(hbaseConfig, Cw09Constants.CW09_DATA_TABLE_BYTES);
		HTable freqTable = new HTable(hbaseConfig, Cw09Constants.CW09_INDEX_TABLE_BYTES);
		PrintWriter pwOut = new PrintWriter(new FileWriter(outputPath));
		
		word = word.toLowerCase();
		byte[] rowKey = Bytes.toBytes(word);
		Scan scan = new Scan();
		scan.setStartRow(rowKey);
		scan.setStopRow(rowKey);
		scan.setBatch(Cw09Constants.CW09_INDEX_SCAN_BATCH / 2);
		ResultScanner rs = freqTable.getScanner(scan);
		int docCount = 0;
		Result r = rs.next();
		scannerLoop: while (r != null) {
			List<KeyValue> kvs = r.list();
			for (KeyValue kv : kvs) {
				byte[] docId = kv.getQualifier();
				pwOut.println(Bytes.toInt(docId));
				if (getContent) {
					Get gData = new Get(docId);
					Result rData = dataTable.get(gData);
					String sData = Bytes.toString(rData.getValue(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES));
					if (!isContentText) {
						sData = Cw09Constants.txtExtractor.htmltoText(sData);
					}
				}
				docCount++;
				if (docLimit >= 0 && docCount >= docLimit) {
					break scannerLoop;
				}
				if (docCount % 100000 == 0) {
					System.out.println(docCount + " documents accessed.");
				}
			}
			r = rs.next();
		}
		rs.close();
		pwOut.close();
		dataTable.close();
		freqTable.close();
		
		long endTime = System.currentTimeMillis();
		long diff = endTime - startTime;
		double avgDoc = docCount * 1000.0 / diff;
		System.out.println(docCount + " documents returned for searching '" + word + "' in " + diff + " ms, " + avgDoc + " docs/s");
	}
	
	/**
	 * Test the performance for searching documents containing two words
	 * @param words
	 * @param docLimit
	 * @param getContent
	 * @param outputPath
	 */
	public void twoWordsSearchTest(String words, int docLimit, boolean getContent, String outputPath, boolean isContentText) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		long startTime = System.currentTimeMillis();
		
		HTable dataTable = new HTable(hbaseConfig, Cw09Constants.CW09_DATA_TABLE_BYTES);
		HTable freqTable = new HTable(hbaseConfig, Cw09Constants.CW09_INDEX_TABLE_BYTES);
		PrintWriter pwOut = new PrintWriter(new FileWriter(outputPath));
		
		words = words.toLowerCase();
		int idx = words.indexOf(',');
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
		Result r1 = rs1.next();
		Result r2 = rs2.next();
		Iterator<KeyValue> iter1 = null;
		Iterator<KeyValue> iter2 = null;
		while (r1 != null && r2 != null) {
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
					docCount++;
					pwOut.println(docId1);
					if (getContent) {
						Get gData = new Get(kv1.getQualifier());
						Result rData = dataTable.get(gData);
						String sData = Bytes.toString(rData.getValue(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES));
						if (!isContentText) {
							sData = Cw09Constants.txtExtractor.htmltoText(sData);
						}
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
		pwOut.close();
		dataTable.close();
		freqTable.close();
		
		long endTime = System.currentTimeMillis();
		long diff = endTime - startTime;
		double avgDoc = docCount * 1000.0 / diff;
		System.out.println(docCount + " documents returned for searching '" + words + "' in " + diff + " ms, " + avgDoc + " docs/s");
	}
	
	protected static void printUsage() {
		System.out.println("Usage: java iu.pti.hbaseapp.clueweb09.TestClientClueWeb09 <command> [<additional arguments>]");
		System.out.println("	where <command> [<additional arguments>] could be one of the following combinations:");
		System.out.println("	single-word-search <word> <max number for documents returned> <true or false for getting document content>"
								+ " <output path> <text or html in document data table>");
		System.out.println("	two-word-search <word1>,<word2> <max number for documents returned> <true or false for getting document content>"
								+ " <output path> <text or html in document data table>");
		System.out.println("	realtime-insert-test <path to .warc.gz file list>");
		System.out.println("	index-scan-test <path to properties file>");
		System.out.println("	index-access-test <path to properties file>");		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			String command = args[0];
			if (command.equals("single-word-search")) {
				TestClientClueWeb09 client = new TestClientClueWeb09();
				String word = args[1];
				int docLimit = Integer.valueOf(args[2]);
				boolean getContent = Boolean.valueOf(args[3]);
				String outputPath = args[4];
				boolean isContentText = args[5].equals("text");
				client.singleWordSearchTest(word, docLimit, getContent, outputPath, isContentText);
			} else if (command.equals("two-word-search")) {
				TestClientClueWeb09 client = new TestClientClueWeb09();
				String words = args[1];
				int docLimit = Integer.valueOf(args[2]);
				boolean getContent = Boolean.valueOf(args[3]);
				String outputPath = args[4];
				boolean isContentText = args[5].equals("text");
				client.twoWordsSearchTest(words, docLimit, getContent, outputPath, isContentText);
			} else if (command.equals("realtime-insert-test")) {
				TestClientClueWeb09 client = new TestClientClueWeb09();
				String listPath = args[1];
				client.realtimeInsertTest(listPath);
			} else if (command.equals("index-scan-test")) {
				TestClientClueWeb09 client = new TestClientClueWeb09();
				String propPath = args[1];
				client.tableScanTest(propPath);
			} else if (command.equals("index-access-test")) {
				TestClientClueWeb09 client = new TestClientClueWeb09();
				String propPath = args[1];
				client.tableAccessTest(propPath);
			} else {
				printUsage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			printUsage();
		}
	}

}
