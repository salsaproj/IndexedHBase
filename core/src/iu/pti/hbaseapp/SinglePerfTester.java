package iu.pti.hbaseapp;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This application can be used to run simple sequential performance tests using a single testing application.
 * <p/>
 * Usage: java iu.pti.hbaseapp.SinglePerfTester [test code] [row source file path] [batch write size] [number of rows to write]
 * <br/>
 * Where [test code] could be one of the following:
 * <br/> 0: Clear up. Delete the test table.
 * <br/> 1: Sequential write test.
 * <br/> 2: Sequential read test.
 * <br/> 3: Random write test.
 * <br/> 4: Random read test.
 * 
 * @author gaoxm
 */
@InterfaceAudience.Private
public class SinglePerfTester {
	
	public static final String TABLE_NAME = "SingleClientPerfTestTable";
	public static final String COLUMN_FAMILY = "testCf";
	public static final String QUALIFIER = "testQual";
	
	public static void usage() {
		System.out.println("Usage:");
		System.out.println("java iu.pti.hbaseapp.SinglePerfTester <test code> <row source file path> <batch write size> <number of rows to write>");
		System.out.println("Where <test code> could be one of the following:");
		System.out.println("  0: Clear up. Delete the test table.");
		System.out.println("  1: Sequential write test.");
		System.out.println("  2: Sequential read test.");
		System.out.println("  3: Random write test.");
		System.out.println("  4: Random read test.");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 3) {
			usage();
			return;
		}
		
		int testCode = Integer.parseInt(args[0]);
		String rowSourceFilePath = args[1];
		int batchSize = Integer.parseInt(args[2]);
		int numOfRows = Integer.parseInt(args[3]);

		switch (testCode) {
		case 0:
			clearUp();
			break;
		case 1:
			seqWriteTest(rowSourceFilePath, batchSize, numOfRows);
			break;
		case 2:
			seqReadTest(numOfRows);
			break;
		case 3:
			randomWriteTest(rowSourceFilePath, batchSize, numOfRows);
			break;
		case 4:
			randomReadTest(numOfRows);
		}
	}
	
	public static void clearUp() {
		try {
			Configuration hbaseConfig = HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
			byte[] tableNameBytes = Bytes.toBytes(TABLE_NAME);
			if (admin.tableExists(tableNameBytes)) {
				admin.disableTable(tableNameBytes);
				admin.deleteTable(tableNameBytes);
			}
			admin.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void seqWriteTest(String rowSourceFilePath, int batchSize, int numOfRows) {
		File fRowSource = new File(rowSourceFilePath);
		if (!fRowSource.exists() || fRowSource.isDirectory()) {
			System.out.println("Error: row source file " + rowSourceFilePath + " does not exist!");
			return;
		}
		int fSize = (int)fRowSource.length();
		byte[] fContent = new byte[fSize];
		try {
			// read the row content from rowSourceFilePath
			FileInputStream fis = new FileInputStream(fRowSource);
			fis.read(fContent);
			fis.close();
			
			// create the test table if not exist
			Configuration hbaseConfig = HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
			byte[] tableName = Bytes.toBytes(TABLE_NAME);
			byte[] cfBytes = Bytes.toBytes(COLUMN_FAMILY);
			if (!admin.tableExists(tableName)) {
				HTableDescriptor tableDes = new HTableDescriptor(tableName);
				HColumnDescriptor cfDes = new HColumnDescriptor(cfBytes);
				cfDes.setMaxVersions(1);
				tableDes.addFamily(cfDes);
				admin.createTable(tableDes);
			}
			
			// test sequential write
			// create a reusable list of fContents for batch write
			ArrayList<byte[]> reusableContents = new ArrayList<byte[]>(batchSize);
			for (int i=0; i<batchSize; i++) {
				reusableContents.add(fContent.clone());
			}
			
			HTable table = new HTable(hbaseConfig, tableName);
			byte[] qualBytes = Bytes.toBytes(QUALIFIER);
			ArrayList<Row> ops = new ArrayList<Row>(batchSize);
			long startTime = System.currentTimeMillis();
			int rowCount = 0;
			long totalSize = 0;
			while (rowCount < numOfRows) {
				if (rowCount + batchSize > numOfRows) {
					batchSize = numOfRows - rowCount;
				}
				
				for (int i=0; i<batchSize; i++) {
					byte[] content = reusableContents.get(i);
					randomizeBytes(content);
					Put row = new Put(Bytes.toBytes(rowCount++));
					row.add(cfBytes, qualBytes, content);
					ops.add(row);
					totalSize += row.heapSize();
					
				}
				Object[] res = table.batch(ops);
				for (int i=0; i<res.length; i++) {
					if (res[i] == null) {
						System.out.println("Failure when doing put for " + (rowCount - batchSize + i) + "th row.");
					} 
				}
				table.flushCommits();
				ops.clear();
			}
			table.close();
			admin.close();
			
			long endTime = System.currentTimeMillis();
			long timeDiff = endTime - startTime;
			long timeInSec = timeDiff / 1000;
			double rowRate = (double)numOfRows / (double)timeInSec;
			double sizeRate = (double)totalSize / (double)timeInSec;
			System.out.println("Sequential write test is complete. " + numOfRows + " rows written in " + timeInSec + " secondes ("
								+ timeDiff + " ms). Total number of bytes written: " + totalSize + ". " + rowRate + " rows/s, "
								+ sizeRate + " bytes/s.");
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	public static void seqReadTest(int numOfRows) {
		try {
			Configuration hbaseConfig = HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
			byte[] tableNameBytes = Bytes.toBytes(TABLE_NAME);
			byte[] cfBytes = Bytes.toBytes(COLUMN_FAMILY);
			byte[] qualBytes = Bytes.toBytes(QUALIFIER);
			if (!admin.tableExists(tableNameBytes)) {
				System.out.println("Error: table " + tableNameBytes + " does not exist!");
				admin.close();
				return;
			}
			
			HTable table = new HTable(hbaseConfig, tableNameBytes);
			ResultScanner rs = table.getScanner(cfBytes, qualBytes);
			System.out.println("scanning table " + TABLE_NAME + "...");
			long startTime = System.currentTimeMillis();
			Result r = rs.next();
			int rowCount = 0;
			long totalSize = 0;
			while (r != null && rowCount++ < numOfRows) {
				totalSize += GeneralHelpers.getHBaseResultKVSize(r);
				r = rs.next();
			}
			rs.close();
			table.close();
			admin.close();
			long endTime = System.currentTimeMillis();
			long timeDiff = endTime - startTime;
			long timeInSec = timeDiff / 1000;
			double rowRate = (double)rowCount / (double)timeInSec;
			double sizeRate = (double)totalSize / (double)timeInSec;
			System.out.println("Sequential read test is complete. " + rowCount + " rows read in " + timeInSec + " secondes ("
								+ timeDiff + " ms). Total number of bytes read: " + totalSize + ". " + rowRate + " rows/s, "
								+ sizeRate + " bytes/s.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void randomWriteTest(String rowSourceFilePath, int batchSize, int numOfRows) {
		File fRowSource = new File(rowSourceFilePath);
		if (!fRowSource.exists() || fRowSource.isDirectory()) {
			System.out.println("Error: row source file " + rowSourceFilePath + " does not exist!");
			return;
		}
		int fSize = (int)fRowSource.length();
		byte[] fContent = new byte[fSize];
		try {
			// read the row content from rowSourceFilePath
			FileInputStream fis = new FileInputStream(fRowSource);
			fis.read(fContent);
			fis.close();
			
			// create the test table if not exist
			Configuration hbaseConfig = HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
			byte[] tableName = Bytes.toBytes(TABLE_NAME);
			byte[] cfBytes = Bytes.toBytes(COLUMN_FAMILY);
			if (!admin.tableExists(tableName)) {
				HTableDescriptor tableDes = new HTableDescriptor(tableName);
				HColumnDescriptor cfDes = new HColumnDescriptor(cfBytes);
				cfDes.setMaxVersions(1);
				tableDes.addFamily(cfDes);
				admin.createTable(tableDes);
			}
			
			// test sequential write
			// create a reusable list of fContents for batch write
			ArrayList<byte[]> reusableContents = new ArrayList<byte[]>(batchSize);
			for (int i=0; i<batchSize; i++) {
				reusableContents.add(fContent.clone());
			}
			
			HTable table = new HTable(hbaseConfig, tableName);
			byte[] qualBytes = Bytes.toBytes(QUALIFIER);
			ArrayList<Row> ops = new ArrayList<Row>(batchSize);
			long startTime = System.currentTimeMillis();
			int rowCount = 0;
			long totalSize = 0;
			while (rowCount < numOfRows) {
				if (rowCount + batchSize > numOfRows) {
					batchSize = numOfRows - rowCount;
				}
				
				for (int i=0; i<batchSize; i++) {
					rowCount++;
					byte[] content = reusableContents.get(i);
					randomizeBytes(content);
					int rowId = (int)Math.round(Math.random() * Integer.MAX_VALUE);
					Put row = new Put(Bytes.toBytes(rowId));
					row.add(cfBytes, qualBytes, content);
					ops.add(row);
					totalSize += row.heapSize();
					
				}
				Object[] res = table.batch(ops);
				for (int i=0; i<res.length; i++) {
					if (res[i] == null) {
						System.out.println("Failure when doing put for " + (rowCount - batchSize + i) + "th row.");
					} 
				}
				table.flushCommits();
				ops.clear();
			}
			table.close();
			admin.close();
			
			long endTime = System.currentTimeMillis();
			long timeDiff = endTime - startTime;
			long timeInSec = timeDiff / 1000;
			double rowRate = (double)numOfRows / (double)timeInSec;
			double sizeRate = (double)totalSize / (double)timeInSec;
			System.out.println("Random write test is complete. " + numOfRows + " rows written in " + timeInSec + " secondes ("
								+ timeDiff + " ms). Total number of bytes written: " + totalSize + ". " + rowRate + " rows/s, "
								+ sizeRate + " bytes/s.");
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	public static void randomReadTest(int numOfRows) {
		try {
			Configuration hbaseConfig = HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
			byte[] tableNameBytes = Bytes.toBytes(TABLE_NAME);
			if (!admin.tableExists(tableNameBytes)) {
				System.out.println("Error: table " + tableNameBytes + " does not exist!");
				admin.close();
				return;
			}
			
			HTable table = new HTable(hbaseConfig, tableNameBytes);
			System.out.println("Random read test for table " + TABLE_NAME + "...");
			long startTime = System.currentTimeMillis();
			int rowCount = 0;
			long totalSize = 0;
			while (rowCount++ < numOfRows) {
				int rowId = (int)Math.round(Math.random() * numOfRows);
				Get get = new Get(Bytes.toBytes(rowId));
				Result r = table.get(get);
				if (r == null) {
					System.out.println("Error: row " + rowId + " does not exist!");
				} else {
					totalSize += GeneralHelpers.getHBaseResultKVSize(r);
				}
			}
			table.close();
			admin.close();
			
			long endTime = System.currentTimeMillis();
			long timeDiff = endTime - startTime;
			long timeInSec = timeDiff / 1000;
			double rowRate = (double)rowCount / (double)timeInSec;
			double sizeRate = (double)totalSize / (double)timeInSec;
			System.out.println("Random read test is complete. " + rowCount + " rows read in " + timeInSec + " secondes ("
								+ timeDiff + " ms). Total number of bytes read: " + totalSize + ". " + rowRate + " rows/s, "
								+ sizeRate + " bytes/s.");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void randomizeBytes(byte[] src) {
		int scope = src.length - 1;
		int pos1 = (int)Math.round(Math.random() * scope);
		int pos2 = (int)Math.round(Math.random() * scope);
		int pos3 = (int)Math.round(Math.random() * scope);
		int pos4 = (int)Math.round(Math.random() * scope);
		byte temp = src[pos1];
		src[pos1] = src[pos2];
		src[pos2] = src[pos3];
		src[pos3] = src[pos4];
		src[pos4] = temp;
	}
}
