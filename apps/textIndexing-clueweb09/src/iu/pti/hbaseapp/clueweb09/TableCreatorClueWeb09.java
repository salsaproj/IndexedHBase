package iu.pti.hbaseapp.clueweb09;

import java.util.ArrayList;

import iu.pti.hbaseapp.GeneralHelpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

public class TableCreatorClueWeb09 {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		boolean dataCfDetailComp= false;
		boolean indexCfFreqComp = false;
		boolean posCfPosComp = false;
		boolean pairFreqCfPosComp = false;
		boolean synonymCfPosComp = false;
		String termSamplePath = null;
		int numOfRegions = 0;
		
		if (args.length >= 7) {
			dataCfDetailComp = args[0].toLowerCase().equals("yes");
			indexCfFreqComp = args[1].toLowerCase().equals("yes");
			posCfPosComp = args[2].toLowerCase().equals("yes");
			pairFreqCfPosComp = args[3].toLowerCase().equals("yes");
			synonymCfPosComp = args[4].toLowerCase().equals("yes");
			termSamplePath = args[5];
			numOfRegions = Integer.valueOf(args[6]);
		} else {
			System.out.println("Usage: java iu.pti.hbaseapp.clueweb09.TableCreatorClueWeb09 [<yes or no for data table compression> "
							+ " <yes or no for index table compression> <yes or no for position vector table compression>"
							+ " <yes or no for word-pair frequency table compression> <yes or no for synonym table compression>"
							+ " <path to term sample file> <number of region for index table>]");
			System.exit(1);
		}
		
		ArrayList<String> sampleTerms = new ArrayList<String>(25000);
		GeneralHelpers.readFileToCollection(termSamplePath, sampleTerms);
		int numPerRegion = sampleTerms.size() / numOfRegions;
		String startTerm = sampleTerms.get(numPerRegion - 1);
		String endTerm = sampleTerms.get(sampleTerms.size() - numPerRegion - 1);
		byte[] startKey = Bytes.toBytes(startTerm);
		byte[] endKey = Bytes.toBytes(endTerm);
		System.out.println("start term: " + startTerm + " end term: " + endTerm + " numOfRegions: " + numOfRegions);
		
		// You need a configuration object to tell the client where to connect.
		// But don't worry, the defaults are pulled from the local config file.
		Configuration hbaseConfig = HBaseConfiguration.create();
		
		// create the clueWeb09 data table
		HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
		byte[] tableName = Cw09Constants.CW09_DATA_TABLE_BYTES;
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		HTableDescriptor tableDes = new HTableDescriptor(tableName);
		HColumnDescriptor cfDes = new HColumnDescriptor(Cw09Constants.CF_DETAILS_BYTES);
		cfDes.setMaxVersions(1);
		if (dataCfDetailComp) {
			cfDes.setCompressionType(Compression.Algorithm.GZ);
		}
		tableDes.addFamily(cfDes);		
		admin.createTable(tableDes);
		
		// test the put, read, and delete operations
		HTable table = new HTable(hbaseConfig, Cw09Constants.CW09_DATA_TABLE_BYTES);
		byte[] uri1Bytes = Bytes.toBytes("http://www.google.com");
		byte[] uri2Bytes = Bytes.toBytes("http://www.bing.com");
		byte[] content1Bytes = Bytes.toBytes("google search");
		byte[] content2Bytes = Bytes.toBytes("bing search");
		byte[] rowKey1 = Bytes.toBytes(1);
		byte[] rowKey2 = Bytes.toBytes(2);
		Put row1 = new Put(rowKey1);
		row1.add(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_URI_BYTES, uri1Bytes);
		row1.add(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES, content1Bytes);
		Put row2 = new Put(rowKey2);
		row2.add(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_URI_BYTES, uri2Bytes);
		row2.add(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES, content2Bytes);
		ArrayList<Row> ops = new ArrayList<Row>(2);
		ops.add(row1);
		ops.add(row2);
		Object[] res = table.batch(ops);
		for (int i=0; i<res.length; i++) {
			if (res[i] == null) {
				res[i] = "null (failure)";
			}
			System.out.println("results for row " + i + " in batch : " + res[i].toString()); 
		}
		table.flushCommits();
		
		ResultScanner rs = table.getScanner(Cw09Constants.CF_DETAILS_BYTES);
		System.out.println("scanning table " + Cw09Constants.CLUEWEB09_DATA_TABLE_NAME + "...");
		Result r = rs.next();
		while (r != null) {
			System.out.println(Bytes.toInt(r.getRow()) + " " 
					+ Bytes.toString(r.getValue(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_URI_BYTES))
					+ " " + Bytes.toString(r.getValue(Cw09Constants.CF_DETAILS_BYTES, Cw09Constants.QUAL_CONTENT_BYTES)));
			r = rs.next();
		}
		rs.close();
		
		Delete row1Del = new Delete(rowKey1);
		Delete row2Del = new Delete(rowKey2);
		ops.clear();
		ops.add(row1Del);
		ops.add(row2Del);
		res = table.batch(ops);
		for (int i=0; i<res.length; i++) {
			if (res[i] == null) {
				res[i] = "null (failure)";
			}
			System.out.println("results for deleting row " + i + " in batch : " + res[i].toString()); 
		}
		table.flushCommits();
		table.close();
		
		// create the texts index table
		tableName = Cw09Constants.CW09_INDEX_TABLE_BYTES;
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(Cw09Constants.CF_FREQUENCIES_BYTES);
		cfDes.setMaxVersions(1);
		if (indexCfFreqComp) {
			cfDes.setCompressionType(Compression.Algorithm.GZ);
		}
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes, startKey, endKey, numOfRegions);
		
		// create the duplicated texts index table -- this is for testing the overhead of hbase
		tableName = Cw09Constants.CW09_DUP_INDEX_TABLE_BYTES;
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(Cw09Constants.CF_FREQUENCIES_BYTES);
		cfDes.setMaxVersions(1);
		if (indexCfFreqComp) {
			cfDes.setCompressionType(Compression.Algorithm.GZ);
		}
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes, startKey, endKey, numOfRegions);
		
		// create the texts position vector table
		tableName = Cw09Constants.CW09_POSVEC_TABLE_BYTES;
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(Cw09Constants.CF_POSITIONS_BYTES);
		cfDes.setMaxVersions(1);
		if (posCfPosComp) {
			cfDes.setCompressionType(Compression.Algorithm.GZ);
		}
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes, startKey, endKey, numOfRegions);
		
		// create the term count table
		tableName = Cw09Constants.CW09_TERM_COUNT_TABLE_BYTES;
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
				
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(Cw09Constants.CF_FREQUENCIES_BYTES);
		cfDes.setMaxVersions(1);
		cfDes.setCompressionType(Compression.Algorithm.GZ);
		cfDes.setBloomFilterType(BloomType.ROW);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		// create the word-pair frequency count vector table
		tableName = Cw09Constants.CW09_PAIRFREQ_TABLE_BYTES;
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(Cw09Constants.CF_FREQUENCIES_BYTES);
		cfDes.setMaxVersions(1);
		if (pairFreqCfPosComp) {
			cfDes.setCompressionType(Compression.Algorithm.GZ);
		}
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		// create the synonym frequency count vector table
		tableName = Cw09Constants.CW09_SYNONYM_TABLE_BYTES;
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(Cw09Constants.CF_SCORES_BYTES);
		cfDes.setMaxVersions(1);
		if (synonymCfPosComp) {
			cfDes.setCompressionType(Compression.Algorithm.GZ);
		}
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		admin.close();
	}

}
