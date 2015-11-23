package iu.pti.hbaseapp.diglib;

import java.util.ArrayList;

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
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This application creates the HBase tables for the digital library.
 * <p/>
 * Usage: java iu.pti.hbaseapp.diglib.DigLibTableCreator
 * 
 * @author gaoxm
 */
public class DigLibTableCreator {
	
	/**
	 * Usage: java iu.pti.hbaseapp.diglib.DigLibTableCreator
	 * <br/>
	 * (No extra arguments needed.)
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// You need a configuration object to tell the client where to connect.
		// But don't worry, the defaults are pulled from the local config file.
		Configuration hbaseConfig = HBaseConfiguration.create();
		
		// create or enable table "bookBibTable"
		HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
		byte[] tableName = Bytes.toBytes(DigLibConstants.BOOK_BIB_TABLE_NAME);
		String cfMeta = DigLibConstants.BOOK_BIB_CF_METADATA;
		byte[] cfMetaBytes = Bytes.toBytes(cfMeta);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		HTableDescriptor tableDes = new HTableDescriptor(tableName);
		HColumnDescriptor cfDes = new HColumnDescriptor(cfMetaBytes);
		cfDes.setMaxVersions(1);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		HTable table = new HTable(hbaseConfig, tableName);
		byte[] title1Bytes = Bytes.toBytes("Computer Networks");
		byte[] title2Bytes = Bytes.toBytes("Art of Programming");
		byte[] author1Bytes = Bytes.toBytes("Larry Peterson, Bruce Davie");
		byte[] author2Bytes = Bytes.toBytes("Donald E. Knuth");
		byte[] qualAuthorBytes = Bytes.toBytes("authors");
		Put row1 = new Put(title1Bytes);
		row1.add(cfMetaBytes, qualAuthorBytes, author1Bytes);
		Put row2 = new Put(title2Bytes);
		row2.add(cfMetaBytes, qualAuthorBytes, author2Bytes);
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
		
		ResultScanner rs = table.getScanner(cfMetaBytes, qualAuthorBytes);
		System.out.println("scanning table " + tableName.toString() + "...");
		Result r = rs.next();
		while (r != null) {
			System.out.println(Bytes.toString(r.getRow()) + " " + Bytes.toString(r.getValue(cfMetaBytes, qualAuthorBytes)));
			r = rs.next();
		}
		rs.close();
		
		Delete row1Del = new Delete(title1Bytes);
		Delete row2Del = new Delete(title2Bytes);
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
		
		// create the book text table
		tableName = Bytes.toBytes(DigLibConstants.BOOK_TEXT_TABLE_NAME);
		String cfPages = DigLibConstants.BOOK_TEXT_CF_PAGES;
		byte[] cfPagesBytes = Bytes.toBytes(cfPages);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(cfPagesBytes);
		cfDes.setMaxVersions(1);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		// create the book image table
		tableName = Bytes.toBytes(DigLibConstants.BOOK_IMAGE_TABLE_NAME);
		String cfImage = DigLibConstants.BOOK_IMAGE_CF_IMAGE;
		byte[] cfImageBytes = Bytes.toBytes(cfImage);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(cfImageBytes);
		cfDes.setMaxVersions(1);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		// create the title index table
		tableName = Bytes.toBytes(DigLibConstants.TITLE_INDEX_TABLE_NAME);
		String cfTitle = DigLibConstants.CF_FREQUENCIES;
		byte[] cfTitleBytes = Bytes.toBytes(cfTitle);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(cfTitleBytes);
		cfDes.setMaxVersions(1);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		// create the category index table
		tableName = Bytes.toBytes(DigLibConstants.CATEGORY_INDEX_TABLE_NAME);
		String cfCate = DigLibConstants.CF_FREQUENCIES;
		byte[] cfCateBytes = Bytes.toBytes(cfCate);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(cfCateBytes);
		cfDes.setMaxVersions(1);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		// create the authors index table
		tableName = Bytes.toBytes(DigLibConstants.AUTHORS_INDEX_TABLE_NAME);
		String cfAuthors = DigLibConstants.CF_FREQUENCIES;
		byte[] cfAuthorsBytes = Bytes.toBytes(cfAuthors);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(cfAuthorsBytes);
		cfDes.setMaxVersions(1);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		// create the keywords index table
		tableName = Bytes.toBytes(DigLibConstants.KEYWORDS_INDEX_TABLE_NAME);
		String cfKeywords = DigLibConstants.CF_FREQUENCIES;
		byte[] cfKeywordsBytes = Bytes.toBytes(cfKeywords);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(cfKeywordsBytes);
		cfDes.setMaxVersions(1);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		// create the texts index table
		tableName = Bytes.toBytes(DigLibConstants.TEXTS_INDEX_TABLE_NAME);
		String cfTexts = DigLibConstants.CF_FREQUENCIES;
		byte[] cfTextsBytes = Bytes.toBytes(cfTexts);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(cfTextsBytes);
		cfDes.setMaxVersions(1);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		// create the texts position vector table
		tableName = Bytes.toBytes(DigLibConstants.TEXTS_POSVEC_TABLE_NAME);
		byte[] cfPosBytes = Bytes.toBytes(DigLibConstants.CF_POSITIONS);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(cfPosBytes);
		cfDes.setMaxVersions(1);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		// create the term document count table
		tableName = Bytes.toBytes(DigLibConstants.TERM_DOC_COUNT_TABLE_NAME);
		byte[] cfDocCountsBytes = Bytes.toBytes(DigLibConstants.CF_DOC_COUNTS);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		tableDes = new HTableDescriptor(tableName);
		cfDes = new HColumnDescriptor(cfDocCountsBytes);
		cfDes.setMaxVersions(1);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		admin.close();
	}
}
