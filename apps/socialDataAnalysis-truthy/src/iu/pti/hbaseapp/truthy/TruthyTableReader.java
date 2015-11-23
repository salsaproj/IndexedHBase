package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;
import iu.pti.hbaseapp.truthy.ConstantsTruthy;
import iu.pti.hbaseapp.truthy.TruthyHelpers;

import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A utility application for reading contents of Truthy HBase tables.
 * <p/>
 * Usage (arguments in '{}' are optional): java iu.pti.hbaseapp.truthy.TruthyTableReader [table name] [rows to read] {[starting row key]}
 * 
 * @author gaoxm
 */
public class TruthyTableReader {

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: java iu.pti.hbaseapp.truthy.TruthyTableReader <table name> <rows to read> [<starting row key>]");
			System.exit(1);
		}
		
		try {
			String tableName = args[0];
			int rows = Integer.valueOf(args[1]);
			String rowKey = null;
			if (args.length > 2) {
				rowKey = args[2];
			}
			
			if (tableName.startsWith(ConstantsTruthy.TWEET_TABLE_NAME)) {
				readTweetTable(tableName, rows, rowKey);
			} else if (tableName.startsWith(ConstantsTruthy.USER_TABLE_NAME)) {
				readUserTable(tableName, rows, rowKey);
			} else if (tableName.startsWith(ConstantsTruthy.USER_TWEETS_TABLE_NAME)) {
				readUserTweetTable(tableName, rows, rowKey);
			} else if (tableName.startsWith(ConstantsTruthy.TEXT_INDEX_TABLE_NAME)) {
				readTextIndexTable(tableName, rows, rowKey);
			} else if (tableName.startsWith(ConstantsTruthy.MEME_INDEX_TABLE_NAME) 
						|| tableName.startsWith(ConstantsTruthy.MEME_USER_INDEX_TABLE_NAME)) {
				readMemeIndexTable(tableName, rows, rowKey);
			} else if (tableName.startsWith(ConstantsTruthy.RETWEET_INDEX_TABLE_NAME)) {
				readRetweetIndexTable(tableName, rows, rowKey);
			} else if (tableName.startsWith(ConstantsTruthy.SNAME_INDEX_TABLE_NAME)) {
				readSnameIndexTable(tableName, rows, rowKey);
			} else if (tableName.startsWith(ConstantsTruthy.TIME_INDEX_TABLE_NAME)) {
			    readTimeIndexTable(tableName, rows, rowKey);			    
			} else if (tableName.startsWith(ConstantsTruthy.GEO_INDEX_TABLE_NAME)) {
			    readGeoIndexTable(tableName, rows, rowKey);
			} else {
				System.out.println("Error: non-existing table name " + tableName);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Read nRows rows from the tweet table.
	 * @param tableName
	 * @param nRows
	 * @param givenTweetId
	 * @throws Exception
	 */
	public static void readTweetTable(String tableName, int nRows, String givenTweetId) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable table = new HTable(hbaseConfig, Bytes.toBytes(tableName));
        int idx = tableName.indexOf("-") + 1;
        String month = tableName.substring(idx);
        boolean useBigInt = TruthyHelpers.checkIfb4June2015(month);
		Scan scan = new Scan();
		scan.addFamily(ConstantsTruthy.CF_DETAIL_BYTES);
		if (givenTweetId != null) {
		    byte[] rowKeyBytes = null;
		    if (useBigInt) {
		        rowKeyBytes = TruthyHelpers.getTweetIdBigIntBytes(givenTweetId);
		    } else {
		        rowKeyBytes = TruthyHelpers.getTweetIdBytes(givenTweetId);
		    }
			scan.setStartRow(rowKeyBytes);
		}
		
		ResultScanner rs = table.getScanner(scan);
		System.out.println("scanning table " + tableName + " on " + ConstantsTruthy.COLUMN_FAMILY_DETAIL + " ...");
		Result r = rs.next();
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		while (r != null && nRows-- > 0) {
			String tweetId = "";
			if (useBigInt) {
			    tweetId = TruthyHelpers.getTweetIDStrFromBigIntBytes(r.getRow());
			} else {
			    tweetId = TruthyHelpers.getTweetIDStrFromBytes(r.getRow());
			}
			System.out.println("------------" + tweetId + "------------");
			
			String text = Bytes.toString(r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_TEXT_BYTES));
			System.out.println("Text: " + text);
			
			long timeMilli = Bytes.toLong(r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_CREATE_TIME_BYTES));
			calTmp.setTimeInMillis(timeMilli);
			System.out.println("Create time: " + GeneralHelpers.getDateTimeString(calTmp));
			
			String memes = Bytes.toString(r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_ENTITIES_BYTES));
			System.out.println("memes: " + memes);
			
			String userId = TruthyHelpers.getUserIDStrFromBytes(r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, 
					ConstantsTruthy.QUAL_USER_ID_BYTES));
			System.out.println("User ID: " + userId);
			
			String userName = Bytes.toString(r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_USER_SNAME_BYTES));
			System.out.println("User screen name: " + userName);
		
			r = rs.next();
		}
		rs.close();
		table.close();
	}
	
	/**
	 * Read nRows rows from the user table.
	 * @param tableName
	 * @param nRows
	 * @param givenUserId
	 * @throws Exception
	 */
	public static void readUserTable(String tableName, int nRows, String givenUserId) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable table = new HTable(hbaseConfig, Bytes.toBytes(tableName));
        int idx = tableName.indexOf("-") + 1;
        String month = tableName.substring(idx);
        boolean useBigInt = TruthyHelpers.checkIfb4June2015(month);     		
		Scan scan = new Scan();
		scan.addFamily(ConstantsTruthy.CF_DETAIL_BYTES);
		if (givenUserId != null) {
			byte[] uidBytes = TruthyHelpers.getUserIdBytes(givenUserId);
			byte[] tidBytes = new byte[8];
			scan.setStartRow(TruthyHelpers.combineBytes(uidBytes, tidBytes));
		}
		ResultScanner rs = table.getScanner(scan);		
		System.out.println("scanning table " + tableName + " on " + ConstantsTruthy.COLUMN_FAMILY_DETAIL + " ...");
		Result r = rs.next();
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		while (r != null && nRows-- > 0) {
			byte[] userTweetIdBytes = r.getRow();
			int uidLen = userTweetIdBytes.length - 8;
			byte[] userIdBytes = Arrays.copyOfRange(userTweetIdBytes, 0, uidLen);
			byte[] tweetIdBytes = Arrays.copyOfRange(userTweetIdBytes, uidLen, 16);
			String userId = TruthyHelpers.getUserIDStrFromBytes(userIdBytes);
            String tweetId = "";
            if (useBigInt) {
                tweetId = TruthyHelpers
                        .getTweetIDStrFromBigIntBytes(r.getRow());
            } else {
                tweetId = TruthyHelpers.getTweetIDStrFromBytes(r.getRow());
            }		
			System.out.println("------------" + userId + "-" + tweetId + "------------");
			
			String screenName = Bytes.toString(r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_SCREEN_NAME_BYTES));
			System.out.println("Screen name: " + screenName);
			
			long timeMilli = Bytes.toLong(r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_CREATE_TIME_BYTES));
			calTmp.setTimeInMillis(timeMilli);
			System.out.println("Create time: " + GeneralHelpers.getDateTimeString(calTmp));
			
			String userDesc = Bytes.toString(r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_DESCRIPTION_BYTES));
			System.out.println("Description: " + userDesc);
			
			long statusCount = Bytes.toLong(r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_STATUS_COUNT_BYTES));
			System.out.println("User status count: " + statusCount);
      
      String friendsCountInString = Bytes.toString(r.getValue(ConstantsTruthy.CF_DETAIL_BYTES, Bytes.toBytes("friends_count")));
      System.out.println("User friends_count: " + friendsCountInString);
			
			r = rs.next();
		}
		rs.close();
		table.close();
	}
	
	/**
	 * Read nRows rows from the user-tweet index table.
	 * @param tableName
	 * @param nRows
	 * @param givenUserId
	 * @throws Exception
	 */
	public static void readUserTweetTable(String tableName, int nRows, String givenUserId) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable table = new HTable(hbaseConfig, Bytes.toBytes(tableName));
        int idx = tableName.indexOf("-") + 1;
        String month = tableName.substring(idx);
        boolean useBigInt = TruthyHelpers.checkIfb4June2015(month);     		
		Scan scan = new Scan();
		scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
		scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
		if (givenUserId != null) {
			byte[] rowKeyBytes = TruthyHelpers.getUserIdBytes(givenUserId);
			scan.setStartRow(rowKeyBytes);
		}
		ResultScanner rs = table.getScanner(scan);
		System.out.println("scanning table " + tableName + " on " + ConstantsTruthy.COLUMN_FAMILY_TWEETS + " ...");
		Result r = rs.next();
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		while (r != null && nRows-- > 0) {
			String userId = TruthyHelpers.getUserIDStrFromBytes(r.getRow());
			System.out.println("------------" + userId + "------------");
			
			int limit = 20;
			for (KeyValue kv : r.list()) {
				byte[] tweetIdBytes = kv.getQualifier();
	            String tweetId = "";
	            if (useBigInt) {
	                tweetId = TruthyHelpers
	                        .getTweetIDStrFromBigIntBytes(tweetIdBytes);
	            } else {
	                tweetId = TruthyHelpers.getTweetIDStrFromBytes(tweetIdBytes);
	            }   
				System.out.println("Tweet ID: " + tweetId);
				
				calTmp.setTimeInMillis(kv.getTimestamp());
				String createTime = GeneralHelpers.getDateTimeString(calTmp);
				System.out.println("Create Time: " + createTime);
				
				if (limit-- <= 0) {
					break;
				}
			}
			
			r = rs.next();
		}
		rs.close();
		table.close();
	}
	
	/**
	 * Read nRows rows from the text-tweet index table.
	 * @param tableName
	 * @param nRows
	 * @param givenText
	 * @throws Exception
	 */
	public static void readTextIndexTable(String tableName, int nRows, String givenText) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable table = new HTable(hbaseConfig, Bytes.toBytes(tableName));
        int idx = tableName.indexOf("-") + 1;
        String month = tableName.substring(idx);
        boolean useBigInt = TruthyHelpers.checkIfb4June2015(month);     		
		Scan scan = new Scan();
		scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
		scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
		if (givenText != null) {
			byte[] rowKeyBytes = Bytes.toBytes(givenText);
			scan.setStartRow(rowKeyBytes);
		}
		ResultScanner rs = table.getScanner(scan);
		System.out.println("scanning table " + tableName + " on " + ConstantsTruthy.COLUMN_FAMILY_TWEETS + " ...");
		Result r = rs.next();
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		while (r != null && nRows-- > 0) {
			String word = Bytes.toString(r.getRow());
			System.out.println("------------" + word + "------------");
			
			int limit = 20;
			for (KeyValue kv : r.list()) {
				byte[] tweetIdBytes = kv.getQualifier();
                String tweetId = "";
                if (useBigInt) {
                    tweetId = TruthyHelpers
                            .getTweetIDStrFromBigIntBytes(tweetIdBytes);
                } else {
                    tweetId = TruthyHelpers.getTweetIDStrFromBytes(tweetIdBytes);
                }  				
				System.out.println("Tweet ID: " + tweetId);
				
				calTmp.setTimeInMillis(kv.getTimestamp());
				String createTime = GeneralHelpers.getDateTimeString(calTmp);
				System.out.println("Create Time: " + createTime);
				
				if (limit-- <= 0) {
					break;
				}
			}
			
			r = rs.next();
		}
		rs.close();
		table.close();
	}
	
	/**
	 * Read nRows rows from the meme-tweet index table.
	 * @param tableName
	 * @param nRows
	 * @param givenMeme
	 * @throws Exception
	 */
	public static void readMemeIndexTable(String tableName, int nRows, String givenMeme) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable table = new HTable(hbaseConfig, Bytes.toBytes(tableName));
        int idx = tableName.indexOf("-") + 1;
        String month = tableName.substring(idx);
        boolean useBigInt = TruthyHelpers.checkIfb4June2015(month);     		
		Scan scan = new Scan();
		scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
		scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
		if (givenMeme != null) {
			byte[] rowKeyBytes = Bytes.toBytes(givenMeme);
			scan.setStartRow(rowKeyBytes);
			//scan.setStopRow(rowKeyBytes);
		}
		ResultScanner rs = table.getScanner(scan);
		System.out.println("scanning table " + tableName + " on " + ConstantsTruthy.COLUMN_FAMILY_TWEETS + " ...");
		Result r = rs.next();
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		long ltStart = -1;
		long ltEnd = -1;
		int nBatches = nRows;
		if (nRows == 1) {
			nBatches = Integer.MAX_VALUE;
		}
		int valueCount = 0;
		boolean firstBatch = true;
		while (r != null && nBatches-- > 0) {
			String meme = Bytes.toString(r.getRow());
			if (nRows == 1) {
				if (firstBatch) {
					System.out.println("------------" + meme + "------------");
				}
			} else {
				System.out.println("------------" + meme + "------------");
			}
			
			int limit = 20;
			for (KeyValue kv : r.list()) {
				valueCount++;
				byte[] tweetIdBytes = kv.getQualifier();
                String tweetId = "";
                if (useBigInt) {
                    tweetId = TruthyHelpers
                            .getTweetIDStrFromBigIntBytes(tweetIdBytes);
                } else {
                    tweetId = TruthyHelpers.getTweetIDStrFromBytes(tweetIdBytes);
                }  				
				if (limit > 0 && (nRows != 1 || firstBatch)) {
					System.out.println("Tweet ID: " + tweetId);	
				}
				
				calTmp.setTimeInMillis(kv.getTimestamp());
				String createTime = GeneralHelpers.getDateTimeString(calTmp);
				if (limit > 0 && (nRows != 1 || firstBatch)) {
					System.out.println("Create Time: " + createTime);
				}
				
				if (tableName.startsWith(ConstantsTruthy.MEME_USER_INDEX_TABLE_NAME)) {
					byte[] userIdBytes = kv.getValue();
					String userId = TruthyHelpers.getUserIDStrFromBytes(userIdBytes);
					if (limit > 0 && (nRows != 1 || firstBatch)) {
						System.out.println("User ID: " + userId);
					}
				}
				
				long time = kv.getTimestamp();
				if (ltStart == -1 || time < ltStart) {
					ltStart = time;
				}
				
				if (ltEnd == -1 || time > ltEnd) {
					ltEnd = time;
				}
				
				if (limit-- <= 0 && nRows != 1) {
					break;
				}
			}
			
			r = rs.next();
			firstBatch = false;
		}
		rs.close();
		table.close();
		
		calTmp.setTimeInMillis(ltStart);
		System.out.println("Life time start: " + GeneralHelpers.getDateTimeString(calTmp));
		calTmp.setTimeInMillis(ltEnd);
		System.out.println("Life time end: " + GeneralHelpers.getDateTimeString(calTmp));
		System.out.println("Life time: " + (ltEnd - ltStart) + " ms");
		System.out.println("Total number of values scanned: " + valueCount);
	}
	
	/**
	 * Read nRows rows from the retweet index table.
	 * @param tableName
	 * @param nRows
	 * @param givenTweetId
	 * @throws Exception
	 */
	public static void readRetweetIndexTable(String tableName, int nRows, String givenTweetId) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable table = new HTable(hbaseConfig, Bytes.toBytes(tableName));
        int idx = tableName.indexOf("-") + 1;
        String month = tableName.substring(idx);
        boolean useBigInt = TruthyHelpers.checkIfb4June2015(month);		
		Scan scan = new Scan();
		scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
		scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
		if (givenTweetId != null) {
            byte[] rowKeyBytes = null;
            if (useBigInt) {
                rowKeyBytes = TruthyHelpers.getTweetIdBigIntBytes(givenTweetId);
            } else {
                rowKeyBytes = TruthyHelpers.getTweetIdBytes(givenTweetId);
            }
			scan.setStartRow(rowKeyBytes);
		}
		ResultScanner rs = table.getScanner(scan);
		System.out.println("scanning table " + tableName + " on " + ConstantsTruthy.COLUMN_FAMILY_TWEETS + " ...");
		Result r = rs.next();
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		while (r != null && nRows-- > 0) {
            String retweetedId = "";
            if (useBigInt) {
                retweetedId = TruthyHelpers
                        .getTweetIDStrFromBigIntBytes(r.getRow());
            } else {
                retweetedId = TruthyHelpers.getTweetIDStrFromBytes(r.getRow());
            }  		    
			System.out.println("------------" + retweetedId + "------------");
			
			int limit = 20;
			for (KeyValue kv : r.list()) {
				byte[] tweetIdBytes = kv.getQualifier();
                String tweetId = "";
                if (useBigInt) {
                    tweetId = TruthyHelpers
                            .getTweetIDStrFromBigIntBytes(tweetIdBytes);
                } else {
                    tweetId = TruthyHelpers.getTweetIDStrFromBytes(tweetIdBytes);
                }  				
				System.out.println("Tweet ID: " + tweetId);
				
				calTmp.setTimeInMillis(kv.getTimestamp());
				String createTime = GeneralHelpers.getDateTimeString(calTmp);
				System.out.println("Create Time: " + createTime);
				
				if (limit-- <= 0) {
					break;
				}
			}
			
			r = rs.next();
		}
		rs.close();
		table.close();
	}
	
	/**
	 * Read nRows rows from the screen name - user index table.
	 * @param tableName
	 * @param nRows
	 * @param givenUserSname
	 * @throws Exception
	 */
	public static void readSnameIndexTable(String tableName, int nRows, String givenUserSname) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable table = new HTable(hbaseConfig, Bytes.toBytes(tableName));
        int idx = tableName.indexOf("-") + 1;
        String month = tableName.substring(idx);
        boolean useBigInt = TruthyHelpers.checkIfb4June2015(month);     		
		Scan scan = new Scan();
		scan.addFamily(ConstantsTruthy.CF_USERS_BYTES);
		scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
		if (givenUserSname != null) {
			byte[] rowKeyBytes = Bytes.toBytes(givenUserSname.toLowerCase());
			scan.setStartRow(rowKeyBytes);
		}
		ResultScanner rs = table.getScanner(scan);
		System.out.println("scanning table " + tableName + " on " + ConstantsTruthy.COLUMN_FAMILY_USERS + " ...");
		Result r = rs.next();
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		while (r != null && nRows-- > 0) {
			String sname = Bytes.toString(r.getRow());
			System.out.println("------------" + sname + "------------");
			
			int limit = 20;
			for (KeyValue kv : r.list()) {
				byte[] userIdBytes = kv.getQualifier();
				String userId = TruthyHelpers.getUserIDStrFromBytes(userIdBytes);
				System.out.println("User ID: " + userId);
				
				calTmp.setTimeInMillis(kv.getTimestamp());
				String createTime = GeneralHelpers.getDateTimeString(calTmp);
				System.out.println("Create Time: " + createTime);
				
				if (limit-- <= 0) {
					break;
				}
			}
			
			r = rs.next();
		}
		rs.close();
		table.close();
	}
	
	/**
	 * Read nRows rows from the time index table.
	 * @param tableName
	 * @param nRows
	 * @param givenTweetId
	 * @throws Exception
	 */	
	public static void readTimeIndexTable(String tableName, int nRows,
			String rowKey)  throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable table = new HTable(hbaseConfig, Bytes.toBytes(tableName));
        int idx = tableName.indexOf("-") + 1;
        String month = tableName.substring(idx);
        boolean useBigInt = TruthyHelpers.checkIfb4June2015(month);     		
		Scan scan = new Scan();
		scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
		scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
		if (rowKey != null) {
			byte[] rowKeyBytes = Bytes.toBytes(Long.valueOf(rowKey));
			scan.setStartRow(rowKeyBytes);
		}
		ResultScanner rs = table.getScanner(scan);
		System.out.println("scanning table " + tableName + " on " + ConstantsTruthy.COLUMN_FAMILY_TWEETS + " ...");
		Result r = rs.next();
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		while (r != null && nRows-- > 0) {
			long timeIndex = Bytes.toLong(r.getRow());			
			System.out.println("------------" + timeIndex + "------------");
			calTmp.setTimeInMillis(timeIndex);
			System.out.println("Original Create time: " + GeneralHelpers.getDateTimeString(calTmp));
			
			int limit = 20;
			for (KeyValue kv : r.list()) {
				byte[] tweetIdBytes = kv.getQualifier();
                String tweetId = "";
                if (useBigInt) {
                    tweetId = TruthyHelpers
                            .getTweetIDStrFromBigIntBytes(tweetIdBytes);
                } else {
                    tweetId = TruthyHelpers.getTweetIDStrFromBytes(tweetIdBytes);
                }  				
				System.out.println("Tweet ID: " + tweetId);
				
				calTmp.setTimeInMillis(kv.getTimestamp());
				String createTime = GeneralHelpers.getDateTimeString(calTmp);
				System.out.println("Index Create Time: " + createTime);
				
				if (limit-- <= 0) {
					break;
				}
			}
			
			r = rs.next();
		}
		rs.close();
		table.close();		
	}
	
	/**
	 * Read nRows rows from the geo-tweet index table.
	 * @param tableName
	 * @param nRows
	 * @param givenGeo
	 * @throws Exception
	 */
	public static void readGeoIndexTable(String tableName, int nRows, String givenGeo) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable table = new HTable(hbaseConfig, Bytes.toBytes(tableName));
        int idx = tableName.indexOf("-") + 1;
        String month = tableName.substring(idx);
        boolean useBigInt = TruthyHelpers.checkIfb4June2015(month);     		
		Scan scan = new Scan();
		scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
		scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
		if (givenGeo != null) {
			byte[] rowKeyBytes = Bytes.toBytes(givenGeo);
			scan.setStartRow(rowKeyBytes);
			//scan.setStopRow(rowKeyBytes);
		}
		ResultScanner rs = table.getScanner(scan);
		System.out.println("scanning table " + tableName + " on " + ConstantsTruthy.COLUMN_FAMILY_TWEETS + " ...");
		Result r = rs.next();
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		long ltStart = -1;
		long ltEnd = -1;
		int nBatches = nRows;
		if (nRows == 1) {
			nBatches = Integer.MAX_VALUE;
		}
		int valueCount = 0;
		boolean firstBatch = true;
		while (r != null && nBatches-- > 0) {
			String geo = Bytes.toString(r.getRow());
			if (nRows == 1) {
				if (firstBatch) {
					System.out.println("------------" + geo + "------------");
				}
			} else {
				System.out.println("------------" + geo + "------------");
			}
			
			int limit = 20;
			for (KeyValue kv : r.list()) {
				valueCount++;
				byte[] tweetIdBytes = kv.getQualifier();
                String tweetId = "";
                if (useBigInt) {
                    tweetId = TruthyHelpers
                            .getTweetIDStrFromBigIntBytes(tweetIdBytes);
                } else {
                    tweetId = TruthyHelpers.getTweetIDStrFromBytes(tweetIdBytes);
                }  				
				if (limit > 0 && (nRows != 1 || firstBatch)) {
					System.out.println("Tweet ID: " + tweetId);	
				}
				
				calTmp.setTimeInMillis(kv.getTimestamp());
				String createTime = GeneralHelpers.getDateTimeString(calTmp);
				if (limit > 0 && (nRows != 1 || firstBatch)) {
					System.out.println("Create Time: " + createTime);
				}
				
				if (tableName.startsWith(ConstantsTruthy.GEO_USER_INDEX_TABLE_NAME)) {
					byte[] userIdBytes = kv.getValue();
					String userId = TruthyHelpers.getUserIDStrFromBytes(userIdBytes);
					if (limit > 0 && (nRows != 1 || firstBatch)) {
						System.out.println("User ID: " + userId);
					}
				}
				
				long time = kv.getTimestamp();
				if (ltStart == -1 || time < ltStart) {
					ltStart = time;
				}
				
				if (ltEnd == -1 || time > ltEnd) {
					ltEnd = time;
				}
				
				if (limit-- <= 0 && nRows != 1) {
					break;
				}
			}
			
			r = rs.next();
			firstBatch = false;
		}
		rs.close();
		table.close();
		
		calTmp.setTimeInMillis(ltStart);
		System.out.println("Life time start: " + GeneralHelpers.getDateTimeString(calTmp));
		calTmp.setTimeInMillis(ltEnd);
		System.out.println("Life time end: " + GeneralHelpers.getDateTimeString(calTmp));
		System.out.println("Life time: " + (ltEnd - ltStart) + " ms");
		System.out.println("Total number of values scanned: " + valueCount);
	}
}
