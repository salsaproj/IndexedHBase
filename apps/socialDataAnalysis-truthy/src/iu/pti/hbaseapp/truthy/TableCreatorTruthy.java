package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.GeneralHelpers;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * An application for managing Truthy tables.
 * <p/>
 * Usage (arguments in '{}' are optional):
 * <br/> java iu.pti.hbaseapp.truthy.TableCreatorTruthy [command] [arguments]
 * <br/> Where '[command] [arguments]' could be one of the following:
 * <br/> create-tables [month] {[number of regions for the tweet table]}
 * <br/> delete-tables [month]
 * <br/> get-region-bounds [table name]
 * <br/> get-system-status
		
 * @author gaoxm
 */
public class TableCreatorTruthy {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			String command = args[0];
			TableCreatorTruthy tct = new TableCreatorTruthy();
			if (command.equals("create-tables")) {
				String month = args[1];
				int numRegions = -1;
				String uidGzPath = null;
				if (args.length > 2) {
					numRegions = Integer.valueOf(args[2]);
					uidGzPath = args[3];
				}
				tct.createTables(month, numRegions, uidGzPath);
			} else if (command.equals("create-single-table")) {
                String month = args[1];
                int numRegions = -1;
                String tableName = null;
                if (args.length > 2) {
                    numRegions = Integer.valueOf(args[2]);
                    tableName = args[3];
                }
                tct.createSingleTable(month, numRegions, tableName);
            } else if (command.equals("delete-tables")) {
				String month = args[1];
				tct.deleteTables(month);
			} else if (command.equals("get-system-status")) {
				tct.getSystemStatus();
			} else if (command.equals("get-region-bounds")) {
				String tableName = args[1];
				tct.getRegionBounds(tableName);
			} else if (command.equals("sort-uid")) {
				String jsonGzPath = args[1];
				String uidPath = args[2];
				tct.sortUidInFile(jsonGzPath, uidPath);
			} else if (command.equals("demo-region-split-keys")) {
				String uidGzPath = args[1];
				int nRegions = Integer.valueOf(args[2]);
				tct.demoRegionSplitKeys(uidGzPath, nRegions);
			} else if (command.equals("region-info-by-rowkey")) {
				String tableName = args[1];
				String rowkey = args[2];
				tct.getRegionInfoByRowKey(tableName, rowkey);
			} else {
				usage();
				System.exit(1);
			}
		} catch (Exception e) {
			e.printStackTrace();
			usage();
			System.exit(1);
		}
	}

	protected static void usage() {
		System.out.println("Usage: java iu.pti.hbaseapp.truthy.TableCreatorTruthy <command> <arguments>");
		System.out.println("	Where '<command> <arguments>' could be one of the following:");
		System.out.println("	create-tables <month> [<number of regions for the user table> <.gz file for sorted user IDs by bytes>]");
		System.out.println("	delete-tables <month>");
		System.out.println("	get-region-bounds <table name>");
		System.out.println("	get-system-status");
		System.out.println("	sort-uid <.json.gz file path> <user ID .gz file path>");
		System.out.println("	demo-region-split-keys <.gz sorted user ID file path> <number of regions>");
		System.out.println("	region-info-by-rowkey <table name> <row key>");
	}
	
	public void getSystemStatus() throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
		
		ClusterStatus status = admin.getClusterStatus();
		System.out.println("server count: " + status.getServersSize() + ", dead servers: " + status.getDeadServers() 
				+ ", avg. load: " + status.getAverageLoad());
		System.out.println();
		for (ServerName serverName : status.getServers()) {
			System.out.println("Regions on " + serverName.toString() + ":");
			
			HServerLoad serverLoad = status.getLoad(serverName);
			for (Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
				RegionLoad regionLoad = entry.getValue();
				String regionName = Bytes.toString(entry.getKey());
				int idx = regionName.indexOf(',');
				String tableName = regionName.substring(0, idx);
				System.out.println("\t" + tableName + ": " + regionLoad.getStorefiles() + " files in " + 
						regionLoad.getStorefileSizeMB() + "M.");
			}
			System.out.println("\t" + serverLoad.getStorefileSizeInMB());
		}
		admin.close();
	}

	public void createTables(String month, int numRegions, String uidGzPath) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
		byte[] tweetTableName =  Bytes.toBytes(ConstantsTruthy.TWEET_TABLE_NAME + "-" + month);
		byte[] userTableName = Bytes.toBytes(ConstantsTruthy.USER_TABLE_NAME + "-" + month);
		byte[] userTweetsTableName = Bytes.toBytes(ConstantsTruthy.USER_TWEETS_TABLE_NAME + "-" + month);
		byte[] textIndexTableName = Bytes.toBytes(ConstantsTruthy.TEXT_INDEX_TABLE_NAME + "-" + month);
		byte[] timeIndexTableName = Bytes.toBytes(ConstantsTruthy.TIME_INDEX_TABLE_NAME + "-" + month);
		byte[] memeIndexTableName = Bytes.toBytes(ConstantsTruthy.MEME_INDEX_TABLE_NAME + "-" + month);
		byte[] retweetIndexTableName = Bytes.toBytes(ConstantsTruthy.RETWEET_INDEX_TABLE_NAME + "-" + month);
		byte[] snameIndexTableName = Bytes.toBytes(ConstantsTruthy.SNAME_INDEX_TABLE_NAME + "-" + month);
		byte[] geoIndexTableName = Bytes.toBytes(ConstantsTruthy.GEO_INDEX_TABLE_NAME + "-" + month);
		
		if (admin.tableExists(tweetTableName) || admin.tableExists(userTableName) || admin.tableExists(userTweetsTableName) 
				|| admin.tableExists(textIndexTableName) || admin.tableExists(memeIndexTableName) || admin.tableExists(retweetIndexTableName) 
				|| admin.tableExists(snameIndexTableName) || admin.tableExists(timeIndexTableName) || admin.tableExists(geoIndexTableName)) {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			boolean toDelete = false;
			while (true) {
				System.out.println("Tables for " + month + " already exist. Do you want to delete the existing tables and recreate new ones?"
						+ "(yes or no):");
				String input = br.readLine();
				if (input.equalsIgnoreCase("yes")) {
					toDelete = true;
					break;
				} else if (input.equalsIgnoreCase("no")) {
					toDelete = false;
					break;
				}
			}
			br.close();
			if (!toDelete) {
				admin.close();
				return;
			} else {
				deleteTables(month);
			}
		}
		
		System.out.println("Creating " + ConstantsTruthy.TWEET_TABLE_NAME + "-" + month + "...");
		HTableDescriptor tableDes = new HTableDescriptor(tweetTableName);
		tableDes.setMaxFileSize(25 * ConstantsTruthy.SIZE_1G);
		HColumnDescriptor cfDes = new HColumnDescriptor(ConstantsTruthy.CF_DETAIL_BYTES);
		cfDes.setMaxVersions(1);
		cfDes.setCompressionType(Compression.Algorithm.GZ);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		System.out.println("Creating " + ConstantsTruthy.USER_TABLE_NAME + "-" + month + "...");
		tableDes = new HTableDescriptor(userTableName);
		tableDes.setMaxFileSize(25 * ConstantsTruthy.SIZE_1G);
		cfDes = new HColumnDescriptor(ConstantsTruthy.CF_DETAIL_BYTES);
		cfDes.setMaxVersions(1);
		cfDes.setCompressionType(Compression.Algorithm.GZ);
		tableDes.addFamily(cfDes);
		if (numRegions <= 0) {
			admin.createTable(tableDes);
		} else {
			tableDes.setValue(HTableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
			byte[][] splitKeys = getRegionSplitKeysFromUidFile(uidGzPath, numRegions);
			admin.createTable(tableDes, splitKeys);
		}
		
		System.out.println("Creating " + ConstantsTruthy.USER_TWEETS_TABLE_NAME + "-" + month + "...");		
		tableDes = new HTableDescriptor(userTweetsTableName);
		tableDes.setMaxFileSize(10 * ConstantsTruthy.SIZE_1G);
		cfDes = new HColumnDescriptor(ConstantsTruthy.CF_TWEETS_BYTES);
		cfDes.setMaxVersions(1);
		cfDes.setCompressionType(Compression.Algorithm.GZ);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);		
		
		System.out.println("Creating " + ConstantsTruthy.TEXT_INDEX_TABLE_NAME + "-" + month + "...");
		tableDes = new HTableDescriptor(textIndexTableName);
		tableDes.setMaxFileSize(15 * ConstantsTruthy.SIZE_1G);
		cfDes = new HColumnDescriptor(ConstantsTruthy.CF_TWEETS_BYTES);
		cfDes.setMaxVersions(1);
		cfDes.setCompressionType(Compression.Algorithm.GZ);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		System.out.println("Creating " + ConstantsTruthy.TIME_INDEX_TABLE_NAME + "-" + month + "...");
		tableDes = new HTableDescriptor(timeIndexTableName);
		tableDes.setMaxFileSize(10 * ConstantsTruthy.SIZE_1G);
		cfDes = new HColumnDescriptor(ConstantsTruthy.CF_TWEETS_BYTES);
		cfDes.setMaxVersions(1);
		cfDes.setCompressionType(Compression.Algorithm.GZ);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		System.out.println("Creating " + ConstantsTruthy.MEME_INDEX_TABLE_NAME + "-" + month + "...");
		tableDes = new HTableDescriptor(memeIndexTableName);
		tableDes.setMaxFileSize(10 * ConstantsTruthy.SIZE_1G);
		cfDes = new HColumnDescriptor(ConstantsTruthy.CF_TWEETS_BYTES);
		cfDes.setMaxVersions(1);
		cfDes.setCompressionType(Compression.Algorithm.GZ);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		System.out.println("Creating " + ConstantsTruthy.RETWEET_INDEX_TABLE_NAME + "-" + month + "...");
		tableDes = new HTableDescriptor(retweetIndexTableName);
		tableDes.setMaxFileSize(10 * ConstantsTruthy.SIZE_1G);
		cfDes = new HColumnDescriptor(ConstantsTruthy.CF_TWEETS_BYTES);
		cfDes.setMaxVersions(1);
		cfDes.setCompressionType(Compression.Algorithm.GZ);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		System.out.println("Creating " + ConstantsTruthy.SNAME_INDEX_TABLE_NAME + "-" + month + "...");
		tableDes = new HTableDescriptor(snameIndexTableName);
		tableDes.setMaxFileSize(10 * ConstantsTruthy.SIZE_1G);
		cfDes = new HColumnDescriptor(ConstantsTruthy.CF_USERS_BYTES);
		cfDes.setMaxVersions(1);
		cfDes.setCompressionType(Compression.Algorithm.GZ);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		System.out.println("Creating " + ConstantsTruthy.GEO_INDEX_TABLE_NAME + "-" + month + "...");
		tableDes = new HTableDescriptor(geoIndexTableName);
		tableDes.setMaxFileSize(10 * ConstantsTruthy.SIZE_1G);
		cfDes = new HColumnDescriptor(ConstantsTruthy.CF_TWEETS_BYTES);
		cfDes.setMaxVersions(1);
		cfDes.setCompressionType(Compression.Algorithm.GZ);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		admin.close();
	}
	
    public void createSingleTable(String month, int numRegions, String tableName) throws Exception {
        Configuration hbaseConfig = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
        byte[] targetTableName = Bytes.toBytes(tableName + "-" + month);
        
        if (admin.tableExists(targetTableName)) {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            boolean toDelete = false;
            while (true) {
                System.out.println("Tables for " + month + " already exist. Do you want to delete the existing tables and recreate new ones?"
                        + "(yes or no):");
                String input = br.readLine();
                if (input.equalsIgnoreCase("yes")) {
                    toDelete = true;
                    break;
                } else if (input.equalsIgnoreCase("no")) {
                    toDelete = false;
                    break;
                }
            }
            br.close();
            if (!toDelete) {
                admin.close();
                return;
            } else {
                deleteTables(month);
            }
        }
        
        System.out.println("Creating " + tableName + "-" + month + "...");
        HTableDescriptor tableDes = new HTableDescriptor(targetTableName);
        
        HColumnDescriptor cfDes = null;
        if (tableName.contains("IndexTable")) {
            cfDes = new HColumnDescriptor(ConstantsTruthy.CF_TWEETS_BYTES);
            tableDes.setMaxFileSize(10 * ConstantsTruthy.SIZE_1G);
        } else if (tableName.contains(ConstantsTruthy.USER_TABLE_NAME)) {
            cfDes = new HColumnDescriptor(ConstantsTruthy.CF_USERS_BYTES);
            tableDes.setMaxFileSize(25 * ConstantsTruthy.SIZE_1G);
        } else {
            cfDes = new HColumnDescriptor(ConstantsTruthy.CF_DETAIL_BYTES);
            tableDes.setMaxFileSize(25 * ConstantsTruthy.SIZE_1G);
        }
        cfDes.setMaxVersions(1);
        cfDes.setCompressionType(Compression.Algorithm.GZ);
        tableDes.addFamily(cfDes);
        admin.createTable(tableDes);        
        
        admin.close();
    }	
	
	public void deleteTables(String month) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
				
		String tweetTableName = ConstantsTruthy.TWEET_TABLE_NAME + "-" + month;
		System.out.println("deleting " + tweetTableName + "...");
		byte[] tweetTableBytes = Bytes.toBytes(tweetTableName);
		if (admin.tableExists(tweetTableBytes)) {
			if (admin.isTableEnabled(tweetTableBytes)) {
				admin.disableTable(tweetTableBytes);
			}
			admin.deleteTable(tweetTableBytes);
		}
		 
		String userTableName = ConstantsTruthy.USER_TABLE_NAME + "-" + month;
		System.out.println("deleting " + userTableName + "...");
		byte[] userTableBytes = Bytes.toBytes(userTableName);
		if (admin.tableExists(userTableBytes)) {
			if (admin.isTableEnabled(userTableBytes)) {
				admin.disableTable(userTableBytes);
			}
			admin.deleteTable(userTableBytes);
		}
		
		String userTweetsTableName = ConstantsTruthy.USER_TWEETS_TABLE_NAME + "-" + month;
		System.out.println("deleting " + userTweetsTableName + "...");
		byte[] userTweetsTableBytes = Bytes.toBytes(userTweetsTableName);
		if (admin.tableExists(userTweetsTableBytes)) {
			if (admin.isTableEnabled(userTweetsTableBytes)) {
				admin.disableTable(userTweetsTableBytes);
			}
			admin.deleteTable(userTweetsTableBytes);
		}
		
		String textIndexTableName = ConstantsTruthy.TEXT_INDEX_TABLE_NAME + "-" + month;
		System.out.println("deleting " + textIndexTableName + "...");
		byte[] textIndexTableBytes = Bytes.toBytes(textIndexTableName);
		if (admin.tableExists(textIndexTableBytes)) {
			if (admin.isTableEnabled(textIndexTableBytes)) {
				admin.disableTable(textIndexTableBytes);
			}
			admin.deleteTable(textIndexTableBytes);
		}
		
		String timeIndexTableName = ConstantsTruthy.TIME_INDEX_TABLE_NAME + "-" + month;
		System.out.println("deleting " + timeIndexTableName + "...");
		byte[] timeIndexTableBytes = Bytes.toBytes(timeIndexTableName);
		if (admin.tableExists(timeIndexTableBytes)) {
			if (admin.isTableEnabled(timeIndexTableBytes)) {
				admin.disableTable(timeIndexTableBytes);
			}
			admin.deleteTable(timeIndexTableBytes);
		}
		
		// delete the meme index table
		String memeIndexTableName = ConstantsTruthy.MEME_INDEX_TABLE_NAME + "-" + month;
		System.out.println("deleting " + memeIndexTableName + "...");
		byte[] memeIndexTableBytes = Bytes.toBytes(memeIndexTableName);
		if (admin.tableExists(memeIndexTableBytes)) {
			if (admin.isTableEnabled(memeIndexTableBytes)) {
				admin.disableTable(memeIndexTableBytes);	
			}			
			admin.deleteTable(memeIndexTableBytes);
		}
		
		String retweetIndexTableName = ConstantsTruthy.RETWEET_INDEX_TABLE_NAME + "-" + month;
		System.out.println("deleting " + retweetIndexTableName + "...");
		byte[] retweetIndexTableBytes = Bytes.toBytes(retweetIndexTableName);
		if (admin.tableExists(retweetIndexTableBytes)) {
			if (admin.isTableEnabled(retweetIndexTableBytes)) {
				admin.disableTable(retweetIndexTableBytes);
			}
			admin.deleteTable(retweetIndexTableBytes);
		}
		
		String snameIndexTableName = ConstantsTruthy.SNAME_INDEX_TABLE_NAME + "-" + month;
		System.out.println("deleting " + snameIndexTableName + "...");
		byte[] snameIndexTableBytes = Bytes.toBytes(snameIndexTableName);
		if (admin.tableExists(snameIndexTableBytes)) {
			if (admin.isTableEnabled(snameIndexTableBytes)) {
				admin.disableTable(snameIndexTableBytes);
			}
			admin.deleteTable(snameIndexTableBytes);
		}
		
		String geoIndexTableName = ConstantsTruthy.GEO_INDEX_TABLE_NAME + "-" + month;
		System.out.println("deleting " + geoIndexTableName + "...");
		byte[] geoIndexTableBytes = Bytes.toBytes(geoIndexTableName);
		if (admin.tableExists(geoIndexTableBytes)) {
			if (admin.isTableEnabled(geoIndexTableBytes)) {
				admin.disableTable(geoIndexTableBytes);	
			}			
			admin.deleteTable(geoIndexTableBytes);
		}
		
		admin.close();
	}
	
	public void getRegionBounds(String tableName) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tableName);
		Pair<byte[][],byte[][]> regionKeys = table.getStartEndKeys();
		byte[][] startKeysBytes = regionKeys.getFirst();
		byte[][] endKeysBytes = regionKeys.getSecond();
		table.close();
		
		if (startKeysBytes.length != endKeysBytes.length) {
			throw new Exception("Length of start keys is not equal to length of end keys!");
		}
		for (int i=0; i<startKeysBytes.length; i++) {
			byte[] regionStart = startKeysBytes[i];
			byte[] regionEnd = endKeysBytes[i];
			System.out.println(GeneralHelpers.byteArrayString(regionStart) + "\t" + GeneralHelpers.byteArrayString(regionEnd));
			System.out.println(Bytes.toString(regionStart) + "\t" + Bytes.toString(regionEnd));
		}
	}
	
	/**
	 * Get information about the region containing <b>rowkey</b> in <b>tableName</b>.
	 * @param tableName
	 * @param rowkey
	 * @throws Exception
	 */
	public void getRegionInfoByRowKey(String tableName, String rowkey) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tableName);
		byte[] rowkeyBytes = null;
		int idx = tableName.indexOf("-") + 1;
		String month = tableName.substring(idx);
		boolean useBigInt = TruthyHelpers.checkIfb4June2015(month);
		if (tableName.startsWith(ConstantsTruthy.TWEET_TABLE_NAME) || tableName.startsWith(ConstantsTruthy.RETWEET_INDEX_TABLE_NAME)) {
		    if (useBigInt) {
		        rowkeyBytes = TruthyHelpers.getTweetIdBigIntBytes(rowkey);
		    } else {
		        rowkeyBytes = TruthyHelpers.getTweetIdBytes(rowkey);
		    }
		} else if (tableName.startsWith(ConstantsTruthy.USER_TWEETS_TABLE_NAME)) {
			rowkeyBytes = TruthyHelpers.getUserIdBytes(rowkey);
		} else if (tableName.startsWith(ConstantsTruthy.USER_TABLE_NAME)) {
			idx = rowkey.indexOf('|');
			if (idx < 0) {
				byte[] uidBytes = TruthyHelpers.getUserIdBytes(rowkey);
				byte[] tidBytes = new byte[8];
				rowkeyBytes = TruthyHelpers.combineBytes(uidBytes, tidBytes);
			} else {
				byte[] uidBytes = TruthyHelpers.getUserIdBytes(rowkey.substring(0, idx));
				byte[] tidBytes = null;
				if (useBigInt) {
				    tidBytes = TruthyHelpers.getTweetIdBigIntBytes(rowkey.substring(idx + 1));
				} else {
				    tidBytes = TruthyHelpers.getTweetIdBytes(rowkey.substring(idx + 1));
				}				
				rowkeyBytes = TruthyHelpers.combineBytes(uidBytes, tidBytes);
			}
		} else if (tableName.startsWith(ConstantsTruthy.MEME_INDEX_TABLE_NAME) || tableName.startsWith(ConstantsTruthy.TEXT_INDEX_TABLE_NAME)
				|| tableName.startsWith(ConstantsTruthy.SNAME_INDEX_TABLE_NAME)
				|| tableName.startsWith(ConstantsTruthy.GEO_INDEX_TABLE_NAME)) {
			rowkeyBytes = Bytes.toBytes(rowkey);			
		} else {
			table.close();
			throw new Exception("Invalid table name " + tableName);
		}
		
		HRegionLocation regLoc = table.getRegionLocation(rowkeyBytes, false);
		table.close();
		System.out.println("Region server hostname and port: " + regLoc.getHostnamePort());
		HRegionInfo regInfo = regLoc.getRegionInfo();
		System.out.println("Region ID: " + regInfo.getRegionId());
		System.out.println("RegionNameAsString: " + regInfo.getRegionNameAsString());
		System.out.println("ShortNameToLog: " + regInfo.getEncodedName());
		System.out.println("RegioName from bytes: " + Bytes.toString(regInfo.getRegionName()));		
	}
	
	/**
	 * Sort the user IDs contained in <b>jsonGzPath</b>, and output the sorted lists to <b>uidPath</b>.
	 * @param jsonGzPath
	 *  Path to a .json.gz file.
	 * @param uidGzPath
	 *  Path to the gzipped output uid file.
	 */
	public void sortUidInFile(String jsonGzPath, String uidGzPath) throws Exception {
		long startTime = System.currentTimeMillis();
		long lastTime = startTime;
		long thisTime = -1;
		TreeSet<byte[]> uids = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		int count = 0;

		GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(jsonGzPath));
		BufferedReader brJson = new BufferedReader(new InputStreamReader(gzInputStream, "UTF-8"));
		String jsonStr = brJson.readLine();
		while (jsonStr != null) {
			try {
				jsonStr = jsonStr.trim();
				JsonObject joTweet = ConstantsTruthy.jsonParser.parse(jsonStr).getAsJsonObject();
				JsonObject joUser = joTweet.get("user").getAsJsonObject();
				byte[] uid = TruthyHelpers.getUserIdBytes(joUser.get("id").getAsString());
				uids.add(uid);
				JsonElement jeRetweeted = joTweet.get("retweeted_status");
				if (jeRetweeted != null && !jeRetweeted.isJsonNull()) {
					JsonObject joRetweeted = jeRetweeted.getAsJsonObject();
					joUser = joRetweeted.get("user").getAsJsonObject();
					uid = TruthyHelpers.getUserIdBytes(joUser.get("id").getAsString());
					uids.add(uid);
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			count++;
			if (count % 300000 == 0) {
				thisTime = System.currentTimeMillis();
				System.out.println("processed " + count + " lines. Time spent on this batch: " + (thisTime - lastTime) / 1000 + " seconds.");
				lastTime = thisTime;
			}

			jsonStr = brJson.readLine();
		}
		brJson.close();

		thisTime = System.currentTimeMillis();
		System.out.println("Done with sorting. Total number of lines processed: " + count + ". user ID count: "
				+ uids.size() + ". Total time taken: " + (thisTime - startTime) / 1000 + " seconds.");

		System.out.println("Writing user IDs...");
		FileOutputStream fos = new FileOutputStream(uidGzPath);
		PrintWriter pwOut = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(fos), "UTF-8"));
		pwOut.println(uids.size());
		for (byte[] uid : uids) {
			pwOut.println(TruthyHelpers.getUserIDStrFromBytes(uid));
		}
		pwOut.close();
		System.out.println("Done.");
	}
	
	/**
	 * Given sorted user ID file and number of regions, print the user IDs that will be
	 * used as region split keys for creating the user table.
	 * @param uidGzPath
	 *  path to the sorted user ID .gz file
	 * @param nRegions
	 *  number of regions
	 */ 
	public void demoRegionSplitKeys(String uidGzPath, int nRegions) throws Exception {
		byte[][] splitKeys = getRegionSplitKeysFromUidFile(uidGzPath, nRegions);
		System.out.println("Region split keys for user table:");
		for (byte[] uidTid : splitKeys) {
			byte[] uid = new byte[uidTid.length - 8];
			for (int i=0; i<uidTid.length - 8; i++) {
				uid[i] = uidTid[i];
			}
			System.out.print(TruthyHelpers.getUserIDStrFromBytes(uid) + " [ ");
			for (byte b : uidTid) {
				System.out.print(Integer.toString(b & 0xff) + " ");
			}
			System.out.println("]");
		}
	}
	
	/**
	 * Given a sorted user ID file and the number of regions, return the list of IDs that will be used as region split keys
	 * for creating the user table.
	 * @param uidGzPath
	 *  path to the sorted ID .gz file
	 * @param nRegions
	 *  number of regions
	 * @return
	 *  A list of byte arrays to be used as region split keys
	 */
	public byte[][] getRegionSplitKeysFromUidFile(String uidGzPath, int nRegions) throws Exception {
		byte[][] splitKeys = new byte[nRegions - 1][];
		byte[] emptyTid = new byte[8];
		int nIds = -1;
		GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(uidGzPath));
		BufferedReader brId = new BufferedReader(new InputStreamReader(gzInputStream, "UTF-8"));
		String line = brId.readLine();
		// first line is the total number of IDs
		nIds = Integer.parseInt(line);
		int nIdPerRegion = nIds / nRegions;
		int lastUidPos = nIdPerRegion * (nRegions - 1);
		int count = 0;
		while (line != null) {
			line = brId.readLine();
			count++;
			if (count % nIdPerRegion == 0) {
				System.out.println("Read " + count + " uids for getting region split keys.");
				byte[] uid = TruthyHelpers.getUserIdBytes(line.trim());
				splitKeys[count / nIdPerRegion - 1] = TruthyHelpers.combineBytes(uid, emptyTid);
			}
			if (count >= lastUidPos) {
				break;
			}
		}
		brId.close();		
		return splitKeys;
	}
}
