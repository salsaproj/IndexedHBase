package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;
import iu.pti.hbaseapp.IndexTableOperator;
import iu.pti.hbaseapp.MultiFileFolderWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;

/**
 * Default Truthy index table operator for getting related tweet IDs from a specific index table.
 * 
 * @author gaoxm
 */
public class DefaultTruthyIndexOperator implements IndexTableOperator {
	public static enum IndexRowKeyType {
		STRING, TWEET_ID, USER_ID, PHRASE, TIME_RANGE,
		STRING_FILE, TWEET_ID_FILE, USER_ID_FILE, PHRASE_FILE,
		COORDINATES, COORDINATES_FILE, SNAME, SNAME_FILE;
	}
	
	byte[] indexTableName;
	IndexRowKeyType rowKeyType;
	String[] rowKeyStrs;
	byte[] columnFamily;
	long tsStart;
	long tsEnd;
	Set<BytesWritable> tweetIdResults;
	Set<String> tweetIdAndTsResults;
	private String month = "";
	private boolean useBigInt = false; 
	private boolean isUserSname = false;
	public static final String COORDINATES_BOX_DELIM = ":";

    public DefaultTruthyIndexOperator(String indexTableName, String rowKeyType, String rowKeys, String columnFamily, long tsStart, long tsEnd, boolean fileAsInput)
			throws IllegalArgumentException {
		if (indexTableName == null || indexTableName.length() == 0) {
			throw new IllegalArgumentException("Invalid index table name: " + indexTableName);
		}
		this.indexTableName = Bytes.toBytes(indexTableName);	
		if (columnFamily == null || columnFamily.length() == 0) {
			throw new IllegalArgumentException("Invalid row keys: " + columnFamily);
		}
		this.columnFamily = Bytes.toBytes(columnFamily);
		
		if (rowKeyType.equalsIgnoreCase("tweetID")) {
			this.rowKeyType = IndexRowKeyType.TWEET_ID;
			if (fileAsInput) {
			    this.rowKeyType = IndexRowKeyType.TWEET_ID_FILE;
			}
		} else if (rowKeyType.equalsIgnoreCase("userID")) {
			this.rowKeyType = IndexRowKeyType.USER_ID;
	         if (fileAsInput) {
	                this.rowKeyType = IndexRowKeyType.USER_ID_FILE;
	            }
		} else if (rowKeyType.equalsIgnoreCase("string")) {
			this.rowKeyType = IndexRowKeyType.STRING;
	         if (fileAsInput) {
	                this.rowKeyType = IndexRowKeyType.STRING_FILE;
	            }
		} else if (rowKeyType.equalsIgnoreCase("phrase")) {
			this.rowKeyType = IndexRowKeyType.PHRASE;
	         if (fileAsInput) {
	                this.rowKeyType = IndexRowKeyType.PHRASE_FILE;
	         }	
		} else if (rowKeyType.equalsIgnoreCase("time")) {
			this.rowKeyType = IndexRowKeyType.TIME_RANGE;
		} else if (rowKeyType.equalsIgnoreCase("coordinates")) {
            this.rowKeyType = IndexRowKeyType.COORDINATES;
            if (fileAsInput) {
                this.rowKeyType = IndexRowKeyType.COORDINATES_FILE;
            }
        } else if (rowKeyType.equalsIgnoreCase("sname")) {
            this.rowKeyType = IndexRowKeyType.SNAME;
            isUserSname = true;
            if (fileAsInput) {
                   this.rowKeyType = IndexRowKeyType.SNAME_FILE;
            }
       } else {
			throw new IllegalArgumentException("Unknown row key type: " + rowKeyType);
		}
		
		if (rowKeys == null || rowKeys.length() == 0) {
			throw new IllegalArgumentException("Invalid row keys: " + rowKeys);
		}
		
		// add fileAsInput support
		try {
            this.setRowKeyStrsArray(rowKeys);
        } catch (Exception e) {            
            e.printStackTrace();
            System.exit(255);
        }
		
		if (tsStart > tsEnd) {
			throw new IllegalArgumentException("Invalid timestamps: " + tsStart + " - " + tsEnd);
		}
		this.tsStart = tsStart;
		this.tsEnd = tsEnd;
		tweetIdResults = null;
		tweetIdAndTsResults = null;
		
		// TODO remove later
		this.month = indexTableName.substring(indexTableName.indexOf("-") + 1);		
		this.useBigInt = TruthyHelpers.checkIfb4June2015(this.month);
	}
	
	private void setRowKeyStrsArray(String rowKeys) throws Exception {
	    // phrase with lucene index will remove command term such as "you", "me", "I"
        if (this.rowKeyType == IndexRowKeyType.PHRASE) {
            System.out.println("Before applying to Lucene key = " + rowKeys);
            this.rowKeyStrs = getPhraseRowKeyStrs(rowKeys);
            for (int i = 0; i < this.rowKeyStrs.length; i++) {
                System.out.println("Lucene key " + i + " = " + this.rowKeyStrs[i]);
            }
        }
        
        // COORDINATES do nothing xxxx_xxxx
        if (this.rowKeyType == IndexRowKeyType.COORDINATES || this.rowKeyType == IndexRowKeyType.COORDINATES_FILE) {
          //TODO make the right rowKeyStrs here later
            this.rowKeyStrs = this.getCoordinatesRowkeys(rowKeys, this.rowKeyType);
        }
        
        if (this.rowKeyType == IndexRowKeyType.TWEET_ID_FILE ||
                this.rowKeyType == IndexRowKeyType.STRING_FILE ||
                this.rowKeyType == IndexRowKeyType.USER_ID_FILE ||
                this.rowKeyType == IndexRowKeyType.PHRASE_FILE ||
                this.rowKeyType == IndexRowKeyType.SNAME_FILE) {
            try {
                this.rowKeyStrs = this.readLineByLineStringFromFile(rowKeys);
            } catch (IOException e) {
                System.err.println("Seed file not found (or read error)" + rowKeys);
                e.printStackTrace();
                System.exit(255);
            }
        }
        else {
            this.rowKeyStrs = rowKeys.replaceAll("^[,\\s]+", "").split("[,\\s]+");
        }   
    }

    private String[] getCoordinatesRowkeys(String rowKeys, IndexRowKeyType rowKeyType) throws Exception {        
        String[] output = null;
        String[] tmpRowKeys = null;
        //GeoCoordinatesHelper geoHelper = new GeoCoordinatesHelper();
        GeoCoordinatesHelper geoHelper = new GeoCoordinatesHelper();
        if (rowKeyType == IndexRowKeyType.COORDINATES_FILE) {
            tmpRowKeys = this.readLineByLineStringFromFile(rowKeys);
        } else {
            tmpRowKeys = rowKeys.replaceAll("^[,\\s]+", "").split("[,\\s]+");
        }
        
        HashSet<String> tmpList = new HashSet<String>();
        for (String key: tmpRowKeys) {
            if (key.contains(DefaultTruthyIndexOperator.COORDINATES_BOX_DELIM)) {
                String[] spiltKeys = key.split(DefaultTruthyIndexOperator.COORDINATES_BOX_DELIM);
                if (spiltKeys.length < 2) {
                    // ignore unexpected format of coordinates box
                    continue;
                }
                String geoLoc1 = spiltKeys[0];
                String geoLoc2 = spiltKeys[1];                
                geoHelper.setCoordinatesBoxUsing(geoLoc1, geoLoc2);
                while(geoHelper.hasNext()) {
                    tmpList.add(geoHelper.getNextGeoCoordinatesStr());
                }                
            } else {
                tmpList.add(key);
            }            
        }
        
        if (tmpList.size() == 0) {
            throw new Exception("Coordinates rowkeys error, cannot proceed index searching");
        }
        System.out.println("Total amount of searching coordinates = " + tmpList.size());
        output = tmpList.toArray(new String[tmpList.size()]);
        
        return output;
    }

    private String[] readLineByLineStringFromFile(String filepath) 
            throws IOException {       
        File file = new File(filepath);
        //System.out.println("reading seed file = " + file.getAbsolutePath());
        BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()));
        ArrayList<String> rowKeyLines = new ArrayList<>();
        String line = "";
        while ((line = br.readLine()) != null) {
            String key = line.replaceAll("^[,\\s]+", "").replace(" ", "");
            // we don't allow empty string to be search.
            if (!key.isEmpty() && key !=null) {
                rowKeyLines.add(key);
            }
            //System.out.println("key = " + key);
        }
        br.close();  
        String[] resultArray = new String[rowKeyLines.size()];
        rowKeyLines.toArray(resultArray);
        //System.out.println("keys size = " + resultArray.length);
        return resultArray;
    }

    /**
	 * Get an array of row key strings by analyzing <b>rowKeys</b> with a Lucene analyzer. This is used when
	 * the row key type is given as 'phrase'.
	 * 
	 * @param rowKeys
	 * @return
	 * @throws Exception
	 */
	protected String[] getPhraseRowKeyStrs(String rowKeys) {
		HashSet<String> terms = new HashSet<String>();
		GeneralHelpers.getTermsByLuceneAnalyzer(Constants.getLuceneAnalyzer(), rowKeys, "dummyField", terms);
		String[] res = new String[terms.size()];
		return terms.toArray(res);
	}
	    
    public boolean isUserSname() {
        return isUserSname;
    }
	
	/**
	 * Sets the starting and ending row key of a scan for a given rowkeyStr.
	 * @param scan
	 * @param rowKeyStr
	 * @return prefix if rowKeyStr represents a prefix search, null otherwise.
	 */
	protected String setRowKeysForScan(Scan scan, String rowKeyStr) throws Exception {
	    rowKeyStr = rowKeyStr.toLowerCase();
	    if (rowKeyType == IndexRowKeyType.TWEET_ID || rowKeyType == IndexRowKeyType.TWEET_ID_FILE) {
	        // TODO big integer modification may further be applied
			byte[] rowkey = null;
			if (this.useBigInt) {
			    rowkey = TruthyHelpers.getTweetIdBigIntBytes(rowKeyStr);
			} else {
			    rowkey = TruthyHelpers.getTweetIdBytes(rowKeyStr);
			}
			scan.setStartRow(rowkey);
			scan.setStopRow(rowkey);
			return null;
		} else if (rowKeyType == IndexRowKeyType.USER_ID || rowKeyType == IndexRowKeyType.USER_ID_FILE) {
			byte[] rowkey = TruthyHelpers.getUserIdBytes(rowKeyStr);
			scan.setStartRow(rowkey);
			scan.setStopRow(rowkey);
			return null;
		} else if (rowKeyType == IndexRowKeyType.TIME_RANGE) {
			String[] timeRangeInString = rowKeyStr.split("-");
			System.out.println(rowKeyStr + ", " + timeRangeInString[0]+", "+timeRangeInString[1]);
			byte[] startKey = Bytes.toBytes(Long.valueOf(timeRangeInString[0]));
			byte[] stopKey = Bytes.toBytes(Long.valueOf(timeRangeInString[1]));
			scan.setStartRow(startKey);
			scan.setStopRow(stopKey);
			return null;
		} else if (rowKeyType == IndexRowKeyType.COORDINATES || rowKeyType == IndexRowKeyType.COORDINATES_FILE) {
		    if (!rowKeyStr.equalsIgnoreCase("null")) {		        
		        byte[] rowkey = Bytes.toBytes(rowKeyStr);		        
	            scan.setStartRow(rowkey);
	            scan.setStopRow(rowkey);
		    }
            return null;
        } else {
			int idxStar = rowKeyStr.indexOf('*');
			int idxQuest = rowKeyStr.indexOf('?');
			if (idxStar < 0 && idxQuest < 0) {
				byte[] rowkey = Bytes.toBytes(rowKeyStr);
				scan.setStartRow(rowkey);
				scan.setStopRow(rowkey);			    
				return null;
			} else if (idxStar == 0 || idxQuest == 0) {
				throw new IllegalArgumentException("Invalid keyword " + rowKeyStr + " for search - it must not start with '*' or '?'.");
			} else if (idxStar > 0 && idxQuest > 0) {
				throw new IllegalArgumentException("Invalid keyword " + rowKeyStr + " for search - it must not contain both '*' and '?'.");
			} else {
				// prefix search.
				int prefixEnd = idxStar > 0 ? idxStar : idxQuest;				
				String prefix = rowKeyStr.substring(0, prefixEnd);
				GeneralHelpers.setRowkeyRangeByPrefix(scan, prefix);
				return prefix;
			}
		}		
	}
	
	/**
	 * Get related tweet IDs based on given row keys and time window. 
	 * @return
	 * @throws Exception
	 */
	public Set<BytesWritable> getRelatedTweetIds() throws Exception {
		if (tweetIdResults != null) {
			return tweetIdResults;
		}
		Configuration hbaseConfig = HBaseConfiguration.create();
		HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConfig);
		boolean isTableExist = hbaseAdmin.tableExists(indexTableName);
		if (!isTableExist) {
		    System.out.println("Table "+ Bytes.toString(this.indexTableName) +" does not exist in HBase");
		    return new TreeSet<BytesWritable>();
		}
		HTable indexTable = new HTable(hbaseConfig, indexTableName);	
		// TODO remove it later
		// System.out.println("indexTableName: " + Bytes.toString(this.indexTableName));
		// tweetIdResults = new HashSet<BytesWritable>(1500000);
		tweetIdResults = new TreeSet<BytesWritable>();
		for (int i = 0; i < rowKeyStrs.length; i++) {
			String rowKeyStr = rowKeyStrs[i];
			//System.out.println("key = " + rowKeyStr);
			TreeSet<BytesWritable> tmpTidSet = null;
			if (rowKeyType == IndexRowKeyType.PHRASE || rowKeyType == IndexRowKeyType.PHRASE_FILE) {
				// in case of 'phrase' row key type, this is used for calculating the intersection with tweetIdResults
				tmpTidSet = new TreeSet<BytesWritable>();
				System.out.println("renewing tmpTidSet");
			}
			Scan scan = new Scan();
			scan.addFamily(columnFamily);
			if (tsStart >= 0 && tsEnd >= 0) {
				if (tsEnd < Long.MAX_VALUE) {
					scan.setTimeRange(tsStart, tsEnd + 1);
				} else {
					scan.setTimeRange(tsStart, tsEnd);
				}
			}
			scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
			String prefix = setRowKeysForScan(scan, rowKeyStr);
			boolean isStarSearch = prefix != null && rowKeyStr.charAt(prefix.length()) == '*';
			ResultScanner rs = indexTable.getScanner(scan);
			Result r = rs.next();
			while (r != null) {
				// for prefix search, first check if prefix matches with the row key
				if (prefix != null) {
					String idxRowKeyStr = Bytes.toString(r.getRow());
					if (!idxRowKeyStr.startsWith(prefix)) {
						r = rs.next();
						continue;
					}
					if (!isStarSearch && idxRowKeyStr.length() - prefix.length() > 1) {
						r = rs.next();
						continue;
					}
				}
				
				for (KeyValue kv : r.list()) {
					BytesWritable bw = new BytesWritable(kv.getQualifier());
					if (rowKeyType == IndexRowKeyType.PHRASE || rowKeyType == IndexRowKeyType.PHRASE_FILE) {
					    // tweetIdResults.add(bw);
						// To search for a phrase, the tweet must contain every term in the phrase
					    // we have to handle it in a external mapper.
						if (i == 0) {
							tmpTidSet.add(bw);
						} else if (tweetIdResults.contains(bw)) {
							tmpTidSet.add(bw);
						}
					} else {
						tweetIdResults.add(bw);
					}
				}
				r = rs.next();
			}
			rs.close();
			if (rowKeyType == IndexRowKeyType.PHRASE || rowKeyType == IndexRowKeyType.PHRASE_FILE) {
			    // if we find any new record for current key, we take it as intersection
			    // otherwise, we just keep the first set of records
			    // e.g. love you where you won't be used.
			    if (tmpTidSet.size() != 0) {
			        tweetIdResults = tmpTidSet;
			    }
				System.out.println("Update unordered phrase intersection to tweet Id Result, size = " + tweetIdResults.size());
			}
		}
		indexTable.close();
//		if (tweetIdResults == null) {
//			tweetIdResults = new TreeSet<BytesWritable>();
//		}
		return tweetIdResults;
	}

	/**
	 * Get related tweet IDs based on given row keys and time window, write those result immediately to Disk
	 * @param numFilesWritten 
	 * @param numTweetsFound 
	 * @param folderUri 
	 * @param filenamePrefix
	 * @param deleteOldContent 
	 * @param maxLinesPerFile 
	 * @return
	 * @throws Exception
	 */
	public MultiFileFolderWriter getRelatedTweetIdsAndWriteToDisk(String outputDir, String filenamePrefix, boolean deleteOldContent, int nTweetsPerFile ) throws Exception {
		if (tweetIdResults != null) {
			return null;
		}
		Configuration hbaseConfig = HBaseConfiguration.create();
        HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConfig);
        boolean isTableExist = hbaseAdmin.tableExists(indexTableName);
        if (!isTableExist) {
            System.out.println("Table " + Bytes.toString(this.indexTableName)
                    + " does not exist in HBase");
            return null;
        }
		HTable indexTable = new HTable(hbaseConfig, indexTableName);
		//tweetIdResults = new HashSet<BytesWritable>(1500000);
		tweetIdResults = null;
		MultiFileFolderWriter writer = new MultiFileFolderWriter(outputDir, filenamePrefix, deleteOldContent, nTweetsPerFile);
		for (int i = 0; i < rowKeyStrs.length; i++) {
			String rowKeyStr = rowKeyStrs[i];
			Scan scan = new Scan();
			scan.addFamily(columnFamily);
			if (tsStart >= 0 && tsEnd >= 0) {
				if (tsEnd < Long.MAX_VALUE) {
					scan.setTimeRange(tsStart, tsEnd + 1);
				} else {
					scan.setTimeRange(tsStart, tsEnd);
				}
			}
			scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
			String prefix = setRowKeysForScan(scan, rowKeyStr);
			boolean isStarSearch = prefix != null && rowKeyStr.charAt(prefix.length()) == '*';
			ResultScanner rs = indexTable.getScanner(scan);
			Result r = rs.next();
			while (r != null) {
				// for prefix search, first check if prefix matches with the row key
				if (prefix != null) {
					String idxRowKeyStr = Bytes.toString(r.getRow());
					if (!idxRowKeyStr.startsWith(prefix)) {
						r = rs.next();
						continue;
					}
					if (!isStarSearch && idxRowKeyStr.length() - prefix.length() > 1) {
						r = rs.next();
						continue;
					}
				}
				
				for (KeyValue kv : r.list()) {
					// write to disk whenever found a tweetID 
					// TODO: duplicates exist but it won't have memory leaks
				    if (this.useBigInt) {
				        writer.writeln(TruthyHelpers.getTweetIDStrFromBigIntBytes(kv.getQualifier()));
				    } else {
				        writer.writeln(TruthyHelpers.getTweetIDStrFromBytes(kv.getQualifier()));
				    }
				}
				r = rs.next();
			}
			rs.close();
		}
		indexTable.close();
		writer.close();
		return writer;
	}	
	
	/**
	 * Get related tweet IDs and creation time based on given row keys and time window. 
	 * @return
	 * @throws Exception
	 */
	public Set<String> getTweetIdAndTs() throws Exception {
		if (tweetIdAndTsResults != null) {
			return tweetIdAndTsResults;
		}		
		if (rowKeyType == IndexRowKeyType.PHRASE || rowKeyType == IndexRowKeyType.PHRASE_FILE) {
			throw new Exception("Row key type 'phrase' is not supported.");
		}
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable indexTable = new HTable(hbaseConfig, indexTableName);	
		tweetIdAndTsResults = new TreeSet<String>();
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		for (String rowKeyStr : rowKeyStrs) {
			Scan scan = new Scan();
			scan.addFamily(columnFamily);
			if (tsStart >= 0 && tsEnd >= 0) {
				scan.setTimeRange(tsStart, tsEnd + 1);
			}
			scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
			String prefix = setRowKeysForScan(scan, rowKeyStr);
			boolean isStarSearch = prefix != null && rowKeyStr.charAt(prefix.length()) == '*';
			ResultScanner rs = indexTable.getScanner(scan);
			Result r = rs.next();
			while (r != null) {
				// for prefix search, first check if prefix matches with the row key
				if (prefix != null) {
					String idxRowKeyStr = Bytes.toString(r.getRow());
					if (!idxRowKeyStr.startsWith(prefix)) {
						r = rs.next();
						continue;
					}
					if (!isStarSearch && idxRowKeyStr.length() - prefix.length() > 1) {
						r = rs.next();
						continue;
					}
				}
				
				for (KeyValue kv : r.list()) {
					String tweetId = ""; 
					if (this.useBigInt) {
					    tweetId = TruthyHelpers.getTweetIDStrFromBigIntBytes(kv.getQualifier());
					} else {
					    tweetId = TruthyHelpers.getTweetIDStrFromBytes(kv.getQualifier());
					}
					calTmp.setTimeInMillis(kv.getTimestamp()); 
					tweetIdAndTsResults.add(tweetId + "\t" + GeneralHelpers.getDateTimeString(calTmp));
				}
				r = rs.next();
			}
			rs.close();
		}
		indexTable.close();
		return tweetIdAndTsResults;
	}
	
	/**
	 * Since DefaultTruthyIndexOperator is designed for finding related tweet IDs, this function is just redirected
	 * to getRelatedTweetIds().
	 */
	public Set<BytesWritable> getQueriedTableElements() throws Exception {
		return getRelatedTweetIds();
	}
	
	/**
	 * Since DefaultTruthyIndexOperator is designed for finding related tweet IDs, this function returns null;
	 */
	public Set<Long> getQueriedTimestamps() throws Exception {
		return null;
	}
	
	/**
	 * Since all constraints are set through the constructor for DefaultTruthyIndexOperator, this function does
	 * nothing.
	 */
	public void setConstraints(String[] constraints) throws Exception {
	}
}
