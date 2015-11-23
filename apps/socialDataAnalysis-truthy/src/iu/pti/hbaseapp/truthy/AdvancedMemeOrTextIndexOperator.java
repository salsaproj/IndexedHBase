package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;

/**
 * Updated version of @Deprecated {@AdvancedMemeIndexOperator}
 * 
 * Advanced meme or text index table operator for executing the 'meme-post-count' 
 * 'text-post-count', 'text-timestamp-count', 'timestamp-count' queries by only using the index.
 * 
 * @author taklwu
 */
public class AdvancedMemeOrTextIndexOperator extends DefaultTruthyIndexOperator {

    public AdvancedMemeOrTextIndexOperator(String tableName, String month, String keys, String keyType,long tsStart, long tsEnd,
            boolean fileAsInput) {
        super(tableName + "-" + month, keyType, keys, ConstantsTruthy.COLUMN_FAMILY_TWEETS, tsStart, tsEnd, fileAsInput);        
    }

    public Map<String, Long> getDateCount() throws Exception {
        Configuration hbaseConfig = HBaseConfiguration.create();
        HTable indexTable = new HTable(hbaseConfig, indexTableName);    
        Map<String, Long> results = new TreeMap<String, Long>();
        Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
        Set<BytesWritable> tweetIds = new HashSet<BytesWritable>(1500000);
        for (String rowKeyStr : rowKeyStrs) {
            Scan scan = new Scan();
            scan.addFamily(columnFamily);
            if (tsStart >= 0 && tsEnd >= 0) {
                scan.setTimeRange(tsStart, tsEnd);
            }
            scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
            String prefix = setRowKeysForScan(scan, rowKeyStr);
            boolean isStarSearch = prefix != null && rowKeyStr.charAt(prefix.length()) == '*';
            ResultScanner rs = indexTable.getScanner(scan);
            Result r = rs.next();
            long createTime = 0L;
            String dateStr = "";
            Long count = 0L;
            while (r != null) {
                String meme = Bytes.toString(r.getRow());               
                // for prefix search, first check if prefix matches with the row key
                if (prefix != null) {   
                    if (!meme.startsWith(prefix)) {
                        r = rs.next();
                        continue;
                    }
                    if (!isStarSearch && meme.length() - prefix.length() > 1) {
                        r = rs.next();
                        continue;
                    }
                }
                
                for (KeyValue kv : r.list()) {
                    BytesWritable bwTid = new BytesWritable(kv.getQualifier());
                    // count the tweet ID as only one post
                    if (!tweetIds.contains(bwTid)) {
                        createTime = kv.getTimestamp();
                        calTmp.setTimeInMillis(createTime);
                        dateStr = GeneralHelpers.getDateString(calTmp);
                        count = results.get(dateStr);
                        if (count != null) {
                            results.put(dateStr, count + 1);
                        } else {
                            results.put(dateStr, 1l);
                        }
                        tweetIds.add(bwTid);
                    }
                }               
                
                r = rs.next();
            }
            rs.close();
        }
        indexTable.close();
        return results;
    }

    public Map<String, Long> getKeyCount() throws Exception {
        Configuration hbaseConfig = HBaseConfiguration.create();
        HTable indexTable = new HTable(hbaseConfig, indexTableName);    
        Map<String, Long> results = new HashMap<String, Long>(rowKeyStrs.length);
        for (String rowKeyStr : rowKeyStrs) {
          //System.out.println("Searching rowkey = " + rowKeyStr + ", indexTableName = " + Bytes.toString(indexTableName));
            Scan scan = new Scan();
            scan.addFamily(columnFamily);
            if (tsStart >= 0 && tsEnd >= 0) {
                scan.setTimeRange(tsStart, tsEnd);
            }
            scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
            String prefix = setRowKeysForScan(scan, rowKeyStr);
            boolean isStarSearch = prefix != null && rowKeyStr.charAt(prefix.length()) == '*';
            ResultScanner rs = indexTable.getScanner(scan);
            Result r = rs.next();
            long countThisBatch = 0L;
            Long count = 0L;
            while (r != null) {
                String key = Bytes.toString(r.getRow());
                if (this.rowKeyType.equals(IndexRowKeyType.USER_ID) || this.rowKeyType.equals(IndexRowKeyType.USER_ID_FILE)) {
                    key = TruthyHelpers.getUserIDStrFromBytes((r.getRow()));
                } 
                
                // for prefix search, first check if prefix matches with the row key
                if (prefix != null) {   
                    if (!key.startsWith(prefix)) {
                        r = rs.next();
                        continue;
                    }
                    if (!isStarSearch && key.length() - prefix.length() > 1) {
                        r = rs.next();
                        continue;
                    }
                }
                
                countThisBatch = r.size();         
                count = results.get(key);
                if (count != null) {
                    results.put(key, count + countThisBatch);
                } else {
                    results.put(key, countThisBatch);
                }
                
                r = rs.next();
            }
            rs.close();
        }
        indexTable.close();
        return results;
    }

}
