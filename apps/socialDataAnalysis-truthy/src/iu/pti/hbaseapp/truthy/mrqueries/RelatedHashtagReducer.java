package iu.pti.hbaseapp.truthy.mrqueries;

import iu.pti.hbaseapp.truthy.ConstantsTruthy;
import iu.pti.hbaseapp.truthy.TruthyHelpers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class for finding hashtags related to a seed hashtag by Jaccard coefficient: <br/>
 * |S * T|/|S + T| > threshold
 * 
 * @author gaoxm
 */
public class RelatedHashtagReducer extends Reducer<Text, LongWritable, Text, Text> {
	Set<String> seedHtgTweetIds = null;
	Map<String, HTable> monthTableMap = null;
	Map<String, long[]> monthTimeMap = null;
	double threshold = 0;
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException {
		String hashtag = key.toString();
		int intersectionSize = 0;
		int unionSize = seedHtgTweetIds.size();
		// go through the meme index tables for all months to get the set of tweet IDs for this hashtag, and compute the sizes of 
		// intersection and union between this set and the tweet ID set of the seed hashtag.
		for (Map.Entry<String, HTable> e : monthTableMap.entrySet()) {
			String month = e.getKey();
			HTable t = e.getValue();
			boolean useBigInt = TruthyHelpers.checkIfb4June2015(month);
			long[] tsRange = monthTimeMap.get(month);
			byte[] rowKey = Bytes.toBytes(hashtag);
			Scan scan = new Scan();
			scan.setStartRow(rowKey);
			scan.setStopRow(rowKey);
			scan.setTimeRange(tsRange[0], tsRange[1]);
			scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
			scan.setBatch(ConstantsTruthy.TRUTHY_TABLE_SCAN_BATCH);
			ResultScanner rs = t.getScanner(scan);
			Result r = rs.next();
			while (r != null) {
				for (KeyValue kv : r.list()) {
					String tid = ""; 
					if (useBigInt) {
					    tid = TruthyHelpers.getTweetIDStrFromBigIntBytes(kv.getQualifier());
					} else {
					    tid = TruthyHelpers.getTweetIDStrFromBytes(kv.getQualifier());
					}					
					if (seedHtgTweetIds.contains(tid)) {
						intersectionSize++;
					} else {
						unionSize++;
					}
				}
				r = rs.next();
			}
			rs.close();
		}
		
		double jaccardCoef = intersectionSize * 1.0 / unionSize;
		if (jaccardCoef >= threshold) {
			context.write(key, new Text(intersectionSize + "\t" + unionSize + "\t" + jaccardCoef));
		}
	}	
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		try {
			Configuration conf = context.getConfiguration();
			String seedTweetIdDir = conf.get(ConstantsTruthy.MRJOB_INPUT_PATH);
			String addArgs = conf.get("additional.arguments");
			String[] args = addArgs.split("\\n");
			//String givenMeme = args[0];
			threshold = Double.valueOf(args[1]);
			String timeWindowStart = args[2];
			String timeWIndowEnd = args[3];

			// read the tweet IDs for the seed hashtag
			seedHtgTweetIds = new HashSet<String>();
			FileSystem fs = FileSystem.get(new URI(seedTweetIdDir), conf);
			Path folderPath = new Path(seedTweetIdDir);
			for (FileStatus fstat : fs.listStatus(folderPath)) {
				if (!fs.isDirectory(fstat.getPath())) {
					BufferedReader brTid = new BufferedReader(new InputStreamReader(fs.open(fstat.getPath())));
					String line = brTid.readLine();
					while (line != null) {
						line = line.trim();
						if (line.length() > 0) {
							seedHtgTweetIds.add(line);
						}
						line = brTid.readLine();
					}
					brTid.close();
				}
			}
			
			// create HTables
			monthTimeMap = TruthyHelpers.splitTimeWindowToMonths(timeWindowStart, timeWIndowEnd);
			monthTableMap = new TreeMap<String, HTable>();
			for (Map.Entry<String, long[]> e : monthTimeMap.entrySet()) {
				String month = e.getKey();
				HTable t = new HTable(conf, ConstantsTruthy.MEME_INDEX_TABLE_NAME + "-" + month);
				monthTableMap.put(month, t);
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	@Override
	protected void cleanup(Context context) {
		try {
			if (monthTableMap != null) {
				context.setStatus("monthTableMap size: " + monthTableMap.size());
				for (Map.Entry<String, HTable> e : monthTableMap.entrySet()) {
					e.getValue().close();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
