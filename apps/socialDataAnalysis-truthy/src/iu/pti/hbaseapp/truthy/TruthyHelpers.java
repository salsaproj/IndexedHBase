package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;
import iu.pti.hbaseapp.MultiFileFolderWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.URI;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Container for Truthy related helper functions.
 * 
 * @author gaoxm
 */
public class TruthyHelpers {
	/**
	 * Split a given .json.gz file into a required number of splits, and save each split as one .json.gz file under
	 * the given output directory.
	 * @param jsonGzPath
	 * @param nSplits
	 * @param outputDirPath
	 * @throws Exception
	 */
	protected static void splitJsonGzFile(String jsonGzPath, int nSplits, String outputDirPath) throws Exception {
		GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(jsonGzPath));
		BufferedReader brJson = new BufferedReader(new InputStreamReader(gzInputStream, "UTF-8"));
		
		PrintWriter[] writers = new PrintWriter[nSplits];
		String fileName = jsonGzPath;
		int idx = jsonGzPath.lastIndexOf(File.separator);
		if (idx >= 0) {
			fileName = jsonGzPath.substring(idx + 1);
		}
		// file name is like 2012-07-09.json.gz
		String fileNamePrefix = fileName.substring(0, fileName.length() - 7);
		for (int i=0; i<nSplits; i++) {
			String splitPath = outputDirPath + File.separator + fileNamePrefix + i + ".json.gz";
			writers[i] = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(splitPath)), "UTF-8"));
		}
		
		String line = brJson.readLine();
		int count = 0;
		while (line != null) {
			writers[count % nSplits].println(line);
			count++;
			if (count % 1000000 == 0) {
				System.out.println("Processed " + count + " lines.");
				for (PrintWriter writer : writers) {
					writer.flush();
				}
			}
			line = brJson.readLine();
		}
		System.out.println("Processed " + count + " lines in total. Now flush and close all split files...");
		
		brJson.close();
		for (PrintWriter writer : writers) {
			writer.flush();
			writer.close();
		}
		System.out.println("Done!");
	}
	
	/**
	 * Set start and end calendars of an interval by their string representations.
	 * @param calStart
	 * @param calEnd
	 * @param startTime
	 * @param endTime
	 */
	public static void setStartAndEndCalendar(Calendar calStart, Calendar calEnd, String startTime, String endTime) {
		if (startTime.indexOf('T') >= 0) {
			GeneralHelpers.setDateTimeByString(calStart, startTime);
		} else {
			GeneralHelpers.setDateByString(calStart, startTime);
		}
		if (endTime.indexOf('T') >= 0) {
			GeneralHelpers.setDateTimeByString(calEnd, endTime);
			calEnd.set(Calendar.MILLISECOND, 999);
		} else {
			GeneralHelpers.setDateByString(calEnd, endTime);
			calEnd.set(Calendar.HOUR_OF_DAY, 23);
			calEnd.set(Calendar.MINUTE, 59);
			calEnd.set(Calendar.SECOND, 59);
			calEnd.set(Calendar.MILLISECOND, 999);
		}
	}
	
	/**
	 * Split a long time window to a sequence of small windows, each covering one month of the whole window.
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public static Map<String, long[]> splitTimeWindowToMonths(String startTime, String endTime) {
		Map<String, long[]> ret = new TreeMap<String, long[]>();
		Calendar calStart = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		Calendar calEnd = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		setStartAndEndCalendar(calStart, calEnd, startTime, endTime);
		boolean firstMonth = true;
		while (calStart.get(Calendar.YEAR) < calEnd.get(Calendar.YEAR) 
				|| calStart.get(Calendar.MONTH) < calEnd.get(Calendar.MONTH)) {
			// dateStr is like "yyyy-mm-dd"
			String dateStr = GeneralHelpers.getDateString(calStart);
			String month = dateStr.substring(0, 7);
			// set calTmp to the last day of the same month as calStart
			calTmp.setTimeInMillis(calStart.getTimeInMillis());
			calTmp.set(Calendar.DAY_OF_MONTH, 1);
			calTmp.add(Calendar.MONTH, 1);
			calTmp.add(Calendar.DAY_OF_MONTH, -1);
			calTmp.set(Calendar.HOUR_OF_DAY, 23);
			calTmp.set(Calendar.MINUTE, 59);
			calTmp.set(Calendar.SECOND, 59);
			calTmp.set(Calendar.MILLISECOND, 999);		
			long[] times = {calStart.getTimeInMillis(), calTmp.getTimeInMillis()};
			ret.put(month, times);
			
			calStart.add(Calendar.MONTH, 1);
			if (firstMonth) {
				calStart.set(Calendar.DAY_OF_MONTH, 1);
				calStart.set(Calendar.HOUR_OF_DAY, 0);
				calStart.set(Calendar.MINUTE, 0);
				calStart.set(Calendar.SECOND, 0);
				calStart.set(Calendar.MILLISECOND, 0);
				firstMonth = false;
			}
		}
		
		String dateStr = GeneralHelpers.getDateString(calStart);
		String month = dateStr.substring(0, 7);
		long[] times = {calStart.getTimeInMillis(), calEnd.getTimeInMillis()};
		ret.put(month, times);
		
		return ret;
	}
	
	/**
	 * Convert <b>tweetIdStr</b> to a 8-byte byte array
	 * The purpose is for better alignment and (non-lexicographically 
	 * / human-readable) sorting in HBase
	 * 
	 * @param tweetIdStr
	 * @return
	 */
	public static byte[] getTweetIdBytes(String tweetIdStr) {
		// we know tweet IDs and user IDs in twitter are 64-bit unsigned integers
		BigInteger bi = new BigInteger(tweetIdStr);
		bi = ConstantsTruthy.max64bBigInt.subtract(bi);
		byte[] value = bi.toByteArray();
		return adjustTo64bit(value);
	}
	
    /**
     * Convert <b>tweetIdStr</b> to a big integer bytes
     * @param tweetIdStr
     * @return
     */
    public static byte[] getTweetIdBigIntBytes(String tweetIdStr) {
        // we know tweet IDs and user IDs in twitter are 64-bit unsigned integers
        BigInteger bi = new BigInteger(tweetIdStr);        
        byte[] value = bi.toByteArray();
        return value;
    }	
	
	/**
	 * Convert <b>userIdStr</b> to a byte array. 
	 * @param userIdStr
	 * @return
	 */
	public static byte[] getUserIdBytes(String userIdStr) {
		// Since most user IDs are less than 4 bytes long, this just returns the bytes representation of a corresponding BigInteger value.
		BigInteger bi = new BigInteger(userIdStr);
		byte[] value = bi.toByteArray();
		return value;
	}

	/**
	 * Take the last 8 bytes out of a byte array. If the array is shorter than 8 bytes, expand it
	 * at the front and fill the leading bytes with 0s.
	 * @param value
	 * @return
	 */
	public static byte[] adjustTo64bit(byte[] value) {
		byte[] ret = null;
		int startPos = 0;
		if (value.length == 8) {
			ret = value;
		} else if (value.length > 8) {
			ret = new byte[8];
			startPos = value.length - 8;
			for (int i=0; i<8; i++) {
				ret[i] = value[startPos + i];
			}
		} else {
			ret = new byte[8];
			startPos = 8 - value.length;
			for (int i=0; i<startPos; i++) {
				ret[i] = 0;
			}
			for (int i=startPos; i<8; i++) {
				ret[i] = value[i - startPos];
			}
		}
		return ret;
	}
	
	/**
	 * Combine head and tail and return the combined array.
	 * @param head
	 * @param tail
	 * @return
	 */
	public static byte[] combineBytes(byte[] head, byte[] tail) {
		byte[] ret = new byte[head.length + tail.length];
		int idx = 0;
		while (idx < head.length) {
			ret[idx] = head[idx];
			idx++;
		}
		while (idx < ret.length) {
			ret[idx] = tail[idx - head.length];
			idx++;
		}
		
		return ret;
	}
	
	/**
	 * Get string representation of tweet ID from its bytes.
	 * @param idBytes
	 * @return
	 */
	public static String getTweetIDStrFromBytes(byte[] idBytes) {
		byte[] val = null;
		// check if an extra byte is necessary for converting an unsigned value
		if (idBytes[0] >= 0) {
			val = idBytes;
		} else {
			val = new byte[idBytes.length + 1];
			val[0] = 0;
			for (int i=1; i<val.length; i++) {
				val[i] = idBytes[i-1];
			}
		}
		BigInteger bi = new BigInteger(val);
		bi = ConstantsTruthy.max64bBigInt.subtract(bi);
		return bi.toString();
	}
	
    /**
     * Get string representation of tweet ID from its big integer bytes.
     * @param idBytes
     * @return
     */
    public static String getTweetIDStrFromBigIntBytes(byte[] idBytes) {
        BigInteger bi = new BigInteger(idBytes);
        return bi.toString();
    }	
	
	/**
	 * Get string representation of user ID from its bytes.
	 * @param uidBytes
	 * @return
	 */
	public static String getUserIDStrFromBytes(byte[] uidBytes) {
		BigInteger bi = new BigInteger(uidBytes);
		return bi.toString();
	}
	
	/**
	 * Calculate the union of all tweet IDs contained in the tweet ID files under <b>srcTidDirs</b>, and write the
	 * results to tweet ID files under <b>dstTidDir</b>.
	 * @param srcTidDirs
	 *  Paths to source directories containing tweet ID files. 
	 * @param dstTidDir
	 *  Path to the destination directory for the result tweet ID files.
	 * @param nTidPerResultFile
	 *  Number of tweet IDs per file in the destination directory.
	 * @throws Exception
	 */
	public static void getTidFileUnion(String[] srcTidDirs, String dstTidDir, int nTidPerResultFile) throws Exception {
		// First change path strings to URI strings starting with 'file:' or 'hdfs:'
		for (int i=0; i<srcTidDirs.length; i++) {
			srcTidDirs[i] = MultiFileFolderWriter.getUriStrForPath(srcTidDirs[i]);
		}
		dstTidDir = MultiFileFolderWriter.getUriStrForPath(dstTidDir);
		if (!MultiFileFolderWriter.deleteIfExist(dstTidDir)) {
			throw new Exception("Failed to delete result directory " + dstTidDir);
		}
		
		// group source tweet ID files by month
		Configuration conf = HBaseConfiguration.create();
		FileSystem fs = FileSystem.get(new URI(srcTidDirs[0]), conf); 
		HashMap<String, List<Path>> monthTidFileMap = new HashMap<String, List<Path>>();
		int idx = -1;
		for (String srcTidDirUri : srcTidDirs) {
			for (FileStatus srcFileStatus : fs.listStatus(new Path(srcTidDirUri))) {
				String srcFileName = srcFileStatus.getPath().getName(); 
				if (srcFileName.endsWith(".txt") && srcFileName.contains("tweetIds")) {
					idx = srcFileName.indexOf('_');
					String month = srcFileName.substring(0, idx);
					List<Path> paths = monthTidFileMap.get(month);
					if (paths != null) {
						paths.add(srcFileStatus.getPath());
					} else {
						paths = new LinkedList<Path>();
						paths.add(srcFileStatus.getPath());
						monthTidFileMap.put(month, paths);
					}
				}
			}
		}
		
		// calculate the tweet ID union by month
		int count = 0;
		for (Map.Entry<String, List<Path>> e : monthTidFileMap.entrySet()) {
			String month = e.getKey();
			boolean useBigInt = TruthyHelpers.checkIfb4June2015(month);
			List<Path> srcTidPaths = e.getValue();
			System.out.println("Calculating union for " + month + "...");
			PriorityQueue<TweetIdHeapEntry> tidHeap = new PriorityQueue<TweetIdHeapEntry>(srcTidPaths.size());
			for (Path p : srcTidPaths) {
				BufferedReader brTid = new BufferedReader(new InputStreamReader(fs.open(p)));
				String tid = brTid.readLine();
				if (tid == null) {
					brTid.close();
					continue;
				}
				tidHeap.offer(new TweetIdHeapEntry(tid, brTid, useBigInt));
				count++;
			}
			MultiFileFolderWriter resWriter = new MultiFileFolderWriter(dstTidDir, month + "_tweetIds", false, nTidPerResultFile);
			TweetIdHeapEntry he = null;
			byte[] lastTidBytes = null;
			while (!tidHeap.isEmpty()) {
				he = tidHeap.remove();
				if (lastTidBytes == null || Bytes.BYTES_COMPARATOR.compare(lastTidBytes, he.tweetIdBytes) != 0) {
					resWriter.writeln(he.tweetIdStr);
					lastTidBytes = he.tweetIdBytes;
				}
				if (he.moveToNextId()) {
					tidHeap.offer(he);
					count++;
					if (count % 10000 == 0) {
						System.out.println("Processed " + count + " source tweet IDs.");
					}
				}			
			}
			resWriter.close();
		}
		System.out.println("Done. Total number of IDs processed: " + count);
	}
	
	/**
	 * Check duplicated tweet IDs in <b>tweetIdDir</b>, and output the duplicates to stdout.
	 * @param tweetIdDir
	 * @throws Exception
	 */
	public static void checkTidDuplicates(String tweetIdDir) throws Exception {
		// First change path strings to URI strings starting with 'file:' or 'hdfs:'
		tweetIdDir = MultiFileFolderWriter.getUriStrForPath(tweetIdDir);
		
		Set<String> tidSet = new HashSet<String>();
		Configuration conf = HBaseConfiguration.create();
		FileSystem fs = FileSystem.get(new URI(tweetIdDir), conf); 
		int dupCount = 0;
		for (FileStatus srcFileStatus : fs.listStatus(new Path(tweetIdDir))) {
			String srcFileName = srcFileStatus.getPath().getName();
			if (srcFileName.endsWith(".txt") && srcFileName.contains("tweetIds")) {
				BufferedReader brTid = new BufferedReader(new InputStreamReader(fs.open(srcFileStatus.getPath())));
				String tid = brTid.readLine();
				while (tid != null) {
					if (tidSet.contains(tid)) {
						System.out.println("Duplicated tweet ID: " + tid);
						dupCount++;
					} else {
						tidSet.add(tid);
					}
					tid = brTid.readLine();
				}
				brTid.close();
			}
		}		
		System.out.println("Number of unique tweet IDs: " + tidSet.size() + ", number of duplicates: " + dupCount);
	}

    public static boolean checkIfb4June2015(String month) {
        Calendar ret = Calendar.getInstance();
        GeneralHelpers.setDateByString(ret, ConstantsTruthy.b4BigIntMonth);
        long june2015 = ret.getTimeInMillis();
        GeneralHelpers.setDateByString(ret, month);
        long currentMonth = ret.getTimeInMillis();
        return (currentMonth > june2015);
    }
	
	public static void usage() {
		System.out.println("java iu.pti.hbaseapp.clueweb09.Helpers <command> [<parameters>]");
		System.out.println("Where '<command> [<parameters>]' could be one of the following:");
		System.out.println("	split-jsongz-file <path to .json.gz file> <number of splits> <output directory>");
		System.out.println("	union-tweetId-file <number of tweet IDs per result file> <sourcd tweet ID dir> <sourcd tweet ID dir>"
				+ " ... <result tweet ID dir>");
		System.out.println("	check-tweetId-duplicates <tweet ID file directory>");
	}
    
    public static void main(String[] args) {
    	try {
			if (args.length <= 0) {
				usage();
				System.exit(1);
			} else if (args[0].equals("split-jsongz-file")) {
				splitJsonGzFile(args[1], Integer.valueOf(args[2]), args[3]);
			} else if (args[0].equals("union-tweetId-file")) {
				int nTidPerFile = Integer.valueOf(args[1]);
				String[] srcDirs = new String[args.length - 3];
				for (int i=0; i<srcDirs.length; i++) {
					srcDirs[i] = args[i + 2];
				}
				String resultDir = args[args.length - 1];
				getTidFileUnion(srcDirs, resultDir, nTidPerFile);
			} else if (args[0].equals("check-tweetId-duplicates")) {
				String tweetIdDir = args[1];
				checkTidDuplicates(tweetIdDir);
			}
    	} catch (Exception e) {
    		e.printStackTrace();
    		usage();
    		System.exit(1);
    	}
	}
}
