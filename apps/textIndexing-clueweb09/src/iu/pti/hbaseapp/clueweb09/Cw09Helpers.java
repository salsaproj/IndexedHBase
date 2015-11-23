package iu.pti.hbaseapp.clueweb09;

import iu.pti.hbaseapp.GeneralHelpers;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.net.DNS;

public class Cw09Helpers {
	 /**
     * create MapReduce input files for DataLoaderClueWeb09 from a directory containing .warc.gz files
     * @param warcDir
     * @param destDir
     * @param nWarcPerFile
     */
    public static boolean createMapReduceInputForWarcDir (String warcDir, String destDir, int nWarcPerFile) throws IllegalArgumentException  {
    	File warcDirFile = new File(warcDir);
    	File destDirFile = new File(destDir);
    	if (warcDirFile.isFile() || destDirFile.isFile()) {
    		throw new IllegalArgumentException("Arguments are not both directories!");
    	}
    	if (warcDirFile.getAbsolutePath().equals(destDirFile.getAbsolutePath())) {
    		throw new IllegalArgumentException("Arguments should point to different directories.");
    	}
    	
    	if (!destDirFile.exists()) {
    		if (!destDirFile.mkdirs()) {
    			System.err.println("Can't create destination directory " + destDir);
    			return false;
    		}
    	} else if (!GeneralHelpers.deleteStuffInDir(destDirFile)) {
    		System.err.println("Error in createMapReduceInputForWarcDir: can't delete files under " + destDir);
    		return false;
    	}
    	
    	LinkedList<String> warcPaths = new LinkedList<String>();
    	GeneralHelpers.listDirRecursive(warcDirFile, warcPaths, ".gz");
    	StringBuffer sbContent = new StringBuffer();
    	int pathCount = 0;
    	int fileCount = 0;
    	for (String path : warcPaths) {
			if (pathCount >= nWarcPerFile) {
				String mrInputPath = destDir + File.separator + "gzPaths_"
						+ fileCount + ".txt";
				GeneralHelpers.writeStrToFile(mrInputPath, sbContent.toString());
				System.out.println("MapReduce job input file " + mrInputPath
						+ " created.");
				fileCount++;

				pathCount = 0;
				sbContent.setLength(0);
			}

			sbContent.append(path).append('\n');
			pathCount++;
    	}
    	
    	if (pathCount > 0) {
    		String mrInputPath = destDir + File.separator + "gzPaths_" + fileCount + ".txt";
    		GeneralHelpers.writeStrToFile(mrInputPath, sbContent.toString());
    	}
    	
    	return true;
    }
	
	/**
	 * Re-create the index tables with a different number of regions.
	 * @param compression
	 * @param termSamplePath
	 * @param numOfRegions
	 * @throws Exception
	 */
	public static void reCreateIndexTables(String compression, String termSamplePath, int numOfRegions) throws Exception {
		boolean indexCfFreqComp = compression.toLowerCase().equals("yes");
		ArrayList<String> sampleTerms = new ArrayList<String>(25000);
		GeneralHelpers.readFileToCollection(termSamplePath, sampleTerms);
		int numPerRegion = sampleTerms.size() / numOfRegions;
		String startTerm = sampleTerms.get(numPerRegion - 1);
		String endTerm = sampleTerms.get(sampleTerms.size() - numPerRegion - 1);
		byte[] startKey = Bytes.toBytes(startTerm);
		byte[] endKey = Bytes.toBytes(endTerm);
		System.out.println("start term: " + startTerm + " end term: " + endTerm + " numOfRegions: " + numOfRegions);
		
		Configuration hbaseConfig = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
		
		// create the texts index table
		byte[] tableName = Cw09Constants.CW09_INDEX_TABLE_BYTES;
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}		
		
		HTableDescriptor tableDes = new HTableDescriptor(tableName);
		HColumnDescriptor cfDes = new HColumnDescriptor(Cw09Constants.CF_FREQUENCIES_BYTES);
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
		admin.close();
	}
	
	
	
	/**
	 * Compare the total document count and total frequencies of all terms in an output file of FreqDistCounter.
	 * @param args
	 * @throws Exception
	 */
	public static void compareDocCountAndFreq(String distFilePath) throws Exception {
		LinkedList<String> lines = new LinkedList<String>();
		GeneralHelpers.readFileToCollection(distFilePath, lines);
		long totalDoc = 0;
		long totalFreq = 0;
		for (String line : lines) {
			String[] cols = line.split("\\t");
			totalDoc += Long.valueOf(cols[1]);
			totalFreq += Long.valueOf(cols[2]);
		}
		
		System.out.println("total doc count: " + totalDoc + ", total freq: " + totalFreq + ", times: " + (totalFreq * 1.0 / totalDoc));
	}
	
	public static void usage() {
		System.out.println("java iu.pti.hbaseapp.clueweb09.Helpers <command> [<parameters>]");
		System.out.println("Where '<command> [<parameters>]' could be one of the following:");
		System.out.println("	create-mr-input <directory for .warc.gz files> <directory for MapReduce input files> <number of .warc.gz file paths per input file>");
		System.out.println("	calc-time-difference <date time 1> <date time 2>");
		System.out.println("	recreate-index-tables <yes or no for compression> <path to term sample file> <number of regions>");
		System.out.println("	get-sorted-dist <path to source sorted file> <path to output distribution file>");
		System.out.println("	compare-doccount-freq <path to distribution file>");
	}
    
    public static void main(String[] args) {
    	if (args.length <= 0) {
    		usage();
    		System.exit(1);
    	} else if (args[0].equals("create-mr-input")) {
    		if (args.length < 4) {
    			usage();
    			System.exit(1);
    		} else {
    			createMapReduceInputForWarcDir(args[1], args[2], Integer.valueOf(args[3]));
    		}
    	} else if (args[0].equals("calc-time-difference")) {
    		Calendar cal1 = GeneralHelpers.getDateTimeFromString(args[1]);
    		Calendar cal2 = GeneralHelpers.getDateTimeFromString(args[2]);
    		long diff = cal2.getTimeInMillis() - cal1.getTimeInMillis();
    		System.out.println("Difference in seconds: " + diff/1000);
    	} else if (args[0].equals("reverse-dns")) {
    		if (args.length < 2) {
    			usage();
    			System.exit(1);
    		} else {
    			Configuration hbaseConfig = HBaseConfiguration.create();
    			String nameServer = hbaseConfig.get("hbase.nameserver.address", null);
    			System.out.println("name server: " + nameServer);
    			String ipAddr = args[1];
    			try {
    				String hostName = DNS.reverseDns(InetAddress.getByName(ipAddr), nameServer);
    				System.out.println("host name: " + hostName);
    			} catch (Exception e) {
    				e.printStackTrace();
    			}
    		}    		
    	} else if (args[0].equals("recreate-index-tables")) {
    		if (args.length < 4) {
    			usage();
    			System.exit(1);
    		} else {
    			try {
    				reCreateIndexTables(args[1], args[2], Integer.valueOf(args[3]));
    			} catch (Exception e) {
    				e.printStackTrace();
    			}
    		}
    	} else if (args[0].equals("get-sorted-dist")) {
    		if (args.length < 3) {
    			usage();
    			System.exit(1);
    		} else {
    			try {
    				GeneralHelpers.getSortedDist(args[1], args[2]);
    			} catch (Exception e) {
    				e.printStackTrace();
    			}
    		}
    	} else if (args[0].equals("compare-doccount-freq")) {
    		if (args.length < 2) {
    			usage();
    			System.exit(1);
    		} else {
    			try {
    				compareDocCountAndFreq(args[1]);
    			} catch (Exception e) {
    				e.printStackTrace();
    			}
    		}
    	}
	}
}
