package iu.pti.hbaseapp;

import iu.pti.hbaseapp.truthy.ConstantsTruthy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * A utility class for roughly testing something. 
 * @author gaoxm
 */
@InterfaceAudience.Private
public class SimpleTester {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// testSearch(args);
			// testKv(args);
			// testSth(args);
			// testBytes(args);
			// testLongBytes(args);
			//testPattern(args);
			//stopWordsToProperties(args[0]);
			//extractSubAdjList(args);
			//countEmptyLines(args);
			//generateNewDocRank(args);
			//calcTotalFileSizeFromList(args);
			//checkGeneralIndexer(args);
			//testHBaseConfigureation();
			//testDatFileFromDiego();
			// findDifferentTids(args[0], args[1]);
			// testKmeansComputation(1562500, 10, 3);
			//testTime();
			//testStrSplit();
			//testHtDateFreqList();
			//testFilenameSort(args);
			//testTreeSetString();
			testEntitiesGson();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void testPattern(String[] args) {
		String regex = "http.*(acm\\.org|arxiv\\.org|nature\\.com|elsevier\\.com|sciencemag\\.org|pnas\\.org|plosone\\.org|plos\\.org|jstor\\.org|ieee\\.org|"
				+ "aps\\.org|doi\\.org|dx\\.doi\\.org|sciencedirect\\.com|springer\\.com|ssrn\\.com|biomedcentral\\.com|wiley\\.com|metapress\\.com|"
				+ "worldscientific\\.com|oxfordjournals\\.org|ams\\.org|acs\\.org|thelancet\\.com|nejm\\.org|mdpi\\.com|cell\\.com|jbc\\.org).*";
		Pattern pat = Pattern.compile(regex);
		Matcher m = pat.matcher("http://acm.org");
		System.out.println(m.find());
		
		m.reset("https://elsevier.org");
		System.out.println(m.find());
		
		String s = "aaa,bbb,ccc,ddd";
		String[] secs = s.split(",");
		for (String sec : secs) {
			System.out.println(sec);
		}
		
		String prefix = GeneralHelpers.getPrefixForRegEx(regex);
		System.out.println(prefix);
	}
	
	public static void testLongBytes(String[] args) {
		long n1 = 253;
		long n2 = 1000;
		byte[] bs1 = Bytes.toBytes(n1);
		byte[] bs2 = Bytes.toBytes(n2);
		int c = Bytes.compareTo(bs1, bs2);
		System.out.println("bs1 compare to bs2: " + c);
		System.out.print("bs1: ");
		for (byte b : bs1) {
			System.out.print((0xff & b) + " ");
		}
		System.out.println();
		System.out.print("bs2: ");
		for (byte b : bs2) {
			System.out.print((0xff & b) + " ");
		}
		System.out.println();
	}
	
	public static void testBytes(String[] args) {
		String s1 = "be:user:alexander";
		String s2 = "beaugleholei";
		byte[] bs1 = Bytes.toBytes(s1);
		byte[] bs2 = Bytes.toBytes(s2);
		int c = Bytes.compareTo(bs1, bs2);
		System.out.println("bs1 compare to bs2: " + c);
		System.out.print("bs1: ");
		for (byte b : bs1) {
			System.out.print((0xff & b) + " ");
		}
		System.out.println();
		System.out.print("bs2: ");
		for (byte b : bs2) {
			System.out.print((0xff & b) + " ");
		}
		System.out.println();
		
		bs1 = new byte[]{1, 2, 3, 4, 5};
		BytesWritable bw = new BytesWritable(bs1);
		bs2 = bw.getBytes();
		System.out.println("after going through BytesWritable:");
		for (byte b : bs2) {
			System.out.print((0xff & b) + " ");
		}
		System.out.println();
		System.out.println("Bytes.hashcode(): " + Bytes.hashCode(bs2) + ", BytesWritable.hashcode(): " + bw.hashCode());
		
	}

	public static void testSth(String[] args) {
		try {
			/*
			for (int i=0; i<args.length; i++) {
				System.out.println(args[i]);
			} */			
			//String s = "C3P Related Papers A Quantum Monte Caelo Study, Hong-Qiang Ding, Miloje Makivic";
			//String s = "Michele Bachmann amenities pressed her allegations that the former head of her Iowa presidential" + 
				//		" bid was bribed by the campaign of rival Ron Paul to endorse him, even as one of her own aides denied the charge.";
			String s = "I am a very @happy boy";
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
			TokenStream ts = analyzer.reusableTokenStream("contents", new StringReader(s));
            CharTermAttribute charTermAttr = ts.addAttribute(CharTermAttribute.class);			
			
            boolean hasMoreTokens = ts.incrementToken();
			while (hasMoreTokens) {
				System.out.println(charTermAttr.toString());
				hasMoreTokens = ts.incrementToken();
			}
			ts.close();
			analyzer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void testSearch(String[] args) {
		String word1 = "test";
		String content = "This is a test. tester is testing, which means he is doing tests.";
		
		boolean word1Found = false;
		int idx = content.indexOf(word1);
		while (idx >= 0) {
			int idxBefore = idx -1;
			int idxAfter = idx + word1.length();
			boolean letterBefore = idxBefore >= 0 && Character.isLetter(content.charAt(idxBefore));
			boolean letterAfter = idxAfter < content.length() && Character.isLetter(content.charAt(idxAfter));
			if (letterBefore || letterAfter) {
				if (idxAfter < content.length()) {
					idx = content.indexOf(word1, idxAfter);
				} else {
					break;
				}
			} else {
				word1Found = true;
				break;
			}
		}
		
		System.out.println("found? " + word1Found);
	}
	
	public static void testKv(String[] args) {
		byte[] row = Bytes.toBytes("row");
		byte[] f = Bytes.toBytes("f");
		byte[] q = Bytes.toBytes(0);
		byte[] v = Bytes.toBytes(1);
		KeyValue kvNull = new KeyValue(row, HConstants.LATEST_TIMESTAMP);
		KeyValue kvEmpty = new KeyValue(row, HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY);
		System.out.println("kvNull vs kvEmpty: " + KeyValue.COMPARATOR.compare(kvNull, kvEmpty));
		
		KeyValue kvFull = new KeyValue(row, f, q, 0, v);
		System.out.println("kvNull vs kvFull: " + KeyValue.COMPARATOR.compare(kvNull, kvFull));
		System.out.println("kvEmpty vs kvFull: " + KeyValue.COMPARATOR.compare(kvEmpty, kvFull));
		
		byte[] row2 = Bytes.toBytes("product");
		KeyValue kvFull2 = new KeyValue(row2, f, q, v);
		System.out.println("kvNull vs kvFull2: " + KeyValue.COMPARATOR.compare(kvNull, kvFull2));
	}
	
	/**
	 * Read stop words from .txt files in stopWordDir, and save them into a properties file.
	 * @param stopWordDir
	 */
	public static void stopWordsToProperties(String stopWordDir) throws Exception {
		Properties prop = new Properties();
		StringBuilder sb = new StringBuilder();
		File swDirFile = new File(stopWordDir);
		File[] files = swDirFile.listFiles();
		for (File f : files) {
			if (f.getName().endsWith(".txt")) {
				String fileName = f.getName();
				sb.setLength(0);
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f), "UTF-8"));
				String line = br.readLine();
				while (line != null) {
					line = line.trim();
					sb.append('\"').append(line).append('\"').append(", ");
					line = br.readLine();
				}
				br.close();
				prop.setProperty(fileName, sb.toString());
			}
		}
		FileOutputStream fo = new FileOutputStream(stopWordDir + File.separator + "allStopWords.properties"); 
		prop.store(fo, "");
		fo.close();
	}
	
	public static  void extractSubAdjList(String[] args) throws Exception {
		String wholeListPath = args[0];
		String nodeIdDocNoPath = args[1];
		String subListPath = args[2];
		
		// read node Ids
		long maxNodeId = 0;
		HashSet<Long> nodeIds = new HashSet<Long>();
		BufferedReader brNodeId = new BufferedReader(new FileReader(nodeIdDocNoPath));
		String line = brNodeId.readLine();
		while (line != null) {
			line = line.trim();
			int idx = line.indexOf('\t');
			if (idx >= 0) {
				String nodeIdStr = line.substring(0, idx);
				long nodeId = Long.valueOf(nodeIdStr);
				if (nodeId > maxNodeId) {
					maxNodeId = nodeId;
				}
				nodeIds.add(nodeId);
			}
			line = brNodeId.readLine();
		}
		brNodeId.close();
		System.out.println("max node ID: " + maxNodeId);
		
		// find lines in the whole list corresponding to the node Ids
		PrintWriter pwSubList = new PrintWriter(new FileWriter(subListPath));
		GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(wholeListPath));
		BufferedReader brWholeList = new BufferedReader(new InputStreamReader(gzInputStream));
		// first line is total node count
		String nodeCount = brWholeList.readLine();
		// second line is the list for node 0, and the following lines are for node 1, 2, ...
		line = brWholeList.readLine();
		long nodeNum = 0;
		while (line != null) {
			if (nodeIds.contains(nodeNum)) {
				pwSubList.println(Long.toString(nodeNum) + " " + line);
			}
			if (nodeNum % 1000000 == 0) {
				System.out.println("processed " + nodeNum + " nodes.");
			}
			
			line = brWholeList.readLine();
			nodeNum++;
			if (nodeNum > maxNodeId) {
				break;
			}			
		}
		pwSubList.close();
		brWholeList.close();
		
		System.out.println("Done! Processed " + nodeNum + " nodes in total. nodeCount from first line: " + nodeCount);
	}
	
	public static void countEmptyLines(String[] args) throws Exception {
		String inputPath = args[2];
		BufferedReader brInput = new BufferedReader(new FileReader(inputPath));
		String line = brInput.readLine();
		int n = 1;
		while (line != null) {
			if (line.length() < 2) {
				System.out.println(n);
			}
			line = brInput.readLine();
			n++;
		}
		brInput.close();
		System.out.println("Done. " + n + " lines processed.");
	}
	
	public static void generateNewDocRank(String args[]) throws Exception {
		String nodeToDocMapFile = args[0];
		String nodeToNewNodeIdFile = args[1];
		String newNodeIdRankFile = args[2];
		String docRankFile = args[3];
		String docIdToNewNodeIdFile = args[4]; 
		
		System.out.println("Reading node ID to doc ID map...");
		HashMap<Integer, String> nodeToDocMap = new HashMap<Integer, String>();
		BufferedReader brNodeDoc = new BufferedReader(new FileReader(nodeToDocMapFile));
		String line = brNodeDoc.readLine();
		while (line != null) {
			line = line.trim();
			if (line.length() > 0) {
				int idx = line.indexOf('\t');
				int nodeId = Integer.valueOf(line.substring(0, idx));
				String docId = line.substring(idx + 1);
				nodeToDocMap.put(nodeId, docId);
			}
			line = brNodeDoc.readLine();
		}
		brNodeDoc.close();
		
		System.out.println("Reading node ID to new node ID map...");
		HashMap<Integer, Integer> nodeToNewNodeIdMap = new HashMap<Integer, Integer>();
		BufferedReader brNodeNewNode = new BufferedReader(new FileReader(nodeToNewNodeIdFile));
		line = brNodeNewNode.readLine();
		while (line != null) {
			line = line.trim();
			if (line.length() > 0) {
				int idx = line.indexOf('\t');
				int nodeId = Integer.valueOf(line.substring(0, idx));
				int newNodeId = Integer.valueOf(line.substring(idx + 1));
				nodeToNewNodeIdMap.put(nodeId, newNodeId);
			}
			line = brNodeNewNode.readLine();
		}
		brNodeNewNode.close();
		
		System.out.println("Reading new node ID ranks...");
		BufferedReader brRank = new BufferedReader(new FileReader(newNodeIdRankFile));
		line = brRank.readLine();
		int newNodeIdCount = Integer.valueOf(line);
		float[] ranks = new float[newNodeIdCount];
		line = brRank.readLine();
		while (line != null) {
			line = line.trim();
			if (line.length() > 0) {
				int idx = line.indexOf(' ');
				int newNodeId = Integer.valueOf(line.substring(0, idx));
				float rank = Float.valueOf(line.substring(idx + 1)) * 10000;
				if (rank > 1) {
					System.out.println("large rank: " + rank);
				}
				ranks[newNodeId] = rank;
			}
			line = brRank.readLine();
		}
		brRank.close();
		
		System.out.println("Generating new doc ID rank file...");
		PrintWriter pwRank = new PrintWriter(new FileWriter(docRankFile));
		for (Map.Entry<Integer, String> e : nodeToDocMap.entrySet()) {
			int nodeId = e.getKey();
			String docId = e.getValue();
			int newNodeId = nodeToNewNodeIdMap.get(nodeId);
			float rank = ranks[newNodeId];
			pwRank.println(docId + "\t" + rank);
		}
		pwRank.close();
		System.out.println("Done!");
		
		System.out.println("Generating new doc ID rank file...");
		PrintWriter pwDocToNewNode = new PrintWriter(new FileWriter(docIdToNewNodeIdFile));
		for (Map.Entry<Integer, String> e : nodeToDocMap.entrySet()) {
			int nodeId = e.getKey();
			String docId = e.getValue();
			int newNodeId = nodeToNewNodeIdMap.get(nodeId);
			
			pwDocToNewNode.println(docId + "\t" + newNodeId);
		}
		pwDocToNewNode.close();
		System.out.println("Done!");
	}
	
	public static void calcTotalFileSizeFromList(String[] args) throws Exception {
		float totalGB = 0;
		float totalMB = 0;
		String fileList = "E:\\projects\\hbaseInvertedIndex\\TruthyRelated\\truthyJsonGzFileList.txt";
		BufferedReader brList = new BufferedReader(new FileReader(fileList));
		String line = brList.readLine();
		while (line != null) {
			line = line.trim();
			if (line.length() > 0) {
				String[] eles = line.split("\\s+");
				String size = eles[4].toUpperCase();
				System.out.println(size);
				if (size.endsWith("G")) {
					totalGB += Float.valueOf(size.substring(0, size.length() - 1));
				} else if (size.endsWith("M")) {
					totalMB += Float.valueOf(size.substring(0, size.length() - 1));
				}
			}
			line = brList.readLine();
		}
		brList.close();
		System.out.println("Total GB: " + totalGB + "; total MB: " + totalMB);
	}
	
	public static void checkGeneralIndexer(String[] args) throws Exception {
		String configFileName = "E:\\projects\\hbaseInvertedIndex\\TruthyRelated\\documents\\hbase\\custom-index.xml";
		GeneralCustomIndexer gci = new GeneralCustomIndexer(configFileName, "memeUserIndexTable-2012-06");
		gci.index("tweetTable-2012-06", null);
		
	}
	
	public static void testHBaseConfigureation() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		System.out.println("fs.default.name : " + conf.get("fs.default.name"));
		System.out.println("hbase.rootdir : " + conf.get("hbase.rootdir"));
		File f = new File("software/../software/hadoop-1.2.1");
		System.out.println("absolute path: " + f.getAbsolutePath());
		System.out.println("canonical path: " + f.getCanonicalPath());
		System.out.println(Text.class.getName());		
		System.out.println("hbase.regions.slop:" + conf.get("hbase.regions.slop"));
		System.out.println("hbase.balancer.period:" + conf.get("hbase.balancer.period"));
		System.out.println("hbase.master.loadbalancer.class:" + conf.get("hbase.master.loadbalancer.class"));
		System.out.println("hbase.master.balancer.stochastic.regionLoadCost:" + conf.get("hbase.master.balancer.stochastic.regionLoadCost"));
		System.out.println("hbase.master.balancer.stochastic.moveCost:" + conf.get("hbase.master.balancer.stochastic.moveCost"));
		System.out.println("hbase.master.balancer.stochastic.tableLoadCost:" + conf.get("hbase.master.balancer.stochastic.tableLoadCost"));
		System.out.println("hbase.master.balancer.stochastic.localityCost:" + conf.get("hbase.master.balancer.stochastic.localityCost"));
		System.out.println("hbase.master.balancer.stochastic.memstoreSizeCost:" + conf.get("hbase.master.balancer.stochastic.memstoreSizeCost"));
		System.out.println("hbase.master.balancer.stochastic.storefileSizeCost:" + conf.get("hbase.master.balancer.stochastic.storefileSizeCost"));
		
		System.out.println("hbase.hregion.memstore.mslab.enabled:" + conf.get("hbase.hregion.memstore.mslab.enabled"));
		System.out.println("hbase.hregion.memstore.chunkpool.maxsize:" + conf.get("hbase.hregion.memstore.chunkpool.maxsize"));
		System.out.println("hbase.hregion.memstore.mslab.chunksize:" + conf.get("hbase.hregion.memstore.mslab.chunksize"));
		System.out.println("hbase.regionserver.handler.count:" + conf.get("hbase.regionserver.handler.count"));		
	}
	
	public static double getEuclidean2(double[] v1, double[] v2) {
		double sum = 0;
		for (int i = 0; i < v1.length - 1; i++) {
			sum += ((v1[i] - v2[i]) * (v1[i] - v2[i]));
		}
		return sum;
	}
	
	public static void testKmeansComputation(int nPoints, int nCentroids, int nDimension) {
		double[][] data = new double[nPoints][nDimension + 1];
		double[][] centroids = new double[nCentroids][nDimension + 1];
		int[] cCounts = new int[nCentroids];
		double[][] newCData = new double[nCentroids][nDimension + 1];
		Random random = new Random();
		
		for (int i=0; i<nPoints; i++) {
			for (int j=0; j<nDimension; j++) {
				data[i][j] = random.nextInt(1000);
			}
			data[i][nDimension] = 0.0;
		}
		
		for (int i=0; i<nCentroids; i++) {
			for (int j=0; j<nDimension; j++) {
				centroids[i][j] = random.nextInt(1000);
			}
			centroids[i][nDimension] = 0.0;
		}
		
		System.out.println("starting calculation....");
		long startTime = System.nanoTime();
		// run through all vectors and get the minimum distance counts
		int count = 0;
		for (int i = 0; i < nPoints; i++) {
			double distance = 0;
			int minCentroid = 0;
			double minDistance = Double.MAX_VALUE;

			for (int j = 0; j < nCentroids; j++) {
				distance = getEuclidean2(centroids[j], data[i]);
				count++;
				if (distance < minDistance) {
					minDistance = distance;
					minCentroid = j;
				}
			}

			for (int j = 0; j < nDimension; j++) {
				newCData[minCentroid][j] += data[i][j];
			}
			cCounts[minCentroid] += 1;
		}
		
		long endTime = System.nanoTime();
		System.out.println("calculation(ms) :" + ((endTime - startTime) / 1000000) + ", num of distance computed: " + count);
	}
	
	public static void testTime() {
		long time = 1390334424713L;
		Calendar calTmp = Calendar.getInstance();
		calTmp.setTimeInMillis(time);
		System.out.println(GeneralHelpers.getDateTimeString(calTmp));
	}
	
	public static void testStrSplit() {
		String source = "dfsf ssf";
		String[] res = source.split(" ");
		System.out.println(res.length);
	}
	
	public static void testHtDateFreqList() {
		String line = "#htTest	2012-02-23|1 2012-03-01|5 2012-03-08|5 2012-04-29|1 2012-05-15|1 2012-10-10|1";
		//String line = "#htTest	2012-03-16|1 2012-03-17|2 2012-03-18|4";
		long ltUnitMilli = 86400000 * 7;
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		int idx = line.indexOf('\t');
		if (idx < 0) {
			return;
		}

		String hashtag = line.substring(0, idx);
		String[] dateFreqs = line.substring(idx + 1).split(" ");
		if (dateFreqs.length < 2) {
			return;
		}
		long consStartTime = -1;
		long lastLtUnitNum = -1;
		LinkedList<String> consWeeksDateFreqList = new LinkedList<String>();
		long thisDateTime = -1;
		long thisLtUnitNum = -1;
		StringBuilder sb = new StringBuilder();
		sb.append(hashtag).append('\t');
		for (String dateFreqStr : dateFreqs) {
			idx = dateFreqStr.indexOf('|');
			String date = dateFreqStr.substring(0, idx);
			GeneralHelpers.setDateByString(calTmp, date);
			thisDateTime = calTmp.getTimeInMillis();
			if (consStartTime < 0) {
				consStartTime = thisDateTime;
				lastLtUnitNum = 1;
				consWeeksDateFreqList.add(dateFreqStr);
			} else {
				thisLtUnitNum = (thisDateTime - consStartTime) / ltUnitMilli + 1;
				if (thisLtUnitNum <= lastLtUnitNum + 1) {
					// still in consecutive weeks
					lastLtUnitNum = thisLtUnitNum;
					consWeeksDateFreqList.add(dateFreqStr);
				} else {
					// not in a consecutive week. We need to output the last
					// consecutive weeks' information, and restart counting
					sb.setLength(hashtag.length() + 1);
					for (String s : consWeeksDateFreqList) {
						sb.append(s).append(' ');
					}
					sb.deleteCharAt(sb.length() - 1);
					System.out.println(lastLtUnitNum + "\t" + sb.toString());

					consStartTime = thisDateTime;
					lastLtUnitNum = 1;
					consWeeksDateFreqList.clear();
					consWeeksDateFreqList.add(dateFreqStr);
				}
			}
		}

		if (lastLtUnitNum > 0) {
			sb.setLength(hashtag.length() + 1);
			for (String s : consWeeksDateFreqList) {
				sb.append(s).append(' ');
			}
			sb.deleteCharAt(sb.length() - 1);
			System.out.println(lastLtUnitNum + "\t" + sb.toString());
		}
	}
	
	public static void testFilenameSort(String args[]) {
		String dir = args[0];
		File[] fs = new File(dir).listFiles();
		Arrays.sort(fs);
		for (File f : fs) {
			System.out.println(f.getAbsolutePath());
		}
	}
	
	public static void testTreeSetString() {
		TreeSet<String> tm = new TreeSet<String>();
		tm.add("a");
		tm.add("b");
		tm.add("c");
		tm.add("d");
		System.out.println(tm.toString());		
	}
	
	public static class TestEntitiesGson {
		JsonElement entities;
	}
	
	public static void testEntitiesGson() throws Exception {
		String path = "E:\\projects\\hbaseInvertedIndex\\TruthyRelated\\data\\test-gaoxm\\2011-04-15-top10.json";
		BufferedReader br = new BufferedReader(new FileReader(path));
		String jsonLine =  br.readLine();
		br.close();
		JsonObject joTweet = ConstantsTruthy.jsonParser.parse(jsonLine).getAsJsonObject();
		JsonElement jeEntities = joTweet.get("entities");
		String entStr = jeEntities.toString();
		System.out.println("Original entities string in .json file:");
		System.out.println(entStr);
		
		Gson gson = new Gson();
		System.out.println("String got by calling Gson.toJson() on JsonElement jeEntities:");
		System.out.println(gson.toJson(jeEntities, JsonElement.class));
		
		TestEntitiesGson testObj = new TestEntitiesGson();
		testObj.entities = jeEntities;
		System.out.println("String got by calling Gson.toJson() on TestEntitiesGson testObj:");
		System.out.println(gson.toJson(testObj, TestEntitiesGson.class));
	}
}
