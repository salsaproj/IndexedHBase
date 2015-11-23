package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;
import iu.pti.hbaseapp.truthy.streaming.ProtomemeCluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TimeZone;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * A utility class for roughly testing something. 
 * @author gaoxm
 */
@InterfaceAudience.Private
public class TruthySimpleTester {

	public static void testBigInt(String[] args) {
		String uid = "1555608336";
		byte[] uidBytes = TruthyHelpers.getUserIdBytes(uid);
		System.out.print("uidBytes: ");
		for (byte b : uidBytes) {
			System.out.print((0xff & b) + " ");
		}
		System.out.println();
		System.out.println("uidBytes back to string: " + TruthyHelpers.getUserIDStrFromBytes(uidBytes));
		
		System.out.println("1 + max 64-bit unsinged integer: " + ConstantsTruthy.max64bBigInt.toString());
		
		String s1 = "18446744073709551615";
		String s2 = "240";
		BigInteger bi1 = new BigInteger(s1);
		BigInteger bi2 = new BigInteger(s2);
		byte[] bs1 = bi1.toByteArray();
		byte[] bs2 = bi2.toByteArray();
		int c = Bytes.compareTo(bs1, bs2);
		System.out.println("BigInteger.toByteArray() - bs1 compare to bs2: " + c);
		System.out.println(bs1.length + " " + bs2.length);
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
		
		System.out.println();
		BigDecimal bd1 = new BigDecimal(s1);
		BigDecimal bd2 = new BigDecimal(s2);
		bs1 = Bytes.toBytes(bd1);
		bs2 = Bytes.toBytes(bd2);
		c = Bytes.compareTo(bs1, bs2);
		System.out.println("Bytes.toBytes(BigDecimal) - bs1 compare to bs2: " + c);
		System.out.println(bs1.length + " " + bs2.length);
		
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
		System.out.println("bi1: " + bi1.toString());
		System.out.println("bi1 from bytes: " + TruthyHelpers.getTweetIDStrFromBytes(bi1.toByteArray()));
		
		long maxId32 = 1L << 32 - 1L;
		long regionSize = maxId32 / 7;
		byte[] firstRegionEndUid = Bytes.toBytes(regionSize);
		byte[] firstRegionEndTid = TruthyHelpers.adjustTo64bit(ConstantsTruthy.maxID64.toByteArray());
		byte[] lastRegionBegionUid = Bytes.toBytes(maxId32 - regionSize);
		byte[] lastRegionBegionTid = new byte[8];
		byte[] firstRegionEnd = TruthyHelpers.combineBytes(firstRegionEndUid, firstRegionEndTid);
		byte[] lastRegionBegin = TruthyHelpers.combineBytes(lastRegionBegionUid, lastRegionBegionTid);
		System.out.println(GeneralHelpers.makeStringByEachByte(firstRegionEnd));
		System.out.println(GeneralHelpers.makeStringByEachByte(lastRegionBegin));
	}
	
	public static void printJsonElement(JsonElement je, int indentLevel, PrintWriter pwOut) {
		String prefix = "";
		for (int i = 0; i < indentLevel; i++) {
			prefix += "+";
		}
		
		if (je.isJsonObject()) {
			JsonObject jo = je.getAsJsonObject();
			for (Entry<String, JsonElement> entry : jo.entrySet()) {
				String key = entry.getKey();
				JsonElement value = entry.getValue();
				pwOut.println(prefix + key + " :");
				printJsonElement(value, indentLevel + 1, pwOut);
			}
		} else if (je.isJsonArray()) {
			JsonArray jsonArray = je.getAsJsonArray();
			Iterator<JsonElement> ije = jsonArray.iterator();
			while (ije.hasNext()) {
				printJsonElement(ije.next(), indentLevel + 1, pwOut);
			}
		} else if (je.isJsonNull()) {
			pwOut.println(prefix + "NULL_JSON");
		} else {
			pwOut.println(prefix + je);
		}
	}
	
	public static void printJsonTweet(JsonObject joTweet) throws Exception {
		String tweetIdStr = joTweet.get("id").getAsString();
		byte[] tweetIdBytes = new BigInteger(tweetIdStr).toByteArray();
		System.out.println("id str: " + tweetIdStr + ", bigint bytes length: " + tweetIdBytes.length);
		SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
		
		for (Map.Entry<String, JsonElement> e : joTweet.entrySet()) {
			String jeName = e.getKey();
			JsonElement je = e.getValue();
			if (je.isJsonNull()) {
				continue;
			}
			
			ConstantsTruthy.FieldType ft = ConstantsTruthy.fieldTypeMap.get(jeName);
			if (ft != null) {
				switch (ft) {
				case _IGNORE:
					break;
				case FAVORITED:
					System.out.println("favorited: " + je.getAsBoolean());
					break;
				case TEXT:
					System.out.println("text: " + je.getAsString());
					break;
				case CREATE_TIME:
					long createTime = sdf.parse(je.getAsString()).getTime();
					Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
					cal.setTimeInMillis(createTime);
					System.out.println("create time: " + createTime + ", " + GeneralHelpers.getDateTimeString(cal));
					break;
				case RETWEET_COUNT:
					System.out.println("retweet count: " + je.getAsLong());
					break;
				case REPLY_STATUS_ID:
					byte[] replyStatusIDBytes = new BigInteger(je.getAsString()).toByteArray();
					System.out.println("reply to status id: " + je.getAsString() + ", bigint bytes length: " + replyStatusIDBytes.length);
					break;
				case REPLY_SCREEN_NAME:
					System.out.println("reply to screen name: " + je.getAsString());
					break;
				case CONTRIBUTORS:
					System.out.println("contributors: " + je.toString());
					break;
				case RETWEETED:
					System.out.println("retweeted: " + je.getAsBoolean());
					break;
				case SOURCE:
					System.out.println("source: " + je.getAsString());
					break;
				case REPLY_USER_ID:
					String replyUserId = je.getAsString();
					byte[] replyUserIdBytes = new BigInteger(replyUserId).toByteArray();
					System.out.println("reply to user id: " + je.getAsString() + ", bigint bytes length: " + replyUserIdBytes.length);
					break;
				case RETWEET_STATUS:
					String retweetIdStr = je.getAsJsonObject().get("id").getAsString();
					byte[] retweetIdBytes = new BigInteger(retweetIdStr).toByteArray();
					System.out.println("rewteet status id: " + retweetIdStr + ", bigint bytes length: " + retweetIdBytes.length);
					break;
				case ENTITIES:
					String memes = TruthyDataLoaderWithGci.getMemesFromEntities(je);
					System.out.println("memes: " + memes);
					break;
				case COORDINATES:
					System.out.println("coordinates: " + je.toString());
					break;
				case GEO:
					System.out.println("geo: " + je.toString());
					break;
				case USER:
					String userIdStr = je.getAsJsonObject().get("id").getAsString();
					String userSnameStr = je.getAsJsonObject().get("screen_name").getAsString();
					System.out.println("user id: " + userIdStr + ", user screen name: " + userSnameStr);
					break;
				case TRUNCATED:
					System.out.println("truncated: " + je.getAsBoolean());
					break;
				case PLACE:
					System.out.println("place: " + je.toString());
					break;
				default:
				}
			}
		}
	}
	
	public static void testGson(String[] args) {
		String jsonPath = "E:\\projects\\hbaseInvertedIndex\\TruthyRelated\\test-gaoxm\\2011-04-15-top10.json";
		String outputPath = "E:\\projects\\hbaseInvertedIndex\\TruthyRelated\\test-gaoxm\\parsedContent.txt";
		if (args.length == 1) {
			jsonPath = args[0];
		}
		
		JsonParser parser = new JsonParser();
		GZIPInputStream gzInputStream = null;
		BufferedReader brJson = null;
		try {
			if (jsonPath.endsWith("gz")) {
				gzInputStream = new GZIPInputStream(new FileInputStream(jsonPath.toString()));
				brJson = new BufferedReader(new InputStreamReader(gzInputStream, "UTF-8"));
			} else {
				brJson = new BufferedReader(new FileReader(jsonPath));
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		PrintWriter pwOut = null;
		try {
			pwOut = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputPath), "UTF-8"));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		try {
			int count = 0;
			String jsonStr = brJson.readLine();
			while (jsonStr != null) {
				jsonStr = jsonStr.trim();
				if (jsonStr.length() == 0) {
					jsonStr = brJson.readLine();
					continue;
				}
				
				JsonElement je = parser.parse(jsonStr);
				pwOut.println("----new json tweet----");
				printJsonElement(je, 0, pwOut);
				
				System.out.println("----new json tweet----");
				printJsonTweet(je.getAsJsonObject());
				jsonStr = brJson.readLine();
				if (count++ >= 10) {
					break;
				}
			}
			brJson.close();
			gzInputStream.close();
			pwOut.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void testJsonFromObject()	{
		TweetData tweet = new TweetData();
		tweet.id_str = "334455667788";
		tweet.text = "to test Gson";
		tweet.user_id_str = "123";
		tweet.user_screen_name = "HelloGson";
		String memes = "{\"user_mentions\":[{\"indices\":[0,8],\"screen_name\":\"BGPetit\",\"id_str\":\"40544748\",\"name\"" + 
				":\"Brandon Gene Petit\",\"id\":40544748}],\"hashtags\":[],\"urls\":[]}";
		tweet.entities = ConstantsTruthy.jsonParser.parse(memes);
		tweet.retweeted_status_id_str = "334455667787";
		tweet.retweeted_status_user_id_str = "321";
		tweet.in_reply_to_user_id_str = "121";
		tweet.retweet_count = 10;
		
		Gson gson = new Gson();
		String tweetJson = gson.toJson(tweet, TweetData.class);
		System.out.println(tweetJson);
		JsonObject joTweet = ConstantsTruthy.jsonParser.parse(tweetJson).getAsJsonObject();
		System.out.println(joTweet.get("memes").getAsString());		
	}
	
	/**
	 * Test the function for splitting intervals to sub-intervals in months.
	 * @param args
	 */
	public static void testSplitIntervals(String[] args) {
		String startTime = "2011-05-07T13:00:03";
		String endTime = "2012-10-11T10:33:22";
		Calendar calStart = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		Calendar calEnd = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		Map<String, long[]> intervals = TruthyHelpers.splitTimeWindowToMonths(startTime, endTime);
		for (Map.Entry<String, long[]> e : intervals.entrySet()) {
			String month = e.getKey();
			calStart.setTimeInMillis(e.getValue()[0]);
			calEnd.setTimeInMillis(e.getValue()[1]);
			String start = GeneralHelpers.getDateTimeString(calStart) + "." + calStart.get(Calendar.MILLISECOND);
			String end = GeneralHelpers.getDateTimeString(calEnd) + "." + calEnd.get(Calendar.MILLISECOND);
			
			System.out.println(month + " : " + start + " to " + end);
		}
	}
	
	/**
	 * Find the 2 largest communities from a community membership result file.
	 * @throws Exception
	 */
	public static void findTop2LargeCommunity() throws Exception {
		String membershipPath = "E:\\projects\\hbaseInvertedIndex\\TruthyRelated\\data\\hbase\\results"
			+ "\\20120924To20121106\\retweetMtxP2Tcot\\retweetMtx\\lpMembership.txt";
		List<String> lines = new LinkedList<String>();
		GeneralHelpers.readFileToCollection(membershipPath, lines);
		Map<Integer, Integer> commSizes = new HashMap<Integer, Integer>();
		for (String line : lines) {
			line = line.trim();
			if (line.length() > 0) {
				String[] commIds = line.split("\\s");
				for (String commId : commIds) {
					if (commId.length() <= 0) {
						continue;
					}
					int id = Integer.parseInt(commId);
					Integer size = commSizes.get(id);
					if (size == null) {
						commSizes.put(id, 1);
					} else {
						commSizes.put(id, size+1);
					}
				}
			}
		}
		
		int commIdFirst = -1;
		int commIdSecond = -1;
		int commSizeFirst = -1;
		int commSizeSecond = -1;
		for (Map.Entry<Integer, Integer> e: commSizes.entrySet()) {
			int commId = e.getKey();
			int commSize = e.getValue();			
			if (commSize > commSizeSecond) {
				commIdSecond = commId;
				commSizeSecond = commSize;
				
				if (commSizeSecond > commSizeFirst) {
					int tmp = commIdFirst;
					commIdFirst = commIdSecond;
					commIdSecond = tmp;
					
					tmp = commSizeFirst;
					commSizeFirst = commSizeSecond;
					commSizeSecond = tmp;
				}
			}
		}
		System.out.println("first community ID(size): " + commIdFirst + "(" + commSizeFirst + "), second community ID(size): " 
				+ commIdSecond + "(" + commSizeSecond + ")");
	}
	
	/**
	 * Find the IDs of vertices in small communities, and reduce the membership vector to only contain
	 * vertices in large communities.
	 * @throws Exception
	 */
	public static void getLargeCommunityMembership() throws Exception {
		String membershipPath = "E:\\projects\\hbaseInvertedIndex\\TruthyRelated\\data\\hbase\\results"
			+ "\\20120924To20121106\\retweetMtxP2Tcot\\retweetMtx\\lpMembership.txt";
		String largeCommMsPath = "E:\\projects\\hbaseInvertedIndex\\TruthyRelated\\data\\hbase\\results"
			+ "\\20120924To20121106\\retweetMtxP2Tcot\\retweetMtx\\lpMsLargeCommunity.txt";
		String smallCommVidPath = "E:\\projects\\hbaseInvertedIndex\\TruthyRelated\\data\\hbase\\results"
			+ "\\20120924To20121106\\retweetMtxP2Tcot\\retweetMtx\\smallCommVid.txt";
		List<Integer> cidList = new LinkedList<Integer>();
		List<String> lines = new LinkedList<String>();
		GeneralHelpers.readFileToCollection(membershipPath, lines);
		for (String line : lines) {
			line = line.trim();
			if (line.length() > 0) {
				String[] commIds = line.split("\\s");
				for (String commId : commIds) {
					if (commId.length() <= 0) {
						continue;
					}
					int cid = Integer.parseInt(commId);
					cidList.add(cid);
				}
			}
		}
		
		PrintWriter pwLargeMs = new PrintWriter(new FileWriter(largeCommMsPath));
		PrintWriter pwSmallVid = new PrintWriter(new FileWriter(smallCommVidPath));
		int vid = 0;
		int smallVidCount = 0;
		int largeVidCount = 0;
		for (int cid : cidList) {
			if (cid != 4 && cid != 9) {
				pwSmallVid.println(vid);
				smallVidCount++;
			} else {
				pwLargeMs.println(cid);
				largeVidCount++;
			}
			vid++;
		}
		pwLargeMs.close();
		pwSmallVid.close();
		
		
		System.out.println("small community VID count: " + smallVidCount + ", large community VID count: " + largeVidCount);
	}
	
	/**
	 * Delete the layout information of vertices in small communities, and only keep layout of large-community
	 * vertices in the layout file.
	 * @throws Exception
	 */
	public static void getLayoutForLargeCommunity() throws Exception {
		String overallLayoutPath = "E:\\projects\\hbaseInvertedIndex\\TruthyRelated\\data\\hbase\\results"
			+ "\\20120924To20121106\\retweetMtxP2Tcot\\retweetMtx\\finalLayoutTwister256Map500Iter.txt";
		String smallCommVidPath = "E:\\projects\\hbaseInvertedIndex\\TruthyRelated\\data\\hbase\\results"
			+ "\\20120924To20121106\\retweetMtxP2Tcot\\retweetMtx\\smallCommVid.txt";
		String largeCommLayoutPath = "E:\\projects\\hbaseInvertedIndex\\TruthyRelated\\data\\hbase\\results"
			+ "\\20120924To20121106\\retweetMtxP2Tcot\\retweetMtx\\largeCommLayout.txt";
		
		List<String> overallLayouts = new LinkedList<String>();
		GeneralHelpers.readFileToCollection(overallLayoutPath, overallLayouts);
		List<String> smallCommVids = new LinkedList<String>();
		GeneralHelpers.readFileToCollection(smallCommVidPath, smallCommVids);
		PrintWriter pwLargeCommLayout = new PrintWriter(new FileWriter(largeCommLayoutPath));
		
		Iterator<String> iterVid = smallCommVids.iterator();
		int nextSmallVid = Integer.parseInt(iterVid.next());
		int loVid = 0;
		int vidDeleted = 0;
		for (String line : overallLayouts) {
			if (loVid != nextSmallVid) {
				pwLargeCommLayout.println(line);
			} else {
				vidDeleted++;
				if (iterVid.hasNext()) {
					nextSmallVid = Integer.parseInt(iterVid.next());
				}
			}
			loVid++;
		}
		pwLargeCommLayout.close();
		System.out.println("Number of small-community VIDs deleted: " + vidDeleted);
	}
	
	public static void checkMaxVertexId() throws Exception {
		String nodeListPath = "E:\\projects\\hbaseInvertedIndex\\TruthyRelated\\data\\hbase\\results"
			+ "\\20100902To20101031\\retweetMtxP2Tcot\\retweetMtx\\nodes.txt";
		List<String> lines = new LinkedList<String>();
		GeneralHelpers.readFileToCollection(nodeListPath, lines);
		double maxId = 0;
		for (String line : lines) {
			line = line.trim();
			double id = Double.parseDouble(line);
			if (id > maxId) {
				maxId = id;
			}
		}
		System.out.println("max node id: " + maxId + ", in Long: " + (long)maxId);
	}
	
	public static void testDatFileFromDiego() throws Exception {
		String datFilePath = "E:\\projects\\hbaseInvertedIndex\\TruthyRelated\\data\\Diego_meme_lifetime_project\\"
				+ "popularidade_hash_sobrou_2012_11_10.dat";
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(datFilePath), "UTF-8"));
		String line = br.readLine();
		int count = 0;
		while (line != null) {
			count++;
			line = br.readLine();
		}
		br.close();
		System.out.println("total line count: " + count);
	}
	
	public static void findDifferentTids(String dirMoreTids, String dirLessTids) throws Exception {
		HashSet<String> lessTids = new HashSet<String>();
		File fDirLessTids = new File(dirLessTids);
		File[] tidFiles = fDirLessTids.listFiles();
		for (File f : tidFiles) {
			if (f.getName().endsWith(".txt")) {
				BufferedReader br = new BufferedReader(new FileReader(f));
				String line = br.readLine();
				while (line != null) {
					line = line.trim();
					if (line.length() > 0) {
						lessTids.add(line);
					}
					line = br.readLine();
				}
				br.close();
			}
		}
		
		File fDirMoreTids = new File(dirMoreTids);
		tidFiles = fDirMoreTids.listFiles();
		for (File f : tidFiles) {
			if (f.getName().endsWith(".txt")) {
				BufferedReader br = new BufferedReader(new FileReader(f));
				String line = br.readLine();
				while (line != null) {
					line = line.trim();
					if (line.length() > 0 && !lessTids.contains(line)) {
						System.out.println(line);
					}
					line = br.readLine();
				}
				br.close();
			}
		}
	}
	
	public static void testMedianByPercent(float percent) {
		System.out.print("numbers: ");
		List<Integer> numbers = new ArrayList<Integer>(20);
		for (int i = 0; i <  20; i++) {
			numbers.add(i);
			System.out.print(i + " ");
		}
		System.out.println();
		int heap1SizeLimit = Math.round(percent * numbers.size());
		int heap2SizeLimit = numbers.size() - heap1SizeLimit;
		PriorityQueue<Integer> heap1 = new PriorityQueue<Integer>(numbers.size() / 2 + 2);
		PriorityQueue<Integer> heap2 = new PriorityQueue<Integer>(numbers.size() / 2 + 2);
		for (Integer n : numbers) {
			if (heap2.size() == 0) {
				heap2.offer(n);
			} else {
				int topHeap2 = heap2.peek();
				if (n < topHeap2) {
					heap1.offer(-n);
					if (heap1.size() > heap1SizeLimit) {
						int topHeap1 = -heap1.poll();
						heap2.offer(topHeap1);
					}
				} else {
					heap2.offer(n);
					if (heap2.size() > heap2SizeLimit) {
						topHeap2 = heap2.poll();
						heap1.offer(-topHeap2);
					}
				}
			}
		}
		int median = heap2.peek();
		if (heap1.size() > heap1SizeLimit) {
			median = -heap1.peek();
		}
		System.out.println("median: " + median);
	}
	
	public static void testHostName() throws Exception {
		InetAddress ina = InetAddress.getLocalHost();
		System.out.println("IP: " + ina.getHostAddress());
		System.out.println("Hostname: " + ina.getHostName());
	}
	
	public static void main(String[] args) {
		try {
			testBigInt(args);
			//testMedianByPercent((float)0.7);
			//testHostName();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
