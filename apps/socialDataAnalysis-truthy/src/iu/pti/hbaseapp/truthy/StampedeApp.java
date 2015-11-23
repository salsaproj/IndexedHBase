package iu.pti.hbaseapp.truthy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Application class encapsulating functions used on the Stampede cluster of TACC. 
 * 
 * @author gaoxm
 */
public class StampedeApp {
	protected static class DailyMemeFreqGenerator extends Thread {
		protected static final int TWEETS_PER_REPORT = 250000;
		
		protected ConcurrentLinkedDeque<String> jsonStrQueue;
		protected Map<String, Long> htCounts;
		protected AtomicInteger consumerCount;
		protected AtomicBoolean producerDone;
		protected int consumerId;
		JsonParser jsonParser;
		
		public DailyMemeFreqGenerator(ConcurrentLinkedDeque<String> jsonStrQueue, Map<String, Long> htCounts, AtomicInteger consumerCount,
				AtomicBoolean producerDone, int consumerId) throws Exception {
			this.jsonStrQueue = jsonStrQueue;
			this.htCounts = htCounts;
			this.consumerCount = consumerCount;
			this.producerDone = producerDone;
			this.consumerId = consumerId;
			jsonParser = new JsonParser();
		}
		
		@Override
		public void run() {
			int nTweetsProcessed = 0;
			String nextJsonStr = null;
			
			try {
				while (true) {
					nextJsonStr = jsonStrQueue.poll();
					if (nextJsonStr == null) {
						if (producerDone.get()) {
							break;
						} else {
							try {
								Thread.sleep(5000);
							} catch (InterruptedException e) {
								synchronized (System.err) {
									System.err.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": Sleep interrupted in run()");
									e.printStackTrace();
								}
							}
						}
					} else {
						countHtInJsonStr(nextJsonStr);
						nTweetsProcessed++;
						if (nTweetsProcessed % TWEETS_PER_REPORT == 0) {
							synchronized (System.err) {
								System.out.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": processed " + nTweetsProcessed 
										+ " tweets.");
							}
						}
					}
				}
				synchronized (System.err) {
					System.out.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": processed " + nTweetsProcessed 
							+ " tweets.");
				}
			} catch (Exception e) {
				synchronized (System.err) {
					System.err.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": Exception in run(): " + e.getMessage() 
							+ ". Decide to quit.");
					e.printStackTrace();
				}
			}
			consumerCount.decrementAndGet();
		}
		
		/**
		 * Find the hashtags in the tweet of the given JSON string and increase their counts in the hashtag count map.
		 * 
		 * @param jsonStr
		 * 	A JSON string of a tweet. 
		 */
		protected void countHtInJsonStr(String jsonStr) {
			jsonStr = jsonStr.trim();
			if (jsonStr.length() == 0) {
				return;
			}
			
			try {
				JsonObject joTweet = jsonParser.parse(jsonStr).getAsJsonObject();
				JsonElement jeEntities = joTweet.get("entities");
				if (jeEntities.isJsonNull()) {
					return;
				}
				
				JsonArray jaHashTags = jeEntities.getAsJsonObject().get("hashtags").getAsJsonArray();
				if (!jaHashTags.isJsonNull() && jaHashTags.size() > 0) {
					Iterator<JsonElement> iht = jaHashTags.iterator();
					while (iht.hasNext()) {
						String hashtag = "#" + iht.next().getAsJsonObject().get("text").getAsString().toLowerCase();
						synchronized (htCounts) {
							Long count = htCounts.get(hashtag);
							if (count == null) {
								htCounts.put(hashtag, 1L);
							} else {
								htCounts.put(hashtag, count + 1);
							}
						}
					}
				}
			} catch (Exception e) {
				synchronized (System.err) {
					System.err.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": Exception in countHtInJsonStr(): " + e.getMessage() 
							+ ".");
					e.printStackTrace();
				}
			}
		}
	}
	
	protected static class MemeRegexMatchConsumer extends Thread {
		protected static final int TWEETS_PER_REPORT = 250000;
		
		protected Pattern pattern;
		protected ConcurrentLinkedDeque<String> jsonStrQueue;
		protected AtomicInteger consumerCount;
		protected AtomicBoolean producerDone;
		protected PrintWriter pwOut;
		protected int consumerId;
		JsonParser jsonParser;
		
		public MemeRegexMatchConsumer(String memeRegex, ConcurrentLinkedDeque<String> jsonStrQueue, AtomicInteger consumerCount, PrintWriter pwOut, 
				AtomicBoolean producerDone, int consumerId) throws Exception {
			pattern = Pattern.compile(memeRegex);
			this.jsonStrQueue = jsonStrQueue;
			this.consumerCount = consumerCount;
			this.producerDone = producerDone;
			this.pwOut = pwOut;
			this.consumerId = consumerId;
			jsonParser = new JsonParser();
		}
		
		@Override
		public void run() {
			int nTweetsProcessed = 0;
			int nMatchFound = 0;
			String nextJsonStr = null;
			
			try {
				while (true) {
					nextJsonStr = jsonStrQueue.poll();
					if (nextJsonStr == null) {
						if (producerDone.get()) {
							break;
						} else {
							try {
								Thread.sleep(2000);
							} catch (InterruptedException e) {
								synchronized (System.err) {
									System.err.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": Sleep interrupted in run()");
									e.printStackTrace();
								}
							}
						}
					} else {
						if (matchJsonStr(nextJsonStr)) {
							synchronized(pwOut) {
								pwOut.println(nextJsonStr);
							}
							nMatchFound++;
						}
						nTweetsProcessed++;
						if (nTweetsProcessed % TWEETS_PER_REPORT == 0) {
							synchronized (System.err) {
								System.out.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": processed " + nTweetsProcessed 
										+ " tweets. nMatchFound: " + nMatchFound);
							}
						}
					}
				}
				synchronized (System.err) {
					System.out.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": processed " + nTweetsProcessed 
							+ " tweets. nMatchFound: " + nMatchFound);
				}
			} catch (Exception e) {
				synchronized (System.err) {
					System.err.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": Exception in run(): " + e.getMessage() 
							+ ". Decide to quit.");
					e.printStackTrace();
				}
			}
			consumerCount.decrementAndGet();
		}
		
		/**
		 * Check if the given JSON string for a tweet contains memes that matches the meme regular expression given in the constructor.
		 * 
		 * @param jsonStr
		 * 	A JSON string of a tweet. 
		 * @return
		 * 	<b>true</b> if a match is found; <b>false</b> otherwise. 
		 */
		protected boolean matchJsonStr(String jsonStr) {
			jsonStr = jsonStr.trim();
			if (jsonStr.length() == 0) {
				return false;
			}
			
			try {
				JsonObject joTweet = jsonParser.parse(jsonStr).getAsJsonObject();
				JsonElement jeEntities = joTweet.get("entities");
				if (jeEntities.isJsonNull()) {
					return false;
				}
				
				JsonArray jaUserMentions = jeEntities.getAsJsonObject().get("user_mentions").getAsJsonArray();
				JsonArray jaHashTags = jeEntities.getAsJsonObject().get("hashtags").getAsJsonArray();
				JsonArray jaUrls = jeEntities.getAsJsonObject().get("urls").getAsJsonArray();
				
				if (!jaUserMentions.isJsonNull() && jaUserMentions.size() > 0) {
					Iterator<JsonElement> ium = jaUserMentions.iterator();
					while (ium.hasNext()) {
						JsonObject jomu = ium.next().getAsJsonObject();
						String mention = "@" + jomu.get("screen_name").getAsString().toLowerCase();
						if (pattern.matcher(mention).matches()) {
							return true;
						}
					}
				}
				
				if (!jaHashTags.isJsonNull() && jaHashTags.size() > 0) {
					Iterator<JsonElement> iht = jaHashTags.iterator();
					while (iht.hasNext()) {
						String hashtag = "#" + iht.next().getAsJsonObject().get("text").getAsString().toLowerCase();
						if (pattern.matcher(hashtag).matches()) {
							return true;
						}
					}
				}
				
				if (!jaUrls.isJsonNull() && jaUrls.size() > 0) {
					Iterator<JsonElement> iurl = jaUrls.iterator();
					while (iurl.hasNext()) {
						JsonObject joUrl = iurl.next().getAsJsonObject();
						JsonElement jeChildUrl = joUrl.getAsJsonObject().get("url");
						JsonElement jeChildEurl = joUrl.getAsJsonObject().get("expanded_url");
						if (jeChildUrl != null && !jeChildUrl.isJsonNull()) {
							if (pattern.matcher(jeChildUrl.getAsString().toLowerCase()).matches()) {
								return true;
							}
						}
						if (jeChildEurl != null && !jeChildEurl.isJsonNull()) {
							if (pattern.matcher(jeChildEurl.getAsString().toLowerCase()).matches()) {
								return true;
							}
						}
					}
				}
				
				return false;
			} catch (Exception e) {
				synchronized (System.err) {
					System.err.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": Exception in matchJsonStr(): " + e.getMessage() 
							+ ".");
					e.printStackTrace();
				}
				return false;
			}
		}
	}
	
	protected static class TextSearchConsumer extends Thread {
		protected static final int TWEETS_PER_REPORT = 250000;
		
		protected Set<String> keywords;
		protected ConcurrentLinkedDeque<String> jsonStrQueue;
		protected AtomicInteger consumerCount;
		protected AtomicBoolean producerDone;
		protected PrintWriter pwOut;
		protected int consumerId;
		JsonParser jsonParser;
		Analyzer analyzer;
		
		public TextSearchConsumer(String keywordsQuery, ConcurrentLinkedDeque<String> jsonStrQueue, AtomicInteger consumerCount, PrintWriter pwOut, 
				AtomicBoolean producerDone, int consumerId) throws Exception {
			String[] allKeywords = keywordsQuery.replaceAll("^[,\\s]+", "").split("[,\\s]+");
			this.keywords = new HashSet<String>();
			for (String kw : allKeywords) {
				keywords.add(kw);
			}
			this.jsonStrQueue = jsonStrQueue;
			this.consumerCount = consumerCount;
			this.producerDone = producerDone;
			this.pwOut = pwOut;
			this.consumerId = consumerId;
			jsonParser = new JsonParser();
			analyzer = new StandardAnalyzer(Version.LUCENE_36);
		}
		
		@Override
		public void run() {
			int nTweetsProcessed = 0;
			int nMatchFound = 0;
			String nextJsonStr = null;
			
			try {
				while (true) {
					nextJsonStr = jsonStrQueue.poll();
					if (nextJsonStr == null) {
						if (producerDone.get()) {
							break;
						} else {
							try {
								Thread.sleep(2000);
							} catch (InterruptedException e) {
								synchronized (System.err) {
									System.err.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": Sleep interrupted in run()");
									e.printStackTrace();
								}
							}
						}
					} else {
						if (findKeywordsInJsonStr(nextJsonStr)) {
							nMatchFound++;
						}
						nTweetsProcessed++;
						if (nTweetsProcessed % TWEETS_PER_REPORT == 0) {
							synchronized (System.err) {
								System.out.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": processed " + nTweetsProcessed 
										+ " tweets. nMatchFound: " + nMatchFound);
							}
						}
					}
				}
				synchronized (System.err) {
					System.out.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": processed " + nTweetsProcessed 
							+ " tweets. nMatchFound: " + nMatchFound);
				}
			} catch (Exception e) {
				synchronized (System.err) {
					System.err.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": Exception in run(): " + e.getMessage() 
							+ ". Decide to quit.");
					e.printStackTrace();
				}
			}
			consumerCount.decrementAndGet();
		}
		
		/**
		 * Check if the given JSON string for a tweet contains any of the given keywords in its text.
		 * If yes, write some fields of the tweet to the output file.
		 * 
		 * @param jsonStr
		 * 	A JSON string of a tweet. 
		 * @return
		 * 	<b>true</b> if some keyword is found; <b>false</b> otherwise. 
		 */
		protected boolean findKeywordsInJsonStr(String jsonStr) {
			jsonStr = jsonStr.trim();
			if (jsonStr.length() == 0) {
				return false;
			}
			
			try {
				JsonObject joTweet = jsonParser.parse(jsonStr).getAsJsonObject();
				JsonElement jeText = joTweet.get("text");
				if (jeText.isJsonNull()) {
					return false;
				}
				String text = jeText.getAsString();
				
				TokenStream ts = analyzer.reusableTokenStream("dummyField", new StringReader(text));
				CharTermAttribute charTermAttr = ts.addAttribute(CharTermAttribute.class);
				while (ts.incrementToken()) {
					String termVal = charTermAttr.toString();
					if (keywords.contains(termVal)) {
						StringBuilder sb = new StringBuilder();
						//sb.append(joTweet.get("id").toString()).append('\t');
						sb.append(text).append('\t');
						//sb.append(joTweet.get("created_at").toString()).append('\t');
						sb.append(joTweet.get("coordinates").toString()).append('\t');
						//sb.append(joTweet.get("geo").toString()).append('\t');
						//sb.append(joTweet.get("place").toString()).append('\t');
						
						JsonObject joUser = joTweet.get("user").getAsJsonObject();
						//sb.append(joUser.get("id").toString()).append('\t');
						//sb.append(joUser.get("screen_name").toString()).append('\t');
						//sb.append(joUser.get("created_at").toString()).append('\t');
						//sb.append(joUser.get("time_zone").toString()).append('\t');
						sb.append(joUser.get("location").toString()).append('\t');
						sb.append(joUser.get("lang").toString());
						
						synchronized(pwOut) {														
							pwOut.println(sb.toString());
						}
						
						return true;
					}
				}
				ts.close();
				
				return false;
			} catch (Exception e) {
				synchronized (System.err) {
					System.err.println(getCurrentDateTimeStr() + " Consumer " + consumerId + ": Exception in matchJsonStr(): " + e.getMessage() 
							+ ".");
					e.printStackTrace();
				}
				return false;
			}
		}
	}
	
	protected static class JsonStringProducer extends Thread {
		protected static final int TWEETS_PER_BATCH = 300000;
		
		protected String jsonGzFilePath;
		protected ConcurrentLinkedDeque<String> jsonStrQueue;
		protected AtomicBoolean producerDone;
		
		public JsonStringProducer(String jsonGzFilePath, ConcurrentLinkedDeque<String> jsonStrQueue, AtomicBoolean producerDone) {
			this.jsonGzFilePath = jsonGzFilePath;
			this.jsonStrQueue = jsonStrQueue;
			this.producerDone = producerDone;
		}
		
		@Override
		public void run() {
			try {
				GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(jsonGzFilePath));
				BufferedReader brInput = new BufferedReader(new InputStreamReader(gzInputStream, "UTF-8"));
				
				int totalCount = 0;
				String line = brInput.readLine();
				while (line != null) {
					int thisCount = 0;
					while (line != null && thisCount < TWEETS_PER_BATCH) {
						jsonStrQueue.add(line);
						thisCount++;
						totalCount++;
						line = brInput.readLine();
					}
					synchronized (System.err) {
						System.out.println(getCurrentDateTimeStr() + " Producer: added " + totalCount + " lines to the queue in total.");
						System.out.flush();
					}
					
					if (line == null) {
						break;
					}
					
					while (jsonStrQueue.size() > TWEETS_PER_BATCH / 10) {
						try {
							Thread.sleep(2000);
						} catch (InterruptedException e) {
							synchronized (System.err) {
								System.err.println(getCurrentDateTimeStr() + " Producer: Sleep interrupted in run()");
								e.printStackTrace();
							}
						}
					}
				}
				
				brInput.close();
			} catch (Exception e) {
				synchronized (System.err) {
					System.err.println(getCurrentDateTimeStr() + " Producer: Exception in run(): " + e.getMessage() + ". Decide to abort.");
					e.printStackTrace();
				}
			}
			producerDone.set(true);
		}
	}
	
	/**
	 * Find the tweets in a given .json.gz file that have memes matching the given regular expression, and write the matched tweets to an
	 * output file.
	 * @param jsonGzFilePath
	 * 	Path to the given .json.gz file.
	 * @param memeRegex
	 * 	Regular expression to match the memes against.
	 * @param outputPath
	 * 	Path to the output file.
	 * @param nConsumer
	 * 	Number of threads to use for matching.
	 * @throws Exception
	 * 	If any errors are encountered during the matching process.
	 */
	public void findTweetsByMemeRegex(String jsonGzFilePath, String memeRegex, String outputPath, int nConsumer) throws Exception {
		System.out.println(getCurrentDateTimeStr() + " start tweets search by meme regex: " + memeRegex);
		long tStart = System.currentTimeMillis();
		ConcurrentLinkedDeque<String> jsonStrQueue = new ConcurrentLinkedDeque<String>();
		AtomicInteger consumerCount = new AtomicInteger(nConsumer);
		AtomicBoolean producerDone = new AtomicBoolean(false);
		PrintWriter pwOut = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(outputPath)), "UTF-8"));
		
		JsonStringProducer producer = new JsonStringProducer(jsonGzFilePath, jsonStrQueue, producerDone);
		producer.start();
		for (int i=0; i<nConsumer; i++) {
			MemeRegexMatchConsumer consumer = new MemeRegexMatchConsumer(memeRegex, jsonStrQueue, consumerCount, pwOut, producerDone, i);
			consumer.start();
		}
		
		while (consumerCount.get() > 0) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				synchronized (System.err) {
					System.err.println(getCurrentDateTimeStr() + " Driver: Sleep interrupted.");
				}
			}
		}
		pwOut.close();
		long tEnd = System.currentTimeMillis();
		double totalTime = (tEnd - tStart) / 1000.0;
		System.out.println(getCurrentDateTimeStr() + " Done! Total time taken: " + totalTime + " seconds.");		
	}
	
	/**
	 * Find the tweets in a given .json.gz file that contain any of the given keywords in their text, and write certain fields of the
	 * tweets to an output file.
	 * @param jsonGzFilePath
	 * 	Path to the given .json.gz file.
	 * @param keywords
	 * 	List of keywords to find in the text.
	 * @param outputPath
	 * 	Path to the output file.
	 * @param nConsumer
	 * 	Number of threads to use for searching.
	 * @throws Exception
	 * 	If any errors are encountered during the matching process.
	 */
	public void findTweetsByTextKeywords(String jsonGzFilePath, String keywords, String outputPath, int nConsumer) throws Exception {
		long tStart = System.currentTimeMillis();
		ConcurrentLinkedDeque<String> jsonStrQueue = new ConcurrentLinkedDeque<String>();
		AtomicInteger consumerCount = new AtomicInteger(nConsumer);
		AtomicBoolean producerDone = new AtomicBoolean(false);
		PrintWriter pwOut = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(outputPath)), "UTF-8"));
		
		JsonStringProducer producer = new JsonStringProducer(jsonGzFilePath, jsonStrQueue, producerDone);
		producer.start();
		for (int i=0; i<nConsumer; i++) {
			TextSearchConsumer consumer = new TextSearchConsumer(keywords, jsonStrQueue, consumerCount, pwOut, producerDone, i);
			consumer.start();
		}
		
		while (consumerCount.get() > 0) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				synchronized (System.err) {
					System.err.println(getCurrentDateTimeStr() + " Driver: Sleep interrupted.");
				}
			}
		}
		pwOut.close();
		long tEnd = System.currentTimeMillis();
		double totalTime = (tEnd - tStart) / 1000.0;
		System.out.println(getCurrentDateTimeStr() + " Done! Total time taken: " + totalTime + " seconds.");		
	}
	
	/**
	 * Count all the hashtags in a given .json.gz file, and write the counts to an output file.
	 * 
	 * @param jsonGzFilePath
	 * 	Path to the given .json.gz file.
	 * @param outputPath
	 * 	Path to the output file.
	 * @param nConsumer
	 * 	Number of threads to use for matching.
	 * @throws Exception
	 * 	If any errors are encountered during the matching process.
	 */
	public void generateDailyMemeFreq(String jsonGzFilePath, String outputPath, int nConsumer) throws Exception {
		long tStart = System.currentTimeMillis();
		ConcurrentLinkedDeque<String> jsonStrQueue = new ConcurrentLinkedDeque<String>();
		AtomicInteger consumerCount = new AtomicInteger(nConsumer);
		AtomicBoolean producerDone = new AtomicBoolean(false);
		HashMap<String, Long> htCounts = new HashMap<String, Long>();
		
		JsonStringProducer producer = new JsonStringProducer(jsonGzFilePath, jsonStrQueue, producerDone);
		producer.start();
		for (int i=0; i<nConsumer; i++) {
			DailyMemeFreqGenerator consumer = new DailyMemeFreqGenerator(jsonStrQueue, htCounts, consumerCount, producerDone, i);
			consumer.start();
		}
		
		while (consumerCount.get() > 0) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				synchronized (System.err) {
					System.err.println(getCurrentDateTimeStr() + " Driver: Sleep interrupted.");
				}
			}
		}
		
		int idx = jsonGzFilePath.lastIndexOf(File.separatorChar);
		String fileName = jsonGzFilePath.substring(idx + 1);
		idx = fileName.indexOf('.');
		String date = fileName.substring(0, idx);
		PrintWriter pwOut = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(outputPath)), "UTF-8"));
		for (Map.Entry<String, Long> e : htCounts.entrySet()) {
			pwOut.println(date + "|" + e.getKey() + "|" + e.getValue());
		}
		pwOut.close();
		long tEnd = System.currentTimeMillis();
		double totalTime = (tEnd - tStart) / 1000.0;
		System.out.println(getCurrentDateTimeStr() + " Done! Total time taken: " + totalTime + " seconds.");		
	}
	
	
	/**
	 * Parse a node list string into a list of host names, and write them to the file specified by outputPath.
	 * 
	 * @param nodesListStr
	 * 	The node list string. Its format is like "c510-209,c514-[803-804,901-904],c515-[804,901-903],..."
	 * @param outputPath
	 * 	The path of the output file. Each line in the output file is one host name, and the host names corresponding
	 *  to the example nodes list string above are "c514-803", "c514-804", "c514-901", "c514-902", ...
	 * 
	 * @throws Exception
	 * 	If case of any exceptions during parsing or output file writing.
	 */
	public void parseStampedeNodesList(String nodesListStr, String outputPath) throws Exception {
		PrintWriter pwOut = new PrintWriter(new FileWriter(outputPath));
		int count = 0;
		
		int idxMinus = nodesListStr.indexOf('-');
		while (idxMinus >= 0) {
			if (nodesListStr.charAt(idxMinus + 1) == '[') {
				String prefix = nodesListStr.substring(0, idxMinus + 1);
				int idxRight = nodesListStr.indexOf(']');
				String[] numRanges = nodesListStr.substring(idxMinus + 2, idxRight).split(",");
				for (String range : numRanges) {
					int idxMinusIn = range.indexOf('-');
					int start = -1;
					int end = -2;
					if (idxMinusIn < 0) {
						start = end = Integer.parseInt(range);
					} else {
						start = Integer.parseInt(range.substring(0, idxMinusIn));
						end = Integer.parseInt(range.substring(idxMinusIn + 1));
					}
					if (start > end) {
						pwOut.close();
						throw new Exception("In valid number range string: " + range);
					}
					while(start <= end) {
						String startStr = Integer.toString(start);
						while (startStr.length() < 3) {
							startStr = "0" + startStr;
						}
						pwOut.println(prefix + startStr);
						count++;
						start++;
					}
				}
				
				if (idxRight == nodesListStr.length() - 1) {
					break;
				} else {
					nodesListStr = nodesListStr.substring(idxRight + 2);
					idxMinus = nodesListStr.indexOf('-');
				}
			} else {
				int idxComma = nodesListStr.indexOf(',');
				if (idxComma < 0) {
					pwOut.println(nodesListStr);
					count++;
					break;
				} else {
					pwOut.println(nodesListStr.substring(0, idxComma));
					count++;
					nodesListStr = nodesListStr.substring(idxComma + 1);
					idxMinus = nodesListStr.indexOf('-');
				}
			}
		}
	
		pwOut.close();
		System.out.println("Done! " + count + " node host names generated in total.");
	}
	
	/**
	 * Recursively find all files under the given directory whose names end with the given suffix, and put the paths of these files to the given 
	 * path list.
	 * 
	 * @param FDir
	 * 	File object representing the directory.
	 * @param pathList
	 * 	List for putting the paths of the matching files.
	 * @param suffix
	 * 	File name suffix to match.
	 */
	protected void getAllFilePathsUnderDir(File fDir, List<String> pathList, String suffix) {
		for (File f : fDir.listFiles()) {
			if (f.isFile() && f.getName().endsWith(suffix)) {
				pathList.add(f.getAbsolutePath());
			} else if (f.isDirectory()) {
				getAllFilePathsUnderDir(f, pathList, suffix);
			}
		}		
	}
	
	/**
	 * Make a task input file that indicates which input .json.gz files will be processed on which nodes. Each line in the task input file
	 * is in the format of "[node hostname] [input .json.gz path] [date] [month]"
	 *  
	 * @param topTweetsDir
	 * 	Top level directory containing all the .json.gz files. 
	 * @param nodesFilePath
	 * 	Path to the nodes list file.
	 * @param taskFilePath
	 * 	Path to the task input file to be generated.
	 * @throws Exception
	 * 	If any exceptions are encountered when making the task input file.
	 */
	public void makeTaskFile(String topTweetsDir, String nodesFilePath, String taskFilePath) throws Exception {
		// read the hostnames of all nodes
		LinkedList<String> nodeList = new LinkedList<String>();
		BufferedReader brNodes = new BufferedReader(new FileReader(nodesFilePath));
		String line = brNodes.readLine();
		while (line != null) {
			line = line.trim();
			if (line.length() > 0) {
				nodeList.add(line);
			}
			line = brNodes.readLine();
		}
		brNodes.close();
		String[] nodes = new String[nodeList.size()];
		nodes = nodeList.toArray(nodes);
		
		// get paths to all .json.gz files
		LinkedList<String> pathList = new LinkedList<String>();
		getAllFilePathsUnderDir(new File(topTweetsDir), pathList, ".json.gz");
		PrintWriter pwTask = new PrintWriter(new FileWriter(taskFilePath));
		int pos = 0;
		for (String path : pathList) {
			String node = nodes[pos % nodes.length];
			int idx = path.lastIndexOf(File.separatorChar);
			String fName = path.substring(idx + 1);
			int idxDot = fName.indexOf('.');
			String date = fName.substring(0, idxDot);
			int idxMinus = date.lastIndexOf('-');
			String month = date.substring(0, idxMinus);
			pwTask.println(node + " " + path + " " + date + " " + month);
			pos++;
		}
		pwTask.close();
		
		System.out.println("Done! Number of .json.gz files in the task file: " + pos);
	}
	
	public static final Calendar calTmp = Calendar.getInstance();
	public static String getCurrentDateTimeStr() {
		calTmp.setTimeInMillis(System.currentTimeMillis());
		StringBuffer sb = new StringBuffer();
		
		int year = calTmp.get(Calendar.YEAR);
		String yearStr = String.valueOf(year);
		for (int i=0; i < 4 - yearStr.length(); i++)
			sb.append('0');
		sb.append(year).append('-');
		
		int month = calTmp.get(Calendar.MONTH) + 1;
		if (month < 10)
			sb.append('0');
		sb.append(month).append('-');
		
		int day = calTmp.get(Calendar.DAY_OF_MONTH);
		if (day < 10)
			sb.append('0');
		sb.append(day);
		sb.append('T');
		
		if (calTmp.get(Calendar.HOUR_OF_DAY) < 10)
			sb.append('0');
		sb.append(calTmp.get(Calendar.HOUR_OF_DAY)).append(':');
		
		if (calTmp.get(Calendar.MINUTE) < 10)
			sb.append('0');
		sb.append(calTmp.get(Calendar.MINUTE)).append(':');
		
		if (calTmp.get(Calendar.SECOND) < 10)
			sb.append('0');
		sb.append(calTmp.get(Calendar.SECOND));
		
		return sb.toString();
	}
	
	/**
	 * Read the file specified by <b>jsonGzPath</b> sequentially and report the time taken.
	 * 
	 * @param jsonGzPath
	 * @throws Exception
	 */
	public void fileReadTest(String jsonGzPath) throws Exception {
		int count = 0;
		System.out.println(getCurrentDateTimeStr() + " start file reading test for " + jsonGzPath);
		long startTime = System.currentTimeMillis();
		GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(jsonGzPath));
		BufferedReader brInput = new BufferedReader(new InputStreamReader(gzInputStream, "UTF-8"));
		String line = brInput.readLine();
		while (line != null) {
			count++;
			if (count % 1000000 == 0) {
				System.out.println(getCurrentDateTimeStr() + " finished reading " + count + " lines.");
			}
			line = brInput.readLine();
		}
		brInput.close();
		long endTime = System.currentTimeMillis();
		double seconds = (endTime - startTime) / 1000.0;
		File fJsonGz = new File(jsonGzPath);
		System.out.println(getCurrentDateTimeStr() + " finished test. Read " + count + " lines in total. Time taken (s): " 
				+ seconds + ". File size (bytes): " + fJsonGz.length() + ". Speed (bytes/s): " + fJsonGz.length() / seconds);
	}

	/**
	 * Print the usage guide of this application.
	 */
	public static void usage() {
		System.out.println("Usage: java iu.pti.hbaseapp.truthy.StampedeApp <command> <arguments>");
		System.out.println("	Where '<command> <arguments>' could be one of the following combinations:");
		System.out.println("	parse-nodes-list <nodes list string> <output file path>");
		System.out.println("	get-tweets-with-meme-regex <input .json.gz file path> <regular expression> <output file path> <number of threads>");
		System.out.println("	get-tweets-with-text <input .json.gz file path> <keyword list> <output file path> <number of threads>");
		System.out.println("	make-task-file <top directory for .json.gz files> <nodes file path> <task file path>");
		System.out.println("	generate-meme-freq <input .json.gz file path> <output file path> <number of threads>");
		System.out.println("	file-read-test <.json.gz file path>");
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			usage();
			System.exit(1);
		}
		String command = args[0];
		StampedeApp app = new StampedeApp();
		
		try {
			if (command.equals("parse-nodes-list")) {
				if (args.length < 3) {
					usage();
					System.exit(1);
				} else {
					app.parseStampedeNodesList(args[1], args[2]);
				}
			} else if (command.equals("get-tweets-with-meme-regex")) {
				if (args.length < 5) {
					usage();
					System.exit(1);
				} else {
					app.findTweetsByMemeRegex(args[1], args[2], args[3], Integer.parseInt(args[4]));
				}
			} else if (command.equals("make-task-file")) {
				if (args.length < 4) {
					usage();
					System.exit(1);
				} else {
					app.makeTaskFile(args[1], args[2], args[3]);
				}
			} else if (command.equals("generate-meme-freq")) {
				if (args.length < 4) {
					usage();
					System.exit(1);
				} else {
					app.generateDailyMemeFreq(args[1], args[2], Integer.parseInt(args[3]));
				}
			} else if (command.equals("get-tweets-with-text")) {
				if (args.length < 5) {
					usage();
					System.exit(1);
				} else {
					app.findTweetsByTextKeywords(args[1], args[2], args[3], Integer.parseInt(args[4]));
				}
			} else if (command.equals("file-read-test")) {
				if (args.length < 2) {
					usage();
					System.exit(1);
				} else {
					app.fileReadTest(args[1]);
				}
			}
		} catch (Exception e) {			
			e.printStackTrace();
			System.out.println("Exception caught when running the application: " + e.getMessage());
			usage();
		}		
	}
}
