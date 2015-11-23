package iu.pti.hbaseapp.truthy.streaming;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;
import iu.pti.hbaseapp.truthy.ConstantsTruthy;
import iu.pti.hbaseapp.truthy.StampedeApp;
import iu.pti.hbaseapp.truthy.TruthyHelpers;
import iu.pti.hbaseapp.truthy.streaming.ProtoMeme.ProtoMemeType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Tester class related to stream meme clustering algorithm.
 * @author gaoxm
 */
public class MemeClusteringTester {	
	protected static final Log LOG = LogFactory.getLog(MemeClusteringTester.class);
	
	/**
	 * Generate protomemes based on the tweets in <b>jsonGzDirPath</b> and report the number of protomemes corresponding to different
	 * number of tweets scanned.
	 * 
	 * @param jsonGzDirPath
	 *  Directory containing .json.gz files.
	 * @param nTweetsToScan
	 *  Total number of tweets to scan.
	 * @param pmType
	 *  What type of protomemes to count. Could be 'url', 'mention', 'hashtag', 'phrase', or 'all'.
	 * @throws Exception
	 */
	public void protoMemeCount(String jsonGzDirPath, int nTweetsToScan, ProtoMemeType pmType) throws Exception {
		HashSet<String> urlProtoMemes = null;
		HashSet<String> umProtoMemes = null;
		HashSet<String> htProtoMemes = null;
		HashSet<String> phraseProtoMemes = null;
		if (pmType == ProtoMemeType.ALL || pmType == ProtoMemeType.URL) {
			urlProtoMemes = new HashSet<String>(5000000);
		}
		if (pmType == ProtoMemeType.ALL || pmType == ProtoMemeType.MENTION) {
			umProtoMemes = new HashSet<String>(5000000);
		}
		if (pmType == ProtoMemeType.ALL || pmType == ProtoMemeType.HASHTAG) {
			htProtoMemes = new HashSet<String>(5000000);
		}
		if (pmType == ProtoMemeType.ALL || pmType == ProtoMemeType.PHRASE) {
			phraseProtoMemes = new HashSet<String>(20000000);
		}
		JsonParser jsonParser = new JsonParser();
		
		int count = 0;
		long startTime = System.currentTimeMillis();
		String firstTweetTime = null;
		String lastTweetTime = null;
		ArrayList<File> fJsonGzs = getSortedJsonGzFiles(jsonGzDirPath);
		for (File f : fJsonGzs) {
			System.out.println(StampedeApp.getCurrentDateTimeStr() + " start protomeme counting for " + f.getName());
			GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(f));
			BufferedReader brInput = new BufferedReader(new InputStreamReader(gzInputStream, "UTF-8"));
			String line = brInput.readLine();
			while (line != null) {
				line = line.trim();
				try {
					JsonObject joTweet = jsonParser.parse(line).getAsJsonObject();
					String tweetTime = joTweet.get("created_at").getAsString();
					if (firstTweetTime == null) {
						firstTweetTime = tweetTime;
					}
					lastTweetTime = tweetTime;
					String text = joTweet.get("text").getAsString();
					byte[] markers = new byte[text.length()];

					JsonElement jeEntities = joTweet.get("entities");
					if (jeEntities != null && !jeEntities.isJsonNull()) {
						JsonArray jaUrls = jeEntities.getAsJsonObject().get("urls").getAsJsonArray();
						updateUrlProtoMemes(jaUrls, urlProtoMemes, markers);						
						JsonArray jaUserMentions = jeEntities.getAsJsonObject().get("user_mentions").getAsJsonArray();
						updateMentionProtoMemes(jaUserMentions, umProtoMemes, markers);						
						JsonArray jaHashTags = jeEntities.getAsJsonObject().get("hashtags").getAsJsonArray();
						updateHtProtoMemes(jaHashTags, htProtoMemes, markers);
					}
					updatePhraseProtoMemes(text, phraseProtoMemes, markers);
								        
					count++;
					if (count % 100000 == 0) {
						int nUrlPm = urlProtoMemes != null ? urlProtoMemes.size() : 0;
						int nUmPm = umProtoMemes != null ? umProtoMemes.size() : 0;
						int nHtPm = htProtoMemes != null ? htProtoMemes.size() : 0;
						int nPhrasePm = phraseProtoMemes != null ? phraseProtoMemes.size() : 0;
						System.out.println("Number of tweets processed: " + count + ", number of protomemes: " +
								(nUrlPm + nUmPm + nHtPm + nPhrasePm) + " (URL " + nUrlPm + ", mention " + nUmPm + ", hashtag " + 
								nHtPm +	", phrase " + nPhrasePm + ").");
					}
					if (count >= nTweetsToScan) {
						break;
					}
				} catch (Exception e) {
					System.out.println("Exception : " + e.getMessage());
				}
				line = brInput.readLine();
			}
			brInput.close();
			
			if (count >= nTweetsToScan) {
				break;
			}
		}
		long endTime = System.currentTimeMillis();
		double seconds = (endTime - startTime) / 1000.0;
		int nUrlPm = urlProtoMemes != null ? urlProtoMemes.size() : 0;
		int nUmPm = umProtoMemes != null ? umProtoMemes.size() : 0;
		int nHtPm = htProtoMemes != null ? htProtoMemes.size() : 0;
		int nPhrasePm = phraseProtoMemes != null ? phraseProtoMemes.size() : 0;
		System.out.println(StampedeApp.getCurrentDateTimeStr() + " finished test. Number of tweets processed: " + count + ", number of protomemes: " +
				(nUrlPm + nUmPm + nHtPm + nPhrasePm) + " (URL " + nUrlPm + ", mention " + nUmPm + ", hashtag " + nHtPm +
				", phrase " + nPhrasePm + "). Time taken (s): " + seconds + ".");
		System.out.println("First tweet creation time: " + firstTweetTime + ", last tweet creation time: " + lastTweetTime);

		int mb = 1024 * 1024;
		Runtime runtime = Runtime.getRuntime();
		System.out.println("##### Heap utilization statistics [MB] #####");
		System.out.println("Used Memory:" + (runtime.totalMemory() - runtime.freeMemory()) / mb);
		System.out.println("Free Memory:" + runtime.freeMemory() / mb);
		System.out.println("Total Memory:" + runtime.totalMemory() / mb);
		System.out.println("Max Memory:" + runtime.maxMemory() / mb);
	}

	/** A string builder for creating string presentations of phrases. */
	protected StringBuilder sbPhrase = new StringBuilder();
	
	/**
	 * Generate a phrase from <b>text</b>, and put it into <b>phraseProtoMemes</b>.  
	 * @param text
	 *  A string containing the text of a tweet.
	 * @param phraseProtoMemes
	 *  Container HashSet for the corresponding phrase.
	 * @param markers
	 *  A byte array with the same length of <b>text</b>, containing 0s and 1s. The 1s mark the positions corresponding to URLs, mentions,
	 *  and hashtags, so that those characters will be removed before phrases are generated.
	 * @throws IOException
	 */
	private void updatePhraseProtoMemes(String text, HashSet<String> phraseProtoMemes, byte[] markers) throws IOException {
		if (phraseProtoMemes != null) {
			char[] ca = text.toCharArray();
			for (int i = 0; i < markers.length; i++) {
				if (markers[i] > 0) {
					ca[i] = ' ';
				}
			}

			String newText = new String(ca);
			TokenStream tokenStream = new StandardTokenizer(Version.LUCENE_36, new StringReader(newText));
			tokenStream = new StopFilter(Version.LUCENE_36, tokenStream, Constants.getStopWordSet());
			tokenStream = new PorterStemFilter(tokenStream);
			sbPhrase.setLength(0);
			CharTermAttribute charTermAttr = tokenStream.getAttribute(CharTermAttribute.class);
			while (tokenStream.incrementToken()) {
				sbPhrase.append(charTermAttr.toString()).append(' ');
			}
			tokenStream.close();
			if (sbPhrase.length() > 0) {
				sbPhrase.deleteCharAt(sbPhrase.length() - 1);
			}
			phraseProtoMemes.add(sbPhrase.toString());
		}
	}

	private void updateHtProtoMemes(JsonArray jaHashTags, HashSet<String> htProtoMemes, byte[] markers) {
		if (!jaHashTags.isJsonNull() && jaHashTags.size() > 0) {
			Iterator<JsonElement> iht = jaHashTags.iterator();
			while (iht.hasNext()) {
				JsonObject joHt = iht.next().getAsJsonObject();
				JsonArray jaIdx = joHt.get("indices").getAsJsonArray();
				for (int i=jaIdx.get(0).getAsInt(); i < jaIdx.get(1).getAsInt(); i++) {
					markers[i] = 1;
				}
				if (htProtoMemes != null) {
					String hashtag = "#" + joHt.get("text").getAsString();
					htProtoMemes.add(hashtag);
				}
			}
		}
	}

	private void updateMentionProtoMemes(JsonArray jaUserMentions, HashSet<String> umProtoMemes, byte[] markers) {
		if (!jaUserMentions.isJsonNull() && jaUserMentions.size() > 0) {
			Iterator<JsonElement> ium = jaUserMentions.iterator();
			while (ium.hasNext()) {
				JsonObject jomu = ium.next().getAsJsonObject();
				JsonArray jaIdx = jomu.get("indices").getAsJsonArray();
				for (int i=jaIdx.get(0).getAsInt(); i < jaIdx.get(1).getAsInt(); i++) {
					markers[i] = 1;
				}
				if (umProtoMemes != null) {
					String mention = "@" + jomu.get("id").getAsString();
					umProtoMemes.add(mention);
				}
			}
		}
	}

	private void updateUrlProtoMemes(JsonArray jaUrls, HashSet<String> urlProtoMemes, byte[] markers) {
		if (!jaUrls.isJsonNull() && jaUrls.size() > 0) {
			Iterator<JsonElement> iurl = jaUrls.iterator();
			while (iurl.hasNext()) {
				JsonObject joUrl = iurl.next().getAsJsonObject();
				JsonArray jaIdx = joUrl.get("indices").getAsJsonArray();
				for (int i=jaIdx.get(0).getAsInt(); i < jaIdx.get(1).getAsInt(); i++) {
					markers[i] = 1;
				}
				if (urlProtoMemes != null) {
					JsonElement jeChildUrl = joUrl.getAsJsonObject().get("url");
					JsonElement jeChildEurl = joUrl.getAsJsonObject().get("expanded_url");
					if (jeChildUrl != null && !jeChildUrl.isJsonNull()) {
						urlProtoMemes.add(jeChildUrl.getAsString());
					} else if (jeChildEurl != null && !jeChildEurl.isJsonNull()) {
						urlProtoMemes.add(jeChildEurl.getAsString());
					}
				}
			}
		}
	}
	
	/**
	 * Read a certain number of tweets from <b>jsonGzPath</b> according to <b>twInMin</b>, generate protomemes
	 * based on them, and compute various statistics about the protomemes.  
	 * @param jsonGzPath
	 *  Path to a .json.gz file.
	 * @param twStartTime
	 *  Start time of a given time window.
	 * @param twInMin
	 *  Length of a time window in minutes.
	 * @param simHistoStepLen
	 *  Length of a step unit (e.g. 0.1, 0.01) in the meme similarity histogram.
	 * @throws Exception
	 */
	public void protomemeStatsByTimeWin(String jsonGzPath, String twStartTime, float twInMin, float simHistoStepLen) throws Exception {
		Map<String, TweetFeatures> tidFeaturesMap = new HashMap<String, TweetFeatures>();
		Map<String, ProtoMeme> protomemeMap = new HashMap<String, ProtoMeme>();
		Map<String, Integer> wordPmCountMap = new HashMap<String, Integer>();		
		readProtoMemesByTimeWin(jsonGzPath, twStartTime, twInMin, tidFeaturesMap, protomemeMap, wordPmCountMap, null);
		
		LOG.info("Start generating meme similarity histogram.");
		int nHistoSteps = (int)(1.0 / simHistoStepLen) + 1;
		long[] histogram = new long[nHistoSteps];
		ProtoMeme[] pms = new ProtoMeme[protomemeMap.size()];
		pms = protomemeMap.values().toArray(pms);
		long simCount = 0;
		for (int i=0; i<pms.length; i++) {
			for (int j=i+1; j<pms.length; j++) {
				double similarity = pms[i].computeSimilarity(pms[j]);
				int histoPos = (int)(similarity / simHistoStepLen);
				histogram[histoPos]++;
				simCount++;
				if (simCount % 2500000 == 0) {
					System.out.println("Computed " + simCount + " similarities.");
				}
			}
		}
		System.out.println("Similarity histogram:");
		for (int i=0; i<histogram.length; i++) {
			System.out.println("[" + i * simHistoStepLen + ", " + (i+1) * simHistoStepLen + "]: " + histogram[i]);
		}
		LOG.info("Done with similarity histogram.");
		System.out.println();
		
		int mb = 1024 * 1024;
		Runtime runtime = Runtime.getRuntime();
		System.out.println("##### Heap utilization statistics [MB] #####");
		System.out.println("Used Memory:" + (runtime.totalMemory() - runtime.freeMemory()) / mb);
		System.out.println("Free Memory:" + runtime.freeMemory() / mb);
		System.out.println("Total Memory:" + runtime.totalMemory() / mb);
		System.out.println("Max Memory:" + runtime.maxMemory() / mb);
	}

	/**
	 * Read a certain number of tweets from <b>jsonGzPath</b> according to <b>twInMin</b>, and generate protomemes.
	 * @param jsonGzPath
	 *  Path to a .json.gz file.
	 * @param twStartTime
	 *  Start time of a given time window.
	 * @param twInMin
	 *  Length of a time window in minutes.
	 * @param tidFeaturesMap
	 *  A map from tweet IDs to TweetFeatures object, filled on the fly.
	 * @param protomemeMap
	 *  A map from protomeme markers to protomemes, filled on the fly.
	 * @param wordPmCountMap
	 *  A map from words to their protomeme counts, filled on the fly.
	 * @param htSet
	 *  A set of ground truth hashtags that need to be ignored when generating protomemes.
	 * @throws Exception
	 */
	protected void readProtoMemesByTimeWin(String jsonGzPath, String twStartTime, float twInMin, Map<String, TweetFeatures> tidFeaturesMap,
			Map<String, ProtoMeme> protomemeMap, Map<String, Integer> wordPmCountMap, Set<String> htSet) throws Exception {
		Calendar calStart = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		GeneralHelpers.setDateTimeByString(calStart, twStartTime);
		long startMilli = calStart.getTimeInMillis();
		long endMilli = startMilli + (int)(twInMin * 60000);
		DiffusionIndices diffIndices = new DiffusionIndices();
		LOG.info("Start making memes for the given time window.");
		GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(jsonGzPath));
		BufferedReader brInput = new BufferedReader(new InputStreamReader(gzInputStream, "UTF-8"));
		readTweetsByTimeWin(tidFeaturesMap, diffIndices, startMilli, endMilli, brInput);
		brInput.close();
				
		getProtomemesByTweets(tidFeaturesMap, diffIndices, protomemeMap, wordPmCountMap, htSet);
		LOG.info("Done with meme generation.");
	}

	/**
	 * For each TweetFeatures object in <b>tidFeaturesMap</b>, generate the corresponding protomemes, and put them
	 * into <b>protomemeMap</b>. 
	 * @param tidFeaturesMap
	 *  A map from tweet IDs to TweetFeatures objects.
	 * @param diffIndices
	 *  An object containing the diffusion indices.
	 * @param protomemeMap
	 *  A map from protomeme main markers to ProtoMeme objects, updated on the fly.
	 * @param wordPmCountMap
	 *  A map recording the global protomeme count of each word in tweets, updated on the fly.
	 * @param htSet
	 *  A set of hashtags that should be ignored when generating the protomemes.
	 * @throws Exception
	 */
	protected void getProtomemesByTweets(Map<String, TweetFeatures> tidFeaturesMap, DiffusionIndices diffIndices,
			Map<String, ProtoMeme> protomemeMap, Map<String, Integer> wordPmCountMap, Set<String> htSet) throws Exception {
		int pmCountForTweetBelong = 0;
		for (TweetFeatures tweet : tidFeaturesMap.values()) {
			int pmCount = processPmsForTweet(protomemeMap, wordPmCountMap, tweet, htSet);
			pmCountForTweetBelong += pmCount;
		}
		Collection<ProtoMeme> pms = protomemeMap.values();
		int maxTidsInPm = 0;
		int minTidsInPm = Integer.MAX_VALUE;
		int totalTidsInPm = 0;
		for (ProtoMeme pm : pms) {
			if (wordPmCountMap == null) {
				pm.computeDiffusionUids(diffIndices);
			} else {
				pm.computeDiffusionAndTfIdf(diffIndices, wordPmCountMap, pms.size());
			}
			int nTids = pm.tweetIds.size();
			if (nTids > maxTidsInPm) {
				maxTidsInPm = nTids;
			}
			if (nTids < minTidsInPm) {
				minTidsInPm = nTids;
			}
			totalTidsInPm += nTids;			
		}		
		System.out.println("Total number of protomemes: " + protomemeMap.size() + "; avg. number of protomemes each tweet belongs to: " +
				pmCountForTweetBelong * 1.0 / tidFeaturesMap.size());
		System.out.println("Max number of tids in a protomeme : " + maxTidsInPm + ", Min number: " + minTidsInPm + ", avg. "
				+ totalTidsInPm * 1.0 / protomemeMap.size());
	}

	/**
	 * Read tweets from <b>brInput</b>. For each tweet, check if its creation time is between <b>startMilli</b> and <b>endMilli</b>; if
	 * yes, generate the corresponding TweetFeatures object, and put it to <b>tidFeaturesMap</b>.
	 * @param tidFeaturesMap
	 *  A map from tweet IDs to TweetFeatures objects, updated on the fly.
	 * @param diffIndices
	 *  An object containing all the diffusion index structures, updated on the fly.
	 * @param startMilli
	 *  Start point of a time window in milliseconds.
	 * @param endMilli
	 *  End point of a time window in milliseconds.
	 * @param brInput
	 *  A BufferedReader object that is backed by an input stream containing JSON strings of tweets.
	 * @return
	 *  Whether the end of <b>brInput</b> has been reached.
	 * @throws IOException
	 */
	protected boolean readTweetsByTimeWin(Map<String, TweetFeatures> tidFeaturesMap, DiffusionIndices diffIndices, long startMilli, long endMilli,
			BufferedReader brInput)	throws IOException {
		int badJsonCount = 0;
		int skipCount = 0;
		int totalTweetCount = 0;
		String line = brInput.readLine();
		boolean toBreak = false;
		while (line != null) {
			TweetFeatures tweet = null;
			try {
				tweet = new TweetFeatures(line);
			} catch (IllegalArgumentException ie) {
				System.out.println(ie.getMessage());
				badJsonCount++;
			}
			if (tweet == null) {
				line = brInput.readLine();
				continue;
			}
			if (tweet.createTimeMilli < startMilli) {
				skipCount++;
				line = brInput.readLine();
				continue;
			}
			if (tweet.createTimeMilli > endMilli) {
				// add this tweet and then break; otherwise we will miss this tweet.
				toBreak = true;
			}
			
			// add this tweet to the set of all valid tweets, and update the in-memory index
			tidFeaturesMap.put(tweet.tweetId, tweet);
			for (String muid : tweet.mentionedUids) {
				Set<String> toUids = diffIndices.mentioningIndex.get(tweet.userId);
				if (toUids == null) {
					toUids = new HashSet<String>();
					diffIndices.mentioningIndex.put(tweet.userId, toUids);
				}
				toUids.add(muid);
				Set<String> fromUids = diffIndices.mentionedByIndex.get(muid);
				if (fromUids == null) {
					fromUids = new HashSet<String>();
					diffIndices.mentionedByIndex.put(muid, fromUids);
				}
				fromUids.add(tweet.userId);
			}
			if (tweet.retweetedTid != null) {
				Set<String> toUids = diffIndices.retweetingIndex.get(tweet.userId);
				if (toUids == null) {
					toUids = new HashSet<String>();
					diffIndices.retweetingIndex.put(tweet.userId, toUids);
				}
				toUids.add(tweet.retweetedUid);
				Set<String> fromUids = diffIndices.retweetedByIndex.get(tweet.retweetedUid);
				if (fromUids == null) {
					fromUids = new HashSet<String>();
					diffIndices.retweetedByIndex.put(tweet.retweetedUid, fromUids);
				}
				fromUids.add(tweet.userId);
			}
			totalTweetCount++;
			if (totalTweetCount % 20000 == 0) {
				System.out.println("Processed " + totalTweetCount + " tweets.");
			}
			
			if (toBreak) {
				break;
			}
			line = brInput.readLine();
		}
		System.out.println("Number of tweets skipped: " + skipCount + "; number of bad JSON: " + badJsonCount +
				"; Number of tweets processed: " + totalTweetCount);
		
		return line == null;
	}
	
	/**
	 * Read <b>nTweetsToRead</b> tweets from <b>jsonGzPath</b>, generate the protomemes, and print the related details of the
	 * tweets and protomemes.
	 * 
	 * @param jsonGzPath
	 *  Path to a .json.gz file.
	 * @param nTweetsToRead
	 *  Number of tweets to read from the file.
	 * @param useTfIdf
	 * 	Whether to use tf-idf values when computing content similarity.
	 * @throws Exception
	 */
	public void testProtomemeDetails(String jsonGzPath, int nTweetsToRead, boolean useTfIdf) throws Exception {
		Map<String, TweetFeatures> tidFeaturesMap = new HashMap<String, TweetFeatures>();
		DiffusionIndices diffIndices = new DiffusionIndices();
		Map<String, ProtoMeme> protomemeMap = new HashMap<String, ProtoMeme>();
		Map<String, Integer> wordPmCountMap = null;
		if (useTfIdf) {
			wordPmCountMap = new HashMap<String, Integer>();
		}
		
		LOG.info("Start making memes for the given time window.");
		int badJsonCount = 0;
		int totalTweetCount = 0;
		int pmCountForTweetBelong = 0;
		GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(jsonGzPath));
		BufferedReader brInput = new BufferedReader(new InputStreamReader(gzInputStream, "UTF-8"));
		String line = brInput.readLine();
		while (line != null) {
			TweetFeatures tweet = null;
			try {
				tweet = new TweetFeatures(line);
			} catch (IllegalArgumentException ie) {
				System.out.println(ie.getMessage());
				badJsonCount++;
			}
			if (tweet == null) {
				line = brInput.readLine();
				continue;
			}
			
			// first, add this tweet to the set of all valid tweets, and update the in-memory index
			tidFeaturesMap.put(tweet.tweetId, tweet);
			for (String muid : tweet.mentionedUids) {
				Set<String> toUids = diffIndices.mentioningIndex.get(tweet.userId);
				if (toUids == null) {
					toUids = new HashSet<String>();
					diffIndices.mentioningIndex.put(tweet.userId, toUids);
				}
				toUids.add(muid);
				Set<String> fromUids = diffIndices.mentionedByIndex.get(muid);
				if (fromUids == null) {
					fromUids = new HashSet<String>();
					diffIndices.mentionedByIndex.put(muid, fromUids);
				}
				fromUids.add(tweet.userId);
			}
			if (tweet.retweetedTid != null) {
				Set<String> toUids = diffIndices.retweetingIndex.get(tweet.userId);
				if (toUids == null) {
					toUids = new HashSet<String>();
					diffIndices.retweetingIndex.put(tweet.userId, toUids);
				}
				toUids.add(tweet.retweetedUid);
				Set<String> fromUids = diffIndices.retweetedByIndex.get(tweet.retweetedUid);
				if (fromUids == null) {
					fromUids = new HashSet<String>();
					diffIndices.retweetedByIndex.put(tweet.retweetedUid, fromUids);
				}
				fromUids.add(tweet.userId);
			}
			
			// then add it to all protomemes it belongs to
			int pmCount = processPmsForTweet(protomemeMap, wordPmCountMap, tweet, null);
			
			totalTweetCount++;
			pmCountForTweetBelong += pmCount;
			if (totalTweetCount >= nTweetsToRead) {
				break;
			}			
			line = brInput.readLine();
		}
		brInput.close();
		LOG.info("Done with meme generation.");
		System.out.println("Number of bad JSON: " + badJsonCount);
		System.out.println("Number of tweets processed: " + totalTweetCount + "; Total number of protomemes: " + protomemeMap.size() +
				"; avg. number of protomemes each tweet belongs to: " + pmCountForTweetBelong * 1.0 / totalTweetCount);
		
		int maxTidsInPm = 0;
		int minTidsInPm = Integer.MAX_VALUE;
		int totalTidsInPm = 0;
		for (ProtoMeme pm : protomemeMap.values()) {
			pm.computeDiffusionUids(diffIndices);
			int nTids = pm.tweetIds.size();
			if (nTids > maxTidsInPm) {
				maxTidsInPm = nTids;
			}
			if (nTids < minTidsInPm) {
				minTidsInPm = nTids;
			}
			totalTidsInPm += nTids;
		}
		System.out.println("Max number of tids in a protomeme : " + maxTidsInPm + ", Min number: " + minTidsInPm + ", avg. "
				+ totalTidsInPm * 1.0 / protomemeMap.size());
		
		Gson gson = new Gson();
		System.out.println("All tweet Features: ");
		for (TweetFeatures t : tidFeaturesMap.values()) {
			System.out.println(gson.toJson(t, TweetFeatures.class));
		}
		
		System.out.println("All prototmemes: ");
		for (ProtoMeme pm : protomemeMap.values()) {
			System.out.println(gson.toJson(pm, ProtoMeme.class));
		}
		
		System.out.println();
		System.out.println("Protomeme count for all words:");
		if (wordPmCountMap == null) {
			System.out.println("Not Available.");
		} else {
			System.out.println(gson.toJson(wordPmCountMap, wordPmCountMap.getClass()));
		}		
		
		System.out.println();
		System.out.println("Computing similarities:");
		int nHistoSteps = (int)(1.0 / 0.01) + 1;
		long[] histogram = new long[nHistoSteps];
		ProtoMeme[] pms = new ProtoMeme[protomemeMap.size()];
		pms = protomemeMap.values().toArray(pms);
		for (int i=0; i<pms.length; i++) {
			for (int j=i+1; j<pms.length; j++) {
				double similarity = pms[i].computeSimilarity(pms[j]);
				int histoPos = (int)(similarity / 0.01);
				histogram[histoPos]++;
			}
		}
		
		System.out.println();
		System.out.println("Similarity histogram:");
		for (int i=0; i<histogram.length; i++) {
			System.out.println("[" + i * 0.01 + ", " + (i+1) * 0.01 + "]: " + histogram[i]);
		}
	}

	/**
	 * Find or create the protomemes that <b>tweet</b> belongs to, and update them. Update <b>wordPmCountMap</b> on the fly by each word
	 * in the text of <b>tweet</b>. 
	 * @param protomemeMap
	 *  Map from protomeme markers to protomemes; updated on the fly.
	 * @param wordPmCountMap
	 *  Global map from words to their 'prtomeme counts'; updated on the fly.
	 * @param tweet
	 *  A given object containing features of a tweet.
	 * @param groundTruthHts
	 *  A set of ground truth hashtags that need to be ignored when generating protomemes.
	 * @return
	 *  Number of protomemes this <b>tweet</b> belongs to.
	 * @throws Exception
	 */
	protected int processPmsForTweet(Map<String, ProtoMeme> protomemeMap, Map<String, Integer> wordPmCountMap, TweetFeatures tweet, 
			Set<String> groundTruthHts) throws Exception {
		HashMap<String, Integer> wordNewPmCount = null;
		if (wordPmCountMap != null) {
			wordNewPmCount = new HashMap<String, Integer>();
			for (String w : tweet.words) {
				wordNewPmCount.put(w, 0);
			}
		}
		
		int pmCount = 0;
		for (String url : tweet.urls) {
			ProtoMeme pm = protomemeMap.get(url);
			if (pm == null) {
				if (wordNewPmCount != null) {
					// each word in the tweet is appearing in one new protomeme
					for (String w : wordNewPmCount.keySet()) {
						wordNewPmCount.put(w, wordNewPmCount.get(w) + 1);
					}
				}
				pm = new ProtoMeme(ProtoMemeType.URL, url);
				pm.addTweet(tweet);
				protomemeMap.put(url, pm);
			} else {
				if (wordNewPmCount != null) {
					// For each word in the tweet, increase its global protomeme count only if it didn't appear in this protomeme before.
					for (String w : wordNewPmCount.keySet()) {
						if (!pm.wordFreqs.containsKey(w)) {
							wordNewPmCount.put(w, wordNewPmCount.get(w) + 1);
						}
					}
				}
				pm.addTweet(tweet);
			}
			pmCount++;
		}
		for (String muid : tweet.mentionedUids) {
			ProtoMeme pm = protomemeMap.get(muid);
			if (pm == null) {
				if (wordNewPmCount != null) {
					for (String w : wordNewPmCount.keySet()) {
						wordNewPmCount.put(w, wordNewPmCount.get(w) + 1);
					}
				}
				pm = new ProtoMeme(ProtoMemeType.MENTION, muid);
				pm.addTweet(tweet);
				protomemeMap.put(muid, pm);
			} else {
				if (wordNewPmCount != null) {
					for (String w : wordNewPmCount.keySet()) {
						if (!pm.wordFreqs.containsKey(w)) {
							wordNewPmCount.put(w, wordNewPmCount.get(w) + 1);
						}
					}
				}
				pm.addTweet(tweet);
			}
			pmCount++;
		}
		for (String ht : tweet.hashtags) {
			if (groundTruthHts != null && groundTruthHts.contains(ht)) {
				continue;
			}
			ProtoMeme pm = protomemeMap.get(ht);
			if (pm == null) {
				if (wordNewPmCount != null) {
					for (String w : wordNewPmCount.keySet()) {
						wordNewPmCount.put(w, wordNewPmCount.get(w) + 1);
					}
				}
				pm = new ProtoMeme(ProtoMemeType.HASHTAG, ht);
				pm.addTweet(tweet);
				protomemeMap.put(ht, pm);
			} else {
				if (wordNewPmCount != null) {
					for (String w : wordNewPmCount.keySet()) {
						if (!pm.wordFreqs.containsKey(w)) {
							wordNewPmCount.put(w, wordNewPmCount.get(w) + 1);
						}
					}
				}
				pm.addTweet(tweet);
			}
			pmCount++;
		}
		if (tweet.words.size() > 0) {
			pmCount++;
			StringBuilder brPhrase = new StringBuilder();
			for (String w : tweet.words) {
				brPhrase.append(w).append(' ');
			}
			brPhrase.deleteCharAt(brPhrase.length() - 1);
			String phrase = brPhrase.toString();
			ProtoMeme pm = protomemeMap.get(phrase);
			if (pm == null) {
				if (wordNewPmCount != null) {
					for (String w : wordNewPmCount.keySet()) {
						wordNewPmCount.put(w, wordNewPmCount.get(w) + 1);
					}
				}
				pm = new ProtoMeme(ProtoMemeType.PHRASE, phrase);
				pm.addTweet(tweet);
				protomemeMap.put(phrase, pm);
			} else {
				if (wordNewPmCount != null) {
					for (String w : wordNewPmCount.keySet()) {
						if (!pm.wordFreqs.containsKey(w)) {
							wordNewPmCount.put(w, wordNewPmCount.get(w) + 1);
						}
					}
				}
				pm.addTweet(tweet);
			}				
		}
		
		if (wordNewPmCount != null) {
			for (Map.Entry<String, Integer> e : wordNewPmCount.entrySet()) {
				String word = e.getKey();
				Integer curCount = wordPmCountMap.get(word);
				if (curCount == null) {
					wordPmCountMap.put(word, e.getValue());
				} else {
					wordPmCountMap.put(word, curCount + e.getValue());
				}
			}
		}
		
		return pmCount;
	}
	
	/**
	 * Comparator class for comparing two protomeme clusters based on their last update time.
	 * @author gaoxm
	 */
	protected static class PmClusterUpdateTimeComparator implements Comparator<ProtomemeCluster> {
		@Override
		public int compare(ProtomemeCluster c1, ProtomemeCluster c2) {
			long diff = c1.latestUpdateTime - c2.latestUpdateTime;
			if (diff != 0) {
				return (int)diff;
			} else {
				if (c1.equals(c2)) {
					return 0;
				} else {
					return -1;
				}
			}
		}
	}
	
	/**
	 * Read a certain number of tweets from <b>jsonGzPath</b> according to <b>twInMin</b>, generate protomemes
	 * based on them, and do online k-means with outlier detection and print the results.  
	 * @param jsonGzPath
	 *  Path to a .json.gz file.
	 * @param twStartTime
	 *  Start time of a given time window.
	 * @param twInMin
	 *  Length of a time window in minutes.
	 * @param nClusters
	 *  Number of clusters for online K-Means.
	 * @param outlierThreshold
	 *  Number of standard deviations from the mean to identify outliers.
	 * @param htsToDelete
	 *  Ground truth hashtags to delete from the tweets when generating protomemes. 
	 * @throws Exception
	 */
	public void onlineKMeansOneTimeWin(String jsonGzPath, String twStartTime, float twInMin, int nClusters, float outlierThreshold, String htsToDelete)
			throws Exception {
		long t0 = System.currentTimeMillis();
		Set<String> htSet = parseHashtags(htsToDelete);
		Map<String, TweetFeatures> tidFeaturesMap = new HashMap<String, TweetFeatures>();
		Map<String, ProtoMeme> protomemeMap = new HashMap<String, ProtoMeme>();
		readProtoMemesByTimeWin(jsonGzPath, twStartTime, twInMin, tidFeaturesMap, protomemeMap, null, htSet);
		long t1 = System.currentTimeMillis();
		long pmGenTime = t1 - t0;
		
		long simCalcTime = 0;
		long centUpdateTime = 0;		
		Collection<ProtoMeme> pms = protomemeMap.values();
		List<ProtomemeCluster> clusters = new ArrayList<ProtomemeCluster>(nClusters);
		List<ProtomemeCluster> deletedClusters = new LinkedList<ProtomemeCluster>();
		int nPmsProcessed = 0;
		double ssPmClusterSim = 0;  // sum of squares of protomeme-cluster similarities, useful for detecting outliers
		double sumPmClusterSim = 0; // sum of protomeme-cluster simialrities, useful for detecting outliers
		int warmUpThreshold = nClusters + nClusters;
		float initOutlierTh = outlierThreshold;
		for (ProtoMeme pm : pms) {			
			if (nPmsProcessed < nClusters) {
				// use the first nClusters protomemes as the initial centroids of the clusters
				t0 = System.currentTimeMillis();
				ProtomemeCluster cluster = new ProtomemeCluster(pm);
				clusters.add(cluster);
				pm.clusterId = clusters.size() - 1;
				pm.similarityToCluster = 1.0;
				sumPmClusterSim += 1.0;
				ssPmClusterSim += 1.0;				
				t1 = System.currentTimeMillis();
				centUpdateTime += t1 - t0;
				nPmsProcessed++;
			} else {
				int idxClosestCluster = -1; // index of the closest cluster (to this protomeme) in the clusters array
				double maxSimilarity = 0; // similarity between this protomeme and the closest cluster
				int idxLruCluster = -1; // the index of the least recently updated cluster in the clusters array
				long lruTimestamp = Long.MAX_VALUE; // update timestamp of the least recently updated cluster
				long lruNanoTime = Long.MAX_VALUE;
				t0 = System.currentTimeMillis();
				for (int i=0; i<clusters.size(); i++) {
					ProtomemeCluster c = clusters.get(i);
					double sim = c.computeSimilarity(pm);
					long updateTs = c.latestUpdateTime;
					if (sim >= maxSimilarity) {
						maxSimilarity = sim;
						idxClosestCluster = i;
					}
					if (updateTs < lruTimestamp || (updateTs == lruTimestamp && c.nanoUpdateTime < lruNanoTime)) {
						lruTimestamp = updateTs;
						lruNanoTime = c.nanoUpdateTime;
						idxLruCluster = i;
					}
				}
				t1 = System.currentTimeMillis();
				simCalcTime += t1 - t0;

				t0 = System.currentTimeMillis();
				double mean = sumPmClusterSim / nPmsProcessed;
				double stdDev = Math.sqrt(ssPmClusterSim / nPmsProcessed - mean * mean);
				if (mean - outlierThreshold * stdDev < 0 && outlierThreshold > 0) {
					outlierThreshold -= 0.5;
					if (outlierThreshold < 0) {
						outlierThreshold = 0;
					}
				} else if (outlierThreshold < initOutlierTh && mean - outlierThreshold * stdDev > 0.5 * stdDev) {
					outlierThreshold += 0.5;
					if (outlierThreshold > initOutlierTh) {
						outlierThreshold = initOutlierTh;
					}
				}
				if ((nPmsProcessed < warmUpThreshold && maxSimilarity > 0.1) || 
					(maxSimilarity > 0 && maxSimilarity > mean - outlierThreshold * stdDev)) {
					// ssShortestDis and sumShortestDis are skewed right after initialization. We don't detect outlier at such time.
					System.out.println(nPmsProcessed + "th protomeme " + pm.mainMarker + " assigned to cluster " + idxClosestCluster +
							". Simialrity: " + maxSimilarity + ", mean: " + mean + ", standard deviation: " + stdDev +
							", outlier threshold:" + outlierThreshold);
					ProtomemeCluster targetCluster = clusters.get(idxClosestCluster);
					targetCluster.addProtoMeme(pm);
					pm.clusterId = idxClosestCluster;
					pm.similarityToCluster = maxSimilarity;
					sumPmClusterSim += maxSimilarity;
					ssPmClusterSim += maxSimilarity * maxSimilarity;
				} else {
					// Outlier processing. Since the outlier will be the  centroid for the new cluster, its similarity to centroid is 1
					System.out.println("Outlier detected for the " + nPmsProcessed + "th protomeme " + pm.mainMarker + ". Similarity: "
							+ maxSimilarity + ", mean: " + mean + ", standard deviation: " + stdDev + ", outlier threshold:" + outlierThreshold
							+ " Cluster to delete: " + idxLruCluster);
					deletedClusters.add(clusters.get(idxLruCluster));
					ProtomemeCluster cluster = new ProtomemeCluster(pm);
					clusters.set(idxLruCluster, cluster);
					pm.clusterId = idxLruCluster;
					pm.similarityToCluster = 1.0;
					sumPmClusterSim += 1.0;
					ssPmClusterSim += 1.0;
				}
				t1 = System.currentTimeMillis();
				centUpdateTime += t1 - t0;
				nPmsProcessed++;
				
				if (nPmsProcessed % 100 == 0) {
					System.out.println("Clustered " + nPmsProcessed + " protomemes. Similarity mean before checking the last protomeme:" + mean +
							", standard deviation: " + stdDev + ". Similarity compuation: " + simCalcTime / 1000.0 + "s, centroids update: " + 
							centUpdateTime / 1000.0 + "s.");
				}
			}
		}
		printFinalAndDeletedClusters(clusters, deletedClusters, false);
		System.out.println("Total protomeme generation time: " + pmGenTime / 1000.0 + "s, similarity calcuation time: " + simCalcTime / 1000.0 +
				"s, centroids update time: " + centUpdateTime / 1000.0 + "s.");
	}
	
	/**
	 * Container class for the global parameters during the clustering process. 
	 * @author gaoxm
	 */
	protected static class GlobalClusteringParams {
		/** number of protomemes processed so far */
		protected int nPmsProcessed = 0;
		
		/** dynamic outlier threshold */
		protected float outlierThd;
		
		/** user defined initial outlier threshold */
		protected float initOutlierThd;
		
		/** outlier threshold adjustment step */
		protected float outlierThdAdjStep;
		
		/** user defined intial number of clusters */
		protected int initClusterNum;
		
		/** Coefficient for deciding the max number of clusters */
		protected float clusterNumMultiplier;
		
		/** number of outliers detected so far */
		protected int outlierCount = 0;
		
		/** number of protomemes counted for similarity mean and standard deviation computation*/ 
		protected int nSimsForMeanStd = 0;
		
		/** sum of squares of protomeme-cluster similarities, useful for detecting outliers */
		double ssSimForMeanStd = 0;  
		
		/** sum of protomeme-cluster simialrities, useful for detecting outliers */
		double sumSimForMeanStd = 0;
		
		/** time (in milliseconds) spent on similarity calculation */
		long simCalcTime = 0;
		
		/** time (in milliseconds) spent on updating centroids */
		long centUpdateTime = 0; 
		
		public GlobalClusteringParams(float initOutlierThd, int initClusterNum, float outlierThdAdjStep, float clusterNumMultiplier) {
			this.initOutlierThd = initOutlierThd;
			this.outlierThd = initOutlierThd;
			this.initClusterNum = initClusterNum;
			this.outlierThdAdjStep = outlierThdAdjStep;
			this.clusterNumMultiplier = clusterNumMultiplier;
		}
	}
	
	/**
	 * In-memory index structure to store the 'mentioning', 'mentioned by', 'retweeting' 'retweeted by'
	 * relationships among users in a time step.
	 * @author gaoxm
	 */
	protected static class DiffusionIndices {
		/** A map from each user ID to other user IDs that he/she mentions */ 
		Map<String, Set<String>> mentioningIndex;
		
		/** A map from each user ID to other user IDs that he/she has been mentioned by */ 
		Map<String, Set<String>> mentionedByIndex;
		
		/** A map from each user ID to other user IDs that he/she retweets */ 
		Map<String, Set<String>> retweetingIndex;
		
		/** A map from each user ID to other user IDs that he/she has been retweeted by */ 
		Map<String, Set<String>> retweetedByIndex;
		
		public DiffusionIndices() {
			mentioningIndex = new HashMap<String, Set<String>>();
			mentionedByIndex = new HashMap<String, Set<String>>();
			retweetingIndex = new HashMap<String, Set<String>>();
			retweetedByIndex = new HashMap<String, Set<String>>();
		}
	}
	
	/**
	 * Read tweets from <b>jsonGzPath</b> with a sliding time window, generating protomemes, and 
	 * do online k-means with outlier detection and print the results.  
	 * @param jsonGzDir
	 *  Path to a directory containing .json.gz files.
	 * @param stepInMin
	 *  Length of a time step in minutes.
	 * @param twInSteps
	 *  Length of the sliding time window in steps.
	 * @param nClusters
	 *  Number of clusters for online K-Means.
	 * @param outlierThreshold
	 *  Number of standard deviations from the mean to identify outliers.
	 * @param outlierThdAdjStep
	 *  Outlier threshold adjustment step
	 * @param clusterNumMult
	 *  Coefficient for deciding the max number of clusters
	 * @param htsToDelete
	 *  Ground truth hashtags to delete from the tweets when generating protomemes. 
	 * @throws Exception
	 */
	public void onlineKMeansSlidingWin(String jsonGzDir, float stepInMin, int twInSteps, int nClusters, float outlierThreshold,
			float outlierThdAdjStep, float clusterNumMult, String htsToDelete)	throws Exception {
		Set<String> htSet = parseHashtags(htsToDelete);
		List<ProtomemeCluster> clusters = new ArrayList<ProtomemeCluster>(nClusters * 2);
		// for storing protomemes at each step of the time window
		ArrayDeque<Map<String, ProtoMeme>> pmMaps = new ArrayDeque<Map<String, ProtoMeme>>(twInSteps + 1);
		//for storing TweetFeature objects at each step of the time window
		ArrayDeque<Map<String, TweetFeatures>> tfMaps = new ArrayDeque<Map<String, TweetFeatures>>(twInSteps + 1);
		
		Map<String, Integer> pmMarkerClusterMap = new HashMap<String, Integer>();
		// record the dynamic number of maintained protomemes for each protomeme marker (URL, mention, hashtag, phrase)
		Map<String, Integer> pmMarkerCount = new HashMap<String, Integer>();
		// record the ground truth clusters formed by tweet IDs sharing the same hashtags
		Map<String, Set<String>> groundTruthClusters = new HashMap<String, Set<String>>();
		NmiUtil nmi = new NmiUtil();
		double nmiTotal = 0;
		int stepCount = 0;
		
		// go through the .json.gz files with a sliding time window and do clustering
		ArrayList<File> fJsonGzs = getSortedJsonGzFiles(jsonGzDir);
		int curFileIdx = 0;
		File fJsonGz = fJsonGzs.get(curFileIdx);
		String fName = fJsonGz.getName();
		int idx = fName.indexOf('.');
		String dateTime = fName.substring(0, idx) + "T13:00:00";
		Calendar calStart = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		GeneralHelpers.setDateTimeByString(calStart, dateTime);
		long startMilli = calStart.getTimeInMillis();
		long endMilli = startMilli + (int)(stepInMin * 60000);
		BufferedReader brInput = null;
		GlobalClusteringParams param = new GlobalClusteringParams(outlierThreshold, nClusters, outlierThdAdjStep, clusterNumMult);
		DiffusionIndices diffIndices = new DiffusionIndices();
		while (true) {
			if (curFileIdx >= fJsonGzs.size()) {
				break;
			}
			calStart.setTimeInMillis(startMilli);
			LOG.info("============Clustering memes for time window starting at " + GeneralHelpers.getDateTimeString(calStart));
			
			// read tweets for the next time window
			Map<String, TweetFeatures> tidFeaturesMap = new HashMap<String, TweetFeatures>();
			boolean mayNeedMoreTweets = true;
			while (mayNeedMoreTweets && curFileIdx < fJsonGzs.size()) {
				if (brInput == null) {
					fJsonGz = fJsonGzs.get(curFileIdx);
					LOG.info("------reading " + fJsonGz.getName() + "...");
					brInput = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fJsonGz)), "UTF-8"));
				}
				mayNeedMoreTweets = readTweetsByTimeWin(tidFeaturesMap, diffIndices, startMilli, endMilli, brInput);
				if (mayNeedMoreTweets) {
					brInput.close();
					brInput = null;
					curFileIdx++;
				}
			}
			
			if (tidFeaturesMap.size() > 0) {
				updateGroundTruthClusters(tidFeaturesMap, htSet, groundTruthClusters);		
				LOG.info("------generating protomemes...");
				Map<String, ProtoMeme> protomemeMap = new HashMap<String, ProtoMeme>();
				getProtomemesByTweets(tidFeaturesMap, diffIndices, protomemeMap, null, htSet);
				pmMaps.add(protomemeMap);
				tfMaps.add(tidFeaturesMap);
				if (pmMaps.size() > twInSteps) {
					LOG.info("------deleting old protomemes... number of tweets in the old time step: " + tfMaps.getFirst().size());
					Map<String, ProtoMeme> oldPmMap = pmMaps.remove();
					deleteOldPms(oldPmMap, clusters, pmMarkerClusterMap, pmMarkerCount);
					Map<String, TweetFeatures> oldTfMap = tfMaps.remove();
					deleteTweetsInGroundTruth(oldTfMap, htSet, groundTruthClusters);
					printFinalAndDeletedClusters(clusters, null, false);
				}

				LOG.info("------clustering protomemes...");
				clusterPmsForOneStep(protomemeMap, clusters, pmMarkerClusterMap, pmMarkerCount, param);
				double lfkNmi = nmi.computeLfkNmi(clusters, groundTruthClusters);
				LOG.info("LFK-NMI between cluster results and ground truth : " + lfkNmi);
				nmiTotal += lfkNmi;
				stepCount++;
			} else {
				LOG.info("------no tweets for this time window.");
			}
			
			// move to the next time window
			startMilli = endMilli + 1;
			endMilli += (int)(stepInMin * 60000);
		}
		if (brInput != null) {
			brInput.close();
		}
		System.out.println();
		LOG.info("Done with all data. Total similarity calcuation time: " + param.simCalcTime / 1000.0 + "s, centroids update time: " +
				param.centUpdateTime / 1000.0 + "s." + stepCount + " valid time steps in total. Avg LFK-NMI: " + nmiTotal / stepCount +
				". Total number of protomemes processed: " + param.nPmsProcessed + ", outliers detected: " + param.outlierCount + " (" +
				param.outlierCount * 100.0 / param.nPmsProcessed + "%).");
		Gson gson = new Gson();
		System.out.println("Final values of global parameters:");
		System.out.println(gson.toJson(param, GlobalClusteringParams.class));
		System.out.println("Protomemes in the final " + clusters.size() + " clusters:");
		for (int i=0; i<clusters.size(); i++) {
			ProtomemeCluster c = clusters.get(i);
			if (c != null) {
				System.out.println("Cluster-" + i);
				for (ProtoMeme pm : c.protomemes) {
					System.out.println(gson.toJson(pm, ProtoMeme.class));
				}
			}
		}
		/*
		String groundTruthTidsPath = jsonGzDir + File.separator + "groundTruthTids.txt";
		PrintWriter pwTids = new PrintWriter(new FileWriter(groundTruthTidsPath));
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Set<String>> e : groundTruthClusters.entrySet()) {
			sb.setLength(0);
			for (String tid : e.getValue()) {
				sb.append(tid).append(',');
			}
			if (sb.length() > 0) {
				sb.deleteCharAt(sb.length() - 1);
			}
			pwTids.println(sb.toString());
		}
		pwTids.close();*/
	}
	
	/**
	 * Add the IDs of tweets containing the ground truth hashtags in <b>htSet</b> to the <b>groundTruthClusters</b>, where tweets
	 * the same ground truthy hashtag are clustered together.
	 * @param tidFeaturesMap
	 *  A map from tweet IDs to TweetFeatures objects generated for the latest time window.
	 * @param htSet
	 *  A set containing the ground truthy hashtags.
	 * @param groundTruthClusters
	 *  A map from ground truth hashtags to the set of tweet IDs containing each hashtag.
	 * @throws Exception
	 */
	protected void updateGroundTruthClusters(Map<String, TweetFeatures> tidFeaturesMap, Set<String> htSet, Map<String,
			Set<String>> groundTruthClusters) throws Exception {
		for (TweetFeatures t : tidFeaturesMap.values()) {
			for (String ht : t.hashtags) {
				if (!htSet.contains(ht)) {
					continue;
				}
				Set<String> tids = groundTruthClusters.get(ht);
				if (tids == null) {
					tids = new HashSet<String>();
					groundTruthClusters.put(ht, tids);
				}
				tids.add(t.tweetId);
			}
		}
	}
	
	/**
	 * Remove the tweet IDs in <b>oldTfMap</b> from the <b>groundTruthClusters</b>.
	 * @param oldTfMap
	 *  A map from tweet IDs to TweetFeatures objects generated in an old time window.
	 * @param htSet
	 *  A set containing the ground truthy hashtags.
	 * @param groundTruthClusters
	 *  A map from ground truth hashtags to the set of tweet IDs containing each hashtag.
	 * @throws Exception
	 */
	protected void deleteTweetsInGroundTruth(Map<String, TweetFeatures> oldTfMap, Set<String> htSet, Map<String,
			Set<String>> groundTruthClusters) throws Exception {
		for (TweetFeatures t : oldTfMap.values()) {
			for (String ht : t.hashtags) {
				if (!htSet.contains(ht)) {
					continue;
				}
				Set<String> tids = groundTruthClusters.get(ht);
				if (tids != null) {
					tids.remove(t.tweetId);
					if (tids.size() == 0) {
						groundTruthClusters.remove(ht);
					}
				}
			}
		}
	}
	
	/**
	 * Delete protomemes in an old time step.
	 * @param pmMap
	 *  A map from protomeme main markers to protomemes for an old time step.
	 * @param clusters
	 *  Global array of clusters.
	 * @param pmMarkerClusterMap
	 *  Global map from protomeme markers to clusters, updated on the fly.
	 * @param pmMarkerCount
	 *  Global map from protomeme markers to protomeme counts, updated on the fly.
	 * @throws Exception
	 */
	protected void deleteOldPms(Map<String, ProtoMeme> pmMap, List<ProtomemeCluster> clusters, Map<String, Integer> pmMarkerClusterMap,
			Map<String, Integer> pmMarkerCount) throws Exception {
		for (Map.Entry<String, ProtoMeme> e : pmMap.entrySet()) {
			String marker = e.getKey();
			ProtoMeme pm = e.getValue();
			// Delete pm from its cluster. If the cluster becomes empty, delete the cluster from the global cluster array.
			if (pm.clusterId >= 0) {
				ProtomemeCluster c = clusters.get(pm.clusterId); 
				c.removeProtoMeme(pm);
				if (c.protomemes.size() <= 0) {
					Integer clusterIdx = pmMarkerClusterMap.get(marker);
					if (clusterIdx != null) {
						clusters.set(clusterIdx, null);
					}
				}
				pm.clusterId = -1;
				pm.similarityToCluster = 0;
			}
			
			// Decrease the protomeme count for the marker. If there are no more protomemes using this marker, remove the marker
			// from the marker-cluster map.
			Integer markerCount = pmMarkerCount.get(marker);
			if (markerCount != null) {
				markerCount--;
				if (markerCount == 0) {
					pmMarkerCount.remove(marker);
					pmMarkerClusterMap.remove(marker);
				} else {
					pmMarkerCount.put(marker, markerCount);
				}
			}
		}
	}
	
	/**
	 * Do clustering to the protomemes in <b>protomemeMap</b>.
	 * @param protomemeMap
	 *  A map containing new protomemes generated in a time step.
	 * @param clusters
	 *  An array of clusters, updated on the fly.
	 * @param pmMarkerClusterMap
	 *  A map recording whether protomemes with specific main markers have already been put into any cluster before.
	 * @param pmMarkerCount
	 *  A map recording the number of protomemes (across multiple time steps) sharing the same main markers.
	 * @param param
	 *  An object containing the dynamic global parameters during the whole clustering process.
	 * @throws Exception
	 */
	protected void clusterPmsForOneStep(Map<String, ProtoMeme> protomemeMap, List<ProtomemeCluster> clusters, Map<String, Integer> pmMarkerClusterMap, 
			Map<String, Integer> pmMarkerCount, GlobalClusteringParams param) throws Exception {
		long t0, t1;
		long simCalcTime = 0;
		long centUpdateTime = 0;
		int outlierCount = 0;
		Collection<ProtoMeme> pms = protomemeMap.values();
		ProtoMeme[] initPms = new ProtoMeme[param.initClusterNum];
		List<ProtomemeCluster> deletedClusters = new LinkedList<ProtomemeCluster>();
		for (ProtoMeme pm : pms) {
			Integer markerCount = pmMarkerCount.get(pm.mainMarker);
			if (markerCount != null) {
				pmMarkerCount.put(pm.mainMarker, markerCount + 1);
			} else {
				pmMarkerCount.put(pm.mainMarker, 1);
			}
			
			ProtomemeCluster targetCluster = null;
			int targetClusterId = -1;
			double simToCluster = 0;
			if (param.nPmsProcessed < param.initClusterNum) {
				// use the first clusters.length protomemes as the initial centroids of the clusters
				t0 = System.currentTimeMillis();
				targetCluster = new ProtomemeCluster(pm);
				centUpdateTime += System.currentTimeMillis() - t0;
				clusters.add(targetCluster);
				targetClusterId = clusters.size() - 1;
				simToCluster = 1.0;
				pmMarkerClusterMap.put(pm.mainMarker, param.nPmsProcessed);
				initPms[param.nPmsProcessed] = pm;
				if (param.nPmsProcessed == param.initClusterNum - 1) {
					initializeSimMeanStdDev(param, initPms);
				}
			} else {
				// check if protomemes with the same marker have already been put into any cluster
				Integer clusterIdx = pmMarkerClusterMap.get(pm.mainMarker);
				if (clusterIdx != null && clusterIdx >= 0) {
					pm.isClusteredByMarker = true;
					targetCluster = clusters.get(clusterIdx);
					t0 = System.currentTimeMillis();
					simToCluster = targetCluster.computeSimilarity(pm);
					t1 = System.currentTimeMillis();
					targetCluster.addProtoMeme(pm);
					targetClusterId = clusterIdx;
					centUpdateTime += System.currentTimeMillis() - t1;
					simCalcTime += t1 - t0;
				} else {
					// assign pm to the closest cluster, or to a new cluster if it is identified as an outlier
					int idxClosestCluster = -1; // index of the closest cluster (to this protomeme) in the clusters array
					ArrayList<Integer> closestCids = new ArrayList<Integer>(clusters.size());
					double maxSimilarity = 0; // similarity between this protomeme and the closest cluster
					int idxLruCluster = -1; // the index of the least recently updated cluster in the clusters array
					long lruTimestamp = Long.MAX_VALUE; // update timestamp of the least recently updated cluster
					int idxNull = -1;
					t0 = System.currentTimeMillis();
					for (int i=0; i<clusters.size(); i++) {
						ProtomemeCluster c = clusters.get(i);
						if (c == null) {
							idxNull = i;
							continue;
						}						
						double sim = c.computeSimilarity(pm);
						long updateTs = c.latestUpdateTime;
						if (sim > maxSimilarity) {
							maxSimilarity = sim;
							idxClosestCluster = i;
							closestCids.clear();
							closestCids.add(i);
						} else if (sim == maxSimilarity) {
							idxClosestCluster = -1;
							closestCids.add(i);
						}
						if (updateTs < lruTimestamp) {
							lruTimestamp = updateTs;
							idxLruCluster = i;
						}
					}
					if (idxClosestCluster < 0) {
						int rand = (int)(Math.random() * closestCids.size());
						idxClosestCluster = closestCids.get(rand);
					}
					simCalcTime += System.currentTimeMillis() - t0;
					
					double mean = param.sumSimForMeanStd / param.nSimsForMeanStd;
					double stdDev = Math.sqrt(param.ssSimForMeanStd / param.nSimsForMeanStd - mean * mean);
					adjustOutlierThreshold(param, mean, stdDev, param.outlierThdAdjStep);
					t0 = System.currentTimeMillis();
					if (maxSimilarity > 0 && maxSimilarity > mean - param.outlierThd * stdDev) {
						// ssShortestDis and sumShortestDis are skewed right after initialization. We don't detect outlier at such time.
						targetCluster = clusters.get(idxClosestCluster);
						targetClusterId = idxClosestCluster;
						targetCluster.addProtoMeme(pm);
						simToCluster = maxSimilarity;
						pmMarkerClusterMap.put(pm.mainMarker, idxClosestCluster);
						param.nSimsForMeanStd++;
						param.sumSimForMeanStd += simToCluster;
						param.ssSimForMeanStd += simToCluster * simToCluster;
					} else {
						// Outlier processing. Since the outlier will be the  centroid for the new cluster, its similarity to centroid is 1
						outlierCount++;
						targetCluster = new ProtomemeCluster(pm);
						targetCluster.startFromOutlier = true;
						simToCluster = 1.0;
						if (idxNull < 0) {
							if (clusters.size() >= param.initClusterNum * param.clusterNumMultiplier) {
								ProtomemeCluster toDelete = clusters.get(idxLruCluster);
								deletedClusters.add(toDelete);
								for (ProtoMeme p : toDelete.protomemes) {
									p.clusterId = -1;
									p.similarityToCluster = 0;
									pmMarkerClusterMap.remove(p.mainMarker);
								}
								clusters.set(idxLruCluster, targetCluster);
								pmMarkerClusterMap.put(pm.mainMarker, idxLruCluster);
								targetClusterId = idxLruCluster;
							} else {
								clusters.add(targetCluster);
								pmMarkerClusterMap.put(pm.mainMarker, clusters.size() - 1);
								targetClusterId = clusters.size() - 1;
							}
						} else {
							//System.out.println(" Available clusters position: " + idxNull);
							clusters.set(idxNull, targetCluster);
							pmMarkerClusterMap.put(pm.mainMarker, idxNull);
							targetClusterId = idxNull;
						}
					}
					centUpdateTime += System.currentTimeMillis() - t0;
					if (param.nPmsProcessed % 2000 == 0) {
						System.out.println("Clustered " + param.nPmsProcessed + " protomemes. Similarity mean :" + mean + ", standard deviation: "
								+ stdDev + ", outlier threshold: " + param.outlierThd + ". Similarity compuation: " + simCalcTime / 1000.0 + 
								"s, centroids update: " + centUpdateTime / 1000.0 + "s.");
					}
				}
			}
			pm.clusterId = targetClusterId;
			pm.similarityToCluster = simToCluster;
			param.nPmsProcessed++;
		}
		param.simCalcTime += simCalcTime;
		param.centUpdateTime += centUpdateTime;
		param.outlierCount += outlierCount;
		printFinalAndDeletedClusters(clusters, deletedClusters, false);
		System.out.println("Number of protomemes clustered: " + pms.size() + ", outlier count: " + outlierCount + " (" +
				outlierCount*100.0/pms.size()+ "%). Similarity calcuation time: " + simCalcTime / 1000.0 + "s, centroids update time: "
				+ centUpdateTime / 1000.0 + "s.");
	}

	/**
	 * Initialize information about similarity mean and standard deviation using the initial cluster centroids stored <b>initPms</b>.
	 * @param param
	 *  Global parameter that contains numbers used to compute the similarity mean and standard deviation, updated on the fly.
	 * @param initPms
	 *  An array containing the protomemes that are used as the initial centroids of the clusters.
	 * @throws Exception
	 */
	protected void initializeSimMeanStdDev(GlobalClusteringParams param, ProtoMeme[] initPms) throws Exception {
		for (int i=0; i<initPms.length; i++) {
			double maxSim = 0;
			for (int j=0; j<initPms.length; j++) {
				if (j != i) {
					double sim = initPms[i].computeSimilarity(initPms[j]);
					if (sim > maxSim) {
						maxSim = sim;
					}
				}
			}
			maxSim = 0.5 + maxSim / 2;
			param.nSimsForMeanStd++;
			param.sumSimForMeanStd += maxSim;
			param.ssSimForMeanStd += maxSim * maxSim;
		}
		double mean = param.sumSimForMeanStd / param.nSimsForMeanStd;
		double stdDev = Math.sqrt(param.ssSimForMeanStd / param.nSimsForMeanStd - mean * mean);
		System.out.println("similarity mean after initialization: " + mean + ", stdDev: " + stdDev);
	}

	/**
	 * Compute the percent-median of the sizes of all clusters, so that <b>percent</b>% of the
	 * cluster sizes are below the returned value.
	 * @param clusters
	 *  A list of protomeme clusters.
	 * @param percent
	 *  A value between 0.0 and 1.0.
	 * @return
	 *  The percent-median of all clusters sizes.
	 */
	protected int getMedianClusterSize(List<ProtomemeCluster> clusters, float percent) {
		int count = 0;
		for (ProtomemeCluster c : clusters) {
			if (c != null) {
				count++;
			}
		}
		
		int heap1SizeLimit = Math.round(percent * count);
		int heap2SizeLimit = count - heap1SizeLimit;
		PriorityQueue<Integer> heap1 = new PriorityQueue<Integer>(heap1SizeLimit + 1);
		PriorityQueue<Integer> heap2 = new PriorityQueue<Integer>(heap2SizeLimit + 1);
		for (ProtomemeCluster c : clusters) {
			if (c == null) {
				continue;
			}
			int size = c.centTidVector.size();
			if (heap2.size() == 0) {
				heap2.offer(size);
			} else {
				int topHeap2 = heap2.peek();
				if (size < topHeap2) {
					heap1.offer(-size);
					if (heap1.size() > heap1SizeLimit) {
						int topHeap1 = -heap1.poll();
						heap2.offer(topHeap1);
					}
				} else {
					heap2.offer(size);
					if (heap2.size() > heap2SizeLimit) {
						topHeap2 = heap2.poll();
						heap1.offer(-topHeap2);
					}
				}
			}
		}
		int medianSize = -heap1.poll();
		if (heap1.size() == heap1SizeLimit) {
			medianSize = -heap1.peek();
		}
		return medianSize;
	}
	
	/**
	 * Compute the percent-median of the update time of all clusters, so that <b>percent</b>% of the
	 * cluster update times are below the returned value.
	 * @param clusters
	 *  A list of protomeme clusters.
	 * @param percent
	 *  A value between 0.0 and 1.0.
	 * @return
	 *  The percent-median of all clusters update times.
	 */
	protected long getMedianClusterUpdateTime(List<ProtomemeCluster> clusters, float percent) {
		int count = 0;
		for (ProtomemeCluster c : clusters) {
			if (c != null) {
				count++;
			}
		}
		int heap1SizeLimit = Math.round(percent * count);
		int heap2SizeLimit = count - heap1SizeLimit;
		PriorityQueue<Long> heap1 = new PriorityQueue<Long>(heap1SizeLimit + 1);
		PriorityQueue<Long> heap2 = new PriorityQueue<Long>(heap2SizeLimit + 1);
		for (ProtomemeCluster c : clusters) {
			if (c == null) {
				continue;
			}
			long time = c.latestUpdateTime;
			if (heap2.size() == 0) {
				heap2.offer(time);
			} else {
				long topHeap2 = heap2.peek();
				if (time < topHeap2) {
					heap1.offer(-time);
					if (heap1.size() > heap1SizeLimit) {
						long topHeap1 = -heap1.poll();
						heap2.offer(topHeap1);
					}
				} else {
					heap2.offer(time);
					if (heap2.size() > heap2SizeLimit) {
						topHeap2 = heap2.poll();
						heap1.offer(-topHeap2);
					}
				}
			}
		}
		long medianTime = -heap1.poll();
		if (heap1.size() == heap1SizeLimit) {
			medianTime = -heap1.peek();
		}
		return medianTime;
	}

	/**
	 * Dynamically adjust the outlier threshold value in <b>para</b> based on the current <b>mean</b> and <b>stdDev</b>.
	 * @param param
	 *  An object containing the dynamic global parameters during the whole clustering process.
	 * @param mean
	 *  Current mean of silimarities.
	 * @param stdDev
	 *  Current standard deviation of similarities.
	 * @param adjStep
	 *  The 'delta' to use when adjusting the outlier threshold.
	 */
	protected void adjustOutlierThreshold(GlobalClusteringParams param, double mean, double stdDev, double adjStep) {
		if (mean - param.outlierThd * stdDev < 0 && param.outlierThd > 0) {
			param.outlierThd -= adjStep;
			if (param.outlierThd < 0) {
				param.outlierThd = 0;
			}
		} else if (param.outlierThd < param.initOutlierThd && mean - param.outlierThd * stdDev > adjStep * stdDev) {
			param.outlierThd += adjStep;
			if (param.outlierThd > param.initOutlierThd) {
				param.outlierThd = param.initOutlierThd;
			}
		}
	}

	/**
	 * Sort the .json.gz files under <b>jsonGzDir</b> by file names (dates).
	 * @param jsonGzDir
	 *  A directory containing .json.gz files.
	 * @return
	 *  A list of sorted File objects.
	 */
	protected ArrayList<File> getSortedJsonGzFiles(String jsonGzDir) {
		File[] files = new File(jsonGzDir).listFiles();
		Arrays.sort(files);
		ArrayList<File> fJsonGzs = new ArrayList<File>(files.length);
		for (File f : files) {
			if (f.getName().endsWith(".json.gz")) {
				fJsonGzs.add(f);
			}
		}
		return fJsonGzs;
	}

	/**
	 * Convert a comma separated string of hashtags to a set of hashtags.
	 * @param htsToDelete
	 *  A string in the form of "#p2,#tcot,#iu,..."
	 * @return
	 */
	protected Set<String> parseHashtags(String htsToDelete) {
		String[] hashtags = null;
		if (htsToDelete.length() > 0) {
			hashtags = htsToDelete.toLowerCase().replaceAll("^[,\\s]+", "").split("[,\\s]+");
		} else {
			hashtags = new String[0];
		}
		Set<String> htSet = new HashSet<String>(hashtags.length);
		for (String ht : hashtags) {
			htSet.add(ht);
		}
		return htSet;
	}

	/**
	 * Print information about the protomemes in the final <b>clusters</b> and <b>deletedClusters</b>.
	 * @param clusters
	 *  An array of final clusters.
	 * @param deletedClusters
	 *  A list of deleted clusters.
	 * @param verbose
	 *  Whether to print the details of clusters.
	 */
	protected void printFinalAndDeletedClusters(List<ProtomemeCluster> clusters, List<ProtomemeCluster> deletedClusters, boolean verbose) {
		int maxTidsInCluster = 0;
		int minTidsInCluster = Integer.MAX_VALUE;
		int totalTidsInCluster = 0;
		int maxUidsInCluster = 0;
		int minUidsInCluster = Integer.MAX_VALUE;
		int totalUidsInCluster = 0;
		int maxWordsInCluster = 0;
		int minWordsInCluster = Integer.MAX_VALUE;
		int totalWordsInCluster = 0;
		int maxDffsInCluster = 0;
		int minDffsInCluster = Integer.MAX_VALUE;
		int totalDffsInCluster = 0;
		int maxTidsOutlierCluster = 0;
		int totalTidsOutlierCluster = 0;
		long earliestTweetTs = Long.MAX_VALUE;
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		if (clusters != null) {
			System.out.println("Final clusters:");
			for (int i = 0; i < clusters.size(); i++) {
				ProtomemeCluster c = clusters.get(i);
				if (c == null || c.latestUpdateTime == Long.MIN_VALUE) {
					continue;
				}
				if (c.centTidVector.size() > maxTidsInCluster) {
					maxTidsInCluster = c.centTidVector.size();
				}
				if (c.centTidVector.size() < minTidsInCluster) {
					minTidsInCluster = c.centTidVector.size();
				}
				totalTidsInCluster += c.centTidVector.size();
				
				if (c.centUserVector.size() > maxUidsInCluster) {
					maxUidsInCluster = c.centUserVector.size();
				}
				if (c.centUserVector.size() < minUidsInCluster) {
					minUidsInCluster = c.centUserVector.size();
				}
				totalUidsInCluster += c.centUserVector.size();
				
				if (c.centWordVector.size() > maxWordsInCluster) {
					maxWordsInCluster = c.centWordVector.size();
				}
				if (c.centWordVector.size() < minWordsInCluster) {
					minWordsInCluster = c.centWordVector.size();
				}
				totalWordsInCluster += c.centWordVector.size();
				
				if (c.centDiffusionVector.size() > maxDffsInCluster) {
					maxDffsInCluster = c.centDiffusionVector.size();
				}
				if (c.centDiffusionVector.size() < minDffsInCluster) {
					minDffsInCluster = c.centDiffusionVector.size();
				}
				totalDffsInCluster += c.centDiffusionVector.size();
				
				if (c.startFromOutlier) {
					totalTidsOutlierCluster += c.centTidVector.size();
					if (c.centTidVector.size() > maxTidsOutlierCluster) {
						maxTidsOutlierCluster = c.centTidVector.size();
					}
				}
				if (c.latestUpdateTime < earliestTweetTs) {
					earliestTweetTs = c.latestUpdateTime;
				}
				for (ProtoMeme pm : c.protomemes) {
					if (pm.earliestTweetTs < earliestTweetTs) {
						earliestTweetTs = pm.earliestTweetTs;
					}
				}

				if (verbose) {
					System.out.println("=============" + c.protomemes.size() + " protomemes in cluster " + i + ":");
					for (ProtoMeme pm : c.protomemes) {
						System.out.println(pm.mainMarker + " " + pm.similarityToCluster);
					}
					System.out.println(c.centTidVector.size() + " tweet IDs in centTidVector:");
					int tidCount = 0;
					for (Map.Entry<String, Integer> e : c.centTidVector.entrySet()) {
						System.out.print(e.getKey() + ":" + e.getValue() + ",");
						if (++tidCount % 20 == 0) {
							System.out.println();
						}
					}
					System.out.println();
				}
			}
			Set<String> tids = new HashSet<String>(maxTidsInCluster * 2);
			for (ProtomemeCluster c : clusters) {
				if (c != null) {
					tids.addAll(c.centTidVector.keySet());
				}
			}
			calTmp.setTimeInMillis(earliestTweetTs);
			System.out.println(clusters.size() + " final clusters. Earliest tweet timestamp: " + earliestTweetTs + "(" 
					+ GeneralHelpers.getDateTimeString(calTmp) + "). Total number of unique tweet IDs: " + tids.size() + 
					". Total number of tweet IDs in outlier-started clusters: " + totalTidsOutlierCluster + ", max: " + maxTidsOutlierCluster);
			System.out.println("max tids in final clusters: " + maxTidsInCluster + ", min: " + minTidsInCluster + ", avg: " +
					totalTidsInCluster * 1.0 / clusters.size() + ", total: " + totalTidsInCluster);
			System.out.println("max uids in final clusters: " + maxUidsInCluster + ", min: " + minUidsInCluster + ", avg: " +
					totalUidsInCluster * 1.0 / clusters.size() + ", total: " + totalUidsInCluster);
			System.out.println("max words in final clusters: " + maxWordsInCluster + ", min: " + minWordsInCluster + ", avg: " +
					totalWordsInCluster * 1.0 / clusters.size() + ", total: " + totalWordsInCluster);
			System.out.println("max Diffusion uids in final clusters: " + maxDffsInCluster + ", min: " + minDffsInCluster + ", avg: " +
					totalDffsInCluster * 1.0 / clusters.size() + ", total: " + totalDffsInCluster);
			System.out.println();
		}
		if (deletedClusters != null) {
			maxTidsInCluster = 0;
			minTidsInCluster = Integer.MAX_VALUE;
			totalTidsInCluster = 0;
			earliestTweetTs = Long.MAX_VALUE;
			maxTidsOutlierCluster = 0;
			totalTidsOutlierCluster = 0;
			System.out.println("Deleted clusters:");
			int n = 0;
			for (ProtomemeCluster c : deletedClusters) {
				if (c.centTidVector.size() > maxTidsInCluster) {
					maxTidsInCluster = c.centTidVector.size();
				}
				if (c.centTidVector.size() < minTidsInCluster) {
					minTidsInCluster = c.centTidVector.size();
				}
				totalTidsInCluster += c.centTidVector.size();
				if (c.startFromOutlier) {
					totalTidsOutlierCluster += c.centTidVector.size();
					if (c.centTidVector.size() > maxTidsOutlierCluster) {
						maxTidsOutlierCluster = c.centTidVector.size();
					}
				}
				for (ProtoMeme pm : c.protomemes) {
					if (pm.earliestTweetTs < earliestTweetTs) {
						earliestTweetTs = pm.earliestTweetTs;
					}
				}

				if (verbose) {
					System.out.println("=============" + c.protomemes.size() + " protomemes in cluster " + n + ":");
					for (ProtoMeme pm : c.protomemes) {
						System.out.println(pm.mainMarker + " " + pm.similarityToCluster);
					}
					System.out.println(c.centTidVector.size() + " tweet IDs in centTidVector:");
					int tidCount = 0;
					for (Map.Entry<String, Integer> e : c.centTidVector.entrySet()) {
						System.out.print(e.getKey() + ":" + e.getValue() + ",");
						if (++tidCount % 20 == 0) {
							System.out.println();
						}
					}
					System.out.println();
				}
				n++;
			}
			Set<String> tids = new HashSet<String>(maxTidsInCluster * 2);
			for (ProtomemeCluster c : deletedClusters) {
				if (c != null) {
					tids.addAll(c.centTidVector.keySet());
				}
			}
			calTmp.setTimeInMillis(earliestTweetTs);
			System.out.println(deletedClusters.size() + " deleted clusters. max tids in deleted clusters: " + maxTidsInCluster + ", min: "
					+ minTidsInCluster + ", avg: " + totalTidsInCluster * 1.0 / deletedClusters.size() + ". Earliest tweet timestamp: " +
					earliestTweetTs + "(" + GeneralHelpers.getDateTimeString(calTmp) + "). Total number of unique tweet IDs: " + tids.size()
					+ ". Total number of tweet IDs in outlier-started clusters: " + totalTidsOutlierCluster + ", max: " + maxTidsOutlierCluster);
			System.out.println();
		}
	}
	
	/**
	 * Compute the content vecotr similarity between a protomeme and a cluster given in <b>contentVectorsPath</b>.
	 * @param contentVectorsPath
	 *  Path to a file containing the content vector of a protomeme and a cluster.
	 * @throws Exception
	 */
	public void testContentSimilarity(String contentVectorsPath) throws Exception {
		BufferedReader brCont = new BufferedReader(new FileReader(contentVectorsPath));
		// The first line: "Protomeme content vector: {...}"
		String pmContent = brCont.readLine();
		// The first line: "cluster content vector: {...}"
		String clusterContent = brCont.readLine();
		brCont.close();
		int idx = pmContent.indexOf(':');
		String pmContJson = pmContent.substring(idx + 2);
		idx = clusterContent.indexOf(':');
		String clusterContJson = clusterContent.substring(idx + 2);
		HashMap<String, Double> tmp = new HashMap<String, Double>();
		Gson gson = new Gson();
		HashMap<String, Double> pmContMapTmp = gson.fromJson(pmContJson, tmp.getClass());
		HashMap<String, Double> clusterContMapTmp = gson.fromJson(clusterContJson, tmp.getClass());
		HashMap<String, Integer> pmContMap = new HashMap<String, Integer>();
		HashMap<String, Integer> clusterContMap = new HashMap<String, Integer>();
		for (Map.Entry<String, Double> e : pmContMapTmp.entrySet()) {
			pmContMap.put(e.getKey(), (int)e.getValue().doubleValue());
		}
		for (Map.Entry<String, Double> e : clusterContMapTmp.entrySet()) {
			clusterContMap.put(e.getKey(), (int)e.getValue().doubleValue());
		}
		
		System.out.println("pm content: " + pmContMap.toString());
		
		long thisSs = GeneralHelpers.sumOfSqures(clusterContMap.values());
		long thatSs = GeneralHelpers.sumOfSqures(pmContMap.values());
		Map<String, Integer> toLoop = pmContMap;
		Map<String, Integer> toMatch = clusterContMap;
		if (clusterContMap.size() < pmContMap.size()) {
			toLoop = clusterContMap;
			toMatch = pmContMap;
		}
		long sumIntersect = 0;
		for (Map.Entry<String, Integer> e : toLoop.entrySet()) {
			String word = e.getKey();
			Integer freq = e.getValue();
			Integer freq2 = toMatch.get(word);
			if (freq2 != null) {
				sumIntersect += freq * freq2;
			}
		}
		double res = sumIntersect / Math.sqrt(thisSs * thatSs);
		System.out.println("Using Long: sumIntersect: " + sumIntersect + ", thisSs: " + thisSs + ", thatSs: " + thatSs + ", res: " + res);
		double thisSsDouble = GeneralHelpers.sumOfSquresDouble(clusterContMap.values());
		res = sumIntersect / Math.sqrt(thisSsDouble * thatSs);
		System.out.println("Using Double: sumIntersect: " + sumIntersect + ", thisSs: " + thisSsDouble + ", thatSs: " + thatSs + ", res: " + res);
		
	}
	
	/**
	 * Scan a .json.gz file and print the number of tweets for every hour. Meanwhile, get the tweets within 
	 * the hour of <b>hourToGet</b>, and write them to <b>resJsonGzPath</b>.
	 * 
	 * @param jsonGzPath
	 *  Path to the source .json.gz file.
	 * @param hourToGet
	 *  A number telling which hour's tweets to get.
	 * @param resJsonGzPath
	 * 	Path to the result .json.gz file.
	 */
	public void scanJsonGzFileByHour(String jsonGzPath, int hourToGet, String resJsonGzPath) throws Exception {
		int lastHourOfDay = 0;
		int nTweetsToLastHour = 0;
		Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		long startTime = System.currentTimeMillis();
		long lastTime = startTime;
		long thisTime = -1;
		int count = 0;

		PrintWriter pwRes = null;
		PrintWriter pw10min = null;
		PrintWriter pw50min = null;
		if (resJsonGzPath != null) {
			pwRes = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(resJsonGzPath)), "UTF-8"));
			pw10min = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(resJsonGzPath + ".10min")), "UTF-8"));
			pw50min = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(resJsonGzPath + ".50min")), "UTF-8"));
		}
		GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(jsonGzPath));
		BufferedReader brJson = new BufferedReader(new InputStreamReader(gzInputStream, "UTF-8"));
		String jsonStr = brJson.readLine();
		while (jsonStr != null) {
			count++;
			try {
				jsonStr = jsonStr.trim();
				JsonObject joTweet = ConstantsTruthy.jsonParser.parse(jsonStr).getAsJsonObject();
				calTmp.setTimeInMillis(ConstantsTruthy.dateFormat.parse(joTweet.get("created_at").getAsString()).getTime());
				int curHour = calTmp.get(Calendar.HOUR_OF_DAY);
				if (curHour > lastHourOfDay) {
					System.out.println("Number of tweets from " + lastHourOfDay + ":00:00 to " + curHour + ":00:00 : " + 
							(count - nTweetsToLastHour));
					lastHourOfDay = curHour;
					nTweetsToLastHour = count;
				}
				if (hourToGet > 0 && pwRes != null && curHour == hourToGet) {
					pwRes.println(jsonStr);
					if (calTmp.get(Calendar.MINUTE) < 10) {
						pw10min.println(jsonStr);
					} else {
						pw50min.println(jsonStr);
					}
				}
				if (curHour > hourToGet) {
					break;
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}			
			if (count % 1000000 == 0) {
				thisTime = System.currentTimeMillis();
				System.out.println("processed " + count + " lines. Time spent on this batch: " + (thisTime - lastTime) / 1000 + " seconds.");
				lastTime = thisTime;
			}
			jsonStr = brJson.readLine();
		}
		brJson.close();
		if (pwRes != null) {
			pwRes.close();
			pw10min.close();
			pw50min.close();
		}		
	}
	
	/**
	 * Print the usage guide of this application.
	 * @param exitCode
	 *  The exit code for System.exit().
	 *  
	 *  String jsonGzPath, String twStartTime, int twInMin, int nClusters, int outlierThreshold
	 */
	public static void usageAndExit(int exitCode) {
		System.out.println("Usage: java iu.pti.hbaseapp.truthy.MemeClusteringTester <command> <arguments>");
		System.out.println("	Where '<command> <arguments>' could be one of the following combinations:");
		System.out.println("	protomeme-counter <.json.gz file path> <number of tweets to scan> <protomeme type: 'url', 'mention', 'hashtag',"
				+ "'phrase', or 'all'>");
		System.out.println("	protomeme-stats <.json.gz file path> <time window start> <time window length> <similarity histogram unit>");
		System.out.println("	test-protomeme-details <.json.gz file path> <number of tweets to process>");
		System.out.println("	online-kmeans-one-window <.json.gz file path> <time window start> <time window length in min> <number of clusters> " +  
				"<list of ground truth hashtags>");
		System.out.println("	online-kmeans-sliding-window <.json.gz directory path> <time step length in min> <time window length in steps> " + 
				"<number of clusters> <number of standard deviation for outlier detection> <outlier threshold ajustment step> " + 
				"<cluster number multiplier> <list of ground truth hashtags>");
		System.out.println("	test-LFK-NMI <first clusters file path> <second clusters file path> ");
		System.out.println("	get-LFK-NMI-olk <first online KMeans result path> <second online KMeans result path>");
		System.out.println("	get-LFK-NMI-olk-gt <online KMeans result path> <ground truth tweet ID clusters path>");
		System.out.println("	scan-json-gz-by-hour <.json.gz file path> [<hour to extract> <.json.gz path for extracted tweets>]");
		System.out.println("	test-content-similarity <content vectors path>");
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			usageAndExit(1);
		}
		String command = args[0];		
		try {
			if (command.equals("protomeme-counter")) {
				if (args.length < 4) {
					usageAndExit(1);
				}
				MemeClusteringTester tester = new MemeClusteringTester();
				tester.protoMemeCount(args[1], Integer.parseInt(args[2]), ProtoMeme.parseProtoMemeType(args[3]));
			} else if (command.equals("protomeme-stats")) {
				if (args.length < 5) {
					usageAndExit(1);
				}
				MemeClusteringTester tester = new MemeClusteringTester();
				tester.protomemeStatsByTimeWin(args[1], args[2], Float.parseFloat(args[3]), Float.parseFloat(args[4]));
			} else if (command.equals("test-protomeme-details")) {
				if (args.length < 3) {
					usageAndExit(1);
				}
				MemeClusteringTester tester = new MemeClusteringTester();
				tester.testProtomemeDetails(args[1], Integer.parseInt(args[2]), false);
			} else if (command.equals("online-kmeans-one-window")) {
				if (args.length < 7) {
					usageAndExit(1);
				}
				MemeClusteringTester tester = new MemeClusteringTester();
				tester.onlineKMeansOneTimeWin(args[1], args[2], Float.parseFloat(args[3]), Integer.parseInt(args[4]),
						Integer.parseInt(args[5]), args[6]);
			} else if (command.equals("online-kmeans-sliding-window")) {
				if (args.length < 9) {
					usageAndExit(1);
				}
				MemeClusteringTester tester = new MemeClusteringTester();
				tester.onlineKMeansSlidingWin(args[1], Float.parseFloat(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]),
						Float.parseFloat(args[5]), Float.parseFloat(args[6]), Float.parseFloat(args[7]), args[8]);
			} else if (command.equals("test-LFK-NMI")) {
				if (args.length < 3) {
					usageAndExit(1);
				}
				NmiUtil nmi = new NmiUtil();
				nmi.testLfkNmiWithFiles(args[1], args[2]);
			} else if (command.equals("get-LFK-NMI-olk")) {
				if (args.length < 3) {
					usageAndExit(1);
				}
				NmiUtil nmi = new NmiUtil();
				nmi.getLfkNmiBetweenOlkRes(args[1], args[2]);
			} else if (command.equals("get-LFK-NMI-olk-gt")) {
				if (args.length < 3) {
					usageAndExit(1);
				}
				NmiUtil nmi = new NmiUtil();
				nmi.getLfkNmiOlkGroundTruth(args[1], args[2]);
			} else if (command.equals("test-content-similarity")) {
				if (args.length < 2) {
					usageAndExit(1);
				}
				MemeClusteringTester tester = new MemeClusteringTester();
				tester.testContentSimilarity(args[1]);
			} else if (command.equals("scan-json-gz-by-hour")) {
				if (args.length < 2) {
					usageAndExit(1);
				}
				String jsonGzPath = args[1];
				int hourToGet = -1;
				String resJsonGzPath = null;
				if (args.length > 2) {
					hourToGet = Integer.parseInt(args[2]);
					resJsonGzPath = args[3];
				}
				MemeClusteringTester tester = new MemeClusteringTester();
				tester.scanJsonGzFileByHour(jsonGzPath, hourToGet, resJsonGzPath);
			} else {
				usageAndExit(-1);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception caught when running the application: " + e.getMessage());
			usageAndExit(2);
		}		
	}
}
