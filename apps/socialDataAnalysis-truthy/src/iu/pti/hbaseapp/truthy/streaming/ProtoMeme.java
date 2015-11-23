package iu.pti.hbaseapp.truthy.streaming;

import iu.pti.hbaseapp.GeneralHelpers;
import iu.pti.hbaseapp.truthy.streaming.MemeClusteringTester.DiffusionIndices;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Represent a protomeme in the meme clustering algorithm.
 * @author gaoxm
 */
public class ProtoMeme {
	/**
	 * Protomeme types: URL, User mention, Hashtag, Phrase, and 'All'.
	 * @author gaoxm
	 */
	public static enum ProtoMemeType {
		UNDEFINED, URL, MENTION, HASHTAG, PHRASE, ALL;
	}
	
	/**
	 * Parsing the string representation of a protomeme type.
	 * 
	 * @param pmType
	 *  The string representation of a protomeme type; it should be one of 'url', 'mention',
	 *  'hashtag', 'phrase', and 'all'.
	 *  
	 * @return
	 *  The corresponding value of <b>ProtoMemeType</b>. 
	 * @throws IllegalArgumentException
	 */
	protected static ProtoMemeType parseProtoMemeType(String pmType) throws IllegalArgumentException {
		if (pmType.equalsIgnoreCase("url")) {
			return ProtoMemeType.URL;
		} else if (pmType.equalsIgnoreCase("mention")) {
			return ProtoMemeType.MENTION;
		} else if (pmType.equalsIgnoreCase("hashtag")) {
			return ProtoMemeType.HASHTAG;
		} else if (pmType.equalsIgnoreCase("phrase")) {
			return ProtoMemeType.PHRASE;
		} else if (pmType.equalsIgnoreCase("all")) {
			return ProtoMemeType.ALL;
		} else {
			throw new IllegalArgumentException("Unsupported protomeme type: " + pmType);
		}
	}
	
	/** The type of this protomeme: URL, MENTION, HASHTAG, or PHRASE */
	protected ProtoMemeType type = ProtoMemeType.UNDEFINED;
	
	/** The string representation of the URL, mention, hashtag, or phrase of this protomeme. */
	protected String mainMarker;
	
	/** IDs of tweets in this protomeme */
	protected HashSet<String> tweetIds;
	
	/** Records how many times each user has posted tweets in this protomeme */
	protected HashMap<String, Integer> userFreqs;
	
	/** Records the word frequencies of all tweets in this protomeme */
	protected HashMap<String, Integer> wordFreqs;
	
	/** The set of users 'covered' by the 'information diffusion' of this protomeme, including authors of tweets in this protomeme, 
	 * users mentioned/retweeted by the authors, and users mentioning/retweeting the authros.*/ 
	protected HashSet<String> diffusionUserIds;
	
	/** Store the tf-idf values of the text words of all tweets in this protomeme */
	protected HashMap<String, Double> wordTfIdfs;
	
	/** 'Sum of squares' of the words */
	protected double tfIdfSumSquares;
	
	/** Whether this protomeme is clustered because its marker has already been assigned to a cluster */ 
	public boolean isClusteredByMarker = false;
	
	/** The ID of the cluster this protomeme belongs to */
	public int clusterId;
	
	/** The similarity with the centroid of the cluster */
	public double similarityToCluster;
	
	/** The timestamp the 'oldest' tweet in this protomeme */
	protected long earliestTweetTs;
	
	/** The timestamp of the 'freshest' tweet in this protomeme */
	protected long latestTweetTs;
	
	/**
	 * Constructor for a protomeme.
	 * @param type
	 *  Must be one of URL, MENTION, HASHTAG, or PHRASE.
	 * @param mainMarker
	 *  String representation of the corresponding URL, mention, hashtag, or phrase.
	 * @throws IllegalArgumentException
	 *  In case <b>type</b> is of other values or <b>mainMarker</b> is <b>null</b>.
	 */
	public ProtoMeme(ProtoMemeType type, String mainMarker) throws IllegalArgumentException {
		if (type != ProtoMemeType.HASHTAG && type != ProtoMemeType.URL 
				&& type != ProtoMemeType.MENTION && type != ProtoMemeType.PHRASE) {
			throw new IllegalArgumentException("'type' is not one of URL, MENTION, HASHTAG, or PHRASE.");
		}
		if (mainMarker == null) {
			throw new IllegalArgumentException("'mainMarker' is null.");
		}
		this.type = type;
		this.mainMarker = mainMarker;
		tweetIds = new HashSet<String>();
		userFreqs = new HashMap<String, Integer>();
		wordFreqs = new HashMap<String, Integer>();
		diffusionUserIds = new HashSet<String>();
		wordTfIdfs = new HashMap<String, Double>();
		tfIdfSumSquares = 0.0;
		earliestTweetTs = Long.MAX_VALUE;
		latestTweetTs = Long.MIN_VALUE;
		clusterId = -1;
	}
	
	/**
	 * Add a <b>tweet</b> to this protomeme.
	 * @param tweet
	 * @throws Exception
	 */
	public void addTweet(TweetFeatures tweet) throws Exception {
		tweetIds.add(tweet.tweetId);
		Integer freq = userFreqs.get(tweet.userId);
		freq = (freq == null) ? 1 : (freq + 1);
		userFreqs.put(tweet.userId, freq);
		for (String word : tweet.words) {
			freq = wordFreqs.get(word);
			freq = (freq == null) ? 1 : (freq + 1);
			wordFreqs.put(word, freq);
		}
		if (tweet.createTimeMilli < earliestTweetTs) {
			earliestTweetTs = tweet.createTimeMilli;
		}
		if (tweet.createTimeMilli > latestTweetTs) {
			latestTweetTs = tweet.createTimeMilli;
		}		
	}
	
	/**
	 * Remove a <b>tweet</b> from this protomeme.
	 * @param tweet
	 * @throws Exception
	 */
	public void removeTweet(TweetFeatures tweet) throws Exception {
		tweetIds.remove(tweet.tweetId);
		Integer freq = userFreqs.get(tweet.userId);
		if (freq != null) {
			freq = freq - 1;
			if (freq > 0) {
				userFreqs.put(tweet.userId, freq - 1);
			} else {
				userFreqs.remove(tweet.userId);
			}
		}
		for (String word : tweet.words) {
			freq = wordFreqs.get(word);
			if (freq != null) {
				freq -= 1;
				if (freq > 0) {
					wordFreqs.put(word, freq);
				} else {
					wordFreqs.remove(word);
				}
			}
		}
	}
	
	/**
	 * Compute this protomeme's diffusion network (user IDs + mentioned user IDs + user IDs who have retweeted this protomeme) and words'
	 * tf-idf values based on the tweets that are in this protomeme.
	 * @param diffIndices
	 *  An object containing the global diffusion indices for the current time step.
	 * @param globalWordPmCounts
	 *  A map from global words to their document counts, useful for computing the tf-idf values.
	 * @param globalPmCount
	 *  Total number of global protomemes. Useful for computing the tf-idf values.
	 * @throws Exception
	 */
	public void computeDiffusionAndTfIdf(DiffusionIndices diffIndices, Map<String, Integer> globalWordPmCounts, int globalPmCount)
			throws Exception {
		computeDiffusionUids(diffIndices);		
		computeWordTfIdf(globalWordPmCounts, globalPmCount);
	}

	/**
	 * Compute this protomeme's words' tf-idf values based on the tweets that are in this protomeme.
	 * @param globalWordPmCounts
	 *  A map from global words to their document counts, useful for computing the tf-idf values.
	 * @param globalPmCount
	 *  Total number of global protomemes.
	 * @throws Exception
	 */
	protected void computeWordTfIdf(Map<String, Integer> globalWordPmCounts, int globalPmCount) {
		wordTfIdfs.clear();
		tfIdfSumSquares = 0;
		for (Map.Entry<String, Integer> e : wordFreqs.entrySet()) {
			String word = e.getKey();
			int freq = e.getValue();
			double tfidf = freq * Math.log(globalPmCount * 1.0 / globalWordPmCounts.get(word));
			wordTfIdfs.put(word, tfidf);
			tfIdfSumSquares += tfidf * tfidf;
		}
	}

	/**
	 * Compute this protomeme's diffusion network (user IDs + mentioned user IDs + user IDs who have retweeted this protomeme).
	 * @param diffIndices
	 *  An object containing the global diffusion indices for the current time step.
	 * @throws Exception
	 */
	protected void computeDiffusionUids(DiffusionIndices diffIndices) {
		diffusionUserIds.clear();
		for (String uid : userFreqs.keySet()) {
			diffusionUserIds.add(uid);
			Set<String> mentionToUids = diffIndices.mentioningIndex.get(uid);
			if (mentionToUids != null) {
				for (String muid : mentionToUids) {
					diffusionUserIds.add(muid);
				}
			}
			Set<String> mentionFromUids = diffIndices.mentionedByIndex.get(uid);
			if (mentionFromUids != null) {
				for (String muid : mentionFromUids) {
					diffusionUserIds.add(muid);
				}
			}
			Set<String> retweetToUids = diffIndices.retweetingIndex.get(uid);
			if (retweetToUids != null) {
				for (String ruid : retweetToUids) {
					diffusionUserIds.add(ruid);
				}
			}
			Set<String> retweetFromUids = diffIndices.retweetedByIndex.get(uid);
			if (retweetFromUids != null) {
				for (String ruid : retweetFromUids) {
					diffusionUserIds.add(ruid);
				}
			}
		}
	}
	
	/**
	 * Compute the similarity between this and <b>that</b> protomeme.
	 * @param that
	 * @return
	 * @throws Exception
	 */
	public double computeSimilarity(ProtoMeme that) throws Exception {
		double result = computeTidSimilarity(that);
		if (result == 1.0) {
			return result;
		}
		double tmp = computeUidSimilarity(that);
		if (tmp > result) {
			result = tmp;
		}
		tmp = computeDiffusionSimilarity(that);
		if (tmp > result) {
			result = tmp;
		}
		tmp = computeContentSimilarity(that);
		if (tmp > result) {
			result = tmp;
		}
		return result;
	}
	
	/**
	 * Compute the common user similarity between this and <b>that</b> protomeme.
	 * @param that
	 * @return
	 * @throws Exception
	 */
	public double computeUidSimilarity(ProtoMeme that) throws Exception {
		double thisSs = GeneralHelpers.sumOfSquresDouble(this.userFreqs.values());
		double thatSs = GeneralHelpers.sumOfSquresDouble(that.userFreqs.values());
		Map<String, Integer> toLoop = this.userFreqs;
		Map<String, Integer> toMatch = that.userFreqs;
		if (this.userFreqs.size() > that.userFreqs.size()) {
			toLoop = that.userFreqs;
			toMatch = this.userFreqs;
		}
		long sumIntersect = 0;
		for (Map.Entry<String, Integer> e : toLoop.entrySet()) {
			String uid = e.getKey();
			Integer freq = e.getValue();
			Integer freq2 = toMatch.get(uid);
			if (freq2 != null) {
				sumIntersect += freq * freq2;
			}
		}
		return sumIntersect / Math.sqrt(thisSs * thatSs);
	}
	
	/**
	 * Compute the common tweet ID similarity between this and <b>that</b> protomeme.
	 * @param that
	 * @return
	 * @throws Exception
	 */
	public double computeTidSimilarity(ProtoMeme that) throws Exception {
		Set<String> toLoop = this.tweetIds;
		Set<String> toMatch = that.tweetIds;
		if (this.tweetIds.size() > that.tweetIds.size()) {
			toLoop = that.tweetIds;
			toMatch = this.tweetIds;
		}
		int count = 0;
		for (String tid : toLoop) {
			if (toMatch.contains(tid)) {
				count++;
			}
		}
		if (count == toLoop.size() && count == toMatch.size()) {
			return 1.0;
		} else {
			return count / Math.sqrt(toLoop.size() * toMatch.size());
		}
	}
	
	/**
	 * Compute the text content similarity between this and <b>that</b> protomeme.
	 * @param that
	 * @return
	 * @throws Exception
	 */
	public double computeContentSimilarity(ProtoMeme that) throws Exception {
		if (this.tfIdfSumSquares == 0.0 || that.tfIdfSumSquares == 0.0) {
			// compute similarity using word frequencies.
			if (this.wordFreqs.size() == 0  || that.wordFreqs.size() == 0) {
				return 0;
			}
			double thisSs = GeneralHelpers.sumOfSquresDouble(this.wordFreqs.values());
			double thatSs = GeneralHelpers.sumOfSquresDouble(that.wordFreqs.values());
			Map<String, Integer> toLoop = this.wordFreqs;
			Map<String, Integer> toMatch = that.wordFreqs;
			if (this.wordFreqs.size() > that.wordFreqs.size()) {
				toLoop = that.wordFreqs;
				toMatch = this.wordFreqs;
			}
			long sumIntersect = 0;
			for (Map.Entry<String, Integer> e : toLoop.entrySet()) {
				String uid = e.getKey();
				Integer freq = e.getValue();
				Integer freq2 = toMatch.get(uid);
				if (freq2 != null) {
					sumIntersect += freq * freq2;
				}
			}
			return sumIntersect / Math.sqrt(thisSs * 1.0 * thatSs);
		} else {
			// compute similarity using word tf-idf values.
			Map<String, Double> toLoop = this.wordTfIdfs;
			Map<String, Double> toMatch = that.wordTfIdfs;
			if (this.wordTfIdfs.size() > that.wordTfIdfs.size()) {
				toLoop = that.wordTfIdfs;
				toMatch = this.wordTfIdfs;
			}
			double sumIntersect = 0;
			for (Map.Entry<String, Double> e : toLoop.entrySet()) {
				String word = e.getKey();
				Double tfidf = e.getValue();
				Double tfidf2 = toMatch.get(word);
				if (tfidf2 != null) {
					sumIntersect += tfidf * tfidf2;
				}
			}
			return sumIntersect / Math.sqrt(this.tfIdfSumSquares * 1.0 * that.tfIdfSumSquares);
		}
	}

	/**
	 * Compute the diffusion network similarity between this and <b>that</b> protomeme.
	 * @param that
	 * @return
	 * @throws Exception
	 */
	public double computeDiffusionSimilarity(ProtoMeme that) throws Exception {
		Set<String> toLoop = this.diffusionUserIds;
		Set<String> toMatch = that.diffusionUserIds;
		if (this.diffusionUserIds.size() > that.diffusionUserIds.size()) {
			toLoop = that.diffusionUserIds;
			toMatch = this.diffusionUserIds;
		}
		long sumIntersect = 0;
		for (String uid : toLoop) {
			if (toMatch.contains(uid)) {
				sumIntersect++;
			}
		}
		return sumIntersect / Math.sqrt(toLoop.size() * toMatch.size());
	}
	
	@Override
	public boolean equals(Object that) {
		if (!(that instanceof ProtoMeme)) {
			return false;
		}
		ProtoMeme thatpm = (ProtoMeme)that;
		return type.equals(thatpm.type) && earliestTweetTs == thatpm.earliestTweetTs 
				&&  mainMarker.equals(thatpm.mainMarker) && tweetIds.equals(thatpm.tweetIds);
	}
	
	@Override
	public int hashCode() {
		return mainMarker.hashCode() + (int)(earliestTweetTs ^ (earliestTweetTs >>> 32));
	}
	
	/**
	 * Get the timestamp of the 'oldest' tweet in this protomeme. We 
	 * regard this as the 'creation timestamp' of this protomeme.
	 * @return
	 *  The timestamp of the 'oldest' tweet in this protomeme.
	 */
	public long getEarliestTweetTs() {
		return earliestTweetTs;
	}
}
