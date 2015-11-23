package iu.pti.hbaseapp.truthy.streaming;

import iu.pti.hbaseapp.GeneralHelpers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;

/**
 * Represents a cluster containing prototmemes as data points.
 * @author gaoxm
 */
public class ProtomemeCluster {
	/**	Protomemes that belong to this cluster */
	protected Set<ProtoMeme> protomemes;
	
	/** The centroid's tweet ID vector. This contains the sum instead of average. Averages are computed on the fly when needed. */
	protected HashMap<String, Integer> centTidVector;
	
	/** The centroid's user ID vector. This contains the sum instead of average. Averages are computed on the fly when needed. */
	protected HashMap<String, Integer> centUserVector;
	
	/** The centroid's words vector. This contains the sum instead of average. Averages are computed on the fly when needed. */
	protected HashMap<String, Integer> centWordVector;
	
	/** The centroid's tweet ID vector. This contains the sum instead of average. Averages are computed on the fly when needed. */
	protected HashMap<String, Integer> centDiffusionVector;
	
	/** Whether to print the similarity computation process to stdout */ 
	public boolean printSimComp = false;
	
	/** Last time this cluster is updated */
	public long latestUpdateTime;
	
	/** Help to distinguish two clusters with equal lastUpdateTime (only in a sequential program) */
	public long nanoUpdateTime;
	
	/** Whether this cluster is created based on an outlier */
	public boolean startFromOutlier = false;
	
	protected Gson gson;
	
	/** Construct an empty cluster. */
	public ProtomemeCluster() {
		protomemes = new HashSet<ProtoMeme>();
		centTidVector = new HashMap<String, Integer>();
		centUserVector = new HashMap<String, Integer>();
		centWordVector = new HashMap<String, Integer>();
		centDiffusionVector = new HashMap<String, Integer>();
		latestUpdateTime = Long.MIN_VALUE;
		nanoUpdateTime = System.nanoTime();
		gson = new Gson();
	}
	
	/**
	 * Construct a cluster containing one protomeme <b>initPm</b>. The centroid vectors
	 * will be initialized by cloning the vectors of <b>initPm</b>.
	 * @param initPm
	 *  The initial protomeme to contain.
	 */
	public ProtomemeCluster(ProtoMeme initPm) {
		this();
		initializeWithPm(initPm);
	}
	
	/**
	 * If this cluster is still empty, initialize it using <b>initPm</b>.
	 * @param initPm
	 *  The initial protomeme to use.
	 */
	public void initializeWithPm(ProtoMeme initPm) {
		if (protomemes.size() > 0) {
			return;
		}
		addProtoMeme(initPm);
	}
	
	/**
	 * Add a protomeme to this cluster.
	 * @param pm
	 *  The protomeme to add.
	 */
	public void addProtoMeme(ProtoMeme pm) {
		if (protomemes.contains(pm)) {
			return;
		}
		protomemes.add(pm);
		Integer count = null;
		for (String tid : pm.tweetIds) {
			count = centTidVector.get(tid);
			if (count == null) {
				centTidVector.put(tid, 1);
			} else {
				centTidVector.put(tid, count + 1);
			}
		}
		for (Map.Entry<String, Integer> e : pm.userFreqs.entrySet()) {
			count = centUserVector.get(e.getKey());
			if (count == null) {
				centUserVector.put(e.getKey(), e.getValue());
			} else {
				centUserVector.put(e.getKey(), count +  e.getValue());
			}
		}
		for (Map.Entry<String, Integer> e : pm.wordFreqs.entrySet()) {
			count = centWordVector.get(e.getKey());
			if (count == null) {
				centWordVector.put(e.getKey(), e.getValue());
			} else {
				centWordVector.put(e.getKey(), count + e.getValue());
			}
		}
		for (String uid : pm.diffusionUserIds) {
			count = centDiffusionVector.get(uid);
			if (count == null) {
				centDiffusionVector.put(uid, 1);
			} else {
				centDiffusionVector.put(uid, count + 1);
			}
		}
		//latestUpdateTime = System.currentTimeMillis();
		if (pm.latestTweetTs > latestUpdateTime) {
			latestUpdateTime = pm.latestTweetTs;
		}
		nanoUpdateTime = System.nanoTime();
	}
	
	/**
	 * Remove a protomeme from this cluster.
	 * @param pm
	 *  The protomeme to remove.
	 */
	public void removeProtoMeme(ProtoMeme pm) throws Exception {
		if (!protomemes.contains(pm)) {
			return;
		}
		protomemes.remove(pm);
		Integer count = null;
		for (String tid : pm.tweetIds) {
			count = centTidVector.get(tid);
			if (count == null || count < 1) {
				throw new Exception("Invalid tweet ID count in centroid when removing protomeme: tid " + tid + ", count " + count);
			} else if (count == 1) {
				centTidVector.remove(tid);
			} else {
				centTidVector.put(tid, count - 1);
			}
		}
		for (Map.Entry<String, Integer> e : pm.userFreqs.entrySet()) {
			count = centUserVector.get(e.getKey());
			if (count == null || count < e.getValue()) {
				throw new Exception("Invalid user ID count in centroid when removing protomeme: uid " + e.getKey() + ", count " + count
						+ " < value to move " + e.getValue());
			} else if (count == e.getValue()) {
				centUserVector.remove(e.getKey());
			} else {
				centUserVector.put(e.getKey(), count - e.getValue());
			}
		}
		for (Map.Entry<String, Integer> e : pm.wordFreqs.entrySet()) {
			count = centWordVector.get(e.getKey());
			if (count == null || count < e.getValue()) {
				throw new Exception("Invalid word count in centroid when removing protomeme: word " + e.getKey() + ", count " + 
						count + " < value to move " + e.getValue());
			} else if (count == e.getValue()) {
				centWordVector.remove(e.getKey());
			} else {
				centWordVector.put(e.getKey(), count - e.getValue());
			}
		}
		for (String uid : pm.diffusionUserIds) {
			count = centDiffusionVector.get(uid);
			if (count == null || count < 1) {
				throw new Exception("Invalid diffusion user ID count in centroid when removing protomeme: uid " + uid + ", count " + 
						count + " < 1");
			} else if (count == 1) {
				centDiffusionVector.remove(uid);
			} else {
				centDiffusionVector.put(uid, count - 1);
			}
		}
	}
	
	/**
	 * Compute the similarity between the centroid and <b>pm</b>.
	 * @param pm
	 *  The target protomeme to compute the similarity against.
	 * @return
	 *  A similarity value normalized between [0.0, 1.0].
	 * @throws Exception
	 */
	public double computeSimilarity(ProtoMeme pm) throws Exception {
		if (printSimComp)
			System.out.println("=========Similarity between centriod <" + this.latestUpdateTime + "> and <" + pm.mainMarker + ">");
		double result = computeTidSimilarity(pm);
		if (printSimComp)
			System.out.println("Tid similarity: " + result);
		if (result == 1.0) {
			return result;
		}
		double tmp = computeUidSimilarity(pm);
		if (printSimComp)
			System.out.println("Uid similarity: " + tmp);
		if (tmp > result) {
			result = tmp;
		}
		tmp = computeDiffusionSimilarity(pm);
		if (printSimComp)
			System.out.println("Diffusion similarity: " + tmp);
		if (tmp > result) {
			result = tmp;
		}
		tmp = computeContentSimilarity(pm);
		if (printSimComp)
			System.out.println("Content similarity: " + tmp);
		if (tmp > result) {
			result = tmp;
		}
		if (printSimComp)
			System.out.println("Overall similarity: " + result);
		return result;
	}
	
	/**
	 * Compute the common user similarity between the centroid and a protomeme.
	 * @param pm
	 *  The target protomeme to compute the similarity against.
	 * @return
	 * @throws Exception
	 */
	public double computeUidSimilarity(ProtoMeme pm) throws Exception {
		// centUserVector actually contains the sum instead of average, and should be divided by cluster size to
		// get the real average. But since we are computing the cosine of two vectors, we don't have to do the
		// division because it only changes the length of the vector, and does not affect the direction.
		double thisSs = GeneralHelpers.sumOfSquresDouble(centUserVector.values());
		double thatSs = GeneralHelpers.sumOfSquresDouble(pm.userFreqs.values());
		Map<String, Integer> toLoop = pm.userFreqs;
		Map<String, Integer> toMatch = centUserVector;
		if (centUserVector.size() < pm.userFreqs.size()) {
			toLoop = centUserVector;
			toMatch = pm.userFreqs;
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
		double res = sumIntersect / Math.sqrt(thisSs * thatSs);
		if (res > 1.0) {
			System.out.println("Invalid uid similarity between protomeme and cluster: " + res);
			System.out.println("Protomeme user vector: " + gson.toJson(pm.userFreqs, pm.userFreqs.getClass()));
			System.out.println("cluster user vector: " + gson.toJson(centUserVector, centUserVector.getClass()));
			throw new Exception("Error when computing uid similarity between protomeme and cluster");
		}
		return res;
	}
	
	/**
	 * Compute the common tweet ID similarity between the centroid and a protomeme.
	 * @param pm
	 *  The target protomeme to compute the similarity against.
	 * @return
	 * @throws Exception
	 */
	public double computeTidSimilarity(ProtoMeme pm) throws Exception {
		double thisSs = GeneralHelpers.sumOfSquresDouble(centTidVector.values());
		long sumIntersect = 0;
		for (String tid : pm.tweetIds) {
			Integer count = centTidVector.get(tid);
			if (count != null) {
				sumIntersect += count;
			}
		}
		double res = sumIntersect / Math.sqrt(thisSs * pm.tweetIds.size());
		if (res > 1.0) {
			System.out.println("Invalid tid similarity between protomeme and cluster: " + res);
			System.out.println("Protomeme tid vector: " + gson.toJson(pm.tweetIds, pm.tweetIds.getClass()));
			System.out.println("cluster tid vector: " + gson.toJson(centTidVector, centTidVector.getClass()));
			throw new Exception("Error when computing tid similarity between protomeme and cluster");
		}
		return res;
	}
	
	/**
	 * Compute the text content similarity between the centroid and a protomeme.
	 * @param pm
	 *  The target protomeme to compute the similarity against.
	 * @return
	 * @throws Exception
	 */
	public double computeContentSimilarity(ProtoMeme pm) throws Exception {
		// compute similarity using word frequencies.
		if (centWordVector.size() == 0 || pm.wordFreqs.size() == 0) {
			return 0;
		}
		// the word counts in the word vector can be large enough to overflow a long, so we use the double version here.
		double thisSs = GeneralHelpers.sumOfSquresDouble(centWordVector.values());
		double thatSs = GeneralHelpers.sumOfSquresDouble(pm.wordFreqs.values());
		Map<String, Integer> toLoop = pm.wordFreqs;
		Map<String, Integer> toMatch = centWordVector;
		if (centWordVector.size() < pm.wordFreqs.size()) {
			toLoop = centWordVector;
			toMatch = pm.wordFreqs;
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
		if (res > 1.0) {
			System.out.println("Invalid content similarity between protomeme and cluster: " + res);
			System.out.println("Protomeme content vector: " + gson.toJson(pm.wordFreqs, pm.wordFreqs.getClass()));
			System.out.println("cluster content vector: " + gson.toJson(centWordVector, centWordVector.getClass()));
			throw new Exception("Error when computing content similarity between protomeme and cluster");
		}
		return res;
	}

	/**
	 * Compute the diffusion network similarity between the centroid and a protomeme.
	 * @param pm
	 *  The target protomeme to compute the similarity against.
	 * @return
	 * @throws Exception
	 */
	public double computeDiffusionSimilarity(ProtoMeme pm) throws Exception {
		double thisSs = GeneralHelpers.sumOfSquresDouble(centDiffusionVector.values());
		long sumIntersect = 0;
		for (String uid : pm.diffusionUserIds) {
			Integer freq = centDiffusionVector.get(uid);
			if (freq != null) {
				sumIntersect += freq;
			}
		}
		double res = sumIntersect / Math.sqrt(thisSs * pm.diffusionUserIds.size());
		if (res > 1.0) {
			System.out.println("Invalid diffusion similarity between protomeme and cluster: " + res);
			System.out.println("Protomeme diffusion vector: " + gson.toJson(pm.diffusionUserIds, pm.diffusionUserIds.getClass()));
			System.out.println("cluster diffusion vector: " + gson.toJson(centDiffusionVector, centDiffusionVector.getClass()));
			throw new Exception("Error when computing diffusion similarity between protomeme and cluster");
		}
		return res;
	}
	
	@Override
	public boolean equals(Object that) {
		if (!(that instanceof ProtomemeCluster)) {
			return false;
		}
		ProtomemeCluster cluster = (ProtomemeCluster)that;
		return latestUpdateTime == cluster.latestUpdateTime && protomemes.equals(cluster.protomemes);
	}
}
