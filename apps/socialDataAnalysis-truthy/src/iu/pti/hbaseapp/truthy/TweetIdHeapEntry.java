package iu.pti.hbaseapp.truthy;

import java.io.BufferedReader;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * HeapEntry class used for getting non-duplicated tweet IDs from the index records of multiple memes/keywords.
 * @author gaoxm
 */
public class TweetIdHeapEntry implements Comparable<TweetIdHeapEntry> {
	public String tweetIdStr = null;
	public byte[] tweetIdBytes = null;
	boolean useBigInt = false;
	BufferedReader brTids = null;
	
	public TweetIdHeapEntry(String tweetIdStr, BufferedReader brTids) throws IllegalArgumentException {
		if (tweetIdStr == null) {
			throw new IllegalArgumentException("Twitter ID is null!");
		}
		this.tweetIdStr = tweetIdStr;
		this.tweetIdBytes = TruthyHelpers.getTweetIdBytes(tweetIdStr);
		this.brTids = brTids;
	}
	
    public TweetIdHeapEntry(String tweetIdStr, BufferedReader brTids, boolean useBigInt) throws IllegalArgumentException {
        if (tweetIdStr == null) {
            throw new IllegalArgumentException("Twitter ID is null!");
        }
        this.useBigInt = useBigInt;
        this.tweetIdStr = tweetIdStr;
        if (useBigInt) {
            this.tweetIdBytes = TruthyHelpers.getTweetIdBigIntBytes(tweetIdStr);
        } else {
            this.tweetIdBytes = TruthyHelpers.getTweetIdBytes(tweetIdStr);
        }
        this.brTids = brTids;
    }	
	
	public int compareTo(TweetIdHeapEntry that) {
		return Bytes.BYTES_COMPARATOR.compare(this.tweetIdBytes, that.tweetIdBytes);
	}
	
	public boolean moveToNextId() throws Exception {
		if (brTids == null) {
			return false;
		}		
		tweetIdStr = brTids.readLine();
		if (tweetIdStr == null) {
			brTids.close();
			brTids = null;
			return false;
		} else {
	        if (useBigInt) {
	            this.tweetIdBytes = TruthyHelpers.getTweetIdBigIntBytes(tweetIdStr);
	        } else {
	            this.tweetIdBytes = TruthyHelpers.getTweetIdBytes(tweetIdStr);
	        }
			return true;
		}
	}
}
