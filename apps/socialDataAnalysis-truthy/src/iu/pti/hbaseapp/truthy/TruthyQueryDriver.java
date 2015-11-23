package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.MapRedIndexOperator;
import iu.pti.hbaseapp.MultiFileFolderWriter;
import iu.pti.hbaseapp.truthy.mrqueries.EdgeCountMapper;
import iu.pti.hbaseapp.truthy.mrqueries.GetTweetsMapper;
import iu.pti.hbaseapp.truthy.mrqueries.MemeCooccurCountMapper;
import iu.pti.hbaseapp.truthy.mrqueries.TextCountReducer;
import iu.pti.hbaseapp.truthy.mrqueries.UserPostCountMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Application for executing Truthy queries and completing parallel analysis on the tweets in the query results.
 * <p/>
 * Usage (arguments in '{}' are optional): <br/>
 * java iu.pti.hbaseapp.truthy.TweetSearcher [command] [arguments] <br/>
 * [command] [arguments] could be one of the following combinations: <br/>
 * get-tweets-with-meme [memes] [start time] [end time] [tweet-id or tweet-content for output] [number of tweets per ID file]
 * [output directory] [<-t or -f as input>] <br/>
 * get-tweets-with-text [keyword] [start time] [end time] [tweet-id or tweet-content for output] [number of tweets per ID file] 
 * [output directory] [<-t or -f as input>] <br/>
 * get-tweets-with-userid [user id] [start time] [end time] [tweet-id or tweet-content for output] [number of tweets per ID file]
 * [output directory] [<-t or -f as input>] <br/>
 * get-retweets [tweet id] [start time] [end time] [tweet-id or tweet-content for output] [number of tweets per ID file] [output directory] [<-t or -f as input>] <br/>
 * get-tweets-with-mrop [index constraint] [start time] [end time] [tweet-id or tweet-content for output] [number of tweets per ID file]
 * [output directory] <br/>
 * timestamp-count [memes] [start time] [end time] [output directory] [-f <input as a file>] <br/>
 * user-post-count [memes] [start time] [end time] [number of tweets per ID file] [output directory] [<-t or -f as input>] <br/>
 * meme-post-count [memes] [start time] [end time] [output directory] [-f <input as a file>] <br/>
 * meme-cooccur-count [meme] [start time] [end time] [number of tweets per ID file] [output directory] [<-t or -f as input>] <br/>
 * get-retweet-edges [memes] [start time] [end time] [in or out] [number of tweets per ID file] [output directory] [<-t or -f as input>] <br/>
 * get-mention-edges [memes] [start time] [end time] [in or out] [number of tweets per ID file] [output directory] [<-t or -f as input>] <br/>
 * get-tweets-and-analyze [get-(re)tweets*] [query constraint] [start time] [end time] [number of tweets per ID file] [mapper class] [reducer class]
 * [map output key class] [map output value class] [compress or nocompress for output] [output direcotry] [<-t or -f as input>] {[additonal parameters]} 
 *  
 * @author gaoxm
 */
public class TruthyQueryDriver {
	
	/**
	 * Thread for executing a partial query using a DefaultTruthyIndexOperator.
	 * @author gaoxm
	 */
	@InterfaceAudience.Private
	public static class DefaultIndexOperatorThread extends Thread {
		protected String month = null;
		protected DefaultTruthyIndexOperator iop = null;
		protected int numTweetsFound = -1;
		protected int numFilesWritten = -1;
		protected String outputDir;
		protected int nTweetsPerFile;
		protected AtomicInteger finishCounter = null;
		protected boolean useBigInt = false;
		
		public DefaultIndexOperatorThread(DefaultTruthyIndexOperator iop, String month, String outputDir, int nTweetsPerFile, 
				AtomicInteger finishCounter) throws IllegalArgumentException {
			if (iop == null) {
				throw new IllegalArgumentException("Index operator is null.");
			}
			this.iop = iop;
			
			if (month == null || month.length() == 0) {
				throw new IllegalArgumentException("Invalid month value.");
			}
			this.month = month;			
			
			if (outputDir == null || outputDir.length() == 0) {
				throw new IllegalArgumentException("Invalid outputDir.");
			}
			this.outputDir = outputDir;
			
			if (finishCounter == null) {
				throw new IllegalArgumentException("Finish counter is null.");
			}
			this.finishCounter = finishCounter;
			
			if (nTweetsPerFile > 0) {
				this.nTweetsPerFile = nTweetsPerFile;
			} else {
				this.nTweetsPerFile = ConstantsTruthy.TWEETS_PER_MAPPER;
			}
			
			this.useBigInt = TruthyHelpers.checkIfb4June2015(month); 
		}
		
		@Override
		public void run() {
			// for general index table, we don't use buffer-and-write style
			if (!iop.rowKeyType.equals(DefaultTruthyIndexOperator.IndexRowKeyType.TIME_RANGE)) {
				try {
					Set<BytesWritable> resultTids = iop.getRelatedTweetIds();
					numTweetsFound = resultTids.size();
					int NumTweetsConverted = 0;
					if (numTweetsFound > 0) {
						MultiFileFolderWriter writer = new MultiFileFolderWriter(outputDir, month + "_tweetIds", false, nTweetsPerFile);
						if (iop.isUserSname()) {
						    writer = new MultiFileFolderWriter(outputDir, month + "_uids", false, nTweetsPerFile);
						}						
						for (BytesWritable bwTid : resultTids) {	
						    // for sname, some odd result got 0 length
						    if (bwTid.getBytes().length > 1) {
    						    if (this.useBigInt) {
    						        writer.writeln(TruthyHelpers.getTweetIDStrFromBigIntBytes(bwTid.getBytes()));
    						    } else if (iop.isUserSname()) {
    						        writer.writeln(TruthyHelpers.getUserIDStrFromBytes(bwTid.getBytes()));
    						    } else {
    						        writer.writeln(TruthyHelpers.getTweetIDStrFromBytes(bwTid.getBytes()));
    						    }	
    						    NumTweetsConverted++;
						    }
						}
						numTweetsFound = NumTweetsConverted;
						writer.close();
						numFilesWritten = writer.getNumFilesWritten();
					} else {
						numFilesWritten = 0;
					}
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(255);
				}
			}
			else { // for time range or any query with larger size of data
				try {
					MultiFileFolderWriter writerResult = iop.getRelatedTweetIdsAndWriteToDisk(outputDir, month + "_tweetIds", false, nTweetsPerFile);
                    if (iop.isUserSname()) {
                        writerResult = new MultiFileFolderWriter(outputDir, month + "_uids", false, nTweetsPerFile);
                    }
					if (writerResult != null) {
						numFilesWritten = writerResult.getNumFilesWritten();
						numTweetsFound = writerResult.getTotalWrittenLine();
					}
						
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(255);
				}
				
			}
			int threadLeft = finishCounter.decrementAndGet();
			if (threadLeft == 0) {
				synchronized (finishCounter) {
					finishCounter.notify();
				}
			}
		}
		
		public int getNumTweetsFound() {
			return numTweetsFound;
		}

		public int getNumFilesWritten() {
			return numFilesWritten;
		}
	}
	
	/**
	 * Thread for executing a partial query using an AdvancedMemeIndexOperator.
	 * @author gaoxm
	 */
	@Deprecated
	@InterfaceAudience.Private
	public static class AdvancedMemeIndexOperatorThread extends Thread {
		protected AdvancedMemeIndexOperator iop = null;
		protected AtomicInteger finishCounter = null;
		String queryType = null;
		Map<String, Long> results = null;
		
		public AdvancedMemeIndexOperatorThread(AdvancedMemeIndexOperator iop, String queryType, AtomicInteger finishCounter) 
				throws IllegalArgumentException {
			if (iop == null) {
				throw new IllegalArgumentException("Index operator is null.");
			}
			this.iop = iop;
			
			if (queryType == null || queryType.length() == 0) {
				throw new IllegalArgumentException("Invalid query type.");
			}
			this.queryType = queryType;
						
			if (finishCounter == null) {
				throw new IllegalArgumentException("Finish counter is null.");
			}
			this.finishCounter = finishCounter;
		}
		
		@Override
		public void run() {
			try {
				if (queryType.equals("timestampCount")) {
					results = iop.getCountByDate();
				} else if (queryType.equals("memePostCount")) {
					results = iop.getCountByMeme();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			int threadLeft = finishCounter.decrementAndGet();
			if (threadLeft == 0) {
				synchronized (finishCounter) {
					finishCounter.notify();
				}
			}
		}
		
		public Map<String, Long> getResults() {
			return results;
		}
	}
	
    /**
     * Thread for executing a partial query using an AdvancedIndexOperator for meme, text and might be userid.
     * @author gaoxm
     */
    @InterfaceAudience.Private
    public static class AdvancedMemeOrTextIndexOperatorThread extends Thread {
        protected AdvancedMemeOrTextIndexOperator iop = null;
        protected AtomicInteger finishCounter = null;
        String queryType = null;
        Map<String, Long> results = null;
        
        public AdvancedMemeOrTextIndexOperatorThread(AdvancedMemeOrTextIndexOperator iop, String queryType, AtomicInteger finishCounter) 
                throws IllegalArgumentException {
            if (iop == null) {
                throw new IllegalArgumentException("Index operator is null.");
            }
            this.iop = iop;
            
            if (queryType == null || queryType.length() == 0) {
                throw new IllegalArgumentException("Invalid query type.");
            }
            this.queryType = queryType;
                        
            if (finishCounter == null) {
                throw new IllegalArgumentException("Finish counter is null.");
            }
            this.finishCounter = finishCounter;
        }
        
        @Override
        public void run() {
            try {
                if (queryType.equals("textTimestampCount") || queryType.equals("timestampCount") || queryType.equals("useridTimestampCount")) {
                    results = iop.getDateCount();
                } else if (queryType.equals("textPostCount") || queryType.equals("memePostCount") || queryType.equals("useridPostCount")) {
                    results = iop.getKeyCount();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            int threadLeft = finishCounter.decrementAndGet();
            if (threadLeft == 0) {
                synchronized (finishCounter) {
                    finishCounter.notify();
                }
            }
        }
        
        public Map<String, Long> getResults() {
            return results;
        }
    }	
	
	/**
	 * Thread for executing a partial query using an MapRedIndexOperator.
	 * @author gaoxm
	 */
	@InterfaceAudience.Private
	public static class MapRedIndexOperatorThread extends Thread {
		protected MapRedIndexOperator iop = null;
		protected AtomicInteger finishCounter = null;
		protected String resultFilePrefix;
		protected String finalOutputDir;
		protected String tmpOutputDir;
		protected int numResultFiles;
		
		public MapRedIndexOperatorThread(MapRedIndexOperator iop, String finalOutputDir, String resultFilePrefix, AtomicInteger finishCounter) 
				throws IllegalArgumentException {
			if (iop == null) {
				throw new IllegalArgumentException("Invalid MapRedIndexOperator.");
			}
			this.iop = iop;
			
			if (resultFilePrefix == null || resultFilePrefix.length() == 0) {
				throw new IllegalArgumentException("Invalid result file prefix.");
			}
			this.resultFilePrefix = resultFilePrefix;
			
			if (finalOutputDir == null || finalOutputDir.length() == 0) {
				throw new IllegalArgumentException("Invalid query type.");
			}
			this.finalOutputDir = finalOutputDir;
			this.tmpOutputDir = iop.getOutputDir();
			
			if (finishCounter == null) {
				throw new IllegalArgumentException("Finish counter is null.");
			}
			this.finishCounter = finishCounter;
			numResultFiles = 0;
		}
		
		@Override
		public void run() {
			try {
				boolean mrSucc = iop.runMrJobForQuery();
				if (!mrSucc) {
					System.out.println("Error when running MapReduce job for the MapReduce index operator.");
					return;
				}
				if (!finalOutputDir.equals(tmpOutputDir)) {
					numResultFiles = MultiFileFolderWriter.moveFilesByNamePrefix(tmpOutputDir, finalOutputDir, resultFilePrefix);
					MultiFileFolderWriter.deleteIfExist(tmpOutputDir);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			int threadLeft = finishCounter.decrementAndGet();
			if (threadLeft == 0) {
				synchronized (finishCounter) {
					finishCounter.notify();
				}
			}
		}
		
		public int getNumResultFiles() {
			return numResultFiles;
		}
		
		public MapRedIndexOperator getIndexOperator() {
			return iop;
		}
	}
	
	/**
	 * Execute "get-tweets-with-meme", "get-tweets-with-text", "get-tweets-with-userid", "get-retweets", and "get-tweets-with-mrop". 
	 * If the content of tweets is needed, launch a MR job to get the tweets' content, and copy them to outputDir.
	 * @param byWhat
	 * @param rowKeyCst
	 * @param startTime
	 * @param endTime
	 * @param needContent
	 * @param nTweetPerFile
	 * @param outputDir
	 * @param fileAsInput 
	 * @return the number of tweet ID files created
	 * @throws Exception
	 */
	protected static void getTweetsQuery(String byWhat, String rowKeyCst, String startTime, String endTime, boolean needContent, int nTweetPerFile, 
			String outputDir, boolean fileAsInput) throws Exception {
		if (!cleanUpOutputDir(outputDir)) {
			throw new Exception("Failed to clean up the output directory " + outputDir + ".");
		}
		long time0 = System.currentTimeMillis();
		String queryType = null;
		String baseIndexTableName = null;
		String rowKeyType = null;
		boolean byMRiop = false;
		boolean byTimeIndex = false;
		if (byWhat.equalsIgnoreCase("meme")) {
			queryType = "getTweetsWithMeme";
			baseIndexTableName = ConstantsTruthy.MEME_INDEX_TABLE_NAME;
			rowKeyType = "string";			
			//rowKeyCst = rowKeyCst.toLowerCase();
		} else if (byWhat.equalsIgnoreCase("text")) {
			queryType = "getTweetsWithText";
			baseIndexTableName = ConstantsTruthy.TEXT_INDEX_TABLE_NAME;
			rowKeyType = "string";
			//rowKeyCst = rowKeyCst.toLowerCase();
		} else if (byWhat.equalsIgnoreCase("phrase")) {
			queryType = "getTweetsWithPhrase";
			baseIndexTableName = ConstantsTruthy.TEXT_INDEX_TABLE_NAME;
			rowKeyType = "phrase";
			//rowKeyCst = rowKeyCst.toLowerCase();
			// the input rowkey must be use double quotes
		} else if (byWhat.equalsIgnoreCase("userId")) {
			queryType = "getTweetsWithUser";
			baseIndexTableName = ConstantsTruthy.USER_TWEETS_TABLE_NAME;
			rowKeyType = "userID";
		} else if (byWhat.equalsIgnoreCase("retweetedId")) {
			queryType = "getRetweets";
			baseIndexTableName = ConstantsTruthy.RETWEET_INDEX_TABLE_NAME;
			rowKeyType = "tweetID";
		} else if (byWhat.equalsIgnoreCase("mapredOp")) {
			byMRiop = true;
			queryType = "getTweetsWithMrop";
			int idx = rowKeyCst.indexOf(':');
			String indexType = rowKeyCst.substring(0, idx).toLowerCase();
			if (indexType.equalsIgnoreCase("meme")) {
				baseIndexTableName = ConstantsTruthy.MEME_INDEX_TABLE_NAME;
			} else if (indexType.equalsIgnoreCase("text")) {
				baseIndexTableName = ConstantsTruthy.TEXT_INDEX_TABLE_NAME;
			}
			rowKeyCst = "rowkey" + rowKeyCst.substring(idx);
		} else if (byWhat.equalsIgnoreCase("time")) {
			queryType = "getTweetsWithTime";
			baseIndexTableName = ConstantsTruthy.TIME_INDEX_TABLE_NAME;
			rowKeyType = "time";
			// make rowkeys (startTime and EndTime) into long
			rowKeyCst = null;
			byTimeIndex = true;
		} else if (byWhat.equalsIgnoreCase("coordinates")) {
            queryType = "getTweetsWithCoordinates";
            baseIndexTableName = ConstantsTruthy.GEO_INDEX_TABLE_NAME;
            rowKeyType = "coordinates";
        } else if (byWhat.equalsIgnoreCase("sname")) {
            queryType = "getUserIdsWithSname";
            baseIndexTableName = ConstantsTruthy.SNAME_INDEX_TABLE_NAME;
            rowKeyType = "sname";
        } else {
			throw new IllegalArgumentException("Unsupported query field: " + byWhat);
		}

		outputDir = MultiFileFolderWriter.getUriStrForPath(outputDir);
		String hdfsQueryDir = null;
		String tweetIdDir = null;
		if (outputDir.startsWith("hdfs:")) {
			hdfsQueryDir = outputDir;			
			tweetIdDir = hdfsQueryDir + "/tweetIds";
		} else {
			if (needContent || byMRiop || byWhat.equalsIgnoreCase("phrase")) {
				hdfsQueryDir = composeHdfsDirForQuery(queryType);
				tweetIdDir = hdfsQueryDir + "/tweetIds";
			} else {
				tweetIdDir = outputDir + "/tweetIds";
			}
		}
		
        if (rowKeyType.equalsIgnoreCase("sname")) {
            tweetIdDir = outputDir + "/uids";
        }
		
        int nFiles = 0;
		if (byMRiop) {
		    nFiles = getTweetIdsByMRIop(baseIndexTableName, rowKeyCst, startTime, endTime, tweetIdDir, nTweetPerFile);
		} else if (byTimeIndex)  { // no startTime and endTime
		    nFiles = getTweetIdsByTimeIop(baseIndexTableName, rowKeyType, rowKeyCst, startTime, endTime, tweetIdDir, nTweetPerFile);
		} else {
		    nFiles = getTweetIdsByDefaultIop(baseIndexTableName, rowKeyType, rowKeyCst, startTime, endTime, tweetIdDir, nTweetPerFile, fileAsInput);
		}
		
		long time1 = System.currentTimeMillis();
		System.out.println("Time for getting tweet IDs: " + (time1 - time0)/1000.0 + " s.");
		
		if (needContent) {
		    
		    if (nFiles <= 0) {
		        System.out.println("Failed to obtain tweet-content without any related tweet Ids");
		        return;
		    }
			// need to get content of tweets and copy to local FS if necessary
			String tweetContentDir = hdfsQueryDir + "/tweetContent";
			String addArgs = "";
			if (byWhat.equalsIgnoreCase("phrase")) {
				addArgs = "true\ntrue\n" + rowKeyCst + "\ncontent";
			}
			boolean succ = TweetSubsetProcessor.runMrProcessor(tweetIdDir, tweetContentDir, 0, GetTweetsMapper.class.getName(),
					"-nored", Text.class.getName(), Writable.class.getName(), true, addArgs);
			if (!succ) {
				throw new Exception("Error happened when trying to get the content of the tweets.");
			}
			long time2 = System.currentTimeMillis();
			if (!outputDir.startsWith("hdfs:")) {
				System.out.println("Copying tweets from HDFS to Local...");
				Configuration conf = HBaseConfiguration.create();
				FileSystem fs = FileSystem.get(conf);
				fs.copyToLocalFile(true, new Path(hdfsQueryDir), new Path(outputDir));
			}
			long time3 = System.currentTimeMillis();
			System.out.println("Time for getting tweet content: " + (time2 - time1)/1000.0 + " s, copying to local: " + (time3 - time2)/1000.0 
					+ " s, total time: " + (time3 - time0)/1000.0 + " s.");
		} else if (byMRiop && !outputDir.startsWith("hdfs:")) {
			// search by MapReduce Index operator, but need to download tweet IDs to output dir on local FS
			System.out.println("Copying tweet IDs from HDFS to Local...");
			Configuration conf = HBaseConfiguration.create();
			FileSystem fs = FileSystem.get(conf);
			fs.copyToLocalFile(true, new Path(hdfsQueryDir), new Path(outputDir));
			long time2 = System.currentTimeMillis();
			System.out.println("Time for copying to local: " + (time2 - time1)/1000.0 + " s, total time: " + (time2 - time0)/1000.0 + " s.");
		} else if (byWhat.equalsIgnoreCase("phrase")) {
			// search by phrase, but only need tweet IDs
			String finalTidDir = hdfsQueryDir + "/finalTweetIds";
			String addArgs = "true\ntrue\n" + rowKeyCst + "\nid";
			boolean succ = TweetSubsetProcessor.runMrProcessor(tweetIdDir, finalTidDir, 0, GetTweetsMapper.class.getName(),
					"-nored", Text.class.getName(), Writable.class.getName(), true, addArgs);
			if (!succ) {
				throw new Exception("Error happened when trying to get the IDs of the exact tweets containing the phrase.");
			}
			long time2 = System.currentTimeMillis();
			if (!outputDir.startsWith("hdfs:")) {
				System.out.println("Copying tweets from HDFS to Local...");
				Configuration conf = HBaseConfiguration.create();
				FileSystem fs = FileSystem.get(conf);
				fs.copyToLocalFile(true, new Path(hdfsQueryDir), new Path(outputDir));
			}
			long time3 = System.currentTimeMillis();
			System.out.println("Time for getting final tweet IDs: " + (time2 - time1)/1000.0 + " s, copying to local: " + (time3 - time2)/1000.0 
					+ " s, total time: " + (time3 - time0)/1000.0 + " s.");
		}
	}

	/**
	 * Get tweet IDs using the default Truthy index operator, and return the number of tweet ID files created.
	 * @param baseIndexTableName
	 * @param rowKeyType
	 * @param rowKeys
	 * @param startTime
	 * @param endTime
	 * @param tweetIdDir
	 * @param nTweetPerFile
	 * @param fileAsInput 
	 * @return the number of tweet ID files created
	 * @throws Exception
	 */
	public static int getTweetIdsByDefaultIop(String baseIndexTableName, String rowKeyType, String rowKeys, String startTime, String endTime, String tweetIdDir,
			int nTweetPerFile, boolean fileAsInput) throws Exception {
		tweetIdDir = MultiFileFolderWriter.getUriStrForPath(tweetIdDir);
		Map<String, long[]> intervals = TruthyHelpers.splitTimeWindowToMonths(startTime, endTime);
		AtomicInteger finishCounter = new AtomicInteger(intervals.size());
		List<DefaultIndexOperatorThread> searchers = new LinkedList<DefaultIndexOperatorThread>();
		for (Map.Entry<String, long[]> e : intervals.entrySet()) {
			String month = e.getKey();
			long[] times = e.getValue();
			String indexTableName = baseIndexTableName + "-" + month;
			DefaultTruthyIndexOperator iop = new DefaultTruthyIndexOperator(indexTableName, rowKeyType, rowKeys, 
					ConstantsTruthy.COLUMN_FAMILY_TWEETS, times[0], times[1], fileAsInput);
	        if (rowKeyType.equalsIgnoreCase("sname")) {
	            // the user create time may not be the same as the current table months
	            // so we accept all found user id in that month
	            iop = new DefaultTruthyIndexOperator(indexTableName, rowKeyType, rowKeys, 
	                    ConstantsTruthy.COLUMN_FAMILY_USERS, -1, -1, fileAsInput);
	        }
			DefaultIndexOperatorThread searchThread = new DefaultIndexOperatorThread(iop, month, tweetIdDir, nTweetPerFile, finishCounter);
			searchThread.start();
			searchers.add(searchThread);
		}
		
		// To deal with the rare case of missing the notify, we only wait for 10 seconds and then start pooling.
		synchronized (finishCounter) {
			finishCounter.wait(10000);
		}
		while (finishCounter.get() > 0) {
			Thread.sleep(1);
		}

		int nFiles = 0;
		long nTweets = 0;
		for (DefaultIndexOperatorThread s : searchers) {
			nFiles += s.getNumFilesWritten();
			nTweets += s.getNumTweetsFound();
		}
		System.out.println("Number of tweet ID files: " + nFiles + "; Number of tweets found: " + nTweets);
		return nFiles;
	}

	/**
	 * Get tweet IDs using the Truthy time index operator, and return the number of tweet ID files created.
	 * @param baseIndexTableName
	 * @param rowKeyType = null
	 * @param rowKeys = null 
	 * @param startTime = YYYY-MM-DDThh:mm:ss e.g. "2008-08-08T08:08:08"
	 * @param endTime = YYYY-MM-DDThh:mm:ss e.g. "2008-08-08T08:08:08"
	 * @param tweetIdDir
	 * @param nTweetPerFile
	 * @return the number of tweet ID files created
	 * @throws Exception
	 */
	public static int getTweetIdsByTimeIop(String baseIndexTableName, String rowKeyType, String rowKeys, String startTime, String endTime, String tweetIdDir,
			int nTweetPerFile) throws Exception {
		tweetIdDir = MultiFileFolderWriter.getUriStrForPath(tweetIdDir);
		Map<String, long[]> intervals = TruthyHelpers.splitTimeWindowToMonths(startTime, endTime);
		AtomicInteger finishCounter = new AtomicInteger(intervals.size());
		List<DefaultIndexOperatorThread> searchers = new LinkedList<DefaultIndexOperatorThread>();
		for (Map.Entry<String, long[]> e : intervals.entrySet()) {
			String month = e.getKey();
			long[] times = e.getValue();
			String indexTableName = baseIndexTableName + "-" + month;
			// combine time into a string with delimiter as "-"
			rowKeys = String.valueOf(times[0])+ "-" + String.valueOf(times[1]);
			// create the default searching operator with start time and end time as -1
			DefaultTruthyIndexOperator iop = new DefaultTruthyIndexOperator(indexTableName, rowKeyType, rowKeys, 
					ConstantsTruthy.COLUMN_FAMILY_TWEETS, -1, -1, false);
			DefaultIndexOperatorThread searchThread = new DefaultIndexOperatorThread(iop, month, tweetIdDir, nTweetPerFile, finishCounter);
			searchThread.start();
			searchers.add(searchThread);
		}
		
		// To deal with the rare case of missing the notify, we only wait for 10 seconds and then start pooling.
		synchronized (finishCounter) {
			finishCounter.wait(10000);
		}
		while (finishCounter.get() > 0) {
			Thread.sleep(1);
		}

		int nFiles = 0;
		long nTweets = 0;
		for (DefaultIndexOperatorThread s : searchers) {
			nFiles += s.getNumFilesWritten();
			nTweets += s.getNumTweetsFound();
		}
		System.out.println("Number of tweet ID files: " + nFiles + "; Number of tweets found: " + nTweets);
		return nFiles;
	}	
	
	/**
	 * Get tweet IDs using the MapReduce index operator, and return the number of tweet ID files created.
	 * @param baseIndexTableName
	 * @param rowKeyCst
	 * @param startTime
	 * @param endTime
	 * @param tweetIdDir
	 * @param nTweetPerFile
	 * @return the number of tweet ID files created
	 * @throws Exception
	 */
	public static int getTweetIdsByMRIop(String baseIndexTableName, String rowKeyCst, String startTime, String endTime, String tweetIdDir,
			int nTweetPerFile) throws Exception {
		tweetIdDir = MultiFileFolderWriter.getUriStrForPath(tweetIdDir);
		if (nTweetPerFile < 0) {
			nTweetPerFile = ConstantsTruthy.TWEETS_PER_MAPPER;
		}
		Map<String, long[]> intervals = TruthyHelpers.splitTimeWindowToMonths(startTime, endTime);
		AtomicInteger finishCounter = new AtomicInteger(intervals.size());
		List<MapRedIndexOperatorThread> searchers = new LinkedList<MapRedIndexOperatorThread>();
		for (Map.Entry<String, long[]> e : intervals.entrySet()) {
			String month = e.getKey();
			long[] times = e.getValue();
			String indexTableName = baseIndexTableName + "-" + month;
			String jobTmpOutputDir = tweetIdDir + "/" + month;
			MapRedIndexOperator iop = new MapRedIndexOperator(indexTableName, TruhtyIndexMROutputFormat.class, jobTmpOutputDir);
			iop.setRowkeyConstraint(rowKeyCst);
			iop.setCfConstraint("cf:{" + ConstantsTruthy.COLUMN_FAMILY_TWEETS + "}");
			iop.setTsConstraint("ts:[" + times[0] + "," + times[1] + "]");
			iop.setReturnType("return:qual");
			iop.addMrJobProperty("month", month);
			iop.addMrJobProperty("result.type", "tweetId");
			iop.addMrJobProperty("max.output.per.file", Integer.toString(nTweetPerFile));
			MapRedIndexOperatorThread searchThread = new MapRedIndexOperatorThread(iop, tweetIdDir, month, finishCounter);
			searchThread.start();
			searchers.add(searchThread);
		}
		
		// To deal with the rare case of missing the notify, we only wait for 10 seconds and then start pooling.
		synchronized (finishCounter) {
			finishCounter.wait(10000);
		}
		while (finishCounter.get() > 0) {
			Thread.sleep(1);
		}

		int nFiles = 0;
		for (MapRedIndexOperatorThread s : searchers) {
			nFiles += s.getNumResultFiles();
		}
		System.out.println("Number of tweet ID files: " + nFiles);
		return nFiles;
	} 
	
	/**
	 * Get tweet IDs using the default Truthy index operator, and return the number of tweet ID files created.
	 * @param baseIndexTableName
	 * @param rowKeyType
	 * @param rowKeys
	 * @param startTime
	 * @param endTime
	 * @param outputDir
	 * @param fileAsInput 
	 * @return
	 * @throws Exception
	 */
	public static int getTweetIdAndTs(String baseIndexTableName, String rowKeyType, String rowKeys, String startTime, String endTime, 
			String outputDir, boolean fileAsInput) throws Exception {
		outputDir = MultiFileFolderWriter.getUriStrForPath(outputDir);
		MultiFileFolderWriter writer = new MultiFileFolderWriter(outputDir, "tweetIdAndTs", true, ConstantsTruthy.TWEETS_PER_MAPPER);
		Map<String, long[]> intervals = TruthyHelpers.splitTimeWindowToMonths(startTime, endTime);		
		for (Map.Entry<String, long[]> e : intervals.entrySet()) {
			String month = e.getKey();
			long[] times = e.getValue();
			String indexTableName = baseIndexTableName + "-" + month;
			DefaultTruthyIndexOperator iop = new DefaultTruthyIndexOperator(indexTableName, rowKeyType, rowKeys, 
					ConstantsTruthy.COLUMN_FAMILY_TWEETS, times[0], times[1], fileAsInput);
			Set<String> results = iop.getTweetIdAndTs();
			for (String idTs : results) {
				writer.writeln(idTs);
			}
		}
		writer.close();
		int nFiles = writer.getNumFilesWritten();
		System.out.println("Done. Number of files created: " + nFiles + ".");
		return nFiles;
	}
	
	/**
	 * Execute "timestamp-count" and "meme-post-count" using the advanced meme index operator.
	 * @param queryType
	 * @param memes
	 * @param startTime
	 * @param endTime
	 * @param outputDir
	 * @param fileAsInput 
	 * @throws Exception
	 */
	public static void queryByAdvMemeIdxOperator(String queryType, String memes, String startTime, String endTime, String outputDir, boolean fileAsInput)
			throws Exception {
		if (!cleanUpOutputDir(outputDir)) {
			throw new Exception("Failed to clean up the output directory " + outputDir + ".");
		}
		long time0 = System.currentTimeMillis();
		outputDir = MultiFileFolderWriter.getUriStrForPath(outputDir);
		Map<String, long[]> intervals = TruthyHelpers.splitTimeWindowToMonths(startTime, endTime);
		AtomicInteger finishCounter = new AtomicInteger(intervals.size());
		List<AdvancedMemeIndexOperatorThread> searchers = new LinkedList<AdvancedMemeIndexOperatorThread>();
		for (Map.Entry<String, long[]> e : intervals.entrySet()) {
			String month = e.getKey();
			long[] times = e.getValue();
			AdvancedMemeIndexOperator iop = new AdvancedMemeIndexOperator(month, memes,	times[0], times[1], fileAsInput);
			AdvancedMemeIndexOperatorThread searchThread = new AdvancedMemeIndexOperatorThread(iop, queryType, finishCounter);
			searchThread.start();
			searchers.add(searchThread);
		}
		
		// To deal with the rare case of missing the notify, we only wait for 10 seconds and then start pooling.
		synchronized (finishCounter) {
			finishCounter.wait(10000);
		}
		while (finishCounter.get() > 0) {
			Thread.sleep(1);
		}

		MultiFileFolderWriter writer = new MultiFileFolderWriter(outputDir, queryType, false, ConstantsTruthy.TWEETS_PER_MAPPER);
		if (queryType.equals("timestampCount")) {
			for (AdvancedMemeIndexOperatorThread s : searchers) {
				Map<String, Long> results = s.getResults();
				for (Map.Entry<String, Long> e : results.entrySet()) {
					writer.writeln(e.getKey() + "\t" + e.getValue());
				}
			}
		} else if (queryType.equals("memePostCount")) {
			Map<String, Long> combinedResults = new HashMap<String, Long>();
			for (AdvancedMemeIndexOperatorThread s : searchers) {
				Map<String, Long> results = s.getResults();
				for (Map.Entry<String, Long> e : results.entrySet()) {
					Long count = combinedResults.get(e.getKey());
					if (count != null) {
						combinedResults.put(e.getKey(), count + e.getValue());
					} else {
						combinedResults.put(e.getKey(), e.getValue());
					}
				}
			}
			for (Map.Entry<String, Long> e : combinedResults.entrySet()) {
				writer.writeln(e.getKey() + "\t" + e.getValue());
			}
		}
		writer.close();
		long time1 = System.currentTimeMillis();
		System.out.println("Time for running query: " + (time1 - time0)/1000.0 + " s.");
	}
	
    /**
     * Execute "text-timeStamp-count" and "text-post-count" using the advanced meme index operator.
     * @param queryType
     * @param memes
     * @param startTime
     * @param endTime
     * @param outputDir
     * @param fileAsInput 
     * @throws Exception
     */
    public static void queryByAdvMemeOrTextIdxOperator(String queryType, String keys, String startTime, String endTime, String outputDir, boolean fileAsInput)
            throws Exception {
        if (!cleanUpOutputDir(outputDir)) {
            throw new Exception("Failed to clean up the output directory " + outputDir + ".");
        }
        long time0 = System.currentTimeMillis();
        outputDir = MultiFileFolderWriter.getUriStrForPath(outputDir);
        Map<String, long[]> intervals = TruthyHelpers.splitTimeWindowToMonths(startTime, endTime);
        AtomicInteger finishCounter = new AtomicInteger(intervals.size());
        List<AdvancedMemeOrTextIndexOperatorThread> searchers = new LinkedList<AdvancedMemeOrTextIndexOperatorThread>();
        for (Map.Entry<String, long[]> e : intervals.entrySet()) {
            String month = e.getKey();
            long[] times = e.getValue();
            AdvancedMemeOrTextIndexOperator iop = null;
            if (queryType.contains("meme") || queryType.equals("timestampCount")) {
                // System.out.println("using meme index table = " + ConstantsTruthy.MEME_INDEX_TABLE_NAME);
                iop = new AdvancedMemeOrTextIndexOperator(ConstantsTruthy.MEME_INDEX_TABLE_NAME, month, keys, "string", times[0], times[1], fileAsInput);
            }
            else if (queryType.contains("text")) {
                // System.out.println("using text index table = " + ConstantsTruthy.TEXT_INDEX_TABLE_NAME);
                iop = new AdvancedMemeOrTextIndexOperator(ConstantsTruthy.TEXT_INDEX_TABLE_NAME, month, keys, "string", times[0], times[1], fileAsInput);
            } else if (queryType.contains("userid")) {
                // System.out.println("using user tweets index table = " + ConstantsTruthy.USER_TWEETS_TABLE_NAME);
                iop = new AdvancedMemeOrTextIndexOperator(ConstantsTruthy.USER_TWEETS_TABLE_NAME, month, keys, "userID", times[0], times[1], fileAsInput);
            } else {                                
                // System.out.println("AdvancedIndexOperator default routine, using text index table = " + ConstantsTruthy.TEXT_INDEX_TABLE_NAME);
                iop = new AdvancedMemeOrTextIndexOperator(ConstantsTruthy.TEXT_INDEX_TABLE_NAME, month, keys, "string", times[0], times[1], fileAsInput);
            }
            AdvancedMemeOrTextIndexOperatorThread searchThread = new AdvancedMemeOrTextIndexOperatorThread(iop, queryType, finishCounter);
            searchThread.start();
            searchers.add(searchThread);
        }
        
        // To deal with the rare case of missing the notify, we only wait for 10 seconds and then start pooling.
        synchronized (finishCounter) {
            finishCounter.wait(10000);
        }
        while (finishCounter.get() > 0) {
            Thread.sleep(1);
        }

        MultiFileFolderWriter writer = new MultiFileFolderWriter(outputDir, queryType, false, ConstantsTruthy.TWEETS_PER_MAPPER);
        if (queryType.equals("textTimestampCount") || queryType.equals("timestampCount") || queryType.equals("useridTimestampCount")) {
            for (AdvancedMemeOrTextIndexOperatorThread s : searchers) {
                Map<String, Long> results = s.getResults();
                for (Map.Entry<String, Long> e : results.entrySet()) {
                    writer.writeln(e.getKey() + "\t" + e.getValue());
                }
            }
        } else if (queryType.equals("textPostCount") || queryType.equals("memePostCount") || queryType.equals("useridPostCount")) {
            Map<String, Long> combinedResults = new HashMap<String, Long>();
            for (AdvancedMemeOrTextIndexOperatorThread s : searchers) {
                Map<String, Long> results = s.getResults();
                for (Map.Entry<String, Long> e : results.entrySet()) {
                    Long count = combinedResults.get(e.getKey());
                    if (count != null) {
                        combinedResults.put(e.getKey(), count + e.getValue());
                    } else {
                        combinedResults.put(e.getKey(), e.getValue());
                    }
                }
            }
            for (Map.Entry<String, Long> e : combinedResults.entrySet()) {
                writer.writeln(e.getKey() + "\t" + e.getValue());
            }
        }
        writer.close();
        long time1 = System.currentTimeMillis();
        System.out.println("Time for running query: " + (time1 - time0)/1000.0 + " s.");
    }	
	
	/**
	 * Execute "user-post-count", "user-post-count-by-text",
	 * "meme-cooccur-count", "get-retweet-edges", "get-mention-edges",  
	 * by using the combination of default index operator and
	 * corresponding MR jobs.
	 * @param queryType
	 * @param rowKeys
	 * @param startTime
	 * @param endTime
	 * @param nTweetPerFile
	 * @param outputDir
	 * @param direction
	 * @param fileAsInput 
	 * @throws Exception
	 */
	protected static void advancedQuery(String queryType, String rowKeys, String startTime, String endTime, int nTweetPerFile, 
			String outputDir, String direction, boolean fileAsInput) throws Exception {
		if (!cleanUpOutputDir(outputDir)) {
			throw new Exception("Failed to clean up the output directory " + outputDir + ".");
		}
		long time0 = System.currentTimeMillis();
		outputDir = MultiFileFolderWriter.getUriStrForPath(outputDir);
		String hdfsQueryDir = null;
		if (outputDir.startsWith("hdfs:")) {
			hdfsQueryDir = outputDir;
		} else {
			hdfsQueryDir = composeHdfsDirForQuery(queryType);
		}
		String tweetIdDir = hdfsQueryDir + "/tweetIds";
		
		rowKeys = rowKeys.toLowerCase();
		int nTweetIdFile = 0;
		
		if (queryType.equals("userPostCountByText")) {
		    nTweetIdFile = getTweetIdsByDefaultIop(ConstantsTruthy.TEXT_INDEX_TABLE_NAME, "string", rowKeys, startTime, endTime, tweetIdDir,
				nTweetPerFile, fileAsInput);
		} else {
		    nTweetIdFile = getTweetIdsByDefaultIop(ConstantsTruthy.MEME_INDEX_TABLE_NAME, "string", rowKeys, startTime, endTime, tweetIdDir,
	                nTweetPerFile, fileAsInput);
		}
		long time1 = System.currentTimeMillis();
		String mrOutputDir = hdfsQueryDir + "/mrOutput";		
		int nReducer = Math.min(nTweetIdFile / 2, 20);
		nReducer = Math.max(nReducer, 1);
		
		String mapClassName = null;
		String extraArgs = "";
		if (queryType.equals("userPostCount") || queryType.equals("userPostCountByText")) {
			mapClassName = UserPostCountMapper.class.getName();
		} else if (queryType.equals("memeCooccurCount")) {
			mapClassName = MemeCooccurCountMapper.class.getName();
			extraArgs = rowKeys;
		} else if (queryType.equals("getRetweetEdges")) {
			mapClassName = EdgeCountMapper.class.getName();
			extraArgs = "retweet\n" + direction;
		} else if (queryType.equals("getMentionEdges")) {
			mapClassName = EdgeCountMapper.class.getName();
			extraArgs = "mention\n" + direction;
		}  
		
        if (nTweetIdFile <= 0) {
            System.out.println("Failed to obtain tweet-content without any related tweet Ids");
            return;
        }
		
		System.out.println("map class name: " + mapClassName);
		boolean succ = TweetSubsetProcessor.runMrProcessor(tweetIdDir, mrOutputDir, nReducer, mapClassName, TextCountReducer.class.getName(),
				Text.class.getName(), LongWritable.class.getName(), false, extraArgs);
		if (!succ) {
			throw new Exception("Error happened when trying to run the MapReduce job.");
		}
		long time2 = System.currentTimeMillis();
		if (!outputDir.startsWith("hdfs:")) {
			System.out.println("Copying tweets from HDFS to Local...");
			Configuration conf = HBaseConfiguration.create();
			FileSystem fs = FileSystem.get(conf);
			fs.copyToLocalFile(true, new Path(hdfsQueryDir), new Path(outputDir));
			long time3 = System.currentTimeMillis();
			System.out.println("Time for getting tweet IDs: " + (time1 - time0)/1000.0 + " s, running MapReduce job: " + (time2 - time1)/1000.0 
					+ " s, copying to local: " + (time3 - time2)/1000.0 + " s, total time: " + (time3 - time0)/1000.0 + " s.");
		} else {
			System.out.println("Time for getting tweet IDs: " + (time1 - time0)/1000.0 + " s, running MapReduce job: " + (time2 - time1)/1000.0 
					+ " s, total time: " + (time2 - time0)/1000.0 + " s.");
		}
	}
	
	/**
	 * First do get-tweets-with-* or get-retweets to find tweet IDs, and then analyze the related tweets with the given map class and reduce class.
	 * @param queryCmd
	 * @param queryCst
	 * @param startTime
	 * @param endTime
	 * @param nTidPerFile
	 * @param mapClass
	 * @param reduceClass
	 * @param mapOutKeyClass
	 * @param mapOutValueClass
	 * @param compressOutput
	 * @param outputDir
	 * @param fileAsInput 
	 * @throws Exception
	 */
	public static void getTweetsAndAnalyze(String queryCmd, String queryCst, String startTime, String endTime, int nTidPerFile, String mapClass,
			String reduceClass, String mapOutKeyClass, String mapOutValueClass, boolean compressOutput, String outputDir, 
			boolean fileAsInput, boolean timestampCount, boolean postCount, String extraArgs) 
			throws Exception {
		if (!cleanUpOutputDir(outputDir)) {
			throw new Exception("Failed to clean up the output directory " + outputDir + ".");
		}
		long time0 = System.currentTimeMillis();
		outputDir = MultiFileFolderWriter.getUriStrForPath(outputDir);
		String hdfsQueryDir = null;
		if (outputDir.startsWith("hdfs:")) {
			hdfsQueryDir = outputDir;
		} else {
			hdfsQueryDir = composeHdfsDirForQuery("getTweetsAndAnalyze");
		}
		String tweetIdDir = hdfsQueryDir + "/tweetIds";
		String finalPhraseTidDir = hdfsQueryDir + "/finalPhraseIntersectionTweetIds";
		
		String baseIndexTableName = null;
		String rowKeyType = null;
		boolean byMRiop = false;
		boolean byTimeIndex = false;
		if (queryCmd.equals("get-tweets-with-meme")) {
			baseIndexTableName = ConstantsTruthy.MEME_INDEX_TABLE_NAME;
			rowKeyType = "string";
		} else if (queryCmd.equals("get-tweets-with-phrase")) {
            baseIndexTableName = ConstantsTruthy.TEXT_INDEX_TABLE_NAME;
            rowKeyType = "phrase";
            //queryCst = queryCst.substring(1, queryCst.length() - 1);
            System.out.println("Phrase queryCst = " + queryCst);
        }  else if (queryCmd.equals("get-tweets-with-text")) {
			baseIndexTableName = ConstantsTruthy.TEXT_INDEX_TABLE_NAME;
			rowKeyType = "string";
		} else if (queryCmd.equals("get-tweets-with-userid")) {
			baseIndexTableName = ConstantsTruthy.USER_TWEETS_TABLE_NAME;
			rowKeyType = "userID";	
		} else if (queryCmd.equals("get-retweets")) {
			baseIndexTableName = ConstantsTruthy.RETWEET_INDEX_TABLE_NAME;
			rowKeyType = "tweetID";
		} else if (queryCmd.equals("get-tweets-with-time")) {
            baseIndexTableName = ConstantsTruthy.TIME_INDEX_TABLE_NAME;
            rowKeyType = "time";
            byTimeIndex = true;
            queryCst = null;
        }  else if (queryCmd.equalsIgnoreCase("get-tweets-with-mrop")) {
			byMRiop = true;
			int idx = queryCst.indexOf(':');
			String indexType = queryCst.substring(0, idx);
			if (indexType.equalsIgnoreCase("meme")) {
				baseIndexTableName = ConstantsTruthy.MEME_INDEX_TABLE_NAME;
			} else if (indexType.equalsIgnoreCase("text")) {
				baseIndexTableName = ConstantsTruthy.TEXT_INDEX_TABLE_NAME;
			}
			queryCst = "rowkey" + queryCst.substring(idx);
		} else {
			throw new IllegalArgumentException("Unsupported query command: " + queryCmd);
		}
		
		int nTidFiles = 0;
		if (byMRiop) {
			TruthyQueryDriver.getTweetIdsByMRIop(baseIndexTableName, queryCst, startTime, endTime, tweetIdDir, nTidPerFile);
		} else if (byTimeIndex)  { // no startTime and endTime
            getTweetIdsByTimeIop(baseIndexTableName, rowKeyType, queryCst, startTime, endTime, tweetIdDir, nTidPerFile);
        } else {
			getTweetIdsByDefaultIop(baseIndexTableName, rowKeyType, queryCst, startTime, endTime, tweetIdDir, nTidPerFile, fileAsInput);
			// add count support information
			if (timestampCount && postCount && queryCmd.equals("get-tweets-with-meme")) {
			    queryByAdvMemeOrTextIdxOperator("timestampCount", queryCst, startTime, endTime, outputDir+File.separator+"timestampCount", fileAsInput);
			    queryByAdvMemeOrTextIdxOperator("memePostCount", queryCst, startTime, endTime, outputDir+File.separator+"memePostCount", fileAsInput);
			}
	        if (timestampCount && postCount && queryCmd.equals("get-tweets-with-text")) {
                queryByAdvMemeOrTextIdxOperator("textTimestampCount", queryCst, startTime, endTime, outputDir+File.separator+"textTimestampCount", fileAsInput);
                queryByAdvMemeOrTextIdxOperator("textPostCount", queryCst, startTime, endTime, outputDir+File.separator+"textPostCount", fileAsInput);
            }
            if (timestampCount && postCount && queryCmd.equals("get-tweets-with-userid")) {
                queryByAdvMemeOrTextIdxOperator("useridTimestampCount", queryCst, startTime, endTime, outputDir+File.separator+"useridTimestampCount", fileAsInput);
                queryByAdvMemeOrTextIdxOperator("useridPostCount", queryCst, startTime, endTime, outputDir+File.separator+"useridPostCount", fileAsInput);                             
            }			    
		}
		
		long time1 = System.currentTimeMillis();
		String mrOutputDir = hdfsQueryDir + "/mrOutput";		
		int nReducer = Math.min(nTidFiles / 2, 20);
		nReducer = Math.max(nReducer, 1);
		boolean succ = TweetSubsetProcessor.runMrProcessor(tweetIdDir, mrOutputDir, nReducer, mapClass, reduceClass,
	                mapOutKeyClass, mapOutValueClass, compressOutput, extraArgs);
		
		if (!succ) {
			throw new Exception("Error happened when trying to run the MapReduce job.");
		}
		long time2 = System.currentTimeMillis();
		if (!outputDir.startsWith("hdfs:")) {
			System.out.println("Copying tweets from HDFS to Local...");
			Configuration conf = HBaseConfiguration.create();
			FileSystem fs = FileSystem.get(conf);
			fs.copyToLocalFile(true, new Path(hdfsQueryDir), new Path(outputDir));
			long time3 = System.currentTimeMillis();
			System.out.println("Time for getting tweet IDs: " + (time1 - time0)/1000.0 + " s, running MapReduce job: " + (time2 - time1)/1000.0 
					+ " s, copying to local: " + (time3 - time2)/1000.0 + " s, total time: " + (time3 - time0)/1000.0 + " s.");
		} else {
			System.out.println("Time for getting tweet IDs: " + (time1 - time0)/1000.0 + " s, running MapReduce job: " + (time2 - time1)/1000.0 
					+ " s, total time: " + (time2 - time0)/1000.0 + " s.");
		}
	}
	
	/**
	 * Compose a HDFS working directory path for a new query.
	 * @param queryType
	 * @return
	 */
	public static String composeHdfsDirForQuery(String queryType) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
		cal.setTimeInMillis(System.currentTimeMillis());
		String curMonth = cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1);
		long msOfDay = cal.getTimeInMillis() % 86400000;
		String curDayTime = cal.get(Calendar.DAY_OF_MONTH) + "-" + msOfDay;
		String relPath = "/truthy/queries/" + curMonth + "/" + queryType + "_" + curDayTime;
		
		Configuration conf = HBaseConfiguration.create();
		return conf.get("fs.default.name") + relPath; 
	}
	
	/**
	 * Try to clean up the output directory before a query is executed.
	 * @param outputDir
	 * @return
	 * @throws Exception
	 */
	public static boolean cleanUpOutputDir(String outputDir) throws Exception {
		String outputUri = MultiFileFolderWriter.getUriStrForPath(outputDir);
		Configuration conf = HBaseConfiguration.create();
		FileSystem fs = FileSystem.get(new URI(outputUri), conf);
		Path outputPath = new Path(outputUri);
		if (!fs.exists(outputPath)) {
			return true;
		}
		if (fs.isFile(outputPath)) {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			boolean toDelete = false;
			while (true) {
				System.out.println("The output path " + outputDir + " already exists as a file, which needs to be deleted to proceed. "
						+ "Do you want to delete it? (yes or no):");
				String input = br.readLine();
				if (input.equalsIgnoreCase("yes")) {
					toDelete = true;
					break;
				} else if (input.equalsIgnoreCase("no")) {
					toDelete = false;
					break;
				}
			}
			br.close();
			if (!toDelete) {
				// The output directory won't be useful unless the current file is deleted.
				return false;
			} else {
				return fs.delete(outputPath, true);
			}
		} else {
			FileStatus[] fStats = fs.listStatus(outputPath);
			if (fStats.length == 0) {
				return true;
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			boolean toDelete = false;
			while (true) {
				System.out.println("The output directory " + outputDir + " contains some existing files, which needs to be deleted to proceed. "
						+ "Do you want to delete them? (yes or no):");
				String input = br.readLine();
				if (input.equalsIgnoreCase("yes")) {
					toDelete = true;
					break;
				} else if (input.equalsIgnoreCase("no")) {
					toDelete = false;
					break;
				}
			}
			br.close();
			if (!toDelete) {
				return false;
			} else {
				for (FileStatus fStat : fStats) {
					if (!fs.delete(fStat.getPath(), true)) {
						return false;
					}
				}
				return true;
			}			
		}
	}
	
	public static void executeQuery(String[] args) throws IllegalArgumentException, Exception {
		if (args.length == 0) {
			throw new IllegalArgumentException("No command is given.");
		}
		String command = args[0];
		boolean fileAsInput = false;
		if (command.equals("get-tweets-with-meme")) {
			if (args.length < 7) {
				throw new IllegalArgumentException("Not enough number of arguments for " + command);
			}
			String memes = args[1];
			String startTime = args[2];
			String endTime = args[3];
			boolean needContent = args[4].equals("tweet-content");
			int nTweetPerFile = Integer.valueOf(args[5]);
			String outputDir = args[6];
			if (args.length == 8) {
			    fileAsInput = args[7].equalsIgnoreCase("-f");
			}			
			getTweetsQuery("meme", memes, startTime, endTime, needContent, nTweetPerFile, outputDir, fileAsInput);
			System.out.println("Done! Check results in " + outputDir + ".");
		} else if (command.equals("get-tweets-with-text")) {
			if (args.length < 7) {
				throw new IllegalArgumentException("Not enough number of arguments for " + command);
			}
			String txtKeywords = args[1];
			String startTime = args[2];
			String endTime = args[3];
			boolean needContent = args[4].equals("tweet-content");
			int nTweetPerFile = Integer.valueOf(args[5]);
			String outputDir = args[6];
            if (args.length == 8) {
                fileAsInput = args[7].equalsIgnoreCase("-f");
            }   			
			getTweetsQuery("text", txtKeywords, startTime, endTime, needContent, nTweetPerFile, outputDir, fileAsInput);
			System.out.println("Done! Check results in " + outputDir + ".");
		} else if (command.equals("get-tweets-with-phrase")) {
			if (args.length < 7) {
				throw new IllegalArgumentException("Not enough number of arguments for " + command);
			}
			String phrase = args[1];
			String startTime = args[2];
			String endTime = args[3];
			boolean needContent = args[4].equals("tweet-content");
			int nTweetPerFile = Integer.valueOf(args[5]);
			String outputDir = args[6];
//            if (args.length == 8) {
//                fileAsInput = args[7].equalsIgnoreCase("-f");
//            }  			
			getTweetsQuery("phrase", phrase, startTime, endTime, needContent, nTweetPerFile, outputDir, false);
			System.out.println("Done! Check results in " + outputDir + ".");
		} else if (command.equals("get-tweets-with-userid")) {
			if (args.length < 7) {
				throw new IllegalArgumentException("Not enough number of arguments for " + command);
			}
			String userId = args[1];
			String startTime = args[2];
			String endTime = args[3];
			boolean needContent = args[4].equals("tweet-content");
			int nTweetPerFile = Integer.valueOf(args[5]);
			String outputDir = args[6];
            if (args.length == 8) {
                fileAsInput = args[7].equalsIgnoreCase("-f");
            }  			
			getTweetsQuery("userId", userId, startTime, endTime, needContent, nTweetPerFile, outputDir, fileAsInput);
			System.out.println("Done! Check results in " + outputDir + ".");			
		} else if (command.equals("get-tweets-with-coordinates")) {
            if (args.length < 7) {
                throw new IllegalArgumentException("Not enough number of arguments for " + command);
            }
            String geoLocation = args[1];
            String startTime = args[2];
            String endTime = args[3];
            boolean needContent = args[4].equals("tweet-content");
            int nTweetPerFile = Integer.valueOf(args[5]);
            String outputDir = args[6];
            if (args.length == 8) {
                fileAsInput = args[7].equalsIgnoreCase("-f");
            }           
            getTweetsQuery("coordinates", geoLocation, startTime, endTime, needContent, nTweetPerFile, outputDir, fileAsInput);
            System.out.println("Done! Check results in " + outputDir + ".");            
        } else if (command.equals("get-uids-with-sname")) {
            if (args.length < 8) {
                throw new IllegalArgumentException("Not enough number of arguments for " + command);
            }
            String snames = args[1];
            String startTime = args[2];
            String endTime = args[3];
            boolean needContent = args[4].equals("tweet-content");
            int nTweetPerFile = Integer.valueOf(args[5]);
            String outputDir = args[6];
            if (args.length == 8) {
                fileAsInput = args[7].equalsIgnoreCase("-f");
            }      
            System.out.println("fileAsInput = " + fileAsInput);
            getTweetsQuery("sname", snames, startTime, endTime, needContent, nTweetPerFile, outputDir, fileAsInput);
            System.out.println("Done! Check results in " + outputDir + ".");            
        } else if (command.equals("get-retweets")) {
			if (args.length < 7) {
				throw new IllegalArgumentException("Not enough number of arguments for " + command);
			}
			String tweetId = args[1];
			String startTime = args[2];
			String endTime = args[3];
			boolean needContent = args[4].equals("tweet-content");
			int nTweetPerFile = Integer.valueOf(args[5]);
			String outputDir = args[6];
            if (args.length == 8) {
                fileAsInput = args[7].equalsIgnoreCase("-f");
            }  			
			getTweetsQuery("retweetedId", tweetId, startTime, endTime, needContent, nTweetPerFile, outputDir, fileAsInput);
			System.out.println("Done! Check results in " + outputDir + ".");
		} else if (command.equals("get-tweets-with-mrop")) {
			if (args.length < 7) {
				throw new IllegalArgumentException("Not enough number of arguments for " + command);
			}
			String constraint = args[1];
			String startTime = args[2];
			String endTime = args[3];
			boolean needContent = args[4].equals("tweet-content");
			int nTweetPerFile = Integer.valueOf(args[5]);
			String outputDir = args[6];
            if (args.length == 8) {
                fileAsInput = args[7].equalsIgnoreCase("-f");
            }  			
			getTweetsQuery("mapredOp", constraint, startTime, endTime, needContent, nTweetPerFile, outputDir, fileAsInput);
			System.out.println("Done! Check results in " + outputDir + ".");
		} else if (command.equals("timestamp-count")) {
			if (args.length < 5) {
				throw new IllegalArgumentException("Not enough number of arguments for " + command);
			}
			String memes = args[1];
			String startTime = args[2];
			String endTime = args[3];
			String outputDir = args[4];
            if (args.length == 6) {
                fileAsInput = args[5].equalsIgnoreCase("-f");
            }  			
            queryByAdvMemeOrTextIdxOperator("timestampCount", memes, startTime, endTime, outputDir, fileAsInput);
			System.out.println("Done! Check results in " + outputDir + ".");
		} else if (command.equals("text-timestamp-count")) {
            if (args.length < 5) {
                throw new IllegalArgumentException("Not enough number of arguments for " + command);
            }
            String text = args[1];
            String startTime = args[2];
            String endTime = args[3];
            String outputDir = args[4];
            if (args.length == 6) {
                fileAsInput = args[5].equalsIgnoreCase("-f");
            }           
            queryByAdvMemeOrTextIdxOperator("textTimestampCount", text, startTime, endTime, outputDir, fileAsInput);
            System.out.println("Done! Check results in " + outputDir + ".");
        } else if (command.equals("userid-timestamp-count")) {
            if (args.length < 5) {
                throw new IllegalArgumentException("Not enough number of arguments for " + command);
            }
            String userid = args[1];
            String startTime = args[2];
            String endTime = args[3];
            String outputDir = args[4];
            if (args.length == 6) {
                fileAsInput = args[5].equalsIgnoreCase("-f");
            }           
            queryByAdvMemeOrTextIdxOperator("useridTimestampCount", userid, startTime, endTime, outputDir, fileAsInput);
            System.out.println("Done! Check results in " + outputDir + ".");
        } else if (command.equals("user-post-count")) {
			if (args.length < 6) {
				throw new IllegalArgumentException("Not enough number of arguments for " + command);
			}
			String memes = args[1];
			String startTime = args[2];
			String endTime = args[3];
			int nTweetPerFile = Integer.parseInt(args[4]);
			String outputDir = args[5];
            if (args.length == 7) {
                fileAsInput = args[6].equalsIgnoreCase("-f");
            }  			
			advancedQuery("userPostCount", memes, startTime, endTime, nTweetPerFile, outputDir, "", fileAsInput);
			System.out.println("Done! Check results in " + outputDir + ".");
		} else if (command.equals("user-post-count-by-text")) {
            if (args.length < 6) {
                throw new IllegalArgumentException("Not enough number of arguments for " + command);
            }
            String text = args[1];
            String startTime = args[2];
            String endTime = args[3];
            int nTweetPerFile = Integer.parseInt(args[4]);
            String outputDir = args[5];
            if (args.length == 7) {
                fileAsInput = args[6].equalsIgnoreCase("-f");
            }           
            advancedQuery("userPostCountByText", text, startTime, endTime, nTweetPerFile, outputDir, "", fileAsInput);
            System.out.println("Done! Check results in " + outputDir + ".");
        } else if (command.equals("meme-post-count")) {
			String memes = args[1];
			String startTime = args[2];
			String endTime = args[3];
			String outputDir = args[4];
            if (args.length == 6) {
                fileAsInput = args[5].equalsIgnoreCase("-f");
            }  			
            queryByAdvMemeOrTextIdxOperator("memePostCount", memes, startTime, endTime, outputDir, fileAsInput);
			System.out.println("Done! Check results in " + outputDir + ".");
		} else if (command.equals("text-post-count")) {
            String text = args[1];
            String startTime = args[2];
            String endTime = args[3];
            String outputDir = args[4];
            if (args.length == 6) {
                fileAsInput = args[5].equalsIgnoreCase("-f");
            }           
            queryByAdvMemeOrTextIdxOperator("textPostCount", text, startTime, endTime, outputDir, fileAsInput);
            System.out.println("Done! Check results in " + outputDir + ".");
        } else if (command.equals("userid-post-count")) {
            String userid = args[1];
            String startTime = args[2];
            String endTime = args[3];
            String outputDir = args[4];
            if (args.length == 6) {
                fileAsInput = args[5].equalsIgnoreCase("-f");
            }           
            queryByAdvMemeOrTextIdxOperator("useridPostCount", userid, startTime, endTime, outputDir, fileAsInput);
            System.out.println("Done! Check results in " + outputDir + ".");
        } else if (command.equals("meme-cooccur-count")) {
			if (args.length < 6) {
				throw new IllegalArgumentException("Not enough number of arguments for " + command);
			}
			String memes = args[1];
			String startTime = args[2];
			String endTime = args[3];
			int nTweetPerFile = Integer.parseInt(args[4]);
			String outputDir = args[5];
            if (args.length == 7) {
                fileAsInput = args[6].equalsIgnoreCase("-f");
            }  			
			advancedQuery("memeCooccurCount", memes, startTime, endTime, nTweetPerFile, outputDir, "", fileAsInput);
			System.out.println("Done! Check results in " + outputDir + ".");
		} else if (command.equals("get-retweet-edges")) {
			if (args.length < 7) {
				throw new IllegalArgumentException("Not enough number of arguments for " + command);
			}
			String memes = args[1];
			String startTime = args[2];
			String endTime = args[3];
			String direction = args[4];
			int nTweetPerFile = Integer.parseInt(args[5]);
			String outputDir = args[6];	
            if (args.length == 8) {
                fileAsInput = args[7].equalsIgnoreCase("-f");
            }  			
			advancedQuery("getRetweetEdges", memes, startTime, endTime, nTweetPerFile, outputDir, direction, fileAsInput);
			System.out.println("Done! Check results in " + outputDir + ".");
		} else if (command.equals("get-mention-edges")) {
			if (args.length < 7) {
				throw new IllegalArgumentException("Not enough number of arguments for " + command);
			}
			String memes = args[1];
			String startTime = args[2];
			String endTime = args[3];
			String direction = args[4];
			int nTweetPerFile = Integer.parseInt(args[5]);
			String outputDir = args[6];		
            if (args.length == 8) {
                fileAsInput = args[7].equalsIgnoreCase("-f");
            }  
			advancedQuery("getMentionEdges", memes, startTime, endTime, nTweetPerFile, outputDir, direction, fileAsInput);
			System.out.println("Done! Check results in " + outputDir + ".");
		} else if (command.equals("get-tweetIdTs")) {
			if (args.length < 7) {
				throw new IllegalArgumentException("Not enough number of arguments for " + command);
			}
			String indexTableType = args[1];
			String rowKeyType = args[2];
			String rowKeys = args[3];
			String startTime = args[4];
			String endTime = args[5];
			String outputDir = args[6];			
			getTweetIdAndTs(indexTableType, rowKeyType, rowKeys, startTime, endTime, outputDir, fileAsInput);
			System.out.println("Done! Check results in " + outputDir + ".");
		} else if (command.equals("get-tweets-and-analyze")) {
			if (args.length < 13) {
				throw new IllegalArgumentException("Not enough number of arguments for " + command);
			}
			String queryCommand = args[1];
			String queriedValues = args[2];
			String startTime = args[3];
			String endTime = args[4];
			int nTidPerFile = Integer.parseInt(args[5]);
			String mapClass = args[6];
			String reduceClass = args[7];
			String mapOutputKeyClass = args[8];
			String mapOutputValueClass = args[9];
			boolean compressOutput = args[10].equalsIgnoreCase("compress");
			String outputDir = args[11];
			fileAsInput = args[12].equalsIgnoreCase("-f");
			boolean timestampCount = true;
			boolean postCount = true;
			
			StringBuilder sb = new StringBuilder();
			int i = 13;
			while (i < args.length) {
				sb.append(args[i++]).append('\n');
			}
			if (sb.length() > 0) {
				sb.deleteCharAt(sb.length() - 1);
			}
			String extraArgs = sb.toString();
			System.out.println("extraArgs = " + extraArgs);
			getTweetsAndAnalyze(queryCommand, queriedValues, startTime, endTime, nTidPerFile, mapClass, reduceClass, mapOutputKeyClass,
					mapOutputValueClass, compressOutput, outputDir, fileAsInput, timestampCount, postCount, extraArgs);
			System.out.println("Done! Check results in " + outputDir + ".");
		} else if (command.equals("get-tweets-with-time")) {
			if (args.length < 5) {
				throw new IllegalArgumentException("Not enough number of arguments for " + command);
			}
			String startTime = args[1];
			String endTime = args[2];
			boolean needContent = args[3].equals("tweet-content");
			int nTweetPerFile = Integer.valueOf(args[4]);
			String outputDir = args[5];
			getTweetsQuery("time", null, startTime, endTime, needContent, nTweetPerFile, outputDir, fileAsInput);
			System.out.println("Done! Check results in " + outputDir + ".");
		} 
		else {
			throw new IllegalArgumentException("Unsupported command: " + command);			
		}
	}
	
	public static void usage(){
		System.out.println("Usage: java iu.pti.hbaseapp.truthy.TweetSearcher <command> <arguments>");
		System.out.println("	<command> <arguments> could be one of the following combinations:");
		System.out.println("	get-tweets-with-meme <memes> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]");
		System.out.println("	get-tweets-with-text <keywords> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]");		
		System.out.println("	get-tweets-with-time <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]");
		System.out.println("	get-tweets-with-userid <userid> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]");
		System.out.println("    get-tweets-with-coordinates <coordinates> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]");
		System.out.println("    get-tweets-with-phrase <phrase in double quotes> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>]");
		System.out.println("	get-retweets <retweeted tweet id> <start time> <end time> <tweet-id or tweet-content> <output directory> [<number of tweets per file>] [<-t or -f as input>]");
		System.out.println("	get-tweets-with-mrop <index constraint> <start time> <end time> <tweet-id or tweet-content for output>"
				+ " <number of tweets per ID file> <output directory>");
		System.out.println("	timestamp-count <memes> <start time> <end time> <output directory> [<-t or -f as input>]");
		System.out.println("    text-timestamp-count <text> <start time> <end time> <output directory> [<-t or -f as input>]");
		System.out.println("    userid-timestamp-count <userid> <start time> <end time> <output directory> [<-t or -f as input>]");
		System.out.println("	user-post-count <memes> <start time> <end time> <number of tweets per ID file> <output directory> [<number of tweets per file>] [<-t or -f as input>]");
		System.out.println("    user-post-count-by-text <text> <start time> <end time> <number of tweets per ID file> <output directory> [<number of tweets per file>] [<-t or -f as input>]");
		System.out.println("	meme-post-count <memes> <start time> <end time> <output directory> [<-t or -f as input>]");
        System.out.println("    text-post-count <text> <start time> <end time> <output directory> [<-t or -f as input>]");		
		System.out.println("	meme-cooccur-count <meme> <start time> <end time> <number of tweets per ID file> <output directory> [<number of tweets per fiile>] [<-t or -f as input>]");
		System.out.println("	get-retweet-edges <memes> <start time> <end time> <in or out> <number of tweets per ID file> <output directory> [<number of tweets per file>] [<-t or -f as input>]");
		System.out.println("	get-mention-edges <memes> <start time> <end time> <in or out> <number of tweets per ID file> <output directory> [<number of tweets per file>] [<-t or -f as input>]");
		System.out.println("	get-tweets-and-analyze <get-(re)tweets* command> <queried value> <start time> <end time> <map class name> <reduce class name> <compress or nocompress for output> <output directory> <-t or -f as input> [<additional arguments>]");
	}

	public static void main(String[] args) {
		try {
		    System.out.println("Job1 start TimeStamp = " + System.currentTimeMillis());
			executeQuery(args);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			System.out.println("Error: wrong arguments!");
			usage();
			System.exit(1);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}