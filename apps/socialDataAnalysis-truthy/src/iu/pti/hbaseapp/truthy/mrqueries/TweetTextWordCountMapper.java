package iu.pti.hbaseapp.truthy.mrqueries;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import iu.pti.hbaseapp.truthy.TweetData;
import iu.pti.hbaseapp.truthy.TweetTableClient;
import iu.pti.hbaseapp.truthy.TweetSubsetProcessor.TwitterIdProcessMapper;

/**
 * Mapper class for performing wordcount to the original words 
 * of tweets (in JSON format) corresponding to a subset of tweet IDs.
 * 
 * @author taklwu
 */
public class TweetTextWordCountMapper extends TwitterIdProcessMapper<Text, LongWritable> {
	private TweetTableClient tweetGetter = null;
	private boolean needUser = false;
	private boolean needRetweeted = false;
	private HashMap<String, Long> wordCountMap = null; 
	
	@Override
	protected void map(LongWritable rowNum, Text txtTweetId, Context context) throws IOException, InterruptedException {
		try {
			String tweetId = txtTweetId.toString();
			TweetData td = tweetGetter.getTweetData(tweetId, needUser, needRetweeted);			
			// perform wordcount when tweet != nul
            if (td != null) {
                String[] words = td.text.toLowerCase().split("\\s+");
                this.updateWordCountMap(words);
                // perform wordcount if retweet text is required
                if (needRetweeted && td.retweeted_status != null) {
                    words = td.retweeted_status.text.toLowerCase().split("\\s+");
                    this.updateWordCountMap(words);
                }
            }

		} catch (Exception e) {
			e.printStackTrace();
			context.setStatus("Exception when analyzing tweet " + txtTweetId.toString());
		}
	}
	
	private void updateWordCountMap(String[] words) {
        for (String word : words) {
            // remove double quotes from word
            if (word.startsWith("\"") && word.endsWith("\"") ) {
                word = word.substring(1, word.length());            
            }
            // remove last symbol(s) as ".", "!", "?" or anything 
            // that is not as English letter or digit.                
            word = this.removeLastSymbol(word);
            // TODO we might find a better regex
            //      for filtered word, #hashtags and @username
            if (word.matches("\\w+") || word.startsWith("#") || word.startsWith("@")) {
                Long count = this.wordCountMap.get(word);
                if (count != null) {
                    this.wordCountMap.put(word, count + 1);
                } else {
                    this.wordCountMap.put(word, 1l);
                }
            }
        }
	}
	
	private String removeLastSymbol(String word) {
	    boolean lastCharIsSymbol = true;
	    while (lastCharIsSymbol) {	        
    	    char lastChar = word.charAt(word.length() - 1);
    	    if (Character.isLetterOrDigit(lastChar)) {
    	        lastCharIsSymbol = false;
    	        break;
    	    } else {
    	        word = word.substring(0, word.length() - 1);
    	    }
	    }
	    return word;
	}

    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
		try {
		    this.tweetGetter = new TweetTableClient(context);
		    this.wordCountMap = new HashMap<String, Long>();
			Configuration conf = context.getConfiguration();
			if (conf.get("additional.arguments") != null && !conf.get("additional.arguments").isEmpty()) {
				String[] args = conf.get("additional.arguments").split("\\n");
				// check if the first argument is include or exclude						
    	        if (args.length > 0)     				
    				needRetweeted = Boolean.valueOf(args[1]);
    		    }
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e.getMessage());
		}
	}
	
	@Override
	protected void cleanup(Context context) {
		try {
			if (this.tweetGetter != null) {
			    this.tweetGetter.close();
			}
			// write the cached word count to HDFS
	        if (this.wordCountMap != null) {
	            for (Map.Entry<String, Long> e : this.wordCountMap.entrySet()) {
	                //TODO if there is any out of memory issue, 
	                //     we have move this line back into map function
	                context.write(new Text(e.getKey()), new LongWritable(e.getValue()));
	            }
	        }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
