package iu.pti.hbaseapp.truthy.mrqueries;

import java.io.IOException;
import iu.pti.hbaseapp.truthy.TweetData;
import iu.pti.hbaseapp.truthy.TweetTableClient;
import iu.pti.hbaseapp.truthy.TweetSubsetProcessor.TwitterIdProcessMapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import com.google.gson.Gson;

public class GetTweetsWithPhraseMapper extends TwitterIdProcessMapper<Text, NullWritable> {
    TweetTableClient tweetGetter = null;
    Gson gson = null;
    private String phrase; 
    private Configuration conf;
    
    @Override
    protected void map(LongWritable rowNum, Text txtTweetId, Context context) throws IOException, InterruptedException {
        String tweetId = txtTweetId.toString();
        try {
            TweetData tweet = tweetGetter.getTweetData(tweetId, true, true);
            String tweetContent = tweet.text;   
            System.out.println("Checking if phrase '" + phrase + "' in content of Tweet " + txtTweetId.toString());
            if (tweetContent.toLowerCase().contains(phrase.toLowerCase())) {                    
                String tweetJson = gson.toJson(tweet, TweetData.class);
                context.write(new Text(tweetJson), NullWritable.get());
            }
        } catch (Exception e) {
            e.printStackTrace();
            context.setStatus("Exception when analyzing tweet " + txtTweetId.toString());
        }
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            tweetGetter = new TweetTableClient(context);
            gson = new Gson();
            this.conf = context.getConfiguration();
            this.phrase = this.getPhraseFromAdditionalArg(this.conf.get("additional.arguments"));
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e.getMessage());
        }
    }
    
    @Override
    protected void cleanup(Context context) {
        try {
            if (tweetGetter != null) {
                tweetGetter.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * each arg is split by the new line
     * @param additionalArgsInDoubleQuote
     * @return
     * @throws IOException 
     */
    private String getPhraseFromAdditionalArg(String additionalArgs) throws IOException{
        String phrases = "";
        String[] args = additionalArgs.split("\n");
        phrases = args[0].replaceAll("^[,\\s]+", "");   
        return phrases;
    }

}
