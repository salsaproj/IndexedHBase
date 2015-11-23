package iu.pti.hbaseapp.truthy.mrqueries;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import iu.pti.hbaseapp.truthy.ConstantsTruthy;
import iu.pti.hbaseapp.truthy.TruthyHelpers;
import iu.pti.hbaseapp.truthy.TweetSubsetProcessor.TwitterIdProcessMapper;

/**
 * Mapper class for executing the "meme-cooccur-count" query.
 * 
 * @author gaoxm
 */
public class MemeCooccurCountMapper extends
        TwitterIdProcessMapper<Text, LongWritable> {
    String givenMeme = null;
    HTable tweetTable = null;
    HashMap<String, Long> memeCountMap = null;
    boolean useBigInt = false;
    private static final Log LOG = LogFactory
            .getLog(MemeCooccurCountMapper.class);
    private long HBaseIOTime = 0L;
    private long UDFStartTime = 0L;

    @Override
    protected void map(LongWritable rowNum, Text txtTweetId, Context context)
            throws IOException, InterruptedException {
        byte[] tweetId = null;
        if (this.useBigInt) {
            tweetId = TruthyHelpers
                    .getTweetIdBigIntBytes(txtTweetId.toString());
        } else {
            tweetId = TruthyHelpers.getTweetIdBytes(txtTweetId.toString());
        }
        long startTime = System.currentTimeMillis();
        Get get = new Get(tweetId);
        get.addColumn(ConstantsTruthy.CF_DETAIL_BYTES,
                ConstantsTruthy.QUAL_ENTITIES_BYTES);
        Result r = tweetTable.get(get);
        this.HBaseIOTime += System.currentTimeMillis() - startTime;
        if (r == null) {
            context.setStatus("Can't find tweet ID " + txtTweetId.toString());
            return;
        }
        String entities = Bytes.toString(r.getValue(
                ConstantsTruthy.CF_DETAIL_BYTES,
                ConstantsTruthy.QUAL_ENTITIES_BYTES));
        JsonObject joEntities = ConstantsTruthy.jsonParser.parse(entities)
                .getAsJsonObject();
        JsonArray jaHashTags = joEntities.get("hashtags").getAsJsonArray();
        if (!jaHashTags.isJsonNull() && jaHashTags.size() > 0) {
            Iterator<JsonElement> iht = jaHashTags.iterator();
            while (iht.hasNext()) {
                String meme = "#"
                        + iht.next().getAsJsonObject().get("text")
                                .getAsString();
                Long count = memeCountMap.get(meme);
                if (count != null) {
                    memeCountMap.put(meme, count + 1);
                } else {
                    memeCountMap.put(meme, 1l);
                }
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        UDFStartTime = System.currentTimeMillis();
        LOG.warn("UDF Start Time = " + UDFStartTime);
        LOG.warn("Mapper setup start TimeStamp = " + UDFStartTime);
        Configuration conf = context.getConfiguration();
        // input file name is like "2012-03_memeTweets_0.txt"
        String inputFileName = ((FileSplit) context.getInputSplit()).getPath()
                .getName();
        int idx = inputFileName.indexOf('_');
        String month = inputFileName.substring(0, idx);
        tweetTable = new HTable(conf, ConstantsTruthy.TWEET_TABLE_NAME + "-"
                + month);
        memeCountMap = new HashMap<String, Long>();
        String addArgs = conf.get("additional.arguments");
        if (addArgs != null) {
            String[] args = addArgs.split("\\n");
            givenMeme = args[0];
        }
        this.useBigInt = TruthyHelpers.checkIfb4June2015(month);
        long endTime = System.currentTimeMillis();
        LOG.warn("Mapper setup endTimeStamp = " + endTime);
        LOG.warn("Mapper setup time takes = " + (endTime - UDFStartTime)
                + " ms");
    }

    @Override
    protected void cleanup(Context context) {
        long startTime = System.currentTimeMillis();
        try {
            if (tweetTable != null) {
                tweetTable.close();
            }
            if (memeCountMap != null) {
                context.setStatus("memeCountMap size: " + memeCountMap.size());
                for (Map.Entry<String, Long> e : memeCountMap.entrySet()) {
                    context.write(new Text(e.getKey()),
                            new LongWritable(e.getValue()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long UDFEndTime = System.currentTimeMillis();
        LOG.warn("UDF End Time = " + UDFEndTime);
        LOG.warn("HBaseIO Time = " + this.HBaseIOTime + " ms");
        long UDFOverallTime = (UDFEndTime - this.UDFStartTime);
        LOG.warn("UDF Computation Time = "
                + (UDFOverallTime - this.HBaseIOTime) + " ms");
        LOG.warn("UDF finish Time = " + UDFOverallTime + " ms");
        long endTime = System.currentTimeMillis();
        LOG.warn("Mapper cleanup End TimeStamp = " + endTime);
        LOG.warn("Mapper cleanup time takes = " + (endTime - startTime) + " ms");
    }
}
