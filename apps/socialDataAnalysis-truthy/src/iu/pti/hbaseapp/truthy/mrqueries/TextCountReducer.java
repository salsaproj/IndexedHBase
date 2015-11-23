package iu.pti.hbaseapp.truthy.mrqueries;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A simple "word count" reducer that is generally useful for multiple queries.
 * 
 * @author gaoxm
 */
public class TextCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private static final Log LOG = LogFactory.getLog(TextCountReducer.class);
    private long computationTime;
    
	@Override
	protected void reduce(Text key, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException {
	    long startTime = System.currentTimeMillis();
		long totalCount = 0;
		for (LongWritable count : counts) {
			totalCount += count.get();
		}
		context.write(key, new LongWritable(totalCount));
		this.computationTime += System.currentTimeMillis() - startTime;
	}	
	
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        LOG.info("Reducer setup start TimeStamp = " + startTime);
        long endTime = System.currentTimeMillis();
        LOG.info("Reducer setup end TimeStamp = " + endTime);
        LOG.info("Reducer setup time takes = " + (endTime - startTime) + " ms");
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        LOG.info("Reducer Computation Time = " + this.computationTime + " ms");
        LOG.info("Reducer finish TimeStamp = " + System.currentTimeMillis());
    }
}
