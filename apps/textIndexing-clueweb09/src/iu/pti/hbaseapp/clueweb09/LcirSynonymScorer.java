package iu.pti.hbaseapp.clueweb09;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class LcirSynonymScorer {
	
	public static class LssMapper extends TableMapper<ImmutableBytesWritable, Put> {
		private HTable cw09TermCountTable = null;
		protected static double scoreThreshold = Cw09Constants.DEFAULT_SYNONYM_THRESHOLD;
		protected static HashMap<String, Long> termHitsMap = new HashMap<String, Long>(100000);
		protected static Queue<String> termQueue = new LinkedList<String>();
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			String termComb = Bytes.toString(rowKey.get());
			if (shouldSkip(termComb)) {
				return;
			}
			
			int forwardCount = Bytes.toInt(result.getValue(Cw09Constants.CF_FREQUENCIES_BYTES, Cw09Constants.QUAL_FORWARD_BYTES));
			int backwardCount = Bytes.toInt(result.getValue(Cw09Constants.CF_FREQUENCIES_BYTES, Cw09Constants.QUAL_BACKWARD_BYTES));
			
			if (forwardCount != 0 && backwardCount != 0) {
				backwardCount = -1 * backwardCount;
				int plusCount = forwardCount + backwardCount;
				int minCount = Math.min(forwardCount, backwardCount);
				int idx = termComb.indexOf(' ');
				String term1 = termComb;
				String term2 = "";
				if (idx >= 0) {
					term1 = termComb.substring(0, idx);
					term2 = termComb.substring(idx+1);
				}

				try {
					long term1Count = getTermCount(term1);
					long term2Count = getTermCount(term2);
					double p = (double) term1Count * (double) term2Count;
					if (p == 0.0) {
						return;
					}					
					double plusScore = plusCount / p;
					double minScore = minCount / p;

					if (minScore > scoreThreshold && p > 1) {
						Put put = new Put(rowKey.get());
						put.add(Cw09Constants.CF_SCORES_BYTES, Cw09Constants.QUAL_PLUS_BYTES, Bytes.toBytes(plusScore));
						put.add(Cw09Constants.CF_SCORES_BYTES, Cw09Constants.QUAL_MIN_BYTES, Bytes.toBytes(minScore));
						put.add(Cw09Constants.CF_SCORES_BYTES, Cw09Constants.QUAL_FORWARD_BYTES, Bytes.toBytes(forwardCount));
						put.add(Cw09Constants.CF_SCORES_BYTES, Cw09Constants.QUAL_BACKWARD_BYTES, Bytes.toBytes(backwardCount));
						put.add(Cw09Constants.CF_SCORES_BYTES, Cw09Constants.QUAL_TERM1COUNT_BYTES, Bytes.toBytes(term1Count));
						put.add(Cw09Constants.CF_SCORES_BYTES, Cw09Constants.QUAL_TERM2COUNT_BYTES, Bytes.toBytes(term2Count));
						context.write(new ImmutableBytesWritable(rowKey.get()),	put);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		/**
		 * Check if a word pair contains invalid words and should be skipped.
		 * @param wordComb
		 * @return
		 */
		protected boolean shouldSkip(String wordComb) {
			char[] chs = wordComb.toCharArray();
			for (char c : chs) {
				if (c == '.' || c == '_' || c == ',' || c == ':') {
					return true;
				}
				
				if (Character.isDigit(c)) {
					return true;
				}
			}
			
			return false;
		}
		
		/**
		 * Get the total frequency of a term in all documents.
		 * @param term
		 * @return
		 * @throws Exception
		 */
		protected long getTermCount(String term) throws Exception {
			Long count = termHitsMap.get(term);
			if (count != null) {
				return count.longValue();
			} else {
				count = new Long(0);
				Get gTermHits = new Get(Bytes.toBytes(term));
				Result termRow = cw09TermCountTable.get(gTermHits);
				if (termRow != null && !termRow.isEmpty()) {
					count = Bytes.toLong(termRow.getValue(Cw09Constants.CF_FREQUENCIES_BYTES, Cw09Constants.QUAL_COUNT_BYTES));
				} else {
					count = 1l;
				}
				if (termHitsMap.size() >= 100000) {
					String toDel = termQueue.poll();
					termHitsMap.remove(toDel);
				}
				termQueue.add(term);
				termHitsMap.put(term, count);
				
				return count.longValue();
			}
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();
			cw09TermCountTable = new HTable(configuration, Cw09Constants.CW09_TERM_COUNT_TABLE_BYTES);
			String threshold = configuration.get("score.threshold");
			if (threshold != null && threshold.length() > 0) {
				try {
					scoreThreshold = Double.valueOf(threshold);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			System.out.println("score threshold: " + scoreThreshold);
		}
		
		@Override
		protected void cleanup(Context context) {
			try {
				if (cw09TermCountTable != null) {
					cw09TermCountTable.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		if (args.length > 0) {
			conf.set("score.threshold", args[0]);
		}
	    Scan scan = new Scan();
	    scan.addFamily(Cw09Constants.CF_FREQUENCIES_BYTES);
	    conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf,	"Scoring synonyms from " + Cw09Constants.CLUEWEB09_PAIRFREQ_TABLE_NAME);
		job.setJarByClass(LssMapper.class);
		TableMapReduceUtil.initTableMapperJob(Cw09Constants.CLUEWEB09_PAIRFREQ_TABLE_NAME, scan, LssMapper.class, ImmutableBytesWritable.class, 
												Writable.class, job, true, CustomizedSplitTableInputFormat.class);
		TableMapReduceUtil.initTableReducerJob(Cw09Constants.CLUEWEB09_SYNONYM_TABLE_NAME, null, job);
		job.setNumReduceTasks(0);
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
