package iu.pti.hbaseapp.diglib;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.GeneralHelpers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This applications builds inverted index tables for the metadata fields of the bibliography data of books.
 * <p/>
 * Usage: java iu.pti.hbaseapp.diglib.MetadataIndexBuilder [book bib table name] [column family name] [what to index] 
 * [what to index]...
 * <br/>
 * Where [what to index] could be one of the following: 'title', 'authors', 'category', 'publishers', 'location',
 * 'additional', 'keywords'.
 * 
 * @author gaoxm
 */
public class MetadataIndexBuilder {
	
	/**
	 * Mapper class used by {@link #MetadataIndexBuilder}.
	 */
	public static class MibMapper extends TableMapper<ImmutableBytesWritable, Put> {
		protected static byte[] columnFamilyBytes = null;
		protected static HashMap<String, Boolean> whatToIndex = null;
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			System.out.println("what to index---:");
			Set<String> idxOpts = whatToIndex.keySet();
			Iterator<String> iterOpts = idxOpts.iterator();
			while (iterOpts.hasNext()) {
				System.out.println(iterOpts.next());
			}
			if (whatToIndex.get(DigLibConstants.INDEX_OPTION_TEXT) != null) {
				byte[] textIdxTableBytes = Bytes.toBytes(DigLibConstants.TEXTS_INDEX_TABLE_NAME);
				byte[] textPosVecTableBytes = Bytes.toBytes(DigLibConstants.TEXTS_POSVEC_TABLE_NAME);
				List<KeyValue> pages = result.list();
				HashMap<String, Integer> freqs = new HashMap<String, Integer>();
				System.out.println("about to build index for " + pages.size() + " pages of book No." + Bytes.toInt(rowKey.get()));
				HashMap<String, ByteArrayOutputStream> termPoses = GeneralHelpers.getTermFreqPosesByLuceneAnalyzer(Constants.getLuceneAnalyzer(),
						pages, DigLibConstants.INDEX_OPTION_TEXT, freqs, false);
				Iterator<String> iterTerms = termPoses.keySet().iterator();
				while (iterTerms.hasNext()) {
					String term = iterTerms.next();
					Put put = new Put(Bytes.toBytes(term));
					put.add(Bytes.toBytes(DigLibConstants.CF_POSITIONS), rowKey.get(), termPoses.get(term).toByteArray());
					context.write(new ImmutableBytesWritable(textPosVecTableBytes), put);
					
					Put put2 = new Put(Bytes.toBytes(term));
					put2.add(Bytes.toBytes(DigLibConstants.CF_FREQUENCIES), rowKey.get(), Bytes.toBytes(freqs.get(term)));
					context.write(new ImmutableBytesWritable(textIdxTableBytes), put2);
				}
			}
			if (whatToIndex.get(DigLibConstants.INDEX_OPTION_TITLE) != null) {
				byte[] titleBytes = result.getValue(columnFamilyBytes, Bytes.toBytes(DigLibConstants.QUALIFIER_TITLE));
				if (titleBytes != null) {
					HashMap<String, Integer> termFreqs = GeneralHelpers.getTermFreqsByLuceneAnalyzer(Constants.getLuceneAnalyzer(), 
							Bytes.toString(titleBytes),	DigLibConstants.INDEX_OPTION_TITLE);
					byte[] titleIdxTableBytes = Bytes.toBytes(DigLibConstants.TITLE_INDEX_TABLE_NAME);
					Iterator<String> iter = termFreqs.keySet().iterator();
					while (iter.hasNext()) {
						String term = iter.next();
						Put put = new Put(Bytes.toBytes(term));
						put.add(Bytes.toBytes(DigLibConstants.CF_FREQUENCIES), rowKey.get(), Bytes.toBytes(termFreqs.get(term)));
						context.write(new ImmutableBytesWritable(titleIdxTableBytes), put);
					}
				}
			}
			if (whatToIndex.get(DigLibConstants.INDEX_OPTION_AUTHORS) != null) {
				byte[] authorsBytes = result.getValue(columnFamilyBytes, Bytes.toBytes(DigLibConstants.QUALIFIER_AUTHORS));
				if (authorsBytes != null) {
					HashMap<String, Integer> termFreqs = GeneralHelpers.getTermFreqsByLuceneAnalyzer(Constants.getLuceneAnalyzer(), 
							Bytes.toString(authorsBytes), DigLibConstants.INDEX_OPTION_AUTHORS);
					byte[] authorsIdxTableBytes = Bytes.toBytes(DigLibConstants.AUTHORS_INDEX_TABLE_NAME);
					Iterator<String> iter = termFreqs.keySet().iterator();
					while (iter.hasNext()) {
						String term = iter.next();
						Put put = new Put(Bytes.toBytes(term));
						put.add(Bytes.toBytes(DigLibConstants.CF_FREQUENCIES), rowKey.get(), Bytes.toBytes(termFreqs.get(term)));
						context.write(new ImmutableBytesWritable(authorsIdxTableBytes), put);
					}
				}
			}
			if (whatToIndex.get(DigLibConstants.INDEX_OPTION_CATEGORY) != null) {
				byte[] categoryBytes = result.getValue(columnFamilyBytes, Bytes.toBytes(DigLibConstants.QUALIFIER_CATEGORY));
				if (categoryBytes != null) {
					HashMap<String, Integer> termFreqs = GeneralHelpers.getTermFreqsBySplit(Bytes.toString(categoryBytes), ",");
					byte[] categoryIdxTableBytes = Bytes.toBytes(DigLibConstants.CATEGORY_INDEX_TABLE_NAME);
					Iterator<String> iter = termFreqs.keySet().iterator();
					while (iter.hasNext()) {
						String term = iter.next();
						Put put = new Put(Bytes.toBytes(term));
						put.add(Bytes.toBytes(DigLibConstants.CF_FREQUENCIES), rowKey.get(), Bytes.toBytes(termFreqs.get(term)));
						context.write(new ImmutableBytesWritable(categoryIdxTableBytes), put);
					}
				}
			}
			if (whatToIndex.get(DigLibConstants.INDEX_OPTION_PUBLISHERS) != null) {
				byte[] publishersBytes = result.getValue(columnFamilyBytes, Bytes.toBytes(DigLibConstants.QUALIFIER_PUBLISHERS));
				if (publishersBytes != null) { 
					HashMap<String, Integer> termFreqs = GeneralHelpers.getTermFreqsBySplit(Bytes.toString(publishersBytes), ",");
					byte[] publishersIdxTableBytes = Bytes.toBytes(DigLibConstants.PUBLISHERS_INDEX_TABLE_NAME);
					Iterator<String> iter = termFreqs.keySet().iterator();
					while (iter.hasNext()) {
						String term = iter.next();
						Put put = new Put(Bytes.toBytes(term));
						put.add(Bytes.toBytes(DigLibConstants.CF_FREQUENCIES), rowKey.get(), Bytes.toBytes(termFreqs.get(term)));
						context.write(new ImmutableBytesWritable(publishersIdxTableBytes), put);
					}
				}
			}
			if (whatToIndex.get(DigLibConstants.INDEX_OPTION_LOCATION) != null) {
				byte[] locationBytes = result.getValue(columnFamilyBytes, Bytes.toBytes(DigLibConstants.QUALIFIER_LOCATION));
				if (locationBytes != null) {
					HashMap<String, Integer> termFreqs = GeneralHelpers.getTermFreqsBySplit(Bytes.toString(locationBytes), ",");
					byte[] locationIdxTableBytes = Bytes.toBytes(DigLibConstants.LOCATION_INDEX_TABLE_NAME);
					Iterator<String> iter = termFreqs.keySet().iterator();
					while (iter.hasNext()) {
						String term = iter.next();
						Put put = new Put(Bytes.toBytes(term));
						put.add(Bytes.toBytes(DigLibConstants.CF_FREQUENCIES), rowKey.get(), Bytes.toBytes(termFreqs.get(term)));
						context.write(new ImmutableBytesWritable(locationIdxTableBytes), put);
					}
				}
			}
			if (whatToIndex.get(DigLibConstants.INDEX_OPTION_ADDITIONAL) != null) {
				byte[] additionalBytes = result.getValue(columnFamilyBytes, Bytes.toBytes(DigLibConstants.QUALIFIER_ADDITIONAL));
				if (additionalBytes != null) {
					HashMap<String, Integer> termFreqs = GeneralHelpers.getTermFreqsByLuceneAnalyzer(Constants.getLuceneAnalyzer(), 
							Bytes.toString(additionalBytes), DigLibConstants.INDEX_OPTION_ADDITIONAL);
					byte[] additionalIdxTableBytes = Bytes.toBytes(DigLibConstants.ADDINFO_INDEX_TABLE_NAME);
					Iterator<String> iter = termFreqs.keySet().iterator();
					while (iter.hasNext()) {
						String term = iter.next();
						Put put = new Put(Bytes.toBytes(term));
						put.add(Bytes.toBytes(DigLibConstants.CF_FREQUENCIES), rowKey.get(), Bytes.toBytes(termFreqs.get(term)));
						context.write(new ImmutableBytesWritable(additionalIdxTableBytes), put);
					}
				}
			}
			if (whatToIndex.get(DigLibConstants.INDEX_OPTION_KEYWORDS) != null) {
				byte[] keywordsBytes = result.getValue(columnFamilyBytes, Bytes.toBytes(DigLibConstants.QUALIFIER_KEYWORDS));
				if (keywordsBytes != null) {
					HashMap<String, Integer> termFreqs = GeneralHelpers.getTermFreqsByLuceneAnalyzer(Constants.getLuceneAnalyzer(),
							Bytes.toString(keywordsBytes), DigLibConstants.INDEX_OPTION_KEYWORDS);
					byte[] keywordsIdxTableBytes = Bytes.toBytes(DigLibConstants.KEYWORDS_INDEX_TABLE_NAME);
					Iterator<String> iter = termFreqs.keySet().iterator();
					while (iter.hasNext()) {
						String term = iter.next();
						Put put = new Put(Bytes.toBytes(term));
						put.add(Bytes.toBytes(DigLibConstants.CF_FREQUENCIES), rowKey.get(), Bytes.toBytes(termFreqs.get(term)));
						context.write(new ImmutableBytesWritable(keywordsIdxTableBytes), put);
					}
				}
			}
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();
			if (columnFamilyBytes == null) {
				columnFamilyBytes = Bytes.toBytes(configuration.get("column.family.name"));
			}
			if (whatToIndex == null) {
				whatToIndex = new HashMap<String, Boolean>();
				String[] idxOptStrs = configuration.get("what.to.index").split(",");
				for (int i=0; i<idxOptStrs.length; i++) {
					whatToIndex.put(idxOptStrs[i], true);
				}
			}
		}
	}
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		String tableName = args[0];
		String columnFamily = args[1];
		conf.set("column.family.name", columnFamily);
	    StringBuffer sbOpts = new StringBuffer();
	    for (int i=2; i<args.length; i++) {
	    	sbOpts = sbOpts.append(args[i]).append(',');
	    }
	    conf.set("what.to.index", sbOpts.toString());
	    
	    Scan scan = new Scan();
	    scan.addFamily(Bytes.toBytes(columnFamily));
	    conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf,	"Building metadata index from " + tableName);
		job.setJarByClass(MibMapper.class);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, MibMapper.class, ImmutableBytesWritable.class, Writable.class, job, true);
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		job.setNumReduceTasks(0);
		
		return job;
	}
	
	/**
	 * Usage: java iu.pti.hbaseapp.diglib.MetadataIndexBuilder [book bib table name] [column family name] [what to index] 
	 * [what to index]...
	 * <br/>
	 * Where [what to index] could be one of the following: 'title', 'authors', 'category', 'publishers', 'location',
	 * 'additional', 'keywords'.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, minimum required: 3");
			System.err.println("Usage: java iu.pti.hbaseapp.diglib.MetadataIndexBuilder <book bib table name> <column family name> "
					+ "<what to index> <what to index>...");
			System.err.println("Where <what to index> could be one of the following: 'title', 'authors', 'category', 'publishers'," +
					"'location', 'additional', 'keywords'.");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
