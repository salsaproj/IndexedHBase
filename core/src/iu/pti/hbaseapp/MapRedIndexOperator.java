package iu.pti.hbaseapp;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * A MapReduce index table operator for carrying out queries that require scanning a large portion of the row
 * keys of the index table with a range, prefix, or regular expression row key constraint.
 * 
 * @author gaoxm
 */
public class MapRedIndexOperator extends BasicIndexOperator {
	
	public static class MrioMapper extends TableMapper<BytesWritable, NullWritable> {
		BasicIndexOperator biop;
		IndexDataConstraint<byte[]> rowkeyConstraint;
		Set<BytesWritable> outputSet;
		BytesWritable lastRowKey;
		long mapCalledTime;
		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			try {
				mapCalledTime++;
				if (mapCalledTime % 10000 == 0) {
					context.setStatus("map called for " + mapCalledTime + " times."); 
				}
				
				byte[] rowkeyBytes = rowKey.get();
				if (rowkeyConstraint != null && !rowkeyConstraint.satisfiedBy(rowkeyBytes)) {
					return;
				}
				
				if (biop.getReturnType() == IdxOpReturnType.ROWKEY) {
					BytesWritable thisRowKey = new BytesWritable(rowkeyBytes);
					if (lastRowKey == null || !thisRowKey.equals(lastRowKey)) {
						outputSet.add(thisRowKey);
						lastRowKey = thisRowKey;
					}
				} else {
					for (KeyValue kv : result.list()) {
						if (!biop.checkCellByConstraints(kv)) {
							continue;
						}
						switch (biop.getReturnType()) {
						case QUALIFIER:
							byte[] qualBytes = kv.getQualifier();
							if (qualBytes.length > 8) {
								throw new Exception("Qualifier is longer than 8 bytes.");
							}
							outputSet.add(new BytesWritable(qualBytes));
							break;
						case CELLVAL:
							outputSet.add(new BytesWritable(kv.getValue()));
							break;
						case TIMESTAMP:
							outputSet.add(new BytesWritable(Bytes.toBytes(kv.getTimestamp())));
						default:
						}
					}
				}
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			try {
				Configuration conf = context.getConfiguration();
				String[] constraints = conf.get("constraints").split("\\n");
				biop = new BasicIndexOperator();
				biop.setConstraints(constraints);
				rowkeyConstraint = biop.getRowKeyConstraint();
				outputSet = new HashSet<BytesWritable>();
				lastRowKey = null;
				mapCalledTime = 0;
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (BytesWritable bw : outputSet) {
				context.write(bw, NullWritable.get());
			}
		}
	}
	
	public static class MrioReducer extends Reducer<BytesWritable, NullWritable, BytesWritable, NullWritable> {
		@Override
		public void reduce(BytesWritable output, Iterable<NullWritable> nulls, Context context) throws IOException, InterruptedException {
			context.write(output, NullWritable.get());
		}
	}
	
	protected Class<? extends OutputFormat<?, ?>> outputFormatClass;
	protected String outputDir;
	protected Properties additionalProps;
	protected String rowkeyCst;
	protected String cfCst;
	protected String qualCst;
	protected String tsCst;
	protected String cvCst;
	protected String returnCst;

	public MapRedIndexOperator(String indexTableName, Class<? extends OutputFormat<?, ?>> outputFormatClass, String outputDir) 
			throws IllegalArgumentException {
		if (indexTableName == null || indexTableName.length() == 0) {
			throw new IllegalArgumentException("Invalid index table name: " + indexTableName);
		}
		this.indexTableName = Bytes.toBytes(indexTableName);
		
		if (outputFormatClass == null) {
			throw new IllegalArgumentException("The outputFormatClass parameter is null.");
		}
		this.outputFormatClass = outputFormatClass;
		
		if (outputDir == null || outputDir.length() == 0) {
			throw new IllegalArgumentException("Invalid output directory: " + outputDir);
		}
		this.outputDir = outputDir;
		additionalProps = new Properties();
		rowkeyCst = null;
		cfCst = null;
		qualCst = null;
		tsCst = null;
		cvCst = null;
		returnCst = null;
	}
	
	/**
	 * Get the output format class for handling the output data from the reducer.
	 * @return
	 */
	public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() {
		return outputFormatClass;
	}

	/**
	 * Set the output format class for handling the output data from the reducer. Useful for dealing with special
	 * output data types such as twiiter IDs.
	 * @param outputFormatClass
	 */
	public void setOutputFormatClass(Class<? extends OutputFormat<?, ?>> outputFormatClass) {
		this.outputFormatClass = outputFormatClass;
	}
	
	public Properties getAdditionalProps() {
		return additionalProps;
	}

	public void setAdditionalProps(Properties additionalProps) {
		if (additionalProps != null) { 
			this.additionalProps = additionalProps;
		}
	}
	
	/**
	 * Add an additional property value for running the MapReduce job, which will be added to the task context
	 * during job execution time. The user specified output format class can access these properties through a 
	 * reference to the task context.
	 * @param key
	 * @param value
	 */
	public void addMrJobProperty(String key, String value) {
		additionalProps.put(key, value);
	}
	
	public String getOutputDir() {
		return outputDir;
	}
	
	/**
	 * MapRedIndexOperator should only deal with range, prefix, and regex row key constraints. Value set constraint
	 * should be handled with BasicIndexOperator.
	 */
	@Override
	public void setRowkeyConstraint(String constraint) throws Exception {
		super.setRowkeyConstraint(constraint);
		if (rowkeyConstraint instanceof ValueSetIdxConstraint) {
			throw new Exception("Value set rowkey constraint is not supported by MapRedTruthyIndexOperator.");
		}
		this.rowkeyCst = constraint;
	}
	
	@Override
	public void setCfConstraint(String constraint) throws Exception {
		super.setCfConstraint(constraint);
		this.cfCst = constraint;		
	}
	
	@Override
	public void setQualConstraint(String constraint) throws Exception {
		super.setQualConstraint(constraint);
		this.qualCst = constraint;
	}
	
	@Override
	public void setTsConstraint(String constraint) throws Exception {
		super.setTsConstraint(constraint);
		this.tsCst = constraint;
	}
	
	@Override
	public void setCellValConstraint(String constraint) throws Exception {
		super.setCellValConstraint(constraint);
		this.cvCst = constraint;
	}
	
	@Override
	public void setReturnType(String constraint) throws Exception {
		super.setReturnType(constraint);
		this.returnCst = constraint;
	}
	
	/**
	 * Concatenate all constraints into one overall string, using '\n' as the separator.
	 * 
	 * @return
	 */
	protected String getAllConstraints() {
		StringBuilder sb = new StringBuilder();
		if (rowkeyCst != null) {
			sb.append(rowkeyCst).append('\n');
		}
		if (cfCst != null) {
			sb.append(cfCst).append('\n');
		}
		if (qualCst != null) {
			sb.append(qualCst).append('\n');
		}
		if (tsCst != null) {
			sb.append(tsCst).append('\n');
		}
		if (cvCst != null) {
			sb.append(cvCst).append('\n');
		}
		if (returnCst != null) {
			sb.append(returnCst);
		}
		return sb.toString();
	}
	
	/**
	 * Predict how many mappers will be run by checking the number of regions in the index table. Implementation is learned 
	 * from TableInputFormatBase.java.
	 * @param scan
	 * @param conf
	 * @return
	 * @throws Exception
	 */
	protected int predictNumMappers(Scan scan, Configuration conf) throws Exception {
		HTable table = new HTable(conf, indexTableName);
		Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
		table.close();

		if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
			return 1;
		}

		int nMappers = 0;
		byte[] startRow = scan.getStartRow();
		byte[] stopRow = scan.getStopRow();
		for (int i = 0; i < keys.getFirst().length; i++) {
			// determine if the given start an stop key fall into the region
			if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes.compareTo(startRow, keys.getSecond()[i]) < 0)
					&& (stopRow.length == 0 || Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
				nMappers++;
			}
		}
		return nMappers;
	}
	
	/**
	 * Run MapReduce operator on the index table name and output required return information to the output directory.
	 * @return
	 * @throws Exception
	 */
	public boolean runMrJobForQuery() throws Exception {
		if (returnType == IdxOpReturnType.UNKNOWN) {
			throw new Exception("Return type is not set.");
		}
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		conf.set("constraints", getAllConstraints());
		for (Map.Entry<Object, Object> e : additionalProps.entrySet()) {
			conf.set((String)e.getKey(), (String)e.getValue());
		}
		
		Scan scan = new Scan();
		scan.setBatch(Constants.TABLE_SCAN_BATCH);
		setRowkeyRangeForScan(scan);
		setColAndTsForScan(scan);
		Job job = Job.getInstance(conf, "MapReduce index operator");
		job.setJarByClass(MapRedIndexOperator.class);
		TableMapReduceUtil.initTableMapperJob(indexTableName, scan, MrioMapper.class, BytesWritable.class, NullWritable.class,
				job, true);
		job.setReducerClass(MrioReducer.class);
		
		int nMappers = predictNumMappers(scan, conf);
		int nReducer = Math.min(nMappers / 2, 20);
		nReducer = Math.max(nReducer, 1);
		job.setNumReduceTasks(nReducer);
		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(outputFormatClass);
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		return job.waitForCompletion(true);
	}
	
	@Override
	public Set<BytesWritable> getQueriedTableElements() throws Exception {
		throw new Exception("Function not supported by MapRedIndexOperator for now. Please use runMrJobForQuery() to"
				+ " query the index table, and then find the results in the directory returned by getOutputDir().");
	}
	
	@Override
	public Set<Long> getQueriedTimestamps() throws Exception {
		throw new Exception("Function not supported by MapRedIndexOperator for now. Please use runMrJobForQuery() to"
				+ " query the index table, and then find the results in the directory returned by getOutputDir().");
	}
}