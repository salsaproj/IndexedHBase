package iu.pti.hbaseapp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * <p>
 * Similar to org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat, this
 * class is a Hadoop output format that writes to one or more HBase tables. The
 * key is taken to be the table name while the output value <em>must</em> be
 * either a {@link Put} or a {@link Delete} instance. All tables must already
 * exist, and all Puts and Deletes must reference only valid column families.
 * 
 * The difference is that when creating HTable instances, it does not specify
 * the 'clearBufferOnFail' parameter as 'true'. Therefore, the failed operations
 * will be preserved in the buffer, and it is up to the mapper or reducer to catch
 * the failure exceptions and do retry later. *   
 * </p> 
 * <p>
 * Write-ahead logging (HLog) for Puts can be disabled by setting
 * {@link #WAL_PROPERTY} to {@link #WAL_OFF}. Default value is {@link #WAL_ON}.
 * Note that disabling write-ahead logging is only appropriate for jobs where
 * loss of data due to region server failure can be tolerated (for example,
 * because it is easy to rerun a bulk import).
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DataPreserveMultiTableOutputFormat extends OutputFormat<ImmutableBytesWritable, Mutation> {	
	/**
	 * Record writer for outputting to multiple HTables.
	 */
	protected static class DataPreserveMultiTableRecordWriter extends RecordWriter<ImmutableBytesWritable, Mutation> {
		protected static final Log LOG = LogFactory.getLog(DataPreserveMultiTableRecordWriter.class);
		protected Map<ImmutableBytesWritable, HTable> tables;
		protected Configuration conf;
		protected boolean useWriteAheadLogging;
		protected HBaseAdmin admin;

		/**
		 * @param conf
		 *            HBaseConfiguration to used
		 * @param useWriteAheadLogging
		 *            whether to use write ahead logging. This can be turned off
		 *            ( <tt>false</tt>) to improve performance when bulk loading
		 *            data.
		 */
		public DataPreserveMultiTableRecordWriter(Configuration conf, boolean useWriteAheadLogging) throws IOException {
			LOG.info("Created new MultiTableRecordReader with WAL " + (useWriteAheadLogging ? "on" : "off"));
			this.tables = new HashMap<ImmutableBytesWritable, HTable>();
			this.conf = conf;
			this.useWriteAheadLogging = useWriteAheadLogging;
			this.admin = new HBaseAdmin(conf);
		}

		/**
		 * @param tableName
		 *            the name of the table, as a string
		 * @return the named table
		 * @throws IOException
		 *             if there is a problem opening a table
		 */
		HTable getTable(ImmutableBytesWritable tableName) throws IOException {
			if (!tables.containsKey(tableName)) {
				String tableNameStr = Bytes.toString(tableName.get());
				HTable table = new HTable(conf, tableNameStr);
				//Our purpose is to avoid losing mutations by waiting and retry later, so HTable should not clear buffer on fail. 
				table.setAutoFlush(false, false);
				tables.put(tableName, table);
			}
			return tables.get(tableName);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException {
			for (HTable table : tables.values()) {
				table.flushCommits();
			}
			
			try {
				for (HTable table : tables.values()) {
					table.close();
				}
				admin.close();
			} catch (Exception e) {
				LOG.warn("Exception when closing HTable and HBaseAdmin instances: " + e.getMessage()); 
			}
		}

		/**
		 * Writes an action (Put or Delete) to the specified table.
		 * 
		 * @param tableName
		 *            the table being updated.
		 * @param action
		 *            the update, either a put or a delete.
		 * @throws IllegalArgumentException
		 *             if the action is not a put or a delete.
		 */
		@Override
		public void write(ImmutableBytesWritable tableName, Mutation action) throws IOException {
			HTable table = getTable(tableName);
			try {
				// The actions are not immutable, so we defensively copy them
				if (action instanceof Put) {
					Put put = new Put((Put) action);
					put.setDurability(useWriteAheadLogging ? Durability.SYNC_WAL : Durability.SKIP_WAL);
					table.put(put);
				} else if (action instanceof Delete) {
					Delete delete = new Delete((Delete) action);
					table.delete(delete);
				} else
					throw new IllegalArgumentException("action must be either Delete or Put");
			} catch (RetriesExhaustedWithDetailsException re) {
				// This is a work-around for the HBase bug https://issues.apache.org/jira/browse/HBASE-10499. 
				try {
					if (re.getMessage().contains("RegionTooBusyException")) {
						HRegionLocation regLoc = table.getRegionLocation(action.getRow(), false);
						admin.flush(regLoc.getRegionInfo().getEncodedName());
					}
				} catch (Exception e) {
					LOG.warn("Exception when trying to do admin.flush() on RetriesExhaustedWithDetailsException: " + e.getMessage());
					e.printStackTrace();
				}
				throw re;
			}
		}
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		// we can't know ahead of time if it's going to blow up when the user
		// passes a table name that doesn't exist, so nothing useful here.
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new TableOutputCommitter();
	}

	@Override
	public RecordWriter<ImmutableBytesWritable, Mutation> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		return new DataPreserveMultiTableRecordWriter(HBaseConfiguration.create(conf), 
				conf.getBoolean(MultiTableOutputFormat.WAL_PROPERTY, MultiTableOutputFormat.WAL_ON));
	}

}
