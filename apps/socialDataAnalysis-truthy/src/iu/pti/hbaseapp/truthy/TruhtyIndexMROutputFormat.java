package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.MultiFileFolderWriter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Output format for usage with {@link #MapRedIndexOperator}. It is aware of Truthy related context, such as
 * tweet IDs as query results from the index tables, corresponding month of the index tables, number of tweet
 * IDs per output file, etc.
 * 
 * @author gaoxm
 */
public class TruhtyIndexMROutputFormat extends FileOutputFormat<BytesWritable, NullWritable> {
	protected static enum RecordType {
		TWEET_ID, STRING, LONG
	}	
	boolean useBigInt = false;
	
	public static class TruthyIdxMRRecordWriter extends RecordWriter<BytesWritable, NullWritable> {	
		MultiFileFolderWriter multiFileWriter;
		RecordType recordType;
		boolean useBigInt = false;

		public TruthyIdxMRRecordWriter(String outputFolder, String filenamePrefix, RecordType recordType, int maxLinesPerFile, boolean isUseBigInt) throws Exception {
			multiFileWriter = new MultiFileFolderWriter(outputFolder, filenamePrefix, false, maxLinesPerFile);
			this.recordType = recordType;
			this.useBigInt = isUseBigInt;
		}

		public void write(BytesWritable key, NullWritable value) throws IOException {
			if (key == null) {
				return;
			}
			try {
				byte[] keyBytes = key.copyBytes();
				switch (recordType) {
				case TWEET_ID:
				    if (this.useBigInt) {
				        multiFileWriter.writeln(TruthyHelpers.getTweetIDStrFromBigIntBytes(keyBytes));
				    } else {
				        multiFileWriter.writeln(TruthyHelpers.getTweetIDStrFromBytes(keyBytes));
				    }
					break;
				case STRING:
					multiFileWriter.writeln(Bytes.toString(keyBytes));
					break;
				case LONG:
					multiFileWriter.writeln(Long.toString(Bytes.toLong(keyBytes)));
				}
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		public void close(TaskAttemptContext contex) throws IOException {
			try {
				multiFileWriter.close();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
	}

	public RecordWriter<BytesWritable, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		try {
			StringBuilder sbFNamePrefix = new StringBuilder();
			Configuration conf = context.getConfiguration();
			String month = conf.get("month");
			this.useBigInt = TruthyHelpers.checkIfb4June2015(month);
			if (month == null) {
				throw new Exception("month is not set in job configuration.");
			} else {
				sbFNamePrefix.append(month).append('_');
			}

			String resultType = conf.get("result.type");
			RecordType recordType = null;
			if (resultType.equalsIgnoreCase("tweetId")) {
				sbFNamePrefix.append("tweetIds_");
				recordType = RecordType.TWEET_ID;
			} else if (resultType.equalsIgnoreCase("string")) {
				recordType = RecordType.STRING;
			} else if (resultType.equalsIgnoreCase("long")) {
				recordType = RecordType.LONG;
			} else {
				throw new Exception("Unsupported result type in configuration: " + resultType);
			}

			TaskID tid = context.getTaskAttemptID().getTaskID();
			switch (tid.getTaskType()) {
			case MAP:
				sbFNamePrefix.append('m');
				break;
			case REDUCE:
				sbFNamePrefix.append('r');
				break;
			default:
				sbFNamePrefix.append('o'); // "o" stands for "other"
			}
			sbFNamePrefix.append(tid.getId());
			String fnamePrefix = sbFNamePrefix.toString();

			FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
			String taskOutputDir = committer.getWorkPath().toString();
			TruthyIdxMRRecordWriter rw = new TruthyIdxMRRecordWriter(taskOutputDir, fnamePrefix, recordType,
					Integer.parseInt(conf.get("max.output.per.file")), useBigInt);
			return rw;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

}
