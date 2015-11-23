package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.CustomIndexer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * A user defined customizable indexer implementation for indexing the screen names of users.
 * 
 * @author gaoxm
 */
public class UserSnameIndexer implements CustomIndexer {

	@Override
	public Map<byte[], List<Put>> index(String tableName, Put sourcePut) throws Exception {
		int idx = tableName.indexOf('-');
		String month = tableName.substring(idx + 1);
		String indexTableName = ConstantsTruthy.SNAME_INDEX_TABLE_NAME + "-" + month;
		byte[] indexTableBytes = Bytes.toBytes(indexTableName);
		Map<byte[], List<Put>> res = new HashMap<byte[], List<Put>>();
		Put userSnamePut = getIndexPut(sourcePut);
		if (userSnamePut != null) {
			List<Put> puts = new LinkedList<Put>();
			puts.add(userSnamePut);
			res.put(indexTableBytes, puts);
		}
		
		return res;
	}

	@Override
	public void index(String tableName, Put sourcePut, TaskInputOutputContext<?, ?, ImmutableBytesWritable, Put> context) throws Exception {
		int idx = tableName.indexOf('-');
		String month = tableName.substring(idx + 1);
		String indexTableName = ConstantsTruthy.SNAME_INDEX_TABLE_NAME + "-" + month;
		byte[] indexTableBytes = Bytes.toBytes(indexTableName);
		Put userSnamePut = getIndexPut(sourcePut);
		
		if (userSnamePut != null) {
			context.write(new ImmutableBytesWritable(indexTableBytes), userSnamePut);
		}
	}

	protected Put getIndexPut(Put userPut) throws Exception {
		byte[] combinedIdBytes = userPut.getRow();
		byte[] userIdBytes = new byte[combinedIdBytes.length - 8];
		// combinedIdBytes is in the form of "[some bytes for user id][8 bytes for tweet id]"
		for (int i=0; i<userIdBytes.length; i++) {
			userIdBytes[i] = combinedIdBytes[i];
		}
		byte[] snameBytes = userPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_SCREEN_NAME_BYTES).get(0).getValue();
		if (snameBytes == null || snameBytes.length == 0) {
			return null;
		}		
		String snameLower = Bytes.toString(snameBytes).toLowerCase();
		byte[] createTimeBytes = userPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_CREATE_TIME_BYTES).get(0).getValue();
		long createTime = Bytes.toLong(createTimeBytes);		
		
		Put userSnamePut = new Put(Bytes.toBytes(snameLower));
		userSnamePut.add(ConstantsTruthy.CF_USERS_BYTES, userIdBytes, createTime, null);
		return userSnamePut;
	}
}
