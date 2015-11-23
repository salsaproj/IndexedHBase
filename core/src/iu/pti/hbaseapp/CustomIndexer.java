package iu.pti.hbaseapp;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;


/**
 * Indexer interface for building customized index records for a given put to the source table.
 * @author gaoxm
 *
 */
public interface CustomIndexer {
	
	/**
	 * Build index records according to the given table name and put. 
	 * @param tableName
	 * @param sourcePut
	 * @return A map from each index table name to a list of put objects for that index table. 
	 */
	public Map<byte[], List<Put>> index(String tableName, Put sourcePut) throws Exception;
	
	/**
	 * Used by a mapper that output table names and puts. Index records are built according to the given table name and put,
	 * and written to output on the fly using the given mapper context.   
	 * @param tableName
	 * @param sourcePut
	 * @param context
	 * @return 
	 */
	public void index(String tableName, Put sourcePut, TaskInputOutputContext<?, ?, ImmutableBytesWritable, Put> context)  
			throws Exception;
}
