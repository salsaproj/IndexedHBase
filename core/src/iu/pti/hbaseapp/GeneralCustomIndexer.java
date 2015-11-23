package iu.pti.hbaseapp;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class GeneralCustomIndexer implements CustomIndexer {
	
	/**
	 * Indicates how an element (column family, qualifier, timestamp, value) of an index record
	 * should be generated from a given record(put) of the source table. 
	 * @author gaoxm
	 */
	public static enum IndexEleValueType {
		CONSTANT, SOURCE_ROWKEY, SOURCE_TS, SOURCE_VAL;
	}
	
	/**
	 * Indicates how the source table value should be processed to generate the index records.
	 * @author gaoxm
	 */
	public static enum SourceValType {
		BYTES, WHITE_SPACED_TEXT, FULL_TEXT;
	}
	
	public static class CustomIndexConfig {
		protected String sourceTable = null;
		protected byte[] sourceTableBytes = null;
		protected String sourceCf = null;
		protected byte[] sourceCfBytes = null;
		protected String sourceQual = null;
		protected byte[] sourceQualBytes = null;
		protected Long sourceTs = null;
		protected SourceValType sourceValType = null;
		
		protected String indexTable = null;
		protected byte[] indexTableBytes = null;
		protected String indexCf = null;
		protected byte[] indexCfBytes = null;
		
		protected IndexEleValueType indexQualType = null;
		protected byte[] indexQualConstBytes = null;
		protected byte[] srcCfForIdxQual = null;
		protected byte[] srcQualForIdxQual = null;
		protected Long srcTsForIdxQual = null;
		
		protected IndexEleValueType indexTsType = null;
		protected Long indexTsConst = null;
		protected byte[] srcCfForIdxTs = null;
		protected byte[] srcQualForIdxTs = null;
		protected Long srcTsForIdxTs = null;
		
		protected IndexEleValueType indexValType = null;
		protected byte[] indexValConstBytes = null;
		protected byte[] srcCfForIdxVal = null;
		protected byte[] srcQualForIdxVal = null;
		protected Long srcTsForIdxVal = null;
		
		protected CustomIndexer userDefinedIndexer = null;
		
		public CustomIndexConfig(String sourceTable, String indexTable, CustomIndexer userDefinedIndexer) throws IllegalArgumentException {
			if (userDefinedIndexer == null) {
				throw new IllegalArgumentException("userDefinedIndexer is NULL!");
			}
			
			try {
				this.sourceTable = sourceTable;
				this.sourceTableBytes = Bytes.toBytes(sourceTable);
				this.indexTable = indexTable;
				this.indexTableBytes = Bytes.toBytes(indexTable);
				this.userDefinedIndexer = userDefinedIndexer;
			} catch (Exception e) {
				throw new IllegalArgumentException(e);
			}
		}
		
		public CustomIndexConfig(String sourceTable, String sourceCf, String sourceQual, String sourceTs, String sourceValType,
				String indexTable, String indexCf, String indexQual, String indexTs, String indexVal) throws IllegalArgumentException {
			try {
				this.sourceTable = sourceTable;
				this.sourceTableBytes = Bytes.toBytes(sourceTable);
				this.sourceCf = sourceCf;
				this.sourceCfBytes = Bytes.toBytes(sourceCf);
				this.sourceQual = sourceQual;
				this.sourceQualBytes = Bytes.toBytes(sourceQual);
				if (sourceTs != null && sourceTs.length() > 0) {
					this.sourceTs = Long.valueOf(sourceTs);
				}

				if (sourceValType.equals("bytes")) {
					this.sourceValType = SourceValType.BYTES;
				} else if (sourceValType.equals("white-spaced-text")) {
					this.sourceValType = SourceValType.WHITE_SPACED_TEXT;
				} else if (sourceValType.equals("full-text")) {
					this.sourceValType = SourceValType.FULL_TEXT;
				} else {
					throw new IllegalArgumentException("Unsupported value type: " + sourceValType);
				}

				this.indexTable = indexTable;
				this.indexTableBytes = Bytes.toBytes(indexTable);
				this.indexCf = indexCf;
				this.indexCfBytes = Bytes.toBytes(indexCf);
				
				if (indexQual.startsWith("{source}")) {
					if (indexQual.endsWith("{rowkey}")) {
						// {source}.{rowkey}
						indexQualType = IndexEleValueType.SOURCE_ROWKEY;
					} else if (indexQual.endsWith("{timestamp}")) {
						// {source}.details.createdAt.{timestamp}
						indexQualType = IndexEleValueType.SOURCE_TS;
						int idx1 = indexQual.indexOf('.');
						int idx2 = indexQual.indexOf('.', idx1 + 1);
						srcCfForIdxQual = Bytes.toBytes(indexQual.substring(idx1 + 1, idx2));
						int idx3 = indexQual.indexOf('.', idx2 + 1);
						srcQualForIdxQual = Bytes.toBytes(indexQual.substring(idx2 + 1, idx3));
					} else {
						// {source}.details.createdAt OR {source}.details.createdAt.224034582
						indexQualType = IndexEleValueType.SOURCE_VAL;
						int idx1 = indexQual.indexOf('.');
						int idx2 = indexQual.indexOf('.', idx1 + 1);
						srcCfForIdxQual = Bytes.toBytes(indexQual.substring(idx1 + 1, idx2));
						int idx3 = indexQual.indexOf('.', idx2 + 1);
						if (idx3 >= 0) {
							srcQualForIdxQual = Bytes.toBytes(indexQual.substring(idx2 + 1, idx3));
							srcTsForIdxQual = Long.valueOf(indexQual.substring(idx3 + 1));
						} else {
							srcQualForIdxQual = Bytes.toBytes(indexQual.substring(idx2 + 1));
						}
					}
				} else {
					indexQualType = IndexEleValueType.CONSTANT;
					indexQualConstBytes = Bytes.toBytes(indexQual);
				}
				
				if (indexTs != null && indexTs.length() > 0) {
					if (indexTs.startsWith("{source}")) {
						if (indexTs.endsWith("{rowkey}")) {
							// {source}.{rowkey}
							indexTsType = IndexEleValueType.SOURCE_ROWKEY;
						} else if (indexTs.endsWith("{timestamp}")) {
							// {source}.details.createdAt.{timestamp}
							indexTsType = IndexEleValueType.SOURCE_TS;
							int idx1 = indexTs.indexOf('.');
							int idx2 = indexTs.indexOf('.', idx1 + 1);
							srcCfForIdxTs = Bytes.toBytes(indexTs.substring(idx1 + 1, idx2));
							int idx3 = indexTs.indexOf('.', idx2 + 1);
							srcQualForIdxTs = Bytes.toBytes(indexTs.substring(idx2 + 1, idx3));
						} else {
							// {source}.details.createdAt OR {source}.details.createdAt.224034582
							indexTsType = IndexEleValueType.SOURCE_VAL;
							int idx1 = indexTs.indexOf('.');
							int idx2 = indexTs.indexOf('.', idx1 + 1);
							srcCfForIdxTs = Bytes.toBytes(indexTs.substring(idx1 + 1, idx2));
							int idx3 = indexTs.indexOf('.', idx2 + 1);
							if (idx3 >= 0) {
								srcQualForIdxTs = Bytes.toBytes(indexTs.substring(idx2 + 1, idx3));
								srcTsForIdxTs = Long.valueOf(indexTs.substring(idx3 + 1));
							} else {
								srcQualForIdxTs = Bytes.toBytes(indexTs.substring(idx2 + 1));
							}
						}
					} else {
						indexTsType = IndexEleValueType.CONSTANT;
						indexTsConst = Long.valueOf(indexTs);
					}
				}
				
				if (indexVal != null && indexVal.length() > 0) {
					if (indexVal.startsWith("{source}")) {
						if (indexVal.endsWith("{rowkey}")) {
							// {source}.{rowkey}
							indexValType = IndexEleValueType.SOURCE_ROWKEY;
						} else if (indexVal.endsWith("{timestamp}")) {
							// {source}.details.createdAt.{timestamp}
							indexValType = IndexEleValueType.SOURCE_TS;
							int idx1 = indexVal.indexOf('.');
							int idx2 = indexVal.indexOf('.', idx1 + 1);
							srcCfForIdxVal = Bytes.toBytes(indexVal.substring(idx1 + 1, idx2));
							int idx3 = indexVal.indexOf('.', idx2 + 1);
							srcQualForIdxVal = Bytes.toBytes(indexVal.substring(idx2 + 1, idx3));
						} else {
							// {source}.details.createdAt OR {source}.details.createdAt.224034582
							indexValType = IndexEleValueType.SOURCE_VAL;
							int idx1 = indexVal.indexOf('.');
							int idx2 = indexVal.indexOf('.', idx1 + 1);
							srcCfForIdxVal = Bytes.toBytes(indexVal.substring(idx1 + 1, idx2));
							int idx3 = indexVal.indexOf('.', idx2 + 1);
							if (idx3 >= 0) {
								srcQualForIdxVal = Bytes.toBytes(indexVal.substring(idx2 + 1, idx3));
								srcTsForIdxVal = Long.valueOf(indexVal.substring(idx3 + 1));
							} else {
								srcQualForIdxVal = Bytes.toBytes(indexVal.substring(idx2 + 1));
							}
						}
					} else {
						indexValType = IndexEleValueType.CONSTANT;
						indexValConstBytes = Bytes.toBytes(indexVal);
					}
				}
			} catch (Exception e) {
				throw new IllegalArgumentException(e);
			}
		}
	}
	
	protected Map<String, List<CustomIndexConfig>> indexConfigMap = new HashMap<String, List<CustomIndexConfig>>();
	
	public GeneralCustomIndexer(String targetIndexTable) throws Exception {
		String configFileName = "custom-index.xml";
		InputStream configStream = this.getClass().getResourceAsStream(configFileName);
		SAXReader reader = new SAXReader();
		Document configDoc = reader.read(configStream);
		configStream.close();
		setConfigByDocument(configDoc, targetIndexTable);
	}
	
	public GeneralCustomIndexer(String configPath, String targetIndexTable) throws Exception {
		InputStream configStream = new FileInputStream(configPath);
		SAXReader reader = new SAXReader();
		Document configDoc = reader.read(configStream);
		configStream.close();
		setConfigByDocument(configDoc, targetIndexTable);
	}
	
	protected void setConfigByDocument(Document configDoc, String targetIndexTable) throws Exception {
		Element rootEle = configDoc.getRootElement();
		List<Element> configEles = rootEle.elements("index-config");
		/*
		 <index-config>
        	<source-table>tweetTable-2012-06</source-table-name>
        	<source-column-family>details</source-column-family>
        	<source-qualifier>text</source-qualifier>                                           
        	<source-value-type>full-text</source-value-type>
        	<index-table>textIndexTable-2012-06</index-table>
        	<index-column-family>tweets</index-column-family>
        	<index-qualifier>{source}.{rowkey}</index-qualifier>
        	<index-timestamp>{source}.details.createdAt</index-timestamp>
    	 </index-config>
    	 <index-config>
        	<source-table>userTable-2012-06</source-table-name>
        	<index-table>snameIndexTable-2012-06</index-table>
        	<indexer-class>iu.pti.hbaseapp.truthy.UserSnameIndexer</indexer-class>
    	 </index-config>
		*/		
		for (Element confEle : configEles) {
			String sourceTable = confEle.elementText("source-table");
			String indexTable = confEle.elementText("index-table");
			if (targetIndexTable != null && !indexTable.equals(targetIndexTable)) {
				continue;
			}
			
			CustomIndexConfig config = null;
			if (confEle.element("indexer-class") != null) {
				String indexerClassName = confEle.elementText("indexer-class");
				CustomIndexer userDefinedIndexer =  (CustomIndexer)Class.forName(indexerClassName).newInstance();
				config = new CustomIndexConfig(sourceTable, indexTable, userDefinedIndexer);
			} else {
				String sourceCf = confEle.elementText("source-column-family");
				String sourceQual = confEle.elementText("source-qualifier");
				String sourceTs = confEle.elementText("source-timestamp");
				String sourceValType = confEle.elementText("source-value-type");
				String indexCf = confEle.elementText("index-column-family");
				String indexQual = confEle.elementText("index-qualifier");
				String indexTs = confEle.elementText("index-timestamp");
				String indexVal = confEle.elementText("index-value");
				config = new CustomIndexConfig(sourceTable, sourceCf, sourceQual, sourceTs, sourceValType, indexTable, 
						indexCf, indexQual, indexTs, indexVal);
			}
			
			List<CustomIndexConfig> configs = indexConfigMap.get(sourceTable);
			if (configs == null) {
				configs =  new LinkedList<CustomIndexConfig>();
				configs.add(config);
				indexConfigMap.put(sourceTable, configs);
			} else {
				configs.add(config);
			}		
		}		
	}

	@Override
	public Map<byte[], List<Put>> index(String tableName, Put sourcePut) throws Exception {
		Map<byte[], List<Put>> results = new HashMap<byte[], List<Put>>();
		List<CustomIndexConfig> configs = indexConfigMap.get(tableName);
		if (configs == null) {
			throw new Exception("Unconfigured source table name: " + tableName);
		}
		
		for (CustomIndexConfig config : configs) {
			if (config.userDefinedIndexer != null) {
				Map<byte[], List<Put>> resPart = config.userDefinedIndexer.index(tableName, sourcePut);
				for (Map.Entry<byte[], List<Put>> e : resPart.entrySet()) {
					results.put(e.getKey(), e.getValue());
				}
			} else {
				List<Put> puts = new LinkedList<Put>();
				generateIndexPuts(sourcePut, config, puts);
				results.put(config.indexTableBytes, puts);
			}
		}
		
		return results;
	}

	@Override
	public void index(String tableName, Put sourcePut, TaskInputOutputContext<?, ?, ImmutableBytesWritable, Put> context) throws Exception {
		List<CustomIndexConfig> configs = indexConfigMap.get(tableName);
		if (configs == null) {
			throw new Exception("Unconfigured source table name: " + tableName);
		}
		
		for (CustomIndexConfig config : configs) {
			if (config.userDefinedIndexer != null) {
				config.userDefinedIndexer.index(tableName, sourcePut, context);
			} else {
				List<Put> puts = new LinkedList<Put>();
				generateIndexPuts(sourcePut, config, puts);
				for (Put p : puts) {
					context.write(new ImmutableBytesWritable(config.indexTableBytes), p);
				}
			}
		}
	}
	
	/**
	 * Generate puts for the index table, and add them to the parameter "results".
	 * @param sourcePut
	 * @param config
	 * @param results
	 */
	protected void generateIndexPuts(Put sourcePut, CustomIndexConfig config, List<Put> results) {
		byte[] sourceCellVal = null;
		List<KeyValue> cells = sourcePut.get(config.sourceCfBytes, config.sourceQualBytes);
		if (config.sourceTs != null) {
			for (KeyValue c : cells) {
				if (c.getTimestamp() == config.sourceTs) {
					sourceCellVal = c.getValue();
					break;
				}
			}
		} else {
			Long maxTs = null;
			for (KeyValue c : cells) {
				if (maxTs == null || maxTs < c.getTimestamp()) {
					maxTs = c.getTimestamp();
					sourceCellVal = c.getValue();
				}
			}
		}
		
		if (sourceCellVal == null) {
			return;
		}
		
		byte[] indexQualBytes = generateIndexPutQual(sourcePut, config);
		Long indexTs = generateIndexPutTs(sourcePut, config);
		byte[] indexValBytes = generateIndexPutVal(sourcePut, config);
		String txt = null;
		switch (config.sourceValType) {
		case BYTES:
			Put put = new Put(sourceCellVal);
			if (indexTs == null) {
				put.add(config.indexCfBytes, indexQualBytes, indexValBytes);
			} else {
				put.add(config.indexCfBytes, indexQualBytes, indexTs, indexValBytes);
			}
			results.add(put);			
			break;
		case WHITE_SPACED_TEXT:
			txt = Bytes.toString(sourceCellVal);
			for (String token : txt.split("\\t")) {
				if (token.length() <= 0) {
					continue;
				}
				byte[] tokenBytes = Bytes.toBytes(token.toLowerCase());
				Put tokenPut = new Put(tokenBytes);
				if (indexTs == null) {
					tokenPut.add(config.indexCfBytes, indexQualBytes, indexValBytes);
				} else {
					tokenPut.add(config.indexCfBytes, indexQualBytes, indexTs, indexValBytes);
				}
				results.add(tokenPut);
			}
			break;
		case FULL_TEXT:
			txt = Bytes.toString(sourceCellVal);
			HashSet<String> terms = new HashSet<String>();
			GeneralHelpers.getTermsByLuceneAnalyzer(Constants.getLuceneAnalyzer(), txt, "dummy", terms);
			for (String term : terms) {
				byte[] termBytes = Bytes.toBytes(term.toLowerCase());
				Put termPut = new Put(termBytes);
				if (indexTs == null) {
					termPut.add(config.indexCfBytes, indexQualBytes, indexValBytes);
				} else {
					termPut.add(config.indexCfBytes, indexQualBytes, indexTs, indexValBytes);
				}
				results.add(termPut);
			}
			break;
		}
	}
	
	/**
	 * Generate the qualifier for a put object for the index table, based on the given index configuration. 
	 * @param sourcePut
	 * @param config
	 * @return
	 */
	protected byte[] generateIndexPutQual(Put sourcePut, CustomIndexConfig config) {
		byte[] indexQualBytes = null;
		Long maxTs = null;
		List<KeyValue> cells = null;
		
		switch (config.indexQualType) {
		case CONSTANT:
			indexQualBytes = config.indexQualConstBytes;
			break;
		case SOURCE_ROWKEY:
			indexQualBytes = sourcePut.getRow();
			break;
		case SOURCE_TS:
			maxTs = null;
			cells = sourcePut.get(config.srcCfForIdxQual, config.srcQualForIdxQual);
			for (KeyValue c : cells) {
				if (maxTs == null || maxTs < c.getTimestamp()) {
					maxTs = c.getTimestamp();
				}
			}
			indexQualBytes = Bytes.toBytes(maxTs);
			break;
		case SOURCE_VAL:
			if (config.srcTsForIdxQual == null) {
				maxTs = null;
				cells = sourcePut.get(config.srcCfForIdxQual, config.srcQualForIdxQual);
				for (KeyValue c : cells) {
					if (maxTs == null || maxTs < c.getTimestamp()) {
						maxTs = c.getTimestamp();
						indexQualBytes = c.getValue();
					}
				}
			} else {
				cells = sourcePut.get(config.srcCfForIdxQual, config.srcQualForIdxQual);
				for (KeyValue c : cells) {
					if (c.getTimestamp() == config.srcTsForIdxQual) {
						indexQualBytes = c.getValue();
						break;
					}
				}
			}
			break;
		}
		
		return indexQualBytes;
	}
	
	/**
	 * Generate the timestamp for a put object for the index table, based on the given index configuration. 
	 * @param sourcePut
	 * @param config
	 * @return
	 */
	protected Long generateIndexPutTs(Put sourcePut, CustomIndexConfig config) {
		if (config.indexTsType == null) {
			return null;
		}
		
		Long indexTs = null;
		Long maxTs = null;
		List<KeyValue> cells = null;
		byte[] srcValBytes = null;
		
		switch (config.indexTsType) {
		case CONSTANT:
			indexTs = config.indexTsConst;
			break;
		case SOURCE_ROWKEY:
			indexTs = Bytes.toLong(sourcePut.getRow());
			break;
		case SOURCE_TS:
			maxTs = null;
			cells = sourcePut.get(config.srcCfForIdxTs, config.srcQualForIdxTs);
			for (KeyValue c : cells) {
				if (maxTs == null || maxTs < c.getTimestamp()) {
					maxTs = c.getTimestamp();
				}
			}
			indexTs = maxTs;
			break;
		case SOURCE_VAL:
			srcValBytes = null;
			if (config.srcTsForIdxTs == null) {
				maxTs = null;				
				cells = sourcePut.get(config.srcCfForIdxTs, config.srcQualForIdxTs);
				for (KeyValue c : cells) {
					if (maxTs == null || maxTs < c.getTimestamp()) {
						maxTs = c.getTimestamp();
						srcValBytes = c.getValue();
					}
				}
			} else {
				cells = sourcePut.get(config.srcCfForIdxTs, config.srcQualForIdxTs);
				for (KeyValue c : cells) {
					if (c.getTimestamp() == config.srcTsForIdxTs) {
						srcValBytes = c.getValue();
						break;
					}
				}				
			}
			indexTs = Bytes.toLong(srcValBytes);
			break;
		}
		
		return indexTs;
	}
	
	/**
	 * Generate the cell value for a put object for the index table, based on the given index configuration. 
	 * @param sourcePut
	 * @param config
	 * @return
	 */
	protected byte[] generateIndexPutVal(Put sourcePut, CustomIndexConfig config) {
		if (config.indexValType == null) {
			return null;
		}
		
		byte[] indexValBytes = null;
		Long maxTs = null;
		List<KeyValue> cells = null;
		
		switch (config.indexValType) {
		case CONSTANT:
			indexValBytes = config.indexValConstBytes;
			break;
		case SOURCE_ROWKEY:
			indexValBytes = sourcePut.getRow();
			break;
		case SOURCE_TS:
			maxTs = null;
			cells = sourcePut.get(config.srcCfForIdxVal, config.srcQualForIdxVal);
			for (KeyValue c : cells) {
				if (maxTs == null || maxTs < c.getTimestamp()) {
					maxTs = c.getTimestamp();
				}
			}
			indexValBytes = Bytes.toBytes(maxTs);
			break;
		case SOURCE_VAL:
			if (config.srcTsForIdxVal == null) {
				maxTs = null;				
				cells = sourcePut.get(config.srcCfForIdxVal, config.srcQualForIdxVal);
				for (KeyValue c : cells) {
					if (maxTs == null || maxTs < c.getTimestamp()) {
						maxTs = c.getTimestamp();
						indexValBytes = c.getValue();
					}
				}
			} else {
				cells = sourcePut.get(config.srcCfForIdxVal, config.srcQualForIdxVal);
				for (KeyValue c : cells) {
					if (c.getTimestamp() == config.srcTsForIdxVal) {
						indexValBytes = c.getValue();
						break;
					}
				}
			}
			break;
		}
		
		return indexValBytes;
	}
}
