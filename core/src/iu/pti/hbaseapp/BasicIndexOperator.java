package iu.pti.hbaseapp;

import java.util.Calendar;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator;
import org.apache.hadoop.io.BytesWritable;

/**
 * Implements a basic operator on an index table. The operator applies a given a set of constraints
 * on the index table, and return required information contained in the qualified KeyValues in the 
 * table.
 * @author gaoxm
 */
public class BasicIndexOperator implements IndexTableOperator {
	
	public enum IdxOpReturnType {
		ROWKEY, QUALIFIER, CELLVAL, TIMESTAMP, UNKNOWN
	}
	
	public static class LongComparator implements Comparator<Long> {
	    public int compare(Long x, Long y) {
	        return Long.compare(x, y);
	    }
	}
	
	protected byte[] indexTableName = null;
	protected IndexDataConstraint<byte[]> rowkeyConstraint = null;
	protected IndexDataConstraint<byte[]> cfConstraint = null;
	protected IndexDataConstraint<byte[]> qualConstraint = null;
	protected IndexDataConstraint<Long> tsConstraint = null;
	protected IndexDataConstraint<byte[]> cellValConstraint = null;
	protected HTable indexTable = null;
	protected IdxOpReturnType returnType = IdxOpReturnType.UNKNOWN;
	
	/**
	 * Default "doing nothing" constructor. Make sure to call the functions for setting constraints after
	 * using this constructor to 'new' a BasicIndexOperator.
	 * @param constraints
	 * @throws IllegalArgumentException
	 */
	public BasicIndexOperator()	{
	}
	
	/**
	 * Construct a basic index operator with constraints described in strings.
	 * @param constraints
	 * @throws IllegalArgumentException
	 */
	public BasicIndexOperator(String indexTableName, String[] constraints) throws IllegalArgumentException {
		try {
			this.indexTableName = Bytes.toBytes(indexTableName);
			setConstraints(constraints);
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	@Override
	public void setConstraints(String[] constraints) throws Exception {
		if (constraints == null) {
			return;
		}
		
		boolean rkCstSet = false;
		boolean cfCstSet = false;
		boolean qualCstSet = false;
		boolean tsCstSet = false;
		boolean cvCstSet = false;
		boolean rtCstSet = false;
		
		for (String constraint : constraints) {
			if (constraint.length() == 0) {
				continue;
			}
			int idx = constraint.indexOf(':');
			if (idx < 0) {
				throw new IllegalArgumentException("Can't decide type of constraint: " + constraint);
			}
			String type = constraint.substring(0, idx);
			if (type.equalsIgnoreCase("rowkey")) {
				if (rkCstSet) {
					throw new Exception("Duplicated rowkey constraint: " + constraint);
				}
				setRowkeyConstraint(constraint);
				rkCstSet = true;
			} else if (type.equalsIgnoreCase("cf")) {
				if (cfCstSet) {
					throw new Exception("Duplicated column family constraint: " + constraint);
				}
				setCfConstraint(constraint);
				cfCstSet = true;
			} else if (type.equalsIgnoreCase("qual")) {
				if (qualCstSet) {
					throw new Exception("Duplicated qualifier constraint: " + constraint);
				}
				setQualConstraint(constraint);
				qualCstSet = true;
			} else if (type.equalsIgnoreCase("ts")) {
				if (tsCstSet) {
					throw new Exception("Duplicated timestamp constraint: " + constraint);
				}
				setTsConstraint(constraint);
				tsCstSet = true;
			} else if (type.equalsIgnoreCase("cellval")) {
				if (cvCstSet) {
					throw new Exception("Duplicated cell value constraint: " + constraint);
				}
				setCellValConstraint(constraint);
				cvCstSet = true;
			} else if (type.equalsIgnoreCase("return")) {
				if (rtCstSet) {
					throw new Exception("Duplicated return constraint: " + constraint);
				}
				setReturnType(constraint);
				rtCstSet = true;
			} else {
				throw new IllegalArgumentException("Unrecognized type of constraint: " + constraint);
			}
		}
	}
	
	/**
	 * Set the row key constraint encoded by a string. 
	 * @param constraint
	 * 	The string encoding of a row key constraint. See {@link #setConstraints(String[])} for the
	 *  supported constraint format.
	 *  
	 * @throws Exception 
	 * 	If <b>constraint</b> is not a valid "rowkey" constraint.
	 */
	public void setRowkeyConstraint(String constraint) throws Exception {
		int idx = constraint.indexOf(':');
		if (idx < 0) {
			throw new IllegalArgumentException("Invalid rowkey constraint format - can't decide type: " + constraint);
		}
		String type = constraint.substring(0, idx);
		if (!type.equalsIgnoreCase("rowkey")) {
			throw new IllegalArgumentException("Invalid rowkey constraint format - type is not 'rowkey': " + constraint);
		}
		rowkeyConstraint = getBytesConstraint(constraint.substring(idx + 1));
	}
	
	/**
	 * Set the column family constraint encoded by a string. See {@link #setConstraints(String[])} for the
	 * supported constraint format.
	 * @param constraint
	 * @throws Exception
	 */
	public void setCfConstraint(String constraint) throws Exception {
		int idx = constraint.indexOf(':');
		if (idx < 0) {
			throw new IllegalArgumentException("Invalid column family constraint format - can't decide type: " + constraint);
		}
		String type = constraint.substring(0, idx);
		if (!type.equalsIgnoreCase("cf")) {
			throw new IllegalArgumentException("Invalid column family constraint format - type is not 'cf': " + constraint);
		}
		cfConstraint = getBytesConstraint(constraint.substring(idx + 1));
	}
	
	/**
	 * Set the qualifier constraint encoded by a string. See {@link #setConstraints(String[])} for the
	 * supported constraint format.
	 * @param constraint
	 * @throws Exception
	 */
	public void setQualConstraint(String constraint) throws Exception {
		int idx = constraint.indexOf(':');
		if (idx < 0) {
			throw new IllegalArgumentException("Invalid qualifier constraint format - can't decide type: " + constraint);
		}
		String type = constraint.substring(0, idx);
		if (!type.equalsIgnoreCase("qual")) {
			throw new IllegalArgumentException("Invalid qualifier constraint format - type is not 'qual': " + constraint);
		}
		qualConstraint = getBytesConstraint(constraint.substring(idx + 1));		
	}
	
	/**
	 * Set the timestamp constraint encoded by a string. See {@link #setConstraints(String[])} for the
	 * supported constraint format.
	 * @param constraint
	 * @throws Exception
	 */
	public void setTsConstraint(String constraint) throws Exception {
		int idx = constraint.indexOf(':');
		if (idx < 0) {
			throw new IllegalArgumentException("Invalid timestamp constraint format - can't decide type: " + constraint);
		}
		String type = constraint.substring(0, idx);
		if (!type.equalsIgnoreCase("ts")) {
			throw new IllegalArgumentException("Invalid timestamp constraint format - type is not 'ts': " + constraint);
		}
		tsConstraint = getLongConstraint(constraint.substring(idx + 1));
	}
	
	/**
	 * Set the cell value constraint encoded by a string. See {@link #setConstraints(String[])} for the
	 * supported constraint format.
	 * @param constraint
	 * @throws Exception
	 */
	public void setCellValConstraint(String constraint) throws Exception {
		int idx = constraint.indexOf(':');
		if (idx < 0) {
			throw new IllegalArgumentException("Invalid cell value constraint format - can't decide type: " + constraint);
		}
		String type = constraint.substring(0, idx);
		if (!type.equalsIgnoreCase("cellval")) {
			throw new IllegalArgumentException("Invalid cell value constraint format - type is not 'cellval': " + constraint);
		}
		cellValConstraint = getBytesConstraint(constraint.substring(idx + 1));
	}
	
	/**
	 * Set the return constraint encoded by a string. See {@link #setConstraints(String[])} for the
	 * supported constraint format.
	 * @param constraint
	 * @throws Exception
	 */
	public void setReturnType(String constraint) throws Exception {
		int idx = constraint.indexOf(':');
		if (idx < 0) {
			throw new IllegalArgumentException("Invalid return constraint format - can't decide type: " + constraint);
		}
		String type = constraint.substring(0, idx);
		if (!type.equalsIgnoreCase("return")) {
			throw new IllegalArgumentException("Invalid return constraint format - type is not 'return': " + constraint);
		}
		String tableEle = constraint.substring(idx + 1);
		if (tableEle.equalsIgnoreCase("rowkey")) {
			returnType = IdxOpReturnType.ROWKEY;
		} else if (tableEle.equalsIgnoreCase("qual")) {
			returnType = IdxOpReturnType.QUALIFIER;
		} else if (tableEle.equalsIgnoreCase("ts")) {
			returnType = IdxOpReturnType.TIMESTAMP;
		} else if (tableEle.equalsIgnoreCase("cellval")) {
			returnType = IdxOpReturnType.CELLVAL;
		} else {
			throw new IllegalArgumentException("Unrecognized table element for return: " + tableEle);
		}
	}
	
	/**
	 * Parse cstBody and return the right type of IndexDataConstraint for HBase table elements.
	 *
	 * @param cstBody
	 * 	A string encoding of a "constraint body". It could be in the following format: <br/>
	 * 	(1) {val1,val2,...} for value set constraint; <br/>
	 * 	(2) [lower,upper] for range constraint; <br/>
	 * 	(3) < regular expression > for regular expression constraint; <br/>
	 * 	(4) ~prefix*~ or ~prefix?~ for prefix constraint. <br/>
	 * 
	 * @return
	 * 	An IndexDataConstraint object in the right type.
	 * @throws Exception
	 *  If errors are encountered when parsing <b>cstBody</b>.
	 */
	public static IndexDataConstraint<byte[]> getBytesConstraint(String cstBody) throws Exception {
		if (cstBody.length() < 3) {
			throw new IllegalArgumentException("Invalid constraint body - length is less than 3: " + cstBody);
		}
		char first = cstBody.charAt(0);
		char last = cstBody.charAt(cstBody.length() - 1);
		String cstDefStr = cstBody.substring(1, cstBody.length() - 1);
		
		if (first == '{' && last == '}') {
			// value set constraint in the form of {val1,val2,...}
			ValueSetIdxConstraint<byte[]> vsCst = new ValueSetIdxConstraint<byte[]>();
			String[] elements = cstDefStr.split(",");
			for (String ele : elements) {
				vsCst.addValue(Bytes.toBytes(ele));
			}
			return vsCst;
		} else if (first == '[' && last == ']') {
			// range constraint in the form of [lower,upper]
			int idx = cstDefStr.indexOf(',');
			if (idx < 0) {
				throw new IllegalArgumentException("Invalid range constraint definition - not in the form of '[lower,upper]': " + cstBody);
			}
			String lower = cstDefStr.substring(0, idx);
			String upper = cstDefStr.substring(idx + 1);
			byte[] lowerBytes = null;
			byte[] upperBytes = null;
			if (lower.length() != 0) {
				lowerBytes = Bytes.toBytes(lower);
			}
			if (upper.length() != 0) {
				upperBytes = Bytes.toBytes(upper);
			}
			return new RangeIdxConstraint<byte[]>(lowerBytes, upperBytes, new ByteArrayComparator());			
		} else if (first == '<' && last == '>') {
			// regular expression constraint in the form of <regex>
			return new RegExIdxConstraint(cstDefStr);
		} else if (first == '~' && last == '~') {
			// prefix constraint in the form of ~prefix*~ or ~prefix?~
			return new PrefixIdxConstraint(cstDefStr);
		} else {
			throw new IllegalArgumentException("Unsupported constraint type marker: " + first + " and " + last);
		}
	}

	/**
	 * Parse cstBody and return the right type of IndexDataConstraint for HBase timestamps. cstBody could be in the following:
	 * (1) {val1,val2,...} for value set constraint;
	 * (2) [lower,upper] for range constraint;
	 * @param cstBody
	 * @return
	 * @throws Exception
	 */
	public static IndexDataConstraint<Long> getLongConstraint(String cstBody) throws Exception {
		if (cstBody.length() < 3) {
			throw new IllegalArgumentException("Invalid constraint body - length is less than 3: " + cstBody);
		}
		char first = cstBody.charAt(0);
		char last = cstBody.charAt(cstBody.length() - 1);
		String cstDefStr = cstBody.substring(1, cstBody.length() - 1);
		
		if (first == '{' && last == '}') {
			// value set constraint in the form of {val1,val2,...}
			ValueSetIdxConstraint<Long> vsCst = new ValueSetIdxConstraint<Long>();
			String[] elements = cstDefStr.split(",");
			for (String ele : elements) {
				vsCst.addValue(cstStrValToLong(ele));
			}
			return vsCst;
		} else if (first == '[' && last == ']') {
			// range constraint in the form of [lower,upper]
			int idx = cstDefStr.indexOf(',');
			if (idx < 0) {
				throw new IllegalArgumentException("Invalid range constraint definition - not in the form of '[lower,upper]': " + cstBody);
			}
			String lower = cstDefStr.substring(0, idx);
			String upper = cstDefStr.substring(idx + 1);
			Long lowerLong = null;
			Long upperLong = null;
			if (lower.length() != 0) {
				lowerLong = cstStrValToLong(lower);
			}
			if (upper.length() != 0) {
				upperLong = cstStrValToLong(upper);
			}
			return new RangeIdxConstraint<Long>(lowerLong, upperLong, new LongComparator());
		} else {
			throw new IllegalArgumentException("Unsupported constraint type marker when creating contraint using Long values: " + first + " and " + last);
		}
	}
	
	protected static Calendar calTmp = Calendar.getInstance(TimeZone.getTimeZone(Constants.TIME_ZONE_GMT));
	
	/**
	 * Convert a string-represented value in the constraint definition string in to a long number. Two possible formats of the string representation
	 * are supported: (1) A string that could be got by Long.toString(), e.g. "12345". (2) A date string like "2012-10-01T23:22:23" that can be
	 * converted to a long number representing milliseconds for the corresponding time. 
	 * @param cstVal
	 * @return
	 */
	protected static Long cstStrValToLong(String cstVal) throws Exception {	
		int idx = cstVal.indexOf('-');
		if (idx < 0) {
			return Long.valueOf(cstVal);
		} else {
			GeneralHelpers.setDateTimeByString(calTmp, cstVal);
			return calTmp.getTimeInMillis();
		}		
	}
	
	@Override
	public Set<BytesWritable> getQueriedTableElements() throws Exception {
		if (returnType == IdxOpReturnType.UNKNOWN || returnType == IdxOpReturnType.TIMESTAMP) {
			throw new Exception("Return type is not set to rowkey, column family, qualifier, or cell value.");
		}
		if (indexTableName == null) {
			throw new Exception("Index table name is null.");
		}
		
		Configuration conf = HBaseConfiguration.create();
		indexTable = new HTable(conf, indexTableName);
		Set<BytesWritable> result = new HashSet<BytesWritable>();
		if (rowkeyConstraint != null && rowkeyConstraint instanceof ValueSetIdxConstraint<?>) {
			if (returnType == IdxOpReturnType.ROWKEY) {
				Set<byte[]> rowkeySet = ((ValueSetIdxConstraint<byte[]>)rowkeyConstraint).getValueSet();
				for (byte[] rowkey : rowkeySet) {
					Get get = new Get(rowkey);
					if (indexTable.exists(get)) {
						result.add(new BytesWritable(rowkey));
					}
				}
				return result;
			} else {
				Set<byte[]> rowkeySet = ((ValueSetIdxConstraint<byte[]>)rowkeyConstraint).getValueSet();
				Scan scan = new Scan();
				scan.setBatch(Constants.TABLE_SCAN_BATCH);
				setColAndTsForScan(scan);
				for (byte[] rowkey : rowkeySet) {
					scan.setStartRow(rowkey);
					scan.setStopRow(rowkey);
					ResultScanner rs = indexTable.getScanner(scan);
					Result r = rs.next();
					while (r != null) {
						for (KeyValue cell : r.list()) {
							addResultsFromCell(cell, result, null);
						}
						r = rs.next();
					}
					rs.close();
				}
			}
		} else {
			Scan scan = new Scan();
			scan.setBatch(Constants.TABLE_SCAN_BATCH);
			setRowkeyRangeForScan(scan);
			setColAndTsForScan(scan);
			ResultScanner rs = indexTable.getScanner(scan);
			Result r = rs.next();
			while (r != null) {
				for (KeyValue cell : r.list()) {
					addResultsFromCell(cell, result, null);
				}
				r = rs.next();
			}
			rs.close();
		}
		
		indexTable.close();
		return result;
	}

	@Override
	public Set<Long> getQueriedTimestamps() throws Exception {
		if (returnType != IdxOpReturnType.TIMESTAMP) {
			throw new Exception("Return type is not timestamp.");
		}
		if (indexTableName == null) {
			throw new Exception("Index table name is null.");
		}
		
		Configuration conf = HBaseConfiguration.create();
		indexTable = new HTable(conf, indexTableName);
		Set<Long> result = new HashSet<Long>();
		if (rowkeyConstraint != null && rowkeyConstraint instanceof ValueSetIdxConstraint<?>) {
			Set<byte[]> rowkeySet = ((ValueSetIdxConstraint<byte[]>) rowkeyConstraint).getValueSet();
			Scan scan = new Scan();
			scan.setBatch(Constants.TABLE_SCAN_BATCH);
			setColAndTsForScan(scan);
			for (byte[] rowkey : rowkeySet) {
				scan.setStartRow(rowkey);
				scan.setStopRow(rowkey);
				ResultScanner rs = indexTable.getScanner(scan);
				Result r = rs.next();
				while (r != null) {
					for (KeyValue cell : r.list()) {
						addResultsFromCell(cell, null, result);
					}
					r = rs.next();
				}
				rs.close();
			}
		} else {
			Scan scan = new Scan();
			scan.setBatch(Constants.TABLE_SCAN_BATCH);
			setRowkeyRangeForScan(scan);
			setColAndTsForScan(scan);
			ResultScanner rs = indexTable.getScanner(scan);
			Result r = rs.next();
			while (r != null) {
				for (KeyValue cell : r.list()) {
					addResultsFromCell(cell, null, result);
				}
				r = rs.next();
			}
			rs.close();
		}
		
		indexTable.close();
		return result;
	}
	
	/**
	 * Set the column family and timestamp constraints for a scan.
	 * @param scan
	 * @throws Exception
	 */
	protected void setColAndTsForScan(Scan scan) throws Exception {
		if (cfConstraint != null && cfConstraint instanceof ValueSetIdxConstraint<?>) {
			Set<byte[]> cfSet = ((ValueSetIdxConstraint<byte[]>)cfConstraint).getValueSet();
			byte[] lastcf = null;
			for (byte[] cf : cfSet) {
				scan.addFamily(cf);
				lastcf = cf;
			}
			
			if (cfSet.size() == 1 && qualConstraint != null && qualConstraint instanceof ValueSetIdxConstraint<?>) {
				Set<byte[]> qualSet = ((ValueSetIdxConstraint<byte[]>)qualConstraint).getValueSet();
				for (byte[] qual : qualSet) {
					scan.addColumn(lastcf, qual);
				}
			}
		}
		
		if (tsConstraint != null) {
			if (tsConstraint instanceof RangeIdxConstraint<?>) {
				List<Long> tsBound = ((RangeIdxConstraint<Long>)tsConstraint).getBoundaryValues();
				Long min = tsBound.get(0);
				Long max = tsBound.get(1);
				if (min == null) {
					min = Long.MIN_VALUE;
				}
				if (max == null) {
					max = Long.MAX_VALUE;
				}
				if (max == Long.MAX_VALUE) {
					max -= 1;
				}
				// the "maxStamp" parameter of the Scan.setTimeRange() function is exclusive, so we should put max+1.
				scan.setTimeRange(min, max+1);
			} else if (tsConstraint instanceof ValueSetIdxConstraint<?>) {
				Set<Long> tsSet = ((ValueSetIdxConstraint<Long>)tsConstraint).getValueSet();
				if (tsSet.size() == 1) {
					for (Long ts : tsSet) { 
						scan.setTimeStamp(ts);
					}
				}
			}
		}
	}
	
	/**
	 * Set the rowkey range for a scan, if the rowkey constraint is of 'range' or 'prefix' type.
	 * @param scan
	 * @throws Exception
	 */
	protected void setRowkeyRangeForScan(Scan scan) throws Exception {
		if (rowkeyConstraint == null) {
			return;
		}
		
		if (rowkeyConstraint instanceof RangeIdxConstraint<?>) {
			List<byte[]> rowkeyRange = ((RangeIdxConstraint<byte[]>)rowkeyConstraint).getBoundaryValues();
			byte[] startRow = rowkeyRange.get(0);
			byte[] stopRow = rowkeyRange.get(1);
			if (startRow != null) {
				scan.setStartRow(startRow);
			}
			if (stopRow != null) {
				scan.setStopRow(stopRow);
			}
		} else if (rowkeyConstraint instanceof PrefixIdxConstraint) {
			String prefix = ((PrefixIdxConstraint)rowkeyConstraint).getPrefix();
			GeneralHelpers.setRowkeyRangeByPrefix(scan, prefix);
		} else if (rowkeyConstraint instanceof RegExIdxConstraint) {
			String regex = ((RegExIdxConstraint)rowkeyConstraint).getRegEx();
			String prefix = GeneralHelpers.getPrefixForRegEx(regex);
			if (prefix.length() > 0) {
				GeneralHelpers.setRowkeyRangeByPrefix(scan, prefix);
			}
		}
	}
	
	/**
	 * Check if the table cell qualifies the constraints; if it does, add the corresponding
	 * information to the result set according to the return constraint.
	 * @param cell
	 * @param bwResults
	 * @param tsSet
	 */
	protected void addResultsFromCell(KeyValue cell, Set<BytesWritable> bwResults, Set<Long> tsResults) throws Exception {
		if (returnType == IdxOpReturnType.UNKNOWN || !checkCellByConstraints(cell)) {
			return;
		}
		
		switch (returnType) {
		case ROWKEY:
			if (bwResults != null) {
				bwResults.add(new BytesWritable(cell.getRow()));
			}
			break;
		case QUALIFIER:
			if (bwResults != null) {
				bwResults.add(new BytesWritable(cell.getQualifier()));
			}
			break;
		case CELLVAL:
			if (bwResults != null) {
				bwResults.add(new BytesWritable(cell.getValue()));
			}
			break;
		case TIMESTAMP:
			if (tsResults != null) {
				tsResults.add(cell.getTimestamp());
			}
		default:
		}		
	}
	
	/**
	 * Check if the given cell satisfies all the constraints. 
	 * @param cell
	 * @return
	 * @throws Exception
	 */
	public boolean checkCellByConstraints(KeyValue cell) throws Exception {
		// if rowkey constraint is of 'value set' or 'range' type, then scan
		// must have been set accordingly, so we don't need to check here.
		if (rowkeyConstraint != null && !(rowkeyConstraint instanceof ValueSetIdxConstraint<?>)
				&& !(rowkeyConstraint instanceof RangeIdxConstraint<?>) && !rowkeyConstraint.satisfiedBy(cell.getRow())) {
			return false;
		}

		// if column family constraint is of 'value set' type, the scan must have been set accordingly
		if (cfConstraint != null && !(cfConstraint instanceof ValueSetIdxConstraint<?>) && !cfConstraint.satisfiedBy(cell.getFamily())) {
			return false;
		}

		if (qualConstraint != null && !qualConstraint.satisfiedBy(cell.getQualifier())) {
			return false;
		}

		if (tsConstraint != null && !(tsConstraint instanceof RangeIdxConstraint<?>) && !tsConstraint.satisfiedBy(cell.getTimestamp())) {
			return false;
		}

		if (cellValConstraint != null && !cellValConstraint.satisfiedBy(cell.getValue())) {
			return false;
		}
		
		return true;
	}
	
	public IndexDataConstraint<byte[]> getRowKeyConstraint() {
		return rowkeyConstraint;
	}
	
	public IndexDataConstraint<byte[]> getCfConstraint() {
		return cfConstraint;
	}
	
	public IndexDataConstraint<byte[]> getQualConstraint() {
		return qualConstraint;
	}
	
	public IndexDataConstraint<Long> getTsConstraint() {
		return tsConstraint;
	}
	
	public IndexDataConstraint<byte[]> getCellValConstraint() {
		return cellValConstraint;
	}
	
	public IdxOpReturnType getReturnType() {
		return returnType;
	}
	
	/**
	 * For internal testing purposes, BasicIndexOperator can be run as an independent Java application.
	 * <p/>
	 * Usage: java iu.pti.hbaseapp.BasicIndexOperator [index table name] [constraints] [bytes to string converter class name] [output directory]
	 * <p/>
	 * The program will apply [constraints] on [index table name], and write the query results to [output directory].
	 * [constraints] must be a string in the format of "CST_TARGET:CST_BODY;CST_TARGET:CST_BODY;...;CST_TARGET:CST_BODY".
	 * @param args
	 */
	@InterfaceAudience.Private
	public static void main(String[] args) {
		try {
			if (args.length < 4) {
				System.out.println("Usage: java iu.pti.hbaseapp.BasicIndexOperator <index table name> <constraints> "
						+ "<bytes to string converter class name> <output directory>");
				System.exit(1);
			}
			String indexTableName = args[0];
			String[] constraints = args[1].split(";");
			String converterClassName = args[2];
			String outputDir = MultiFileFolderWriter.getUriStrForPath(args[3]);
			BasicIndexOperator iop = new BasicIndexOperator(indexTableName, constraints);
			Set<BytesWritable> results = iop.getQueriedTableElements();
			MultiFileFolderWriter writer = new MultiFileFolderWriter(outputDir, "basicIndexOpRes", true, Constants.RESULTS_PER_FILE);
			BytesStringConverter bsConverter = Class.forName(converterClassName).asSubclass(BytesStringConverter.class).newInstance();
			for (BytesWritable bw : results) {
				writer.writeln(bsConverter.bytesToString(bw.getBytes()));
			}
			writer.close();
			System.out.println("Number of results: " + writer.getNumLinesWritten() + ", Number of files: " + writer.getNumFilesWritten());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}