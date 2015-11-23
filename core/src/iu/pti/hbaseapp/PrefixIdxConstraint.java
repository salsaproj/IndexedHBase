package iu.pti.hbaseapp;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * A string prefix constraint defined by strings like "prefix*" or "prefix?". 
 * @author gaoxm
 *
 */
public class PrefixIdxConstraint implements IndexDataConstraint<byte[]> {
	protected List<String> definition = null;
	protected String prefix = null;
	protected boolean isStarMatch = false;
	
	/**
	 * Construct a prefix constraint. The prefix and constraint type (*-match or ?-match)
	 * are decided based on the first appearance of '*' or '?'.
	 * @param definitionStr
	 * @throws IllegalArgumentException if definitionStr starts with '*' or '?'; or if it does not contain either;
	 * or if it contains both.
	 */
	public PrefixIdxConstraint(String definitionStr) throws IllegalArgumentException {
		int idxStar = definitionStr.indexOf('*');
		int idxQuest = definitionStr.indexOf('?');
		if (idxStar < 0 && idxQuest < 0) {
			throw new IllegalArgumentException("Definition string does not contain '*' or '?'.");
		}
		if (idxStar == 0 || idxQuest == 0) {
			throw new IllegalArgumentException("Invalid definition string " + definitionStr + " - it must not start with '*' or '?'.");
		}
		if (idxStar > 0 && idxQuest > 0) {
			throw new IllegalArgumentException("Invalid definition string " + definitionStr + " - it must not contain both '*' and '?'.");
		}
		definition = new LinkedList<String>();
		definition.add(definitionStr);
		isStarMatch = idxStar > 0;
		int prefixEnd = isStarMatch ? idxStar : idxQuest;				
		prefix = definitionStr.substring(0, prefixEnd);
	}
	
	@Override
	public boolean satisfiedBy(byte[] value) throws Exception {
		String valStr = Bytes.toString(value);
		if (!valStr.startsWith(prefix)) {
			return false;
		}
		if (isStarMatch) {
			return true;
		} else {
			return valStr.length() == prefix.length() + 1;
		}
	}

	/**
	 * Is this a "prefix*" type of constraint?
	 * @return
	 * 	true for star match <br/>
	 * 	false for question mark match
	 */
	public boolean isStarMatch() {
		return isStarMatch;
	}
	
	public String getPrefix() {
		return prefix;
	}
}
