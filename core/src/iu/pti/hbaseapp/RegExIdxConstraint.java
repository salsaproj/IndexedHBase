package iu.pti.hbaseapp;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Regular expression constraint defined by a regular expression.
 * @author gaoxm
 *
 * @param <T>
 */
public class RegExIdxConstraint implements IndexDataConstraint<byte[]> {
	String regex = null;
	Pattern pattern = null;
	Matcher matcher = null;

	/**
	 * Construct a RegExIdxConstraint with a regular expression string.
	 * @param regex
	 * @throws IllegalArgumentException
	 */
	public RegExIdxConstraint(String regex) throws IllegalArgumentException {
		try {
			pattern = Pattern.compile(regex);
			this.regex = regex;
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	@Override
	public boolean satisfiedBy(byte[] value) throws Exception {
		String valStr = Bytes.toString(value);
		if (matcher == null) {
			matcher = pattern.matcher(valStr);
		} else {
			matcher.reset(valStr);
		}
		return matcher.matches();
	}

	/**
	 * Get the regular expression string.
	 * @return
	 */
	public String getRegEx() {
		return regex;
	}
}
