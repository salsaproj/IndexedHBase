package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.BytesStringConverter;

/**
 * For conversions between twitter IDs in bytes and strings.
 * 
 * @author gaoxm
 */
public class TweetIdStringConverter implements BytesStringConverter {

	@Override
	public String bytesToString(byte[] bytes) throws IllegalArgumentException {
		try {
			return TruthyHelpers.getTweetIDStrFromBytes(bytes);
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public byte[] stringToBytes(String str) throws IllegalArgumentException {
		try {
			return TruthyHelpers.getTweetIdBytes(str);
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

}
