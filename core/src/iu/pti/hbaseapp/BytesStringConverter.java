package iu.pti.hbaseapp;

/**
 * This interface declares two functions for make conversions between byte array and string. Useful for parsing and
 * interpreting HBase table elements, including row keys, column family names, column names, and cell values. 
 * 
 * @author gaoxm
 */
public interface BytesStringConverter {
	
	/**
	 * Convert a byte array to a string.
	 * @param bytes
	 * @return
	 */
	public String bytesToString(byte[] bytes) throws IllegalArgumentException;
	
	/**
	 * Convert a string to a byte array.	
	 * @param str
	 * @return
	 * @throws IllegalArgumentException
	 */
	public byte[] stringToBytes(String str) throws IllegalArgumentException;
}