package iu.pti.hbaseapp;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * Container for general public helper functions.
 * 
 * @author gaoxm
 */
public class GeneralHelpers {
	/**
     * Delete a non-empty directory in a POSIX file system.
     * @param success or not
     * @return
     */
    public static boolean deleteDirectory(File dir) {
    	if (dir.exists()) {
			File[] files = dir.listFiles();
			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					if (!deleteDirectory(files[i]))
						return false;
				} else {
					if (!files[i].delete())
						return false;
				}
			}
			return dir.delete();
		}
		return true;
    }
    
    /**
     * Delete things under a directory in a POSIX file system.
     * @param dir
     * @return
     */
    public static boolean deleteStuffInDir(File dir) {
    	if (dir.exists()) {
			File[] files = dir.listFiles();
			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					if (!deleteDirectory(files[i]))
						return false;
				} else {
					if (!files[i].delete())
						return false;
				}
			}
		}
    	return true;
    }
    
    /**
     * Recursively list the files in directory, put the file paths ending with suffixMatch in to paths.
     * Works in POSIX file system.
     * @param directory
     * @param paths
     * @param suffixMatch
     */
    public static void listDirRecursive(File directory, Collection<String> paths, String suffixMatch) {
    	if (!directory.exists() || !directory.isDirectory()) {
    		return;
    	}
    	
    	File[] files = directory.listFiles();
    	for (File f : files) {
    		if (f.isDirectory()) {
    			listDirRecursive(f, paths, suffixMatch);
    		} else {
    			String p = f.getPath();
    			if (p.endsWith(suffixMatch)) {
    				paths.add(p);
    			}
    		}
    	}
    }
    
    /**
	 * Set the calendar value of theDateTime according to a string like "2008-08-08T08:08:08".
	 * @param theDateTime
	 * @param str
	 */
	public static void setDateTimeByString(Calendar theDateTime, String str) {
		str = str.trim();
		String year, month, day;
	  	int i1, i2, iT;
	  	i1 = str.indexOf("-");
	  	i2 = str.indexOf("-", i1+1);
	  	iT = str.indexOf('T');
	  	year = str.substring(0, i1);
	  	month = str.substring(i1+1, i2);
	  	day = str.substring(i2+1, iT);
	  	i1 = str.indexOf(':', iT+1);
	  	i2 = str.indexOf(':', i1+1);
	  	
	  	theDateTime.set(Calendar.MILLISECOND, 0);
	  	theDateTime.set(Calendar.SECOND, Integer.parseInt(str.substring(i2+1)));
	  	theDateTime.set(Calendar.MINUTE, Integer.parseInt(str.substring(i1+1, i2)));
	  	theDateTime.set(Calendar.HOUR_OF_DAY, Integer.parseInt(str.substring(iT+1, i1)));
	  	theDateTime.set(Calendar.DAY_OF_MONTH, 1);
	  	theDateTime.set(Calendar.MONTH, Integer.parseInt(month, 10)-1);
	  	theDateTime.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day, 10));
	  	theDateTime.set(Calendar.YEAR, Integer.parseInt(year, 10));
	}
	
	/** Get date-time from a string like "2007-03-08T08:08:08". */
	public static Calendar getDateTimeFromString(String str) { 	
	  	Calendar ret = Calendar.getInstance();
	  	setDateTimeByString(ret, str);  	
	  	return ret;
	}
	
	/**
	 * Set the calendar value of theDateTime according to a string like "2008-08-08T08:08:08".
	 * @param theDate
	 * @param str
	 */
	public static void setDateByString(Calendar theDate, String str) {
        str = str.trim();
        String year, month, day = "28";
        int i1, i2;
        i1 = str.indexOf("-");
        i2 = str.length();
        boolean hasDay = (i1 != str.lastIndexOf("-")); 
        if (hasDay) {
            i2 = str.indexOf("-", i1+1);    
        }
        year = str.substring(0, i1);
        month = str.substring(i1+1, i2);
        if (hasDay) {
            day = str.substring(i2+1);
        }
        
        theDate.set(Calendar.MILLISECOND, 0);
        theDate.set(Calendar.SECOND, 0);
        theDate.set(Calendar.MINUTE, 0);
        theDate.set(Calendar.HOUR_OF_DAY, 0);
        theDate.set(Calendar.DAY_OF_MONTH, 1);
        theDate.set(Calendar.YEAR, Integer.parseInt(year, 10));
        theDate.set(Calendar.MONTH, Integer.parseInt(month, 10)-1);
        if (hasDay) {
            theDate.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day, 10));
        }	  	
	}
    
	/**
	 * Get the "2007-02-15" alike string form of the date.
	 * 
	 * @param date
	 * @return
	 */
	public static String getDateString(Calendar date) {
		StringBuffer sb = new StringBuffer();
		
		int year = date.get(Calendar.YEAR);
		String yearStr = String.valueOf(year);
		for (int i=0; i < 4 - yearStr.length(); i++)
			sb.append('0');
		sb.append(year).append('-');
		
		int month = date.get(Calendar.MONTH) + 1;
		if (month < 10)
			sb.append('0');
		sb.append(month).append('-');
		
		int day = date.get(Calendar.DAY_OF_MONTH);
		if (day < 10)
			sb.append('0');
		sb.append(day);
		
		return sb.toString();
	}
	
	/**
	 * Get the "2007-02-15T13:23:22" alike string form of the date and time.
	 * 
	 * @param date
	 * @return
	 */
	public static String getDateTimeString(Calendar date) {
		StringBuffer sb = new StringBuffer(getDateString(date));
		sb.append('T');
		
		if (date.get(Calendar.HOUR_OF_DAY) < 10)
			sb.append('0');
		sb.append(date.get(Calendar.HOUR_OF_DAY)).append(':');
		
		if (date.get(Calendar.MINUTE) < 10)
			sb.append('0');
		sb.append(date.get(Calendar.MINUTE)).append(':');
		
		if (date.get(Calendar.SECOND) < 10)
			sb.append('0');
		sb.append(date.get(Calendar.SECOND));
		
		return sb.toString();
	}
	
	/**
	 * Write content to filePath in a POSIX file system.
	 * @param filePath
	 * @param content
	 */
	public static void writeStrToFile(String filePath, String content) {
		try {
			PrintWriter pwOut = new PrintWriter(new FileWriter(filePath));
			pwOut.write(content);
			pwOut.flush();
			pwOut.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Check if str is a string representation of a number (int, float, or hex).
	 * @param str
	 * @return
	 */
	public static boolean isNumberString(String str) {
		int dotCount = 0;
		// "other char" means characters that are not digits, ',', '.', or hex letters
		boolean hasLetterChar = false;
		boolean hasHexChar = false;
		int codeA = Character.getNumericValue('a');
		int codeF = Character.getNumericValue('f');
		str = str.toLowerCase().trim();
		int len = str.length();
		
		// first, test if it's a number like 0xffffff
		if (str.startsWith("0x")) {
			if (len > 2) {
				for (int i = 2; i < len; i++) {
					char c = str.charAt(i);
					int code = Character.getNumericValue(c);
					if (!Character.isDigit(c) && !(code >= codeA && code <= codeF)) {
						return false;
					}
				}
				return true;
			} else {
				return false;
			}
		}
		
		for (int i=0; i<len; i++) {
			char c = str.charAt(i);
			int code = Character.getNumericValue(c);
			if (!Character.isDigit(c) && c != ',') {
				if (c == '.') {
					dotCount++;
				} else if (code >= codeA && code <= codeF) {
					hasHexChar = true;
				} else if (Character.isLetter(c)) {
					hasLetterChar = true;
				}
			}			
		}
		
		if (!hasLetterChar) {
			return true;
		}
		
		if (hasLetterChar || dotCount > 1) {
			return false;
		}
		
		// if it's like a000.0b99, we don't understand it as a number
		if (hasHexChar && dotCount > 0) {
			return false;
		}
		
		return true;		
	}
	
	/**
	 * Read the lines of a text file at path to coll. Works for files in a POSIX file system.
	 * @param path
	 * @param coll
	 */
	public static void readFileToCollection(String path, Collection<String> coll) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		while (line != null) {
			line = line.trim();
			if (line.length() > 0) {
				coll.add(line);
			}			
			line = br.readLine();
		}
		br.close();
	}
	
	/** A HaseSet containing all the metacharacters for regular expressions.*/
	public static HashSet<Character> REGEX_META_CHARS = new HashSet<Character>();
	static {
		char[] regexMetaChars = {'<', '(', '[', '{', '\\', '^', '-', '=', '$', '!', '|', ']', '}', ')', '?', '*', '+', '.', '>'};
		for (char metaChar : regexMetaChars) {
			REGEX_META_CHARS.add(metaChar);
		}
	}
	/**
	 * Get the "constant prefix" of a regular expression. For example, the prefix for "http*com" is "http".
	 * @param regex
	 * @return
	 */
	public static String getPrefixForRegEx(String regex) {
		StringBuilder sb = new StringBuilder();
		int len = regex.length();
		for (int i=0; i<len; i++) {
			char c = regex.charAt(i);
			if (!REGEX_META_CHARS.contains(c)) {
				sb.append(c);
			} else {
				break;
			}
		}
		return sb.toString();
	}
	
	/**
	 * Take a descendantly sorted file as input, and generate the distribution of numbers in the file.
	 * @param sourceSortedFile
	 * @param outputDistFile
	 * @throws Exception
	 */
	public static void getSortedDist(String sourceSortedFile, String outputDistFile) throws Exception {
		BufferedReader brInput = new BufferedReader(new FileReader(sourceSortedFile));
		int lastValue = -1;
		int lastCount = -1;
		int lineCount = 0;
		Stack<Map.Entry<Integer, Integer>> distStack = new Stack<Map.Entry<Integer, Integer>>();
		String line = brInput.readLine();
		while (line != null) {
			int value = Integer.valueOf(line);
			if (value == lastValue) {
				lastCount++;
			} else {
				if (lastValue > 0) {
					distStack.push(new AbstractMap.SimpleEntry<Integer, Integer>(lastValue, lastCount));
				}
				lastValue = value;
				lastCount = 1;
			}
			line = brInput.readLine();
			
			if (lineCount++ % 1000000 == 0) {
				System.out.println(lineCount + " lines processed.");
			}
		}
		brInput.close();
		System.out.println(lineCount + " lines processed.");
		
		if (lastValue > 0) {
			distStack.push(new AbstractMap.SimpleEntry<Integer, Integer>(lastValue, lastCount));
		}
		
		PrintWriter pwDist = new PrintWriter(new FileWriter(outputDistFile));
		lineCount = 0;
		while (!distStack.isEmpty()) {
			Map.Entry<Integer, Integer> vc = distStack.pop();
			pwDist.println(vc.getKey() + "\t" + vc.getValue());
			if (lineCount++ % 200000 == 0) {
				System.out.println(lineCount + " lines written.");
			}
		}
		pwDist.close();
		System.out.println(lineCount + " lines written.");
	}
	
	/**
	 * Set the rowkey range of scan to cover a string prefix.
	 * @param scan
	 * @param prefix
	 */
	public static void setRowkeyRangeByPrefix(Scan scan, String prefix) {
		byte[] startRow = Bytes.toBytes(prefix);
		scan.setStartRow(startRow);
		boolean stopRowSet = false;
		byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
		for (int i=stopRow.length - 1; i>=0; i--) {
			if (stopRow[i] != (byte)255) {
				stopRow[i] += 1;
				stopRowSet = true;
				break;
			} else {
				stopRow[i] = 0;
			}
		}
		if (stopRowSet) {
			scan.setStopRow(stopRow);
		}
	}
	
	/**
	 * Number of bytes used to store the length of row key, column family, qualifier, and value, as well as timestamp in an
	 * HBase table cell.
	 * row length: short; family length: byte; qualifier length: int; value length: int; timestamp: long
	 */
	static int nBytesForKeyLengths = Bytes.SIZEOF_SHORT + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT + Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG;
	
	/**
	 * Get the total size in bytes of the KeyValue objects in an HBase Result object. 
	 * @param r
	 * @return
	 */
	public static int getHBaseResultKVSize(Result r) {
		return (int)r.getWritableSize();
	}
	
	/**
	 * Get a string containing the string representation of each byte in the given byte array.
	 * @param bytes
	 * @return
	 */
	public static String makeStringByEachByte(byte[] bytes) {
		StringBuilder sb = new StringBuilder();
		for (byte b : bytes) {
			sb.append(b).append(' ');
		}
		return sb.toString();
	}
	
	/**
	 * Get the terms and their positions in a given string using a Lucene analyzer.
	 * @param analyzer
	 * @param text
	 * @param fieldName
	 * @return 
	 * 	A map from each term value to a DataOutputStream, which contains a byte array in the format of [position][position]...[position].
	 */
	public static HashMap<String, ByteArrayOutputStream> getTermPosesByLuceneAnalyzer(Analyzer analyzer, String text, String fieldName) {
		// result as a map from each term value to a DataOutputStream
		HashMap<String, ByteArrayOutputStream> res = new HashMap<String, ByteArrayOutputStream>();
		HashMap<String, DataOutputStream> resHelper = new HashMap<String, DataOutputStream>();
		
		try {
			TokenStream ts = analyzer.reusableTokenStream(fieldName, new StringReader(text));
			CharTermAttribute charTermAttr = ts.addAttribute(CharTermAttribute.class);
			int pos = 0;
			while (ts.incrementToken()) {
				String termVal = charTermAttr.toString();
				if (GeneralHelpers.isNumberString(termVal)) {
					continue;
				}
				
				if (resHelper.containsKey(termVal)) {
					resHelper.get(termVal).writeInt(pos);
				} else {
					ByteArrayOutputStream bs = new ByteArrayOutputStream();
					DataOutputStream ds = new DataOutputStream(bs);
					ds.writeInt(pos);
					res.put(termVal, bs);
					resHelper.put(termVal, ds);
				}
				
				pos++;
			}
			ts.close();
			
			Iterator<String> iterTerms = resHelper.keySet().iterator();
			while (iterTerms.hasNext()) {
				String term = iterTerms.next();
				DataOutputStream dsTerm = resHelper.get(term);
				dsTerm.close();
				res.get(term).close();
			}	
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return res;
	}
	
	/**
	 * get the terms, their frequences and page numbers in all the pages in a book by using a Lucene analyzer
	 * @param analyzer
	 * @param pages
	 * @param fieldName
	 * @param freqs
	 * @return A map from each term value to a DataOutputStream, which contains a byte array in the format of 
	 * [page-num][page-num]...[page-num][frequency]. freqs will be filled with term frequencies.
	 */
	public static HashMap<String, ByteArrayOutputStream> getTermFreqPosesByLuceneAnalyzer(Analyzer analyzer, List<KeyValue> pages, 
			String fieldName, HashMap<String, Integer> freqs, boolean addFreqToPos) {
		// result as a map from each term value to a DataOutputStream
		HashMap<String, ByteArrayOutputStream> res = new HashMap<String, ByteArrayOutputStream>();
		HashMap<String, DataOutputStream> resHelper = new HashMap<String, DataOutputStream>();
		
		// for the terms in each page, this records whether the page number has been written to the index byte array for that term in the result
		HashMap<String, Boolean> termsInPage = new HashMap<String, Boolean>();
		
		try {
			Iterator<KeyValue> iterPages = pages.iterator();
			while (iterPages.hasNext()) {
				KeyValue page = iterPages.next();
				int pageNum = Bytes.toInt(page.getQualifier());
				String pageContent = Bytes.toString(page.getValue());
				termsInPage.clear();
				
				TokenStream ts = analyzer.reusableTokenStream(fieldName, new StringReader(pageContent));
				CharTermAttribute charTermAttr = ts.addAttribute(CharTermAttribute.class);
				while (ts.incrementToken()) {
					String termVal = charTermAttr.toString();
					if (GeneralHelpers.isNumberString(termVal)) {
						continue;
					}
					
					if (freqs.containsKey(termVal)) {
						freqs.put(termVal, freqs.get(termVal)+1);
					} else {
						freqs.put(termVal, 1);
					}
					
					if (resHelper.containsKey(termVal)) {
						if (!termsInPage.containsKey(termVal)) {
							resHelper.get(termVal).writeInt(pageNum);
							termsInPage.put(termVal, true);
						}
					} else {
						ByteArrayOutputStream bs = new ByteArrayOutputStream((pageNum+1)*Integer.SIZE / Byte.SIZE);
						DataOutputStream ds = new DataOutputStream(bs);
						ds.writeInt(pageNum);
						res.put(termVal, bs);
						resHelper.put(termVal, ds);
						termsInPage.put(termVal, true);
					}
				}
				ts.close();				
			}
			
			Iterator<String> iterTerms = resHelper.keySet().iterator();
			while (iterTerms.hasNext()) {
				String term = iterTerms.next();
				DataOutputStream dsTerm = resHelper.get(term);
				if (addFreqToPos) {
					dsTerm.writeInt(freqs.get(term));
				}
				dsTerm.close();
				res.get(term).close();
			}			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}
	
	/**
	 * get the terms, their frequencies and positions in a given string using a Lucene analyzer
	 * @param analyzer
	 * @param text
	 * @param fieldName
	 * @param freqs
	 * @return 
	 * 	A map from each term value to a DataOutputStream, which contains a byte array in the format of 
	 * 	[position][position]...[position][frequency]. freqs will be filled with term frequencies.
	 */
	public static HashMap<String, ByteArrayOutputStream> getTermFreqPosesByLuceneAnalyzer(Analyzer analyzer, String text, String fieldName,
			HashMap<String, Integer> freqs, boolean addFreqToPos) {
		// result as a map from each term value to a DataOutputStream
		HashMap<String, ByteArrayOutputStream> res = new HashMap<String, ByteArrayOutputStream>();
		HashMap<String, DataOutputStream> resHelper = new HashMap<String, DataOutputStream>();
		
		try {
			TokenStream ts = analyzer.reusableTokenStream(fieldName, new StringReader(text));
			CharTermAttribute charTermAttr = ts.addAttribute(CharTermAttribute.class);
			int pos = 0;
			while (ts.incrementToken()) {
				String termVal = charTermAttr.toString();
				if (GeneralHelpers.isNumberString(termVal)) {
					continue;
				}
				
				if (freqs.containsKey(termVal)) {
					freqs.put(termVal, freqs.get(termVal)+1);
				} else {
					freqs.put(termVal, 1);
				}
				
				if (resHelper.containsKey(termVal)) {
					resHelper.get(termVal).writeInt(pos);
				} else {
					ByteArrayOutputStream bs = new ByteArrayOutputStream();
					DataOutputStream ds = new DataOutputStream(bs);
					ds.writeInt(pos);
					res.put(termVal, bs);
					resHelper.put(termVal, ds);
				}
				
				pos++;
			}
			ts.close();
			
			Iterator<String> iterTerms = resHelper.keySet().iterator();
			while (iterTerms.hasNext()) {
				String term = iterTerms.next();
				DataOutputStream dsTerm = resHelper.get(term);
				if (addFreqToPos) {
					dsTerm.writeInt(freqs.get(term));
				}
				dsTerm.close();
				res.get(term).close();
			}	
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return res;
	}
	
	/**
	 * Get the terms in a string and their frequences by using a Lucene analyzer
	 * @param analyzer
	 * @param s
	 * @param fieldName
	 * @return
	 */
	public static HashMap<String, Integer> getTermFreqsByLuceneAnalyzer(Analyzer analyzer, String s, String fieldName) {
		HashMap<String, Integer> res = new HashMap<String, Integer>();
		try {
			TokenStream ts = analyzer.reusableTokenStream(fieldName, new StringReader(s));
			CharTermAttribute charTermAttr = ts.addAttribute(CharTermAttribute.class);
			
			while (ts.incrementToken()) {
				String termVal = charTermAttr.toString();
				if (GeneralHelpers.isNumberString(termVal)) {
					continue;
				}
				
				if (res.containsKey(termVal)) {
					res.put(termVal, res.get(termVal)+1);
				} else {
					res.put(termVal, 1);
				}
			}
			ts.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}
	
	/**
	 * Get the terms in a string by splitting the string and lower casing and trimming each token.
	 * @param s
	 * @param splitter
	 * @return
	 */
	public static HashMap<String, Integer> getTermFreqsBySplit(String s, String splitter) {
		String[] terms = s.split(splitter);
		HashMap<String, Integer> res = new HashMap<String, Integer>(terms.length);
		for (int i=0; i<terms.length; i++) {
			String termVal = terms[i].toLowerCase().trim();
			if (res.containsKey(termVal)) {
				res.put(termVal, res.get(termVal)+1);
			} else {
				res.put(termVal, 1);
			}
		}
		return res;
	}
	
	/**
	 * Get the terms in a given string using a Lucene analyzer.
	 * @param analyzer
	 * @param text
	 * @param fieldName
	 * @param terms
	 */
	public static void getTermsByLuceneAnalyzer(Analyzer analyzer, String text, String fieldName, Set<String> terms) {
		try {
			TokenStream ts = analyzer.reusableTokenStream(fieldName, new StringReader(text));
			CharTermAttribute charTermAttr = ts.addAttribute(CharTermAttribute.class);
			while (ts.incrementToken()) {
				String termVal = charTermAttr.toString();
				terms.add(termVal);
			}
			ts.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Convert a byte array <b>bs</b> to a string representation containing all the numbers.
	 * @param bs
	 * @return
	 *  A string like "33,43,-220,57,...".
	 */
	public static String byteArrayString(byte[] bs) { 
		if (bs == null || bs.length == 0) {
			return "";
		}
		
		StringBuilder sb = new StringBuilder();
		for (byte b : bs) {
			sb.append(b).append(',');
		}
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}
	
	/**
	 * Given two sets <b>set1</b> and <b>set2</b>, return the intersection of them.
	 * @param set1
	 * @param set2
	 * @return
	 */
	public static <T> Set<T> intersect(Set<T> set1, Set<T> set2) {
		Set<T> smaller = null;
		Set<T> larger = null;
		if (set1.size() < set2.size()) {
			smaller = set1;
			larger = set2;
		} else {
			smaller = set2;
			larger = set1;
		}
				
		TreeSet<T> result = new TreeSet<T>();
		for (T e : smaller) {
			if (larger.contains(e)) {
				result.add(e);
			}
		}		
		return result;
	}
	
	/**
	 * Compute the root of the sum of squares of a collection of integers.
	 * @param values
	 * @return
	 */
	public static long sumOfSqures(Collection<Integer> values) {
		long sumSquare = 0;
		for (Integer v : values) {
			sumSquare += v * v;
		}
		return sumSquare;
	}
	
	/**
	 * Compute the root of the sum of squares of a collection of integers.
	 * @param values
	 * @return
	 */
	public static double sumOfSquresDouble(Collection<Integer> values) {
		double sumSquare = 0;
		for (Integer v : values) {
			sumSquare += (double)v.intValue() * (double)v.intValue();
		}
		return sumSquare;
	}
}
