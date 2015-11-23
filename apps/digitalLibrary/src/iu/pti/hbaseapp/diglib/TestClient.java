package iu.pti.hbaseapp.diglib;

import iu.pti.hbaseapp.GeneralHelpers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This application provides a command line interface to test text queries on book metadata and text data.
 * <p/>
 * Usage: java iu.pti.hbaseapp.TestClient {[sub-query string] [sub-query string] ...}
 * <br/>
 * Where [sub-query string] is in the form of "[field name]:[value],[value],...[value]"
 * <br/>
 * The [sub-query string]s in '{}' are optional. Once the program is running and prompting for a query, 
 * type one or multiple [sub-query string]s and press ENTER to search withe more queries.
 *  
 * @author gaoxm
 *
 */
public class TestClient {
	private static HTable titleIndexTable = null;
	private static HTable authorsIndexTable = null;
	private static HTable categoryIndexTable = null;
	private static HTable keywordsIndexTable = null;
	private static HTable textIndexTable = null;
	private static HTable publishersIndexTable = null;
	private static HTable locationIndexTable = null;
	private static HTable addInfoIndexTable = null;
	private static HTable bookBibTable = null;
	private static Configuration hbaseConfig = HBaseConfiguration.create();
		
	public static HTable getBookBibTable() throws Exception {
		if (bookBibTable == null) {
			bookBibTable = new HTable(hbaseConfig, DigLibConstants.BOOK_BIB_TABLE_NAME);
		}
		return bookBibTable;
	}	
	
	public static HTable getTitleIndexTable() throws Exception {
		if (titleIndexTable == null) {
			titleIndexTable = new HTable(hbaseConfig, DigLibConstants.TITLE_INDEX_TABLE_NAME);
		}
		return titleIndexTable;
	}

	public static HTable getAuthorsIndexTable() throws Exception {
		if (authorsIndexTable == null) {
			authorsIndexTable = new HTable(hbaseConfig, DigLibConstants.AUTHORS_INDEX_TABLE_NAME);
		}
		return authorsIndexTable;
	}

	public static HTable getCategoryIndexTable() throws Exception {
		if (categoryIndexTable == null) {
			categoryIndexTable = new HTable(hbaseConfig, DigLibConstants.CATEGORY_INDEX_TABLE_NAME);
		}
		return categoryIndexTable;
	}

	public static HTable getKeywordsIndexTable() throws Exception {
		if (keywordsIndexTable == null) {
			keywordsIndexTable = new HTable(hbaseConfig, DigLibConstants.KEYWORDS_INDEX_TABLE_NAME);
		}
		return keywordsIndexTable;
	}
	
	public static HTable getTextIndexTable() throws Exception {
		if (textIndexTable == null) {
			textIndexTable = new HTable(hbaseConfig, DigLibConstants.TEXTS_INDEX_TABLE_NAME);
		}
		return textIndexTable;
	}

	public static HTable getPublishersIndexTable() throws Exception {
		if (publishersIndexTable == null) {
			publishersIndexTable = new HTable(hbaseConfig, DigLibConstants.PUBLISHERS_INDEX_TABLE_NAME);
		}
		return publishersIndexTable;
	}

	public static HTable getLocationIndexTable() throws Exception {
		if (locationIndexTable == null) {
			locationIndexTable = new HTable(hbaseConfig, DigLibConstants.LOCATION_INDEX_TABLE_NAME);
		}
		return locationIndexTable;
	}

	public static HTable getAddInfoIndexTable() throws Exception {
		if (addInfoIndexTable == null) {
			addInfoIndexTable = new HTable(hbaseConfig, DigLibConstants.ADDINFO_INDEX_TABLE_NAME);
		}
		return addInfoIndexTable;
	}
	
	public static void usage() {
		System.out.println("java iu.pti.hbaseapp.TestClient [<sub-query string> <sub-query string> ...]");
		System.out.println("Where <sub-query string> is in the form of \"<field name>:<value>,<value>,...<value>\" ");
		System.out.println("Once the program is running and prompting for a query, type one or multiple <sub-query string>s " + 
							"and press ENTER to search withe more queries.");
	}
	
	public static void main(String[] args) throws Exception {
		TestClient tc = new TestClient();
		
		HashMap<String, Set<String>> query = new HashMap<String, Set<String>>();
		if (args.length > 0) {
			for (int i=0; i<args.length; i++) {
				addSubQueryByString(query, args[i]);
			}
		} else {
			usage();
		}
		if (query.size() > 0) {
			tc.evaluateQuery(query);
			query.clear();
		} else {
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
			while (true) {
				System.out.println("Enter query string (\\q to quit):");
				String line = in.readLine();
				if (line.equals("\\q")) {
					break;
				}
				if (line == null || line.length() <= 0) {
					continue;
				}
			
				LinkedList<String> subQueryStrings = parseQueryString(line);
				Iterator<String> iter = subQueryStrings.iterator();
				while (iter.hasNext()) {
					addSubQueryByString(query, iter.next());
				}
				tc.evaluateQuery(query);
				query.clear();
			}
		}
	}
	
	/**
	 * Break a long query string into a list of subquery strings.
	 * @param queryString
	 * 	An overall query string in the form of
	 *  <br/> a:va b:vb,vb2 "c:vc vcc,vc2 vc2" d:vd "e:ve vee,ve2" ...	 *  
	 * @return
	 * 	A list of subquery strings. Each subquery string is in the form of
	 * 	<br/>
	 *  c:vc vcc,vc2 vc2 
	 */
	protected static LinkedList<String> parseQueryString(String queryString) {
		LinkedList<String> res = new LinkedList<String>();
		// queryString could be like a:va b:vb,vb2 "c:vc vcc,vc2 vc2" d:vd "e:ve vee,ve2" ...
		queryString = queryString.trim();
		
		int idx1 = queryString.indexOf('\"');
		while (idx1 >= 0) {
			String prefix = queryString.substring(0, idx1).trim();
			String[] preSubQueryStrings = prefix.split(" \t");
			for (int i=0; i<preSubQueryStrings.length; i++) {
				res.add(preSubQueryStrings[i]);
			}
			
			int idx2 = queryString.indexOf('\"', idx1+1);
			if (idx2 < 0) {
				System.out.println("Bad query string!");
				res.clear();
				return res;
			}
			String thisSubQueryString = queryString.substring(idx1+1, idx2);
			res.add(thisSubQueryString);
			queryString = queryString.substring(idx2+1).trim();
			idx1 = queryString.indexOf('\"');
		}
		
		if (queryString.length() > 0) {
			String[] lastSubQueryStrings = queryString.split(" \t");
			for (int i=0; i<lastSubQueryStrings.length; i++) {
				res.add(lastSubQueryStrings[i]);
			}
		}
		
		return res;
	}
	
	/**
	 * Evaluate a query and print the results.
	 * @param query
	 */
	public void evaluateQuery(HashMap<String, Set<String>> query) {
		Set<byte[]> result = null;
		Set<String> fields = query.keySet();
		Iterator<String> iterFields = fields.iterator();
		while (iterFields.hasNext()) {
			String field = iterFields.next();
			try {
				HTable idxTable = null;
				if (field.equals(DigLibConstants.INDEX_OPTION_TITLE)) {
					idxTable = getTitleIndexTable();
				} else if (field.equals(DigLibConstants.INDEX_OPTION_AUTHORS)) {
					idxTable = getAuthorsIndexTable();
				} else if (field.equals(DigLibConstants.INDEX_OPTION_CATEGORY)) {
					idxTable = getCategoryIndexTable();
				} else if (field.equals(DigLibConstants.INDEX_OPTION_LOCATION)) {
					idxTable = getLocationIndexTable();
				} else if (field.equals(DigLibConstants.INDEX_OPTION_PUBLISHERS)) {
					idxTable = getPublishersIndexTable();
				} else if (field.equals(DigLibConstants.INDEX_OPTION_KEYWORDS)) {
					idxTable = getKeywordsIndexTable();
				} else if (field.equals(DigLibConstants.INDEX_OPTION_ADDITIONAL)) {
					idxTable = getAddInfoIndexTable();
				} 
				
				if (idxTable == null) {
					continue;
				}
				
				Set<String> values = query.get(field);
				Iterator<String> iterValues = values.iterator();
				while (iterValues.hasNext()) {
					// find the ids of the documents containing this value
					String value = iterValues.next();
					Get idxGet = new Get(Bytes.toBytes(value));
					Result idxRes = idxTable.get(idxGet);
					Map<byte[], byte[]> docIdMap = idxRes.getFamilyMap(Bytes.toBytes(DigLibConstants.CF_FREQUENCIES));
					Set<byte[]>	docIds = docIdMap.keySet();
					if (result == null) {
						result = docIds;
					} else {
						// new result is the intersection of result and docIds
						result = calcSetIntersection(result, docIds);
					}
				}				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("IDs of documents for the query:");
		if (result != null) {
			Iterator<byte[]> iterRes = result.iterator();
			while (iterRes.hasNext()) {
				System.out.println(Bytes.toString(iterRes.next()));
			}
		}
	}
	
	/**
	 * calculate the intersection of two sets: set1 and set2
	 * @param <T>
	 * @param set1
	 * @param set2
	 * @return
	 */
	public static <T> Set<T> calcSetIntersection(Set<T> set1, Set<T> set2) {
		if (set1.size() > set2.size()) {
			Set<T> tmp = set1;
			set1 = set2;
			set2 = tmp;
		}
		Set<T> res = new HashSet<T>();
		Iterator<T> iter = set1.iterator();
		while (iter.hasNext()) {
			T ele = iter.next();
			if (set2.contains(ele)) {
				res.add(ele);
			}
		}
		return res;
	}
	
	/**
	 * add a sub-query string in the form of [field name]:[value],[value],...[value] to query
	 * @param query
	 * @param subQueryString
	 */
	public static void addSubQueryByString(HashMap<String, Set<String>> query, String subQueryString) {
		int idx = subQueryString.indexOf(':');
		if (idx < 0) {
			return;
		}
		String field = subQueryString.substring(0, idx).toLowerCase().trim();
		String values = subQueryString.substring(idx+1);
		Set<String> valueList = GeneralHelpers.getTermFreqsBySplit(values, ",").keySet();
		Set<String> oldValueList = query.get(field);
		if (oldValueList == null) {
			query.put(field, valueList);
		} else {
			Iterator<String> iter = valueList.iterator();
			while (iter.hasNext()) {
				oldValueList.add(iter.next());
			}
		}
	}
	
	/**
	 * Take row keys from rowKeyListPath, and access tableName for the corresponding record.
	 * Repeat this for multiple times.
	 * @param tableName
	 * @param rowKeyListPath
	 * @param times
	 */
	public void testTableAccess(String tableName, String rowKeyListPath, int times) {
		try {
			HTable tableToTest = null;
			if (tableName.equals(DigLibConstants.ADDINFO_INDEX_TABLE_NAME)) {
				tableToTest = getAddInfoIndexTable();
			} else if (tableName.equals(DigLibConstants.AUTHORS_INDEX_TABLE_NAME)) {
				tableToTest = getAuthorsIndexTable();
			} else if (tableName.equals(DigLibConstants.CATEGORY_INDEX_TABLE_NAME)) {
				tableToTest = getCategoryIndexTable();
			} else if (tableName.equals(DigLibConstants.KEYWORDS_INDEX_TABLE_NAME)) {
				tableToTest = getKeywordsIndexTable();
			} else if (tableName.equals(DigLibConstants.LOCATION_INDEX_TABLE_NAME)) {
				tableToTest = getLocationIndexTable();
			} else if (tableName.equals(DigLibConstants.PUBLISHERS_INDEX_TABLE_NAME)) {
				tableToTest = getPublishersIndexTable();
			} else if (tableName.equals(DigLibConstants.TEXTS_INDEX_TABLE_NAME)) {
				tableToTest = getTextIndexTable();
			} else if (tableName.equals(DigLibConstants.TITLE_INDEX_TABLE_NAME)) {
				tableToTest = getTitleIndexTable();
			} else if (tableName.equals(DigLibConstants.BOOK_BIB_TABLE_NAME)) {
				tableToTest = getBookBibTable();
			} else {
				System.out.println("Unsupported table name: " + tableName);
				return;
			}
			
			// read file content into an arraylist
			ArrayList<String> aKeys = new ArrayList<String>();
			BufferedReader br = new BufferedReader(new FileReader(rowKeyListPath));
			String line = br.readLine();
			while (line != null) {
				if (line.length() > 0) {
					aKeys.add(line);
				}
				line = br.readLine();
			}
			br.close();
			
			int r = 0;
			for (int i=0; i<times; i++) {
				r = (int)Math.random() * aKeys.size();
				Get get = new Get(Bytes.toBytes(aKeys.get(r)));
				Result res = tableToTest.get(get);
				System.out.println("key: " + aKeys.get(r) + "; record size: " + GeneralHelpers.getHBaseResultKVSize(res));
			}			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
