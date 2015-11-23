package iu.pti.hbaseapp.clueweb09;

import org.apache.hadoop.hbase.util.Bytes;

public class Cw09Constants {
//	public static final String CF_FREQUENCIES = "frequencies";
//	public static final String CF_POSITIONS = "positions";
	
	public static final String CF_FREQUENCIES = "f";
	public static final String CF_POSITIONS = "p";
	
	public static final byte[] CF_FREQUENCIES_BYTES = Bytes.toBytes(CF_FREQUENCIES);
	public static final byte[] CF_POSITIONS_BYTES = Bytes.toBytes(CF_POSITIONS);
	
	public static final String INDEX_OPTION_TEXT = "text";
	
	public static final String TERM_DOC_COUNT_TABLE_NAME = "termDocCountTable";
//	public static final String CF_DOC_COUNTS = "docCounts";
	public static final String CF_DOC_COUNTS = "c";
	
	// Specially for the ClueWeb09 data set:
	public static final String CLUEWEB09_DATA_TABLE_NAME = "clueWeb09DataTable";
	public static final String CLUEWEB09_INDEX_TABLE_NAME = "clueWeb09IndexTable";
	public static final String CLUEWEB09_POSVEC_TABLE_NAME = "clueWeb09PosVecTable";
	public static final String CLUEWEB09_PAIRFREQ_TABLE_NAME = "clueWeb09PairFreqTable";
	public static final String CLUEWEB09_SYNONYM_TABLE_NAME = "clueWeb09SynonymTable";
	public static final String CLUEWEB09_TERM_COUNT_TABLE_NAME = "clueWeb09TermCountTable";
	public static final String CLUEWEB09_DUP_INDEX_TABLE_NAME = "clueWeb09DupIndexTable";
	
	public static final byte[] CW09_DATA_TABLE_BYTES = Bytes.toBytes(CLUEWEB09_DATA_TABLE_NAME);
	public static final byte[] CW09_INDEX_TABLE_BYTES = Bytes.toBytes(CLUEWEB09_INDEX_TABLE_NAME);
	public static final byte[] CW09_POSVEC_TABLE_BYTES = Bytes.toBytes(CLUEWEB09_POSVEC_TABLE_NAME);
	public static final byte[] CW09_PAIRFREQ_TABLE_BYTES = Bytes.toBytes(CLUEWEB09_PAIRFREQ_TABLE_NAME);
	public static final byte[] CW09_SYNONYM_TABLE_BYTES = Bytes.toBytes(CLUEWEB09_SYNONYM_TABLE_NAME);
	public static final byte[] CW09_TERM_COUNT_TABLE_BYTES = Bytes.toBytes(CLUEWEB09_TERM_COUNT_TABLE_NAME);
	public static final byte[] CW09_DUP_INDEX_TABLE_BYTES = Bytes.toBytes(CLUEWEB09_DUP_INDEX_TABLE_NAME);
	
//	public static final String CF_DETAILS = "details";
//	public static final String CF_SCORES = "scores";

	public static final String CF_DETAILS = "d";
	public static final String CF_SCORES = "s";
	
	public static final byte[] CF_DETAILS_BYTES = Bytes.toBytes(CF_DETAILS);
	public static final byte[] CF_SCORES_BYTES = Bytes.toBytes(CF_SCORES);
	
//	public static final String QUALIFIER_URI = "URI";
//	public static final String QUALIFIER_CONTENT = "content";
//	public static final String QUALIFIER_MIN = "min";
//	public static final String QUALIFIER_PLUS = "plus";
//	public static final String QUALIFIER_FORWARD = "->";
//	public static final String QUALIFIER_BACKWARD = "<-";
//	public static final String QUALIFIER_COUNT = "count";
//	public static final String QUALIFIER_TERM1COUNT = "t1Count";
//	public static final String QUALIFIER_TERM2COUNT = "t2Count";
	
	public static final String QUALIFIER_URI = "U";
	public static final String QUALIFIER_CONTENT = "t";
	public static final String QUALIFIER_MIN = "m";
	public static final String QUALIFIER_PLUS = "p";
	public static final String QUALIFIER_FORWARD = ">";
	public static final String QUALIFIER_BACKWARD = "<";
	public static final String QUALIFIER_COUNT = "c";
	public static final String QUALIFIER_TERM1COUNT = "1";
	public static final String QUALIFIER_TERM2COUNT = "2";
	
	public static final byte[] QUAL_CONTENT_BYTES = Bytes.toBytes(QUALIFIER_CONTENT);
	public static final byte[] QUAL_URI_BYTES = Bytes.toBytes(QUALIFIER_URI);
	public static final byte[] QUAL_MIN_BYTES = Bytes.toBytes(QUALIFIER_MIN);
	public static final byte[] QUAL_PLUS_BYTES = Bytes.toBytes(QUALIFIER_PLUS);
	public static final byte[] QUAL_FORWARD_BYTES = Bytes.toBytes(QUALIFIER_FORWARD);
	public static final byte[] QUAL_BACKWARD_BYTES = Bytes.toBytes(QUALIFIER_BACKWARD);
	public static final byte[] QUAL_COUNT_BYTES = Bytes.toBytes(QUALIFIER_COUNT);
	public static final byte[] QUAL_TERM1COUNT_BYTES = Bytes.toBytes(QUALIFIER_TERM1COUNT);
	public static final byte[] QUAL_TERM2COUNT_BYTES = Bytes.toBytes(QUALIFIER_TERM2COUNT);
	
	// html-text extractor:	
	public static final HTMLTextParser txtExtractor = new HTMLTextParser();
	
	// threshold value for identifying synonyms
	public static final double DEFAULT_SYNONYM_THRESHOLD = 0.000001;
	
	public static final int CW09_INDEX_SCAN_BATCH = 10000;
}
