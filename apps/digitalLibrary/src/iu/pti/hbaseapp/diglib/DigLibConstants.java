package iu.pti.hbaseapp.diglib;

import org.apache.hadoop.hbase.util.Bytes;

public class DigLibConstants {
	public static final String BOOK_IMAGE_CF_IMAGE = "image";
	public static final String BOOK_IMAGE_TABLE_NAME = "bookImageTable";
	public static final String BOOK_TEXT_CF_PAGES = "pages";
	public static final String BOOK_TEXT_TABLE_NAME = "bookTextTable";
	public static final String BOOK_BIB_CF_METADATA = "md";
	public static final String BOOK_BIB_TABLE_NAME = "bookBibTable";

	public static final String TEXTS_INDEX_TABLE_NAME = "textsIndexTable";
	public static final String TEXTS_POSVEC_TABLE_NAME = "textsPosVecTable";
	public static final String KEYWORDS_INDEX_TABLE_NAME = "keywordsIndexTable";
	public static final String AUTHORS_INDEX_TABLE_NAME = "authorsIndexTable";
	public static final String CATEGORY_INDEX_TABLE_NAME = "categoryIndexTable";
	public static final String TITLE_INDEX_TABLE_NAME = "titleIndexTable";
	public static final String PUBLISHERS_INDEX_TABLE_NAME = "publishersIndexTable";
	public static final String LOCATION_INDEX_TABLE_NAME = "locationIndexTable";
	public static final String ADDINFO_INDEX_TABLE_NAME = "addInfoIndexTable";

	// public static final String CF_FREQUENCIES = "frequencies";
	// public static final String CF_POSITIONS = "positions";

	public static final String CF_FREQUENCIES = "f";
	public static final String CF_POSITIONS = "p";

	public static final byte[] CF_FREQUENCIES_BYTES = Bytes.toBytes(CF_FREQUENCIES);
	public static final byte[] CF_POSITIONS_BYTES = Bytes.toBytes(CF_POSITIONS);

	public static final String QUALIFIER_KEYWORDS = "Keywords";
	public static final String QUALIFIER_ADDITIONAL = "Additional";
	public static final String QUALIFIER_LOCATION = "Location";
	public static final String QUALIFIER_PUBLISHERS = "Publishers";
	public static final String QUALIFIER_CATEGORY = "Category";
	public static final String QUALIFIER_AUTHORS = "Authors";
	public static final String QUALIFIER_TITLE = "Title";
	public static final String QUALIFIER_YEAR = "CreatedYear";
	public static final String QUALIFIER_START_PAGE = "StartPage";
	public static final String QUALIFIER_CURRENT_PAGE = "CurrentPage";
	public static final String QUALIFIER_ISBN = "ISBN";
	public static final String QUALIFIER_CONTENT_DIR = "DirPath";

	public static final String VAL_NON_AVAILABLE = "N/A";

	public static final String INDEX_OPTION_TITLE = "title";
	public static final String INDEX_OPTION_AUTHORS = "authors";
	public static final String INDEX_OPTION_CATEGORY = "category";
	public static final String INDEX_OPTION_PUBLISHERS = "publishers";
	public static final String INDEX_OPTION_LOCATION = "location";
	public static final String INDEX_OPTION_ADDITIONAL = "additional";
	public static final String INDEX_OPTION_KEYWORDS = "keywords";
	public static final String INDEX_OPTION_TEXT = "text";

	public static final String TERM_DOC_COUNT_TABLE_NAME = "termDocCountTable";
	// public static final String CF_DOC_COUNTS = "docCounts";
	public static final String CF_DOC_COUNTS = "c";
	
	protected static final String CSV_SPLITTER = "###";
}
