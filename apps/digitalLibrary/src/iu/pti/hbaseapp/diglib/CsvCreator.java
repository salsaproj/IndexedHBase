package iu.pti.hbaseapp.diglib;

import java.io.File;

import java.io.FileWriter;
import java.io.PrintWriter;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * This application read in a set of XML files in the "articles" directory of the digital library dataset,
 * and save the metadata contained in these XML files to a .csv file, which will then be read by 
 * SampleUploader to save the metadata in a HBase table.
 * </p>
 * Usage as a Java application: java iu.pti.hbaseapp.diglib.CsvCreator [directory for XMLs] [result CSV file]
 * <p/>
 * @author gaoxm
 *
 */
public class CsvCreator {
	
	public static void usage() {
		System.out.println("Usage: java iu.pti.hbaseapp.CsvCreator <directory for XMLs> <result CSV file>");
	}
	
	/**
	 * Usage: java iu.pti.hbaseapp.diglib.CsvCreator [directory for XMLs] [result CSV file]
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			usage();
		}
		String xmlDir = args[0];
		String csvPath = args[1];
		File xmlDirFile = new File(xmlDir);
		if (xmlDirFile.isFile()) {
			System.out.println("Invalid directory for XMLs.");
			System.exit(1);
		}
		try {
			PrintWriter pwCsv = new PrintWriter(new FileWriter(csvPath));
			File[] xmlFiles = xmlDirFile.listFiles();
			SAXReader xmlReader = new SAXReader();
			for (int i=0; i<xmlFiles.length; i++) {
				String fileName = xmlFiles[i].getName();
				if (fileName.toLowerCase().endsWith("xml")) {
					System.out.println("Processing file " + fileName + "...");
					Document xmlDoc = xmlReader.read(xmlFiles[i]);
					Element eleMeta = xmlDoc.getRootElement();
					Element eleBib = eleMeta.element("Bibliography");
					String category = eleBib.elementTextTrim("Category");
					String title = eleBib.elementTextTrim("Title");					
					String authors = eleBib.elementTextTrim("Authors");
					String createdYear = eleBib.elementTextTrim("CreatedYear");
					String publishers = eleBib.elementTextTrim("Publishers");
					String location = eleBib.elementTextTrim("Location");
					String startPage = eleBib.elementTextTrim("StartPage");
					String currentPage = eleBib.elementTextTrim("CurrentPage");
					String isbn = eleBib.elementTextTrim("ISBN");
					String additional = eleBib.elementTextTrim("Additional");
					String dirPath = eleBib.elementTextTrim("ImageFileLocation");
					String keywords = eleBib.elementTextTrim("Keywords");
					
					String row = fileName.substring(0, fileName.lastIndexOf('.'));
					
					// column family is always "md"
					String linePrefix = row + DigLibConstants.CSV_SPLITTER + DigLibConstants.BOOK_BIB_CF_METADATA + DigLibConstants.CSV_SPLITTER;
					if (!title.equals(DigLibConstants.VAL_NON_AVAILABLE)) {
						pwCsv.println(linePrefix + DigLibConstants.QUALIFIER_TITLE + DigLibConstants.CSV_SPLITTER + title);
					}
					if (!category.equalsIgnoreCase(DigLibConstants.VAL_NON_AVAILABLE)) {
						pwCsv.println(linePrefix + DigLibConstants.QUALIFIER_CATEGORY + DigLibConstants.CSV_SPLITTER + category);
					}
					if (!authors.equalsIgnoreCase(DigLibConstants.VAL_NON_AVAILABLE)) {
						pwCsv.println(linePrefix + DigLibConstants.QUALIFIER_AUTHORS + DigLibConstants.CSV_SPLITTER + authors);
					}
					if (!createdYear.equalsIgnoreCase(DigLibConstants.VAL_NON_AVAILABLE)) {
						pwCsv.println(linePrefix + DigLibConstants.QUALIFIER_YEAR + DigLibConstants.CSV_SPLITTER + createdYear);
					}
					if (!publishers.equalsIgnoreCase(DigLibConstants.VAL_NON_AVAILABLE)) {
						pwCsv.println(linePrefix + DigLibConstants.QUALIFIER_PUBLISHERS + DigLibConstants.CSV_SPLITTER + publishers);
					}
					if (!location.equalsIgnoreCase(DigLibConstants.VAL_NON_AVAILABLE)) {
						pwCsv.println(linePrefix + DigLibConstants.QUALIFIER_LOCATION + DigLibConstants.CSV_SPLITTER + location);
					}
					if (!startPage.equalsIgnoreCase(DigLibConstants.VAL_NON_AVAILABLE)) {
						pwCsv.println(linePrefix + DigLibConstants.QUALIFIER_START_PAGE + DigLibConstants.CSV_SPLITTER + startPage);
					}
					if (!currentPage.equalsIgnoreCase(DigLibConstants.VAL_NON_AVAILABLE)) {
						pwCsv.println(linePrefix + DigLibConstants.QUALIFIER_CURRENT_PAGE + DigLibConstants.CSV_SPLITTER + currentPage);
					}
					if (!isbn.equalsIgnoreCase(DigLibConstants.VAL_NON_AVAILABLE)) {
						pwCsv.println(linePrefix + DigLibConstants.QUALIFIER_ISBN + DigLibConstants.CSV_SPLITTER + isbn);
					}
					if (!additional.equalsIgnoreCase(DigLibConstants.VAL_NON_AVAILABLE)) {
						pwCsv.println(linePrefix + DigLibConstants.QUALIFIER_ADDITIONAL + DigLibConstants.CSV_SPLITTER + additional);
					}
					if (!dirPath.equalsIgnoreCase(DigLibConstants.VAL_NON_AVAILABLE)) {
						pwCsv.println(linePrefix + DigLibConstants.QUALIFIER_CONTENT_DIR + DigLibConstants.CSV_SPLITTER + dirPath);
					}
					if (!keywords.equalsIgnoreCase(DigLibConstants.VAL_NON_AVAILABLE)) {
						pwCsv.println(linePrefix + DigLibConstants.QUALIFIER_KEYWORDS + DigLibConstants.CSV_SPLITTER + keywords);
					}
				}
			}
			pwCsv.flush();
			pwCsv.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}