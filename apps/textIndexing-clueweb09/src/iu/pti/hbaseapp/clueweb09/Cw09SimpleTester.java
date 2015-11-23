package iu.pti.hbaseapp.clueweb09;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A utility class for roughly testing something. 
 * @author gaoxm
 */
@InterfaceAudience.Private
public class Cw09SimpleTester {

	public static void main(String[] args) {
		try {
			testWarc(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testWarc(String[] args) throws Exception {
		String warcPath = args[0];
		String rankPath = args[1];
		String outputPath = args[2];
		
		// read rank
		BufferedReader brRank = new BufferedReader(new FileReader(rankPath));
		HashMap<String, String> rankMap = new HashMap<String, String>(3500000);
		String line = brRank.readLine();
		while (line != null) {
			line = line.trim();
			int idx = line.indexOf('\t');
			String id = line.substring(0, idx);
			String rank = line.substring(idx + 1);
			rankMap.put(id, rank);
			line = brRank.readLine();
		}
		brRank.close();		
		
		// get id, URL, and rank
		int count = 0;
		PrintWriter pwOut = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputPath), "UTF-8"));
		GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(warcPath));
		DataInputStream inStream;
	    inStream = new DataInputStream(gzInputStream);
	    WarcRecord thisWarcRecord;
		while ((thisWarcRecord = WarcRecord.readNextWarcRecord(inStream)) != null) {
			// see if it's a response record
			if (thisWarcRecord.getHeaderRecordType().equals("response")) {
				// it is - create a WarcHTML record
				WarcHTMLResponseRecord htmlRecord = new WarcHTMLResponseRecord(thisWarcRecord);
				// get our TREC ID and target URI
				String thisTRECID = htmlRecord.getTargetTrecID();
				String thisTargetURI = htmlRecord.getTargetURI();
				String rank = rankMap.get(thisTRECID);
				
				pwOut.println(thisTRECID + "\t" + thisTargetURI + "\t" + rank);
				count++;
				if (count % 1000 == 0) {
					System.out.println("processed " + count + " records.");
				}
			}
		}
		System.out.println("processed " + count + " records.");
		pwOut.close();
		inStream.close();
	}
}
