package iu.pti.hbaseapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Given a file containing a list rows, each row containing the ID and multiple field values of an object, this
 * class can generate the value distribution of each field among all objects as one separate output file.
 * <p/>
 * For example, assume each line in the input file is in the form of <br/> 
 * [word] [document count of this word] [total frequency of this word] [related index record size of this word] <br/>
 * <p/>
 * For this input file, DistributionSeparator will generate
 * 3 output files: one for the document count distribution of all words, one for the total frequency distribution
 * of all words, and one for the index record size distribution of all words. Each line in the document count distribution
 * file will be in the form of <br/> 
 * [document count] [number of words having this document count]
 * <p/>
 * The format of the other distribution files are similar.
 * </p>
 * Usage as a Java application: java iu.pti.hbaseapp.DistributionSeparator [input file path] [output directory path]
 * 
 * @author gaoxm
 */
public class DistributionSeparator {
	/**
	 * A TermItem records the counts of different features about a term. Right now the features include number of documents
	 * containing it, the number its total appearances in all documents, and its corresponding record size in an index table. 
	 * @author gaoxm
	 */
	private class TermItem {
		@SuppressWarnings("unused")
		public String term;
		public long[] featureCounts;
		
		@SuppressWarnings("unused")
		public TermItem(String term, long[] featureCounts) {
			this.term = term;
			this.featureCounts = featureCounts;
		}
		
		public TermItem(String line) throws IllegalArgumentException{
			String[] eles = line.split("\\s+");
			if (eles.length < 2) {
				throw new IllegalArgumentException("Invalid line! A line must be a term value followed by one or more numbers, separated by tabs.");
			}
			term = eles[0];
			featureCounts = new long[eles.length - 1];
			try {
				for (int i=0; i<featureCounts.length; i++) {
					featureCounts[i] = Long.valueOf(eles[i+1]);
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Invalid line! A line must be a term value followed by one or more numbers, separated by tabs.");
			}
		}
	}
	
	/**
	 * Generate separate distribution files in outputDirPath by analyzing the file content of inputPath.
	 * @param inputPath
	 * @param outputDirPath
	 * @throws Exception
	 */
	public void analyzeFile(String inputPath, String outputDirPath) throws Exception {
		//read in the count numbers for all terms
		LinkedList<TermItem> terms = new LinkedList<TermItem>();
		BufferedReader brInput = new BufferedReader(new FileReader(inputPath));
		String line = brInput.readLine();
		while (line != null) {
			terms.add(new TermItem(line));
			line = brInput.readLine();
		}
		brInput.close();
		
		// generate the distribution of numbers in each column of the input file
		if (terms.size() <= 0) {
			return;
		}
		
		File outputDirFile = new File(outputDirPath);
		if (!outputDirFile.exists()) {
			if (!outputDirFile.mkdirs()) {
				System.err.println("Error: can't create output directory");
				return;
			}
		}
		
		int colCount = terms.getFirst().featureCounts.length;
		HashMap<Long, Integer> dist = new HashMap<Long, Integer>(terms.size() / 2);
		for (int i=0; i<colCount; i++) {
			// put distribution for ith column to hashmap
			dist.clear();
			Iterator<TermItem> iter = terms.listIterator(0);
			while (iter.hasNext()) {
				long countVal = iter.next().featureCounts[i];
				if (dist.containsKey(countVal)) {
					dist.put(countVal, dist.get(countVal) + 1);
				} else {
					dist.put(countVal, 1);
				}
			}
			
			// write hashmap to result file
			String colFilePath = outputDirPath + File.separator + "distCol" + i + ".txt";
			PrintWriter pwOut = new PrintWriter(new FileWriter(colFilePath));
			Iterator<Long> iterKey = dist.keySet().iterator();
			while (iterKey.hasNext()) {
				long key = iterKey.next();
				int freq = dist.get(key);
				pwOut.print(key);
				pwOut.print('\t');
				pwOut.println(freq);
			}
			pwOut.close();
		}
	}
	
	/**
	 * Usage: java iu.pti.hbaseapp.DistributionSeparator <input file path> <output directory path>
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: java iu.pti.hbaseapp.DistributionSeparator <input file path> <output directory path>");
			System.exit(1);
		}
		
		DistributionSeparator distSep = new DistributionSeparator();
		try {
			distSep.analyzeFile(args[0], args[1]);
			System.out.println("done!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
