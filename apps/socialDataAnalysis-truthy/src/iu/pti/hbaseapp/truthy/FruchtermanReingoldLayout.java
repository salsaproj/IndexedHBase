package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.GeneralHelpers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * A sequential implementation of the Fruchterman-Reingold graph layout algorithm.
 * 
 * @author gaoxm
 */
public class FruchtermanReingoldLayout {
	public static class AdjMtxLine implements Comparable<AdjMtxLine> {
		String line;
		int edgeCount;
		
		public AdjMtxLine(String line) {
			this.line = line;
			edgeCount = 0;
			// each line is like "<vertext id> <neighbor id> <neighbor id> ..."
			char[] chars = line.toCharArray();
			for (char c : chars) {
				if (c == '\t') {
					edgeCount++;
				}
			}
		}
		
		public AdjMtxLine(String line, int edgeCount) { 
			this.line = line;
			this.edgeCount = edgeCount;
		}
		
		public String getLine() {
			return line;
		}

		public void setLine(String line) {
			this.line = line;
		}

		public int getEdgeCount() {
			return edgeCount;
		}

		public void setEdgeCount(int edgeCount) {
			this.edgeCount = edgeCount;
		}
		
		@Override
		public int compareTo(AdjMtxLine that) {
			if (this.edgeCount == that.edgeCount) {
				return this.line.compareTo(that.line);
			} else {
				return this.edgeCount - that.edgeCount;
			}
		}
	}
	
	Map<String, Integer> vertexNameToIdMap = null;
	String[] vertexNames = null;
	int[][] graphEdges = null;
	float[][] positions = null;
	String outputPath = null;
	
	public FruchtermanReingoldLayout(String adjMtxPath, String nodeListPath) throws Exception {
		//process mapping information between vertex ID and name
		File fNodeList = new File(nodeListPath);
		if (!fNodeList.exists() || fNodeList.isDirectory()) {
			throw new IllegalArgumentException("Invalide node list path " + nodeListPath);
		}
		vertexNameToIdMap = new HashMap<String, Integer>();
		BufferedReader brNodes = new BufferedReader(new FileReader(fNodeList));
		int id = 0;
		String line = brNodes.readLine();
		while (line != null) {
			line = line.trim();
			if (line.length() > 0) {
				vertexNameToIdMap.put(line, id);
				id++;
			}
			line = brNodes.readLine();
		}
		brNodes.close();
		vertexNames = new String[vertexNameToIdMap.size()];
		for (Map.Entry<String, Integer> e : vertexNameToIdMap.entrySet()) {
			vertexNames[e.getValue()] = e.getKey();
		}
		
		//process adjacency matrix of the graph
		File fAdjMtx = new File(adjMtxPath);
		if (!fAdjMtx.exists()) {
			throw new IllegalArgumentException("Adjacency matrix path " + adjMtxPath + " does not exist!");
		}		
		List<String> graphStrs = new LinkedList<String>();
		if (fAdjMtx.isFile()) {
			GeneralHelpers.readFileToCollection(adjMtxPath, graphStrs);
			outputPath = fAdjMtx.getParentFile().getAbsolutePath() + File.separator + "layout.txt";
		} else {
			File[] inputFiles = fAdjMtx.listFiles();
			for (File f : inputFiles) {
				if (f.getName().startsWith("part-")) {
					GeneralHelpers.readFileToCollection(f.getAbsolutePath(), graphStrs);
				}
			}
			outputPath = fAdjMtx.getAbsolutePath() + File.separator + "layout.txt";
		}
		
		// the total number of lines in the input files is the total number of vertices in the graph
		int vc = graphStrs.size();		
		graphEdges = new int[vc][];
		positions = new float[vc][2];
		
		for (String s : graphStrs) {
			// each "s" is like "<this vertex name> <neighbor name> <neighbor name> <neighbor name> ..." 
			String[] vNames = s.trim().split("\t");
			String vThis = vNames[0];
			int thisId = vertexNameToIdMap.get(vThis);
			
			// each row of graphEdges corresponds to one vertex, and contains the IDs of its neighbor vertices
			graphEdges[thisId] = new int[vNames.length - 1];
			for (int i=1; i<vNames.length; i++) {
				String vNeighbor = vNames[i];
				int neighborId = vertexNameToIdMap.get(vNeighbor);
				graphEdges[thisId][i-1] = neighborId;
			}
		}
		
		/*
		for (int i=0; i<graphEdges.length; i++) {
			System.out.print(i + "(" + vertexNames[i] + "): ");
			for (int j=0; j<graphEdges[i].length; j++) {
				int neighborId = graphEdges[i][j];
				System.out.print(neighborId + "(" + vertexNames[neighborId] + "), ");
			}
			System.out.println();
		} */
	}
	
	public void layout(int iterations) throws Exception {
		if (iterations <= 0) {
			throw new IllegalArgumentException("The parameter 'iterations' is not positive!");
		}
		long startTime = System.currentTimeMillis();
		
		int vc = graphEdges.length;
		float area = vc * vc;
		float maxDelta = vc;
		float coolExp = 1.5f;
		//float repulseRad = area * vc;
		float k = (float)Math.sqrt(area / vc);
		float ks = vc;
		Random r = new Random();
		
		// random initial positions
		for (float[] p : positions) {
			p[0] = (float)(r.nextFloat() * 2 - 1.0);
			p[1] = (float)(r.nextFloat() * 2 - 1.0);
		}		
		float[][] disps = new float[vc][2];
		
		for (int i=iterations; i>0; i--) {
			System.out.print("Start iteration " + (iterations - i) + "...");
					
			//clear displacements for this iteration
			for (float[] disp : disps) {
				disp[0] = disp[1] = 0;
			}
			
			int rzCount = 0;
			int azCount = 0;
			//calculate repulsive forces and displacements
			for (int m=0; m<positions.length; m++) {
				for (int n=0; n<positions.length; n++) {
					if (m == n) {
						continue;
					}
					
					float xd = positions[m][0] - positions[n][0];
					float yd = positions[m][1] - positions[n][1];
					float dedS = xd * xd + yd * yd;
					float ded = (float)Math.sqrt(dedS);
					float rf = 0;
					if (ded != 0) {
						/*
						xd /= ded;
						yd /= ded;
						rf = ks * (float)(1.0 / ded - dedS / repulseRad);
						disps[m][0] += xd * rf;
						disps[m][1] += yd * rf;
						*/						
						disps[m][0] += xd * (ks / dedS - ded / area);
						disps[m][1] += yd * (ks / dedS - ded / area);
					} else {
						rzCount++;
						xd = (float)(r.nextGaussian() * 0.1);
						yd = (float)(r.nextGaussian() * 0.1);
						rf = (float)(r.nextGaussian() * 0.1);
						disps[m][0] += xd * rf;
						disps[m][1] += yd * rf;
					}					
				}
			}
			
			//calculate attractive forces and displacements
		    for (int m=0; m<graphEdges.length; m++) {
		    	int[] neighborIds = graphEdges[m];
		    	for (int n=0; n<neighborIds.length; n++) {
		    		int nid = neighborIds[n];
		    		float xd = positions[m][0] - positions[nid][0];
		    		float yd = positions[m][1] - positions[nid][1];
		    		float ded = (float)Math.sqrt(xd * xd + yd * yd);
		    		float af = 0;
		    		if (ded != 0) {
		    			/*
		    			xd /= ded;
		    			yd /= ded;
		    			af = ded * ded / k;
		    			disps[m][0] -= xd * af;
			    		disps[m][1] -= yd * af;
			    		*/
		    			disps[m][0] -= xd * ded / k;
			    		disps[m][1] -= yd * ded / k;
		    		} else {
		    			azCount++;
		    			xd = (float)(r.nextGaussian() * 0.1);
						yd = (float)(r.nextGaussian() * 0.1);
						af = (float)(r.nextGaussian() * 0.1);
						disps[m][0] -= xd * af;
			    		disps[m][1] -= yd * af;
		    		}		    		
		    	}
		    }
		    
		    // Move the points with displacements limited by temperature   
			float t = maxDelta * (float)Math.pow(i/(double)iterations, coolExp);	
		    for (int m=0; m<positions.length; m++) {
		    	float ded = (float)Math.sqrt(disps[m][0] * disps[m][0] + disps[m][1] * disps[m][1]);
		    	if (ded > t) {
		    		ded = t / ded;
		    		disps[m][0] *= ded;
		    		disps[m][1] *= ded;
		    	}
		    	positions[m][0] += disps[m][0];
		    	positions[m][1] += disps[m][1];
		    }
			
			System.out.println("Done! zero count for repulsive: " + rzCount + ", zero count for attractive: " + azCount);
		} 
		
		float minX = 0;
		float maxX = 0;
		float minY = 0;
		float maxY = 0;
		PrintWriter pwOut = new PrintWriter(new FileWriter(outputPath));
		for (int m=0; m<positions.length; m++) {
			float x = positions[m][0];
			float y = positions[m][1];
			if (x < minX)
				minX = x;
			if (x > maxX)
				maxX = x;
			if (y < minY)
				minY = y;
			if (y > maxY)
				maxY = y;
			pwOut.println(x + " " + y);
		}
		pwOut.close();
		System.out.println("minX: " + minX + ", maxX: " + maxX + ", minY: " + minY + ", maxY: " + maxY);
		
		long endTime = System.currentTimeMillis();
		float timeUsed = (endTime - startTime) / 1000.0f;
		System.out.println(timeUsed + " seconds used for " + iterations + " iteratirons.");
	}
	
	public static void convertNameToIdInMtx(String adjMtxDir, String nodeListPath) throws Exception {
		// read mapping from vertex names to IDs
		Map<String, Integer> vertexNameToIdMap = new HashMap<String, Integer>();
		BufferedReader brNodes = new BufferedReader(new FileReader(nodeListPath));
		int id = 0;
		String line = brNodes.readLine();
		while (line != null) {
			line = line.trim();
			if (line.length() > 0) {
				vertexNameToIdMap.put(line, id);
				id++;
			}
			line = brNodes.readLine();
		}
		brNodes.close();
		
		// convert adjacency matrix files one by one
		File fAdjMtx = new File(adjMtxDir);		
		File[] inputFiles = fAdjMtx.listFiles();
		for (File f : inputFiles) {
			if (f.getName().startsWith("part-")) {
				System.out.println("Processing " + f.getName() + "...");
				List<String> graphStrs = new LinkedList<String>();
				GeneralHelpers.readFileToCollection(f.getAbsolutePath(), graphStrs);
				String outputPath = f.getAbsolutePath() + ".vid";
				PrintWriter pwOut = new PrintWriter(new FileWriter(outputPath));
				for (String s : graphStrs) {
					String[] names = s.split("\\t");
					StringBuilder sb = new StringBuilder();
					for (String vName : names) {
						if (vName.length() <= 0) {
							continue;
						}
						int vid = vertexNameToIdMap.get(vName);
						sb.append(vid).append('\t');
					}
					sb.deleteCharAt(sb.length() - 1);
					pwOut.println(sb.toString());
				}
				pwOut.close();
			}
		}
		System.out.println("Done!");
	}
	
	public static void splitAdjMtx(String adjMtxFilePath, int splitCount) throws Exception {
		List<String> graphStrs = new LinkedList<String>();
		GeneralHelpers.readFileToCollection(adjMtxFilePath, graphStrs);
		int splitSize = graphStrs.size() / splitCount;
		if (graphStrs.size() % splitCount != 0) {
			splitSize++;
		}
		
		File fAdjMtx = new File(adjMtxFilePath);
		String folder = fAdjMtx.getParentFile().getAbsolutePath();
		PrintWriter pwSplit = null;
		int lines = 0;
		int fileNum = 0;
		for (String s : graphStrs) {
			if (pwSplit == null) {
				System.out.println("Creating file split-" + fileNum + ".txt");
				pwSplit = new PrintWriter(new FileWriter(folder + File.separator + "split-" + fileNum + ".txt"));
				fileNum++;
				lines = 0;
			}
			pwSplit.println(s);
			lines++;
			if (lines >= splitSize) {
				pwSplit.close();
				pwSplit = null;
				lines = 0;
			}
		}
		if (pwSplit != null) {
			pwSplit.close();
		}
		
		System.out.println("Done!");		
	}
	
	/**
	 * Count the total number of edges in a sub adjacency matrix.
	 * @param s
	 * @return
	 */
	public static int countEdgesInAmlSet(Set<AdjMtxLine> s) {
		int count = 0;
		for (AdjMtxLine aml : s) {
			count += aml.getEdgeCount();
		}
		return count;
	}
	
	/**
	 * Adjust the elements in ascendSet and descendSet so that the total number of edges are as close
	 * as possible between the two.
	 * @param ascendSet
	 * @param descendSet
	 */
	public static void balanceTwoAmlSets(TreeSet<AdjMtxLine> ascendSet, TreeSet<AdjMtxLine> descendSet) {
		int asEdgeCount = countEdgesInAmlSet(ascendSet);
		int dsEdgeCount = countEdgesInAmlSet(descendSet);
		
		int startDiff = dsEdgeCount - asEdgeCount;
		if (startDiff == 0) {
			return;
		}
		System.out.println("difference before adjustment : " + startDiff + ", set sizes: " + ascendSet.size() + "," + descendSet.size());
		
		// find potential swaps
		int diff = startDiff;
		List<AdjMtxLine[]> swaps = new LinkedList<AdjMtxLine[]>();
		Iterator<AdjMtxLine> ias = ascendSet.iterator();
		Iterator<AdjMtxLine> ids = descendSet.descendingIterator();
		while (ias.hasNext() && ids.hasNext() && diff != 0) {
			AdjMtxLine amlA = ias.next();
			AdjMtxLine amlD = ids.next();
			int aEdgeCount = amlA.getEdgeCount();
			int dEdgeCount = amlD.getEdgeCount();
			if (aEdgeCount == dEdgeCount) {
				continue;
			}
			int newDiff = diff - 2 * dEdgeCount + 2 * aEdgeCount;
			if (Math.abs(newDiff) < Math.abs(diff)) {
				AdjMtxLine[] swap = {amlA, amlD};
				swaps.add(swap);
				diff = newDiff;
			}
		}
		System.out.println("number of potential swaps: " + swaps.size() + ", potential diff after swaps: " + diff);
		
		// do swaps
		for (AdjMtxLine[] swap : swaps) {
			ascendSet.remove(swap[0]);
			ascendSet.add(swap[1]);
			descendSet.remove(swap[1]);
			descendSet.add(swap[0]);			
		}
		
		// check result
		asEdgeCount = countEdgesInAmlSet(ascendSet);
		dsEdgeCount = countEdgesInAmlSet(descendSet);
		int endDiff = dsEdgeCount - asEdgeCount;
		System.out.println("difference after adjustment : " + endDiff + ", set sizes: " + ascendSet.size() + "," + descendSet.size());		
	}
	
	/**
	 * Split the given adjacency matrix to a given number of sub-matrixes.
	 * @param s
	 * @param numOfSubSet
	 * @return
	 */
	public static List<TreeSet<AdjMtxLine>> splitAmlSet(TreeSet<AdjMtxLine> s, int numOfSubSet) {
		List<TreeSet<AdjMtxLine>> subs = new ArrayList<TreeSet<AdjMtxLine>>(numOfSubSet);
		for (int i=0; i<numOfSubSet; i++) {
			subs.add(new TreeSet<AdjMtxLine>());
		}
		
		Iterator<AdjMtxLine> iterLines = s.descendingIterator();
		boolean forward = true;
		while (true) {
			if (forward) {
				for (int i=0; i<subs.size(); i++) {
					if (iterLines.hasNext()) {
						subs.get(i).add(iterLines.next());
					} else {
						break;
					}
				}
			} else {
				for (int i=subs.size()-1; i>=0; i--) {
					if (iterLines.hasNext()) {
						subs.get(i).add(iterLines.next());
					} else {
						break;
					}
				}
			}
			if (!iterLines.hasNext()) {
				break;
			}
			forward = !forward;
		}
		return subs;		
	}
	
	/**
	 * Check the node and edge distribution in 
	 * @param splitDir
	 */
	public static void checkDataDistInSplitDir(String splitDir) throws Exception {
		File fSplitDir = new File(splitDir);
		File[] fSplits = fSplitDir.listFiles();
		int fCount = fSplits.length;
		int maxNodeCount = -1;
		int minNodeCount = -1;
		double avgNodeCount = -1;
		int maxEdgeCount = -1;
		int minEdgeCount = -1;
		double avgEdgeCount = -1;
		int totalNodeCount = 0;
		int totalEdgeCount = 0;
		for (File fSplit : fSplits) {
			int nodeCount = 0;
			int edgeCount = 0;
			BufferedReader br = new BufferedReader(new FileReader(fSplit));
			String line = br.readLine();
			while (line != null) {
				line = line.trim();
				if (line.length() > 0) {
					AdjMtxLine aml = new AdjMtxLine(line);
					nodeCount++;
					edgeCount += aml.getEdgeCount();
				}
				line = br.readLine();
			}
			br.close();
			
			totalNodeCount += nodeCount;
			totalEdgeCount += edgeCount;
			if (maxNodeCount < 0 || nodeCount > maxNodeCount) {
				maxNodeCount = nodeCount;
			}
			if (minNodeCount < 0 || nodeCount < minNodeCount) {
				minNodeCount = nodeCount;
			}
			if (maxEdgeCount < 0 || edgeCount > maxEdgeCount) {
				maxEdgeCount = edgeCount;
			}
			if (minEdgeCount < 0 || edgeCount < minEdgeCount) {
				minEdgeCount = edgeCount;
			}
		}
		avgNodeCount = totalNodeCount * 1.0 / fCount;
		avgEdgeCount = totalEdgeCount * 1.0 / fCount;
		System.out.println("totalNodeCount: " + totalNodeCount + ", maxNodeCount: " + maxNodeCount + ", minNodeCount: " + minNodeCount
				+ ", avgNodeCount: " + avgNodeCount);
		System.out.println("totalEdgeCount: " + totalEdgeCount + ", maxEdgeCount: " + maxEdgeCount + ", minEdgeCount: " + minEdgeCount
				+ ", avgEdgeCount: " + avgEdgeCount);
	}
	
	/**
	 * Partition the adjacency matrix into splitCount files, so that each file contains the same number of vertices (i.e. lines),
	 * and the number of edges in each file is more balanced.
	 * @param adjMtxFilePath
	 * @param splitCount
	 * @throws Exception
	 */
	public static void splitAdjMtxInBalance(String adjMtxFilePath, int splitCount) throws Exception {
		// sort lines in the adjacency matrix reversely according to the number of edges each line contains
		System.out.println("sorting...");
		TreeSet<AdjMtxLine> sortedLines = new TreeSet<AdjMtxLine>();
		BufferedReader br = new BufferedReader(new FileReader(adjMtxFilePath));
		String line = br.readLine();
		while (line != null) {
			line = line.trim();
			if (line.length() > 0) {
				sortedLines.add(new AdjMtxLine(line));
			}
			line = br.readLine();
		}
		br.close();
		
		File fAdjMtx = new File(adjMtxFilePath);
		String folder = fAdjMtx.getParentFile().getAbsolutePath();
		List<TreeSet<AdjMtxLine>> globalList = new LinkedList<TreeSet<AdjMtxLine>>();
		globalList.add(sortedLines);
		int curSize = globalList.size();
		while (curSize <= splitCount) {
			System.out.println("Writing files for " + curSize + " splits");
			// make subFolder like "1_splits"
			String subFolder = folder + File.separator + curSize + "_splits";
			File fSubFolder = new File(subFolder);
			if (fSubFolder.exists()) {
				if (fSubFolder.isFile())
					throw new Exception("Sub-folder name " + fSubFolder.getAbsolutePath() + " already exists as a file.");
			} else {
				if (!fSubFolder.mkdirs()) {
					throw new Exception("Error when creating sub-folder " + fSubFolder.getAbsolutePath() + ".");
				}
			}
			// write files
			int fileNum = 0;
			for (TreeSet<AdjMtxLine> amlSet : globalList) {
				PrintWriter pw = new PrintWriter(new FileWriter(subFolder + File.separator + "split-" + fileNum + ".txt"));
				for (AdjMtxLine aml : amlSet) {
					pw.println(aml.getLine());
				}
				pw.close();
				fileNum++;
			}
			
			if (curSize == splitCount) {
				break;
			}
			// split each adjacency sub-matrix to the next level
			System.out.println("Generating " + (curSize * 2) + " subsets using " + curSize);
			for (int i=0; i<curSize; i++) {
				TreeSet<AdjMtxLine> amlSet = globalList.remove(0);
				List<TreeSet<AdjMtxLine>> subSets = splitAmlSet(amlSet, 2);
				balanceTwoAmlSets(subSets.get(0), subSets.get(1));
				globalList.addAll(subSets);				
			}
			curSize = globalList.size();
		}
	}
	
	public static void generateRandomLayout(String outputPath, int numVertices) throws Exception {
		Random r = new Random();
		PrintWriter pwOut = new PrintWriter(new FileWriter(outputPath));
		for (int i=0; i<numVertices; i++) {
			pwOut.print((float)(r.nextFloat() * 2 - 1.0));
			pwOut.print(' ');
			pwOut.println((float)(r.nextFloat() * 2 - 1.0));
		}
		pwOut.close();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String command = args[0];	
		try {
			if (command.equals("generate-layout")) {
				String adjMtxPath = args[1];
				String nodeListPath = args[2];
				FruchtermanReingoldLayout frl = new FruchtermanReingoldLayout(adjMtxPath, nodeListPath);
				frl.layout(10);
			} else if (command.equals("convert-adjacency-matrix")) {
				String adjMtxPath = args[1];
				String nodeListPath = args[2];
				convertNameToIdInMtx(adjMtxPath, nodeListPath);
			} else if (command.equals("split-adjacency-matrix")) {
				String adjMtxPath = args[1];
				int splitCount = Integer.valueOf(args[2]);
				splitAdjMtxInBalance(adjMtxPath, splitCount);
			} else if (command.equals("generate-random-layout")) {
				String layoutFilePath = args[1];
				int numVertices = Integer.valueOf(args[2]);
				generateRandomLayout(layoutFilePath, numVertices);
			} else if (command.equals("check-split-balance")) {
				String splitDirPath = args[1];
				checkDataDistInSplitDir(splitDirPath);
			} else {
				System.err.println("Error: unsupported command!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
