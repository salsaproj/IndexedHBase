package iu.pti.hbaseapp;

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Given a folder and file size limit, this writer automatically creates and writes
 * to new files as the size of current file reaches the limit.
 * @author gaoxm
 *
 */
public class MultiFileFolderWriter {
	String folderUri;
	boolean deleteOldContent;
	String filenamePrefix;
	int maxLinesPerFile;
	int numFilesWritten = 0;
	
	FileSystem fs = null;
	PrintWriter pw = null;
	int lineCount = 0;
	int totalLineCount=0;
	
	public MultiFileFolderWriter(String folderUri, String filenamePrefix, boolean deleteOldContent, int maxLinesPerFile) throws Exception {
		this.folderUri = folderUri;
		this.filenamePrefix = filenamePrefix;
		this.deleteOldContent = deleteOldContent;
		this.maxLinesPerFile = maxLinesPerFile;
		
		Configuration conf = HBaseConfiguration.create();
		fs = FileSystem.get(new URI(folderUri), conf);
		Path folderPath = new Path(folderUri);
		if (!fs.exists(folderPath)) {
			if (!fs.mkdirs(folderPath)) {
				throw new Exception("Failed to create folder " + folderUri + ".");
			}
		} else {
			if (deleteOldContent) {
				if (!fs.delete(folderPath, true)) {
					throw new Exception("Failed to delete old stuff in folder " + folderUri + ".");
				}
				if (!fs.mkdirs(folderPath)) {
					throw new Exception("Failed to re-create folder " + folderUri + " after deleting old stuff.");
				}
			}
		}
	}
	
	public void writeln(String line) throws Exception {
		if (pw != null) {
			if (maxLinesPerFile <= 0 || lineCount < maxLinesPerFile) {
				pw.println(line);				
				lineCount++;
				totalLineCount++;
				return;
			} else {
				pw.flush();
				pw.close();
				pw = null;
				lineCount = 0;
			}
		}
		
		String filePath = folderUri + "/" + filenamePrefix + "_" + numFilesWritten + ".txt";
		pw = new PrintWriter(new OutputStreamWriter(fs.create(new Path(filePath), true), "UTF-8"));
		numFilesWritten++;
		pw.println(line);
		lineCount++;
	}
	
	public int getNumFilesWritten() {
		return numFilesWritten;
	}

	public int getTotalWrittenLine() {
		return totalLineCount;
	}	
	
	public long getNumLinesWritten() {
		if (maxLinesPerFile <= 0) {
			return lineCount;
		} else if (numFilesWritten > 0) {
			return (long)(numFilesWritten - 1) * (long)maxLinesPerFile + lineCount;
		} else {
			return 0;
		}
	}
	
	public void close() throws Exception {
		if (pw != null) {
			pw.flush();
			pw.close();
		}
		pw = null;
		fs = null;
	}
	
	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}
	
	/**
	 * Delete a file or directory if it exists.
	 * @param path
	 * @return
	 */
	public static boolean deleteIfExist(String path) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		FileSystem fs = FileSystem.get(new URI(path), conf);
		Path p = new Path(path);
		if (fs.exists(p)) {
			return fs.delete(p, true);
		} else {
			return true;
		}		
	}
	
	/**
	 * Return the URI form (starting with 'file:' or 'hdfs:' for a file or directory path.
	 * @param path
	 * @return
	 */
	public static String getUriStrForPath(String path) throws Exception {
		if (path.startsWith("file:") || path.startsWith("hdfs:")) {
			return path;
		}
		// If no protocol is given, take it as a local FS path.
		File fPath = new File(path);
		return "file:" + fPath.getAbsolutePath();
	}
	
	/**
	 * Move files from srcDir to dstDir by the given fnamePrefix, and return the number of
	 * files moved.
	 * @param srcDir
	 * @param dstDir
	 * @param fnamePrefix
	 * @return
	 */
	public static int moveFilesByNamePrefix(String srcDir, String dstDir, String fnamePrefix) throws Exception {
		int fileCount = 0;
		Path srcDirPath = new Path(srcDir);
		Path dstDirPath = new Path(dstDir);
		Configuration conf = HBaseConfiguration.create();
		FileSystem fs = FileSystem.get(new URI(srcDir), conf);
		if (fs.isFile(srcDirPath) || fs.isFile(dstDirPath)) {
			throw new IllegalArgumentException("Invalid source direcotry or destination directory: it is a file.");
		}
		FileStatus[] srcFiles = fs.listStatus(srcDirPath);
		for (FileStatus srcFile : srcFiles) {
			Path srcFilePath = srcFile.getPath();
			if (fs.isFile(srcFilePath) && srcFilePath.getName().startsWith(fnamePrefix)) {
				Path dstFilePath = new Path(dstDirPath, srcFilePath.getName());
				if (fs.rename(srcFilePath, dstFilePath)) {
					fileCount++;
				}
			}
		}
		return fileCount;
	}
}
