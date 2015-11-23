package iu.pti.hbaseapp.clueweb09;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;

public class CustomizedSplitTableInputFormat extends TableInputFormat {

	public CustomizedSplitTableInputFormat() {
	}

	/**
	 * Calculates the splits that will serve as input for the map tasks. The
	 * number of splits matches the number of regions in a table.
	 * 
	 * @param context The current job context.
	 * @return The list of input splits.
	 * @throws IOException When creating the list of splits fails.
	 * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
	 */
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		List<InputSplit> splits = super.getSplits(context);
		List<InputSplit> result = new ArrayList<InputSplit>(splits.size());
		
		for (InputSplit split : splits) {
			TableSplit ts = (TableSplit)split;
			String regionLocation = ts.getRegionLocation();
			if (regionLocation.endsWith(".")) {
				regionLocation = regionLocation.substring(0, regionLocation.length() - 1);
			}
			/*
			int idx = -1;
			idx = regionLocation.indexOf(".quarry");
			if (idx >= 0) {
				regionLocation = regionLocation.substring(0, idx);
			}*/
			result.add(new TableSplit(ts.getTableName(), ts.getStartRow(), ts.getEndRow(), regionLocation));
		}
		
		return result;
	}
}

