package iu.pti.hbaseapp.truthy.streaming;

import java.util.ArrayList;
import java.util.List;

import iu.pti.hbaseapp.truthy.streaming.MemeClusteringTester.GlobalClusteringParams;

public class SyncMessage {
	protected GlobalClusteringParams params;
	protected List<ProtomemeClusterDelta> deltas;
	
	public SyncMessage(int numClusters) {
		deltas = new ArrayList<ProtomemeClusterDelta>(numClusters);
	}
}
