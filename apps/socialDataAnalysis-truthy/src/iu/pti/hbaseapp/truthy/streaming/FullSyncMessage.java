package iu.pti.hbaseapp.truthy.streaming;

import iu.pti.hbaseapp.truthy.streaming.MemeClusteringTester.GlobalClusteringParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FullSyncMessage {
	
	public static class FullSyncClusterInfo implements Comparable<FullSyncClusterInfo> {
		/**	Markers of protomemes that belong to this cluster */
		protected Set<String> pmMarkers;
		
		/** The centroid's tweet ID vector. This contains the sum instead of average. Averages are computed on the fly when needed. */
		protected HashMap<String, Integer> centTidVector;
		
		/** The centroid's user ID vector. This contains the sum instead of average. Averages are computed on the fly when needed. */
		protected HashMap<String, Integer> centUserVector;
		
		/** The centroid's words vector. This contains the sum instead of average. Averages are computed on the fly when needed. */
		protected HashMap<String, Integer> centWordVector;
		
		/** The centroid's tweet ID vector. This contains the sum instead of average. Averages are computed on the fly when needed. */
		protected HashMap<String, Integer> centDiffusionVector;
		
		/** Last time this cluster is updated */
		public long latestUpdateTime;
		
		/** Help to distinguish two clusters with equal lastUpdateTime (only in a sequential program) */
		public long nanoUpdateTime;
		
		/** ID of the cluster in the list of all clusters */
		public int clusterId;
		
		public FullSyncClusterInfo(ProtomemeCluster cluster, int clusterId) {
			this.clusterId = clusterId;
			this.centTidVector = cluster.centTidVector;
			this.centUserVector = cluster.centUserVector;
			this.centWordVector = cluster.centWordVector;
			this.centDiffusionVector = cluster.centDiffusionVector;
			this.latestUpdateTime = cluster.latestUpdateTime;
			this.nanoUpdateTime = cluster.nanoUpdateTime;
		}
		
		public void fillPmMarkers(ProtomemeCluster cluster) {
			pmMarkers = new HashSet<String>(cluster.protomemes.size() / 2);
			for (ProtoMeme pm : cluster.protomemes) {
				pmMarkers.add(pm.mainMarker);
			}
		}
		
		@Override
		public int compareTo(FullSyncClusterInfo that) {
			if (this.latestUpdateTime != that.latestUpdateTime) {
				return this.latestUpdateTime > that.latestUpdateTime ? 1 : -1;
			}
			if (this.nanoUpdateTime != that.nanoUpdateTime) {
				return this.nanoUpdateTime > that.nanoUpdateTime ? 1 : -1;
			}
			if (this.clusterId == that.clusterId) {
				return 0;
			}
			return Math.random() > 0.5 ? 1 : -1;
		}
	}
	
	protected GlobalClusteringParams params;
	protected List<FullSyncClusterInfo> clusters;
	
	public FullSyncMessage(int numClusters) {
		clusters = new ArrayList<FullSyncClusterInfo>(numClusters);
		for (int i=0; i<numClusters; i++) {
			clusters.add(null);
		}
	}
}
