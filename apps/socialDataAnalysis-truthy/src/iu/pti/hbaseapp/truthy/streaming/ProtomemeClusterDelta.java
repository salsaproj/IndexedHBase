package iu.pti.hbaseapp.truthy.streaming;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to record the dynamic changes (delta) to a cluster between two synchronizations.  
 * @author gaoxm
 */
public class ProtomemeClusterDelta implements Comparable<ProtomemeClusterDelta> {
	protected int clusterId;
	protected long latestUpdateMilli;
	protected List<ProtoMeme> pms;
	
	public ProtomemeClusterDelta() {
		clusterId = -1;
		pms = new ArrayList<ProtoMeme>();
		latestUpdateMilli = Long.MIN_VALUE;
	}
	
	public ProtomemeClusterDelta(int clusterId, long latestUpdateMilli, ProtoMeme pm) {
		this.clusterId = clusterId;
		this.latestUpdateMilli = latestUpdateMilli;
		pms = new ArrayList<ProtoMeme>();
		pms.add(pm);
	}
	
	@Override
	public int compareTo(ProtomemeClusterDelta that) {
		if (this.latestUpdateMilli != that.latestUpdateMilli) {
			return this.latestUpdateMilli > that.latestUpdateMilli ? 1 : -1;
		}
		if (this.clusterId == that.clusterId) {
			return 0;
		}
		return Math.random() > 0.5 ? 1 : -1;
	}
	
	/** 
	 * Add a protomeme to the changes of this cluster.
	 * @param pm
	 *  The protomeme to be added.
	 */
	public void addProtomeme(ProtoMeme pm) {
		pms.add(pm);
		//latestUpdateMilli = System.currentTimeMillis();
		if (pm.latestTweetTs > latestUpdateMilli) {
			latestUpdateMilli = pm.latestTweetTs;
		}
	}
}
