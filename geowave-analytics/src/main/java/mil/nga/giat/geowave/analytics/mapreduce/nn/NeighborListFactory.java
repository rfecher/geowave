package mil.nga.giat.geowave.analytics.mapreduce.nn;

import mil.nga.giat.geowave.index.ByteArrayId;

public interface NeighborListFactory<NNTYPE>
{
	public NeighborList<NNTYPE> buildNeighborList(
			ByteArrayId cnterId,
			NNTYPE center );
}