package mil.nga.giat.geowave.analytics.mapreduce.nn;

public interface NeighborListFactory<NNTYPE>
{
	public NeighborList<NNTYPE> buildNeighborList(
			NNTYPE center );
}