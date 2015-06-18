package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.util.Map.Entry;

import mil.nga.giat.geowave.index.ByteArrayId;

public interface NeighborList<NNTYPE> extends
		Iterable<Entry<ByteArrayId, NNTYPE>>
{
	public boolean add(
			Entry<ByteArrayId, NNTYPE> entry );

	public boolean contains(
			ByteArrayId key );

	public void clear();

	public int size();

	public boolean isEmpty();
}
