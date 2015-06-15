package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.util.Map.Entry;

import mil.nga.giat.geowave.index.ByteArrayId;

public interface NeighborList<NNTYPE> extends
		Iterable<Entry<ByteArrayId, NNTYPE>>
{
	public boolean add(
			Entry<ByteArrayId, NNTYPE> entry );

	public NNTYPE get(
			ByteArrayId key );

	public boolean contains(
			ByteArrayId key );

	public void clear();

	public int size();

	public boolean isEmpty();

	public void merge(
			NeighborList<NNTYPE> otherList,
			Callback<NNTYPE> callback );

	public interface Callback<NNTYPE>
	{
		public void add(
				ByteArrayId key );
	}
}
