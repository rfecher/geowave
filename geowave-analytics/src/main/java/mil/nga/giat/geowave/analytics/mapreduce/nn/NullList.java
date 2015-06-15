package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.util.Iterator;
import java.util.Map.Entry;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.index.ByteArrayId;

public class NullList<NNTYPE> implements
		NeighborList<NNTYPE>
{

	@Override
	public boolean add(
			Entry<ByteArrayId, NNTYPE> entry ) {
		return false;
	}

	@Override
	public boolean contains(
			ByteArrayId key ) {
		return false;
	}

	@Override
	public void clear() {

	}

	@Override
	public Iterator<Entry<ByteArrayId, NNTYPE>> iterator() {
		return Iterators.emptyIterator();
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public NNTYPE get(
			ByteArrayId key ) {
		return null;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public void merge(
			NeighborList<NNTYPE> otherList,
			mil.nga.giat.geowave.analytics.mapreduce.nn.NeighborList.Callback<NNTYPE> callback ) {

	}

}
