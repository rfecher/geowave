package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import mil.nga.giat.geowave.index.ByteArrayId;

public class DefaultNeighborList<NNTYPE> implements
		NeighborList<NNTYPE>
{
	private final List<Entry<ByteArrayId, NNTYPE>> list = new ArrayList<Entry<ByteArrayId, NNTYPE>>();

	public boolean add(
			Entry<ByteArrayId, NNTYPE> entry ) {
		if (!contains(entry.getKey())) {
			list.add(entry);
			return true;
		}
		return false;

	}

	public boolean contains(
			ByteArrayId key ) {
		for (Entry<ByteArrayId, NNTYPE> entry : list) {
			if (entry.getKey().equals(
					key)) return true;
		}
		return false;
	}

	public void clear() {
		list.clear();
	}

	@Override
	public Iterator<Entry<ByteArrayId, NNTYPE>> iterator() {
		return list.iterator();
	}

	@Override
	public int size() {
		return list.size();
	}

	public static class DefaultNeighborListFactory<NNTYPE> implements
			NeighborListFactory<NNTYPE>
	{
		@Override
		public NeighborList<NNTYPE> buildNeighborList(
				ByteArrayId centerId,
				NNTYPE center ) {
			return new DefaultNeighborList<NNTYPE>();
		}
	}

	@Override
	public boolean isEmpty() {
		return list.isEmpty();
	}

	public NNTYPE get(
			ByteArrayId key ) {
		for (Entry<ByteArrayId, NNTYPE> entry : list) {
			if (entry.getKey().equals(
					key)) return entry.getValue();
		}
		return null;
	}

}
