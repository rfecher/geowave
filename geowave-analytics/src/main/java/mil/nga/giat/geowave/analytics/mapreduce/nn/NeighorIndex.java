package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.index.ByteArrayId;

public class NeighorIndex<NNTYPE>
{
	final Map<ByteArrayId, NeighborList<NNTYPE>> index = new HashMap<ByteArrayId, NeighborList<NNTYPE>>();
	final NeighborListFactory<NNTYPE> listFactory;

	final NullList<NNTYPE> nullList = new NullList<NNTYPE>();

	public NeighorIndex(
			final NeighborListFactory<NNTYPE> listFactory ) {
		super();
		this.listFactory = listFactory;
	}

	public NeighborList<NNTYPE> get(
			final ByteArrayId id ) {
		return index.get(id);
	}

	public boolean contains(
			final Map.Entry<ByteArrayId, NNTYPE> node,
			final Map.Entry<ByteArrayId, NNTYPE> neighbor ) {
		return resolve(
				this.index.get(node.getKey())).contains(
				neighbor.getKey());
	}

	private NeighborList<NNTYPE> resolve(
			NeighborList<NNTYPE> list ) {
		return (list == null) ? nullList : list;
	}

	public void add(
			final Map.Entry<ByteArrayId, NNTYPE> node,
			final Map.Entry<ByteArrayId, NNTYPE> neighbor,
			final boolean addReciprical ) {
		this.addToList(
				node,
				neighbor);
		if (addReciprical) {
			this.addToList(
					neighbor,
					node);
		}
	}

	public void empty(
			final ByteArrayId id ) {
		index.put(
				id,
				nullList);
	}

	private void addToList(
			final Map.Entry<ByteArrayId, NNTYPE> center,
			final Map.Entry<ByteArrayId, NNTYPE> neighbor ) {
		NeighborList<NNTYPE> neighbors = index.get(center.getKey());
		if (neighbors == null) {
			neighbors = listFactory.buildNeighborList(center.getValue());
			index.put(
					center.getKey(),
					neighbors);
		}
		neighbors.add(neighbor);
	}

}
