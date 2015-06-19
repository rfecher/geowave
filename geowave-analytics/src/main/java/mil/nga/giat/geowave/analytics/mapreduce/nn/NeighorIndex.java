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

	public NeighborList<NNTYPE> init(
			final Map.Entry<ByteArrayId, NNTYPE> node ) {
		NeighborList<NNTYPE> neighbors = index.get(node.getKey());
		if (neighbors == null) {
			neighbors = listFactory.buildNeighborList(
					node.getKey(),
					node.getValue());
			index.put(
					node.getKey(),
					neighbors);
		}
		neighbors.init();
		return neighbors;
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
			neighbors = listFactory.buildNeighborList(
					center.getKey(),
					center.getValue());
			index.put(
					center.getKey(),
					neighbors);
		}
		neighbors.add(neighbor);
	}

}
