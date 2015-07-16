package mil.nga.giat.geowave.core.store.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;

/**
 * This is a simple HashMap based in-memory implementation of the IndexStore and
 * can be useful if it is undesirable to persist and query objects within
 * another storage mechanism such as an accumulo table.
 */
public class MemoryIndexStore implements
		PrimaryIndexStore
{
	private final Map<ByteArrayId, PrimaryIndex> indexMap = new HashMap<ByteArrayId, PrimaryIndex>();

	public MemoryIndexStore(
			final PrimaryIndex[] initialIndices ) {
		for (final PrimaryIndex index : initialIndices) {
			addIndex(index);
		}
	}

	@Override
	public void addIndex(
			final PrimaryIndex index ) {
		indexMap.put(
				index.getId(),
				index);
	}

	@Override
	public PrimaryIndex getIndex(
			final ByteArrayId indexId ) {
		return indexMap.get(indexId);
	}

	@Override
	public boolean indexExists(
			final ByteArrayId indexId ) {
		return indexMap.containsKey(indexId);
	}

	@Override
	public CloseableIterator<PrimaryIndex> getIndices() {
		return new CloseableIterator.Wrapper<PrimaryIndex>(
				new ArrayList<PrimaryIndex>(
						indexMap.values()).iterator());
	}

}
