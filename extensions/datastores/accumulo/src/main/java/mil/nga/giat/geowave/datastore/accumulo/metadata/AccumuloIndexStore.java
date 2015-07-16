package mil.nga.giat.geowave.datastore.accumulo.metadata;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;

/**
 * This class will persist Index objects within an Accumulo table for GeoWave
 * metadata. The indices will be persisted in an "INDEX" column family.
 * 
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 * 
 **/
public class AccumuloIndexStore extends
		AbstractAccumuloPersistence<PrimaryIndex> implements
		PrimaryIndexStore
{
	private static final String INDEX_CF = "INDEX";

	public AccumuloIndexStore(
			final AccumuloOperations accumuloOperations ) {
		super(
				accumuloOperations);
	}

	@Override
	public void addIndex(
			final PrimaryIndex index ) {
		addObject(index);
	}

	@Override
	public PrimaryIndex getIndex(
			final ByteArrayId indexId ) {
		return getObject(
				indexId,
				null);
	}

	@Override
	protected String getPersistenceTypeName() {
		return INDEX_CF;
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final PrimaryIndex persistedObject ) {
		return persistedObject.getId();
	}

	@Override
	public boolean indexExists(
			final ByteArrayId id ) {
		return objectExists(
				id,
				null);
	}

	@Override
	public CloseableIterator<PrimaryIndex> getIndices() {
		return getObjects();
	}

}
