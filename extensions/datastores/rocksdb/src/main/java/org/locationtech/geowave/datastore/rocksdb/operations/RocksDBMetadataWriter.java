package org.locationtech.geowave.datastore.rocksdb.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBMetadataTable;

public class RocksDBMetadataWriter implements
		MetadataWriter
{
	private final RocksDBMetadataTable table;

	public RocksDBMetadataWriter(
			final RocksDBMetadataTable table ) {
		this.table = table;
	}

	@Override
	public void write(
			final GeoWaveMetadata metadata ) {
		table
				.add(metadata);
	}

	@Override
	public void flush() {}

	@Override
	public void close()
			throws Exception {}
}
