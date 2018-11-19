package org.locationtech.geowave.datastore.rocksdb.util;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

public class RocksDBGeoWaveMetadata extends
		GeoWaveMetadata
{
	private final byte[] originalKey;

	public RocksDBGeoWaveMetadata(
			final byte[] primaryId,
			final byte[] secondaryId,
			final byte[] visibility,
			final byte[] value,
			final byte[] originalKey ) {
		super(
				primaryId,
				secondaryId,
				visibility,
				value);
		this.originalKey = originalKey;
	}

	public byte[] getKey() {
		return originalKey;
	}

}
