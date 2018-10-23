package org.locationtech.geowave.datastore.redis.operations;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;

public class RedisMetadataReader implements MetadataReader
{

	@Override
	public CloseableIterator<GeoWaveMetadata> query(
			MetadataQuery query ) {
		// TODO Auto-generated method stub
		return null;
	}

}
