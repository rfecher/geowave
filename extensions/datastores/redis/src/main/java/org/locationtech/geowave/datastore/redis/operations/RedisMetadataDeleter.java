package org.locationtech.geowave.datastore.redis.operations;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.redisson.api.RScoredSortedSet;

public class RedisMetadataDeleter implements
		MetadataDeleter
{
	private RScoredSortedSet<GeoWaveMetadata> set;
	private MetadataType metadataType;

	public RedisMetadataDeleter(
			RScoredSortedSet<GeoWaveMetadata> set,
			MetadataType metadataType ) {
		this.set = set;
		this.metadataType = metadataType;
	}

	@Override
	public boolean delete(
			final MetadataQuery query ) {
		boolean atLeastOneDeletion = false;

		boolean noFailures = true;
		try (CloseableIterator<GeoWaveMetadata> it = new RedisMetadataReader(
				set,
				metadataType).query(query)) {
			while (it.hasNext()) {
				if (set.remove(it.next())) {
					atLeastOneDeletion = true;
				}
				else {
					noFailures = false;
				}
			}
		}
		return atLeastOneDeletion && noFailures;
	}

	@Override
	public void flush() {}

	@Override
	public void close()
			throws Exception {}

}
