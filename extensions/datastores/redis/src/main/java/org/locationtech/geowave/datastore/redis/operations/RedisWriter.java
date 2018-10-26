package org.locationtech.geowave.datastore.redis.operations;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisPersistedRow;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RedisWriter implements
		RowWriter
{
	private static ByteArrayId EMPTY_PARTITION_KEY = new ByteArrayId();
	private final RedissonClient client;
	private final String setNamePrefix;
	private final LoadingCache<ByteArrayId, RScoredSortedSet<GeoWaveRedisPersistedRow>> setCache = Caffeine
			.newBuilder()
			.build(
					partitionKey -> getSet(
							partitionKey.getBytes()));

	public RedisWriter(
			final RedissonClient client,
			final String namespace,
			final String typeName,
			final String indexName ) {
		this.client = client;
		setNamePrefix = RedisUtils
				.getRowSetPrefix(
						namespace,
						typeName,
						indexName);
	}

	private RScoredSortedSet<GeoWaveRedisPersistedRow> getSet(
			final byte[] partitionKey ) {
		return RedisUtils
				.getRowSet(
						client,
						setNamePrefix,
						partitionKey);
	}

	@Override
	public void write(
			final GeoWaveRow[] rows ) {
		for (final GeoWaveRow row : rows) {
			write(
					row);
		}
	}

	@Override
	public void write(
			final GeoWaveRow row ) {
		ByteArrayId partitionKey;
		if ((row.getPartitionKey() == null) || (row.getPartitionKey().length == 0)) {
			partitionKey = EMPTY_PARTITION_KEY;
		}
		else {
			partitionKey = new ByteArrayId(
					row.getPartitionKey());
		}
		for (final GeoWaveValue value : row.getFieldValues()) {
			setCache
					.get(
							partitionKey)
					.add(
							RedisUtils
									.getScore(
											row.getSortKey()),
							new GeoWaveRedisPersistedRow(
									(short) row.getNumberOfDuplicates(),
									row.getDataId(),
									value));
		}
	}

	@Override
	public void flush() {}

	@Override
	public void close()
			throws Exception {}

}
