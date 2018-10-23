package org.locationtech.geowave.datastore.redis.operations;

import java.util.Arrays;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisPersistedRow;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RedisRowDeleter implements
		RowDeleter
{

	private final LoadingCache<String, RScoredSortedSet<GeoWaveRedisPersistedRow>> setCache = Caffeine
			.newBuilder()
			.build(
					name -> getSet(
							name));
	private final RedissonClient client;
	private final ReaderParams<?> params;
	private final String namespace;

	public RedisRowDeleter(
			final RedissonClient client,
			final ReaderParams<?> params,
			final String namespace ) {
		this.client = client;
		this.params = params;
		this.namespace = namespace;
	}

	@Override
	public void close()
			throws Exception {}

	private RScoredSortedSet<GeoWaveRedisPersistedRow> getSet(
			final String setName ) {
		return RedisUtils
				.getRowSet(
						client,
						setName);
	}

	@Override
	public void delete(
			final GeoWaveRow row ) {
		final RScoredSortedSet<GeoWaveRedisPersistedRow> set = setCache
				.get(
						RedisUtils
								.getRowSetName(
										namespace,
										params
												.getInternalAdapterStore()
												.getTypeName(
														row.getAdapterId()),
										params.getIndex().getName(),
										row.getPartitionKey()));
		Arrays
				.stream(
						row.getFieldValues())
				.forEach(
						v -> set
								.remove(
										new GeoWaveRedisPersistedRow(
												(short) row.getNumberOfDuplicates(),
												row.getDataId(),
												v)));
	}

}
