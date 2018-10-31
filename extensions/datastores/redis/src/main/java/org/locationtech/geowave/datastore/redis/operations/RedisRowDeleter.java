package org.locationtech.geowave.datastore.redis.operations;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisPersistedRow;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisRow;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RedisRowDeleter implements
		RowDeleter
{

	private final LoadingCache<Pair<String, Short>, RScoredSortedSet<GeoWaveRedisPersistedRow>> setCache = Caffeine
			.newBuilder()
			.build(
					nameAndAdapterId -> getSet(
							nameAndAdapterId));
	private final RedissonClient client;
	private final ReaderParams<?> params;
	private final String namespace;
	private final Set<ByteArray> partitions = new HashSet<>();

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
			throws Exception {
		// TODO its unclear whether this is necessary
		// setCache
		// .asMap()
		// .forEach(
		// (
		// k,
		// v ) -> {
		// if (v.isEmpty()) {
		// v.delete();
		// }
		// });
	}

	private RScoredSortedSet<GeoWaveRedisPersistedRow> getSet(
			final Pair<String, Short> setNameAndAdapterId ) {
		return RedisUtils
				.getRowSet(
						client,
						setNameAndAdapterId.getLeft(),
						RedisUtils
								.isSortByTime(
										params
												.getAdapterStore()
												.getAdapter(
														setNameAndAdapterId.getRight())));
	}

	@Override
	public void delete(
			final GeoWaveRow row ) {
		partitions
				.add(
						new ByteArray(
								row.getPartitionKey()));
		final RScoredSortedSet<GeoWaveRedisPersistedRow> set = setCache
				.get(
						Pair
								.of(
										RedisUtils
												.getRowSetName(
														namespace,
														params
																.getInternalAdapterStore()
																.getTypeName(
																		row.getAdapterId()),
														params.getIndex().getName(),
														row.getPartitionKey()),
										row.getAdapterId()));
		set
				.remove(
						((GeoWaveRedisRow) row).getPersistedRow());
	}

}
