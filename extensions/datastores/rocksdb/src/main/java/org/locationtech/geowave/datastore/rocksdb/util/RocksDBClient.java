package org.locationtech.geowave.datastore.rocksdb.util;

import java.io.Closeable;

import org.locationtech.geowave.core.store.operations.MetadataType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RocksDBClient implements
		Closeable
{
	private static class CacheKey
	{
		private final String directory;
		boolean requiresTimestamp;

		public CacheKey(
				final String directory,
				final boolean requiresTimestamp ) {
			this.directory = directory;
			this.requiresTimestamp = requiresTimestamp;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((directory == null) ? 0 : directory.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final CacheKey other = (CacheKey) obj;
			if (directory == null) {
				if (other.directory != null) {
					return false;
				}
			}
			else if (!directory
					.equals(
							other.directory)) {
				return false;
			}
			return true;
		}
	}

	private final Cache<String, CacheKey> keyCache = Caffeine.newBuilder().weakKeys().weakValues().build();
	private final LoadingCache<CacheKey, RocksDBIndexTable> indexTableCache = Caffeine
			.newBuilder()
			.build(
					key -> {
						return new RocksDBIndexTable(
								RocksDB
										.open(
												indexOptions,
												key.directory),
								key.requiresTimestamp);
					});
	private final LoadingCache<CacheKey, RocksDBMetadataTable> metadataTableCache = Caffeine
			.newBuilder()
			.build(
					key -> {
						return new RocksDBMetadataTable(
								RocksDB
										.open(
												metadataOptions,
												key.directory),
								key.requiresTimestamp);
					});
	private final String subDirectory;

	protected static Options indexOptions = null;
	protected static Options metadataOptions = null;

	public RocksDBClient(
			final String subDirectory ) {
		this.subDirectory = subDirectory;
	}

	public synchronized RocksDBIndexTable getIndexTable(
			final String tableName,
			final boolean requiresTimestamp ) {

		if (indexOptions == null) {
			RocksDB.loadLibrary();
			indexOptions = new Options()
					.setCreateIfMissing(
							true)
					.prepareForBulkLoad();
		}
		final String directory = subDirectory + "/" + tableName;
		return indexTableCache
				.get(
						keyCache
								.get(
										directory,
										d -> new CacheKey(
												d,
												requiresTimestamp)));
	}

	public synchronized RocksDBMetadataTable getMetadataTable(
			final MetadataType type ) {

		if (metadataOptions == null) {
			RocksDB.loadLibrary();
			metadataOptions = new Options()
					.setCreateIfMissing(
							true)
					.optimizeForSmallDb();
		}
		final String directory = subDirectory + "/" + type.name();
		return metadataTableCache
				.get(
						keyCache
								.get(
										directory,
										d -> new CacheKey(
												d,
												type
														.equals(
																MetadataType.STATS))));
	}

	public boolean tableExists(
			final String tableName ) {
		return indexTableCache
				.getIfPresent(
						tableName) != null;
	}

	@Override
	public void close() {
		indexTableCache
				.asMap()
				.values()
				.forEach(
						db -> db.close());
		indexTableCache.invalidateAll();
		metadataTableCache
				.asMap()
				.values()
				.forEach(
						db -> db.close());
		metadataTableCache.invalidateAll();
	}
}
