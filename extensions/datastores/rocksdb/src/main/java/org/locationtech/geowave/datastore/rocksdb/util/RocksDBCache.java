package org.locationtech.geowave.datastore.rocksdb.util;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RocksDBCache
{
	private static RocksDBCache singletonInstance;

	public static synchronized RocksDBCache getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new RocksDBCache();
		}
		return singletonInstance;
	}

	private static Options opts = null;

	private final LoadingCache<String, RocksDB> clientCache = Caffeine
			.newBuilder()
			.build(
					directory -> {
						return RocksDB
								.open(
										opts,
										directory);
					});

	protected RocksDBCache() {}

	public synchronized RocksDB getTable(
			final String directory ) {
		if (opts == null) {
			RocksDB.loadLibrary();
			opts = new Options()
					.setCreateIfMissing(
							true);
		}
		return clientCache
				.get(
						directory);
	}

	public synchronized void close(
			final String directory ) {
		final RocksDB db = clientCache
				.getIfPresent(
						directory);
		if (db != null) {
			clientCache
					.invalidate(
							directory);
			db.close();
		}
		if (clientCache.estimatedSize() == 0) {
			opts.close();
			opts = null;
		}
	}
}
