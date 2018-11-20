package org.locationtech.geowave.datastore.rocksdb.util;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RocksDBClientCache
{
	private static RocksDBClientCache singletonInstance;

	public static synchronized RocksDBClientCache getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new RocksDBClientCache();
		}
		return singletonInstance;
	}


	private final LoadingCache<String, RocksDBClient> clientCache = Caffeine
			.newBuilder()
			.build(
					subDirectory -> {
						return new RocksDBClient(subDirectory);
					});

	protected RocksDBClientCache() {}

	public RocksDBClient getClient(
			final String directory ) {
		return clientCache
				.get(
						directory);
	}

	public synchronized void close(
			final String directory ) {
		final RocksDBClient client = clientCache
				.getIfPresent(
						directory);
		if (client != null) {
			clientCache
					.invalidate(
							directory);
			client.close();
		}
		if (clientCache.estimatedSize() == 0) {

			RocksDBClient.metadataOptions.close();
			RocksDBClient.metadataOptions = null;
			
			RocksDBClient.indexOptions.close();
			RocksDBClient.indexOptions = null;			
		}
	}
}
