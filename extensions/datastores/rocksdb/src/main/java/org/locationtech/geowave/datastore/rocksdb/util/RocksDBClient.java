package org.locationtech.geowave.datastore.rocksdb.util;

import java.io.Closeable;

import org.rocksdb.RocksDB;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RocksDBClient implements Closeable
{

	private final LoadingCache<String, RocksDB> tableCache = Caffeine
			.newBuilder()
			.build(
					directory -> {
						return RocksDB
								.open(
										opts,
										directory);
					});
}
