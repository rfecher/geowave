package org.locationtech.geowave.datastore.redis;

import org.locationtech.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import org.locationtech.geowave.core.store.metadata.AdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.metadata.IndexStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.SecondaryIndexStoreImpl;
import org.locationtech.geowave.datastore.redis.config.RedisOptions;
import org.locationtech.geowave.datastore.redis.operations.RedisOperations;
import org.locationtech.geowave.mapreduce.BaseMapReduceDataStore;

public class RedisDataStore extends
		BaseMapReduceDataStore
{
	public RedisDataStore(
			final RedisOperations operations,
			final RedisOptions options ) {
		super(
				new IndexStoreImpl(
						operations,
						options.getStoreOptions()),
				new AdapterStoreImpl(
						operations,
						options.getStoreOptions()),
				new DataStatisticsStoreImpl(
						operations,
						options.getStoreOptions()),
				new AdapterIndexMappingStoreImpl(
						operations,
						options.getStoreOptions()),
				new SecondaryIndexStoreImpl(),
				operations,
				options.getStoreOptions(),
				new InternalAdapterStoreImpl(
						operations));
	}
}
