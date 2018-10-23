package org.locationtech.geowave.datastore.redis;

import java.net.URL;
import java.util.Arrays;

import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IngestOptions;
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

	@Override
	public void addIndex(
			final String typeName,
			final Index... indices ) {
		super.addIndex(
				typeName,
				transformTo52BitSortKeyArray(
						indices));
	}

	@Override
	public <T> void addType(
			final DataTypeAdapter<T> dataTypeAdapter,
			final Index... initialIndices ) {
		super.addType(
				dataTypeAdapter,
				transformTo52BitSortKeyArray(
						initialIndices));
	}

	private static Index[] transformTo52BitSortKeyArray(
			final Index[] indices ) {
		return Arrays
				.stream(
						indices)
				.map(
						i -> transformTo52BitSortKey(
								i))
				.toArray(
						i -> new Index[i]);
	}

	private static Index transformTo52BitSortKey(
			final Index index ) {
		// TODO we may want to duplicate this index but cap its precision to 52
		// bits so that it can be well represented by the matissa of the redis
		// z-score
		return index;
	}

	@Override
	public <T> void ingest(
			final URL url,
			final Index... index )
			throws MismatchedIndexToAdapterMapping {
		super.ingest(
				url,
				transformTo52BitSortKeyArray(
						index));
	}

	@Override
	public <T> void ingest(
			final URL url,
			final IngestOptions<T> options,
			final Index... index )
			throws MismatchedIndexToAdapterMapping {
		super.ingest(
				url,
				options,
				transformTo52BitSortKeyArray(
						index));
	}
}
