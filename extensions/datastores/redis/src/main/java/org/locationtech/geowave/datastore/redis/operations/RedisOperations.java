/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.datastore.redis.operations;

import java.io.IOException;

import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.operations.Deleter;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.core.store.operations.QueryAndDeleteByRow;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.redis.config.RedisOptions;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.locationtech.geowave.datastore.redis.util.RedissonClientCache;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

public class RedisOperations implements
		MapReduceDataStoreOperations
{
	private final static Logger LOGGER = LoggerFactory.getLogger(RedisOperations.class);
	// private final static int WRITE_RESPONSE_THREAD_SIZE = 16;
	// private final static int READ_RESPONSE_THREAD_SIZE = 16;
	// protected final static ExecutorService WRITE_RESPONSE_THREADS =
	// MoreExecutors
	// .getExitingExecutorService((ThreadPoolExecutor)
	// Executors.newFixedThreadPool(WRITE_RESPONSE_THREAD_SIZE));
	// protected final static ExecutorService READ_RESPONSE_THREADS =
	// MoreExecutors
	// .getExitingExecutorService((ThreadPoolExecutor)
	// Executors.newFixedThreadPool(READ_RESPONSE_THREAD_SIZE));
	private final String gwNamespace;
	// private final RedisOptions options;
	private final RedissonClient client;

	public RedisOperations(
			final RedisOptions options ) {
		if ((options.getGeowaveNamespace() == null) || options.getGeowaveNamespace().equals(
				"")) {
			gwNamespace = "geowave";
		}
		else {
			gwNamespace = options.getGeowaveNamespace();
		}
		// this.options = options;
		client = RedissonClientCache.getInstance().getClient(
				options.getAddress());
	}

	@Override
	public boolean indexExists(
			final String indexName )
			throws IOException {
		return true;
	}

	@Override
	public boolean createIndex(
			final Index index )
			throws IOException {
		return true;
	}

	@Override
	public boolean metadataExists(
			final MetadataType type )
			throws IOException {
		return true;
	}

	private void deleteByPattern(
			final String pattern ) {
		final RKeys keySet = client.getKeys();
//		final String[] keys = Iterators.toArray(
//				keySet.getKeysByPattern(
//						pattern).iterator(),
//				String.class);
//		keySet.delete(keys);

		keySet.getKeysByPattern(
				pattern).forEach(k -> keySet.delete(k));
	}

	@Override
	public void deleteAll()
			throws Exception {
		deleteByPattern(gwNamespace + "_*");
	}

	@Override
	public boolean deleteAll(
			final String indexName,
			final String typeName,
			final Short adapterId,
			final String... additionalAuthorizations ) {
		deleteByPattern(RedisUtils.getRowSetPrefix(
				gwNamespace,
				typeName,
				indexName) + "*");
		return true;
	}

	@Override
	public boolean ensureAuthorizations(
			final String clientUser,
			final String... authorizations ) {
		return true;
	}

	@Override
	public RowWriter createWriter(
			final Index index,
			final String typeName,
			final short adapterId ) {
		return new RedisWriter(
				client,
				gwNamespace,
				typeName,
				index.getName());
	}

	@Override
	public MetadataWriter createMetadataWriter(
			final MetadataType metadataType ) {
		return new RedisMetadataWriter(
				RedisUtils.getMetadataSet(
						client,
						gwNamespace,
						metadataType));
	}

	@Override
	public MetadataReader createMetadataReader(
			final MetadataType metadataType ) {
		return new RedisMetadataReader(
				RedisUtils.getMetadataSet(
						client,
						gwNamespace,
						metadataType),
				metadataType);
	}

	@Override
	public MetadataDeleter createMetadataDeleter(
			final MetadataType metadataType ) {
		return new RedisMetadataDeleter(
				RedisUtils.getMetadataSet(
						client,
						gwNamespace,
						metadataType),
				metadataType);
	}

	@Override
	public <T> RowReader<T> createReader(
			final ReaderParams<T> readerParams ) {
		return new RedisReader<>(
				client,
				readerParams,
				gwNamespace);
	}

	public RowDeleter createDeleter(
			final ReaderParams<?> readerParams,
			final String... authorizations ) {
		return new RedisRowDeleter(
				client,
				readerParams,
				gwNamespace);
	}

	@Override
	public <T> Deleter<T> createDeleter(
			final ReaderParams<T> readerParams ) {
		return new QueryAndDeleteByRow<>(
				createDeleter(
						readerParams,
						readerParams.getAdditionalAuthorizations()),
				createReader(readerParams));
	}

	@Override
	public boolean mergeData(
			final Index index,
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {
		return DataStoreUtils.mergeData(
				index,
				adapterStore,
				adapterIndexMappingStore);
	}

	@Override
	public boolean mergeStats(
			final DataStatisticsStore statsStore,
			final InternalAdapterStore internalAdapterStore ) {
		return DataStoreUtils.mergeStats(
				statsStore,
				internalAdapterStore);
	}

	@Override
	public <T> RowReader<T> createReader(
			final RecordReaderParams<T> readerParams ) {
		return new RedisReader<>(
				client,
				readerParams,
				gwNamespace);
	}

}
