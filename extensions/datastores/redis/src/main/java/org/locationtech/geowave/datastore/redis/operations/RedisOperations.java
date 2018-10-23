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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
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
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

public class RedisOperations implements
		MapReduceDataStoreOperations
{
	private final static Logger LOGGER = LoggerFactory.getLogger(RedisOperations.class);
	private final String gwNamespace;
	private final RedisOptions options;

	public RedisOperations(
			final RedisOptions options ) {
		if ((options.getGeowaveNamespace() == null) || options.getGeowaveNamespace().equals(
				"")) {
			gwNamespace = "geowave";
		}
		else {
			gwNamespace = getCassandraSafeName(options.getGeowaveNamespace());
		}
	}

	@Override
	public boolean indexExists(
			String indexName )
			throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean createIndex(
			Index index )
			throws IOException {
		return false;
	}

	@Override
	public boolean metadataExists(
			MetadataType type )
			throws IOException {
		return false;
	}

	@Override
	public void deleteAll()
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean deleteAll(
			String indexName,
			Short adapterId,
			String... additionalAuthorizations ) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ensureAuthorizations(
			String clientUser,
			String... authorizations ) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public RowWriter createWriter(
			Index index,
			String typeName,
			short adapterId ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MetadataWriter createMetadataWriter(
			MetadataType metadataType ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MetadataReader createMetadataReader(
			MetadataType metadataType ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MetadataDeleter createMetadataDeleter(
			MetadataType metadataType ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> RowReader<T> createReader(
			ReaderParams<T> readerParams ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Deleter<T> createDeleter(
			ReaderParams<T> readerParams ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean mergeData(
			Index index,
			PersistentAdapterStore adapterStore,
			AdapterIndexMappingStore adapterIndexMappingStore ) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean mergeStats(
			DataStatisticsStore statsStore,
			InternalAdapterStore internalAdapterStore ) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> RowReader<T> createReader(
			RecordReaderParams<T> readerParams ) {
		// TODO Auto-generated method stub
		return null;
	}

}
