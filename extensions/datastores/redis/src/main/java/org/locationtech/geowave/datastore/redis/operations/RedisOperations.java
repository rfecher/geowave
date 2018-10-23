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
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.redis.config.RedisOptions;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisOperations implements
		MapReduceDataStoreOperations
{
	private final static Logger LOGGER = LoggerFactory
			.getLogger(
					RedisOperations.class);
	private final String gwNamespace;
	private final RedisOptions options;

	public RedisOperations(
			final RedisOptions options ) {
		if ((options.getGeowaveNamespace() == null) || options
				.getGeowaveNamespace()
				.equals(
						"")) {
			gwNamespace = "geowave";
		}
		else {
			gwNamespace = options.getGeowaveNamespace();
		}
		this.options = options;
	}

	@Override
	public boolean indexExists(
			final String indexName )
			throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean createIndex(
			final Index index )
			throws IOException {
		return false;
	}

	@Override
	public boolean metadataExists(
			final MetadataType type )
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
			final String indexName,
			final Short adapterId,
			final String... additionalAuthorizations ) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ensureAuthorizations(
			final String clientUser,
			final String... authorizations ) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public RowWriter createWriter(
			final Index index,
			final String typeName,
			final short adapterId ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MetadataWriter createMetadataWriter(
			final MetadataType metadataType ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MetadataReader createMetadataReader(
			final MetadataType metadataType ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MetadataDeleter createMetadataDeleter(
			final MetadataType metadataType ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> RowReader<T> createReader(
			final ReaderParams<T> readerParams ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Deleter<T> createDeleter(
			final ReaderParams<T> readerParams ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean mergeData(
			final Index index,
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean mergeStats(
			final DataStatisticsStore statsStore,
			final InternalAdapterStore internalAdapterStore ) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> RowReader<T> createReader(
			final RecordReaderParams<T> readerParams ) {
		// TODO Auto-generated method stub
		return null;
	}

}
