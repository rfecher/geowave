/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.operations;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParams;
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
import org.locationtech.geowave.datastore.filesystem.config.FileSystemOptions;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemClient;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemClientCache;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemDataIndexTable;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemUtils;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemOperations implements MapReduceDataStoreOperations, Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemOperations.class);
  private static final boolean READER_ASYNC = true;
  private final FileSystemClient client;
  private final String directory;
  private final boolean visibilityEnabled;
  private final String format;

  public FileSystemOperations(final FileSystemOptions options) {
    directory =
        options.getDirectory()
            + File.separator
            + ((options.getGeoWaveNamespace() == null)
                || options.getGeoWaveNamespace().trim().isEmpty()
                || "null".equalsIgnoreCase(options.getGeoWaveNamespace()) ? "default"
                    : options.getGeoWaveNamespace());

    visibilityEnabled = options.getStoreOptions().isVisibilityEnabled();
    // a factory method that returns a RocksDB instance
    client = FileSystemClientCache.getInstance().getClient(directory, visibilityEnabled);
    format = options.getFormat();
  }

  @Override
  public boolean mergeData(
      final Index index,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final AdapterIndexMappingStore adapterIndexMappingStore,
      final Integer maxRangeDecomposition) {
    final boolean retVal =
        MapReduceDataStoreOperations.super.mergeData(
            index,
            adapterStore,
            internalAdapterStore,
            adapterIndexMappingStore,
            maxRangeDecomposition);
    return retVal;
  }

  @Override
  public boolean mergeStats(
      final DataStatisticsStore statsStore,
      final InternalAdapterStore internalAdapterStore) {
    final boolean retVal =
        MapReduceDataStoreOperations.super.mergeStats(statsStore, internalAdapterStore);
    return retVal;
  }

  @Override
  public boolean indexExists(final String indexName) throws IOException {
    return client.indexTableExists(indexName);
  }

  @Override
  public boolean metadataExists(final MetadataType type) throws IOException {
    return client.metadataTableExists(type);
  }

  @Override
  public void deleteAll() throws Exception {
    close();
    FileUtils.deleteDirectory(new File(directory));
  }

  @Override
  public boolean deleteAll(
      final String indexName,
      final String typeName,
      final Short adapterId,
      final String... additionalAuthorizations) {
    final String prefix = FileSystemUtils.getTablePrefix(typeName, indexName);
    client.close(indexName, typeName);
    Arrays.stream(new File(directory).listFiles((dir, name) -> name.startsWith(prefix))).forEach(
        f -> {
          try {
            FileUtils.deleteDirectory(f);
          } catch (final IOException e) {
            LOGGER.warn("Unable to delete directory '" + f.getAbsolutePath() + "'", e);
          }
        });
    return true;
  }

  @Override
  public boolean ensureAuthorizations(final String clientUser, final String... authorizations) {
    return true;
  }

  @Override
  public RowWriter createWriter(final Index index, final InternalDataAdapter<?> adapter) {
    return new FileSystemWriter(
        client,
        adapter.getAdapterId(),
        adapter.getTypeName(),
        index.getName(),
        format,
        FileSystemUtils.isSortByTime(adapter));
  }

  @Override
  public RowWriter createDataIndexWriter(final InternalDataAdapter<?> adapter) {
    return new FileSystemDataIndexWriter(
        client,
        adapter.getAdapterId(),
        adapter.getTypeName(),
        format);
  }

  @Override
  public MetadataWriter createMetadataWriter(final MetadataType metadataType) {
    return new FileSystemMetadataWriter(FileSystemUtils.getMetadataTable(client, metadataType));
  }

  @Override
  public MetadataReader createMetadataReader(final MetadataType metadataType) {
    return new FileSystemMetadataReader(
        FileSystemUtils.getMetadataTable(client, metadataType),
        metadataType);
  }

  @Override
  public MetadataDeleter createMetadataDeleter(final MetadataType metadataType) {
    return new FileSystemMetadataDeleter(
        FileSystemUtils.getMetadataTable(client, metadataType),
        metadataType);
  }

  @Override
  public <T> RowReader<T> createReader(final ReaderParams<T> readerParams) {
    return new FileSystemReader<>(client, readerParams, READER_ASYNC, format);
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final DataIndexReaderParams readerParams) {
    return new FileSystemReader<>(client, readerParams);
  }

  @Override
  public <T> Deleter<T> createDeleter(final ReaderParams<T> readerParams) {
    return new QueryAndDeleteByRow<>(
        createRowDeleter(
            readerParams.getIndex().getName(),
            readerParams.getAdapterStore(),
            readerParams.getInternalAdapterStore(),
            readerParams.getAdditionalAuthorizations()),
        // intentionally don't run this reader as async because it does
        // not work well while simultaneously deleting rows
        new FileSystemReader<>(client, readerParams, false, format));
  }

  @Override
  public void delete(final DataIndexReaderParams readerParams) {
    final String typeName =
        readerParams.getInternalAdapterStore().getTypeName(readerParams.getAdapterId());
    deleteRowsFromDataIndex(readerParams.getDataIds(), readerParams.getAdapterId(), typeName);
  }

  public void deleteRowsFromDataIndex(
      final byte[][] dataIds,
      final short adapterId,
      final String typeName) {
    final FileSystemDataIndexTable table =
        FileSystemUtils.getDataIndexTable(client, typeName, adapterId, format);
    Arrays.stream(dataIds).forEach(d -> table.delete(d));
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final RecordReaderParams readerParams) {
    return new FileSystemReader<>(client, readerParams);
  }

  @Override
  public RowDeleter createRowDeleter(
      final String indexName,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final String... authorizations) {
    return new FileSystemRowDeleter(client, adapterStore, internalAdapterStore, indexName, format);
  }

  @Override
  public void close() {
    FileSystemClientCache.getInstance().close(directory, visibilityEnabled);
  }
}
