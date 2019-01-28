/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.halodb.operations;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.locationtech.geowave.core.store.CloseableIterator.Wrapper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParams;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.RowReaderWrapper;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.query.filter.ClientVisibilityFilter;
import org.locationtech.geowave.datastore.halodb.HaloDBOptions;
import org.locationtech.geowave.datastore.halodb.util.HaloDBCache;
import org.locationtech.geowave.datastore.halodb.util.HaloDBClient;
import org.locationtech.geowave.datastore.halodb.util.HaloDBDataIndexTable;
import org.locationtech.geowave.datastore.halodb.util.HaloDBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

public class HaloDBOperations implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HaloDBOperations.class);
  private final HaloDBClient client;
  private final String directory;
  private final boolean visibilityEnabled;

  public HaloDBOperations(
      final HaloDBOptions haloDbOptions,
      final StoreFactoryOptions storeOptions) {
    directory = haloDbOptions.getDirectory() + "/" + storeOptions.getGeoWaveNamespace();

    visibilityEnabled = storeOptions.getStoreOptions().isVisibilityEnabled();
    // a factory method that returns a RocksDB instance
    client = HaloDBCache.getInstance().getClient(haloDbOptions, directory, visibilityEnabled);
  }

  public void deleteAll() throws Exception {
    close();
    FileUtils.deleteDirectory(new File(directory));
  }

  public boolean deleteAll(
      final String typeName,
      final Short adapterId,
      final String... additionalAuthorizations) {
    final String prefix = HaloDBUtils.getTableName(typeName);
    try {
      client.close(typeName);
    } catch (final IOException e) {
      LOGGER.warn("unable to close data index for type '" + typeName + "'", e);
    }
    Arrays.stream(new File(directory).list((dir, name) -> name.startsWith(prefix))).forEach(d -> {
      try {
        FileUtils.deleteDirectory(new File(d));
      } catch (final IOException e) {
        LOGGER.warn("Unable to delete directory '" + d + "'");
      }
    });
    return true;
  }

  public RowWriter createDataIndexWriter(final InternalDataAdapter<?> adapter) {
    return new HaloDBDataIndexWriter(
        client.getDataIndexTable(adapter.getTypeName(), adapter.getAdapterId()));
  }

  public RowReader<GeoWaveRow> createReader(final DataIndexReaderParams readerParams) {
    final String typeName =
        readerParams.getInternalAdapterStore().getTypeName(readerParams.getAdapterId());
    return new RowReaderWrapper(
        new Wrapper<>(
            getDataIndexResults(
                readerParams.getDataIds(),
                readerParams.getAdapterId(),
                typeName,
                readerParams.getAdditionalAuthorizations())));
  }


  public Iterator<GeoWaveRow> getDataIndexResults(
      final byte[][] dataIds,
      final short adapterId,
      final String typeName,
      final String... additionalAuthorizations) {
    if ((dataIds == null) || (dataIds.length == 0)) {
      return Collections.emptyIterator();
    }
    final Iterator<GeoWaveRow> iterator =
        client.getDataIndexTable(typeName, adapterId).dataIndexIterator(dataIds);
    if (visibilityEnabled) {
      final Set<String> authorizations = Sets.newHashSet(additionalAuthorizations);
      return Iterators.filter(iterator, new ClientVisibilityFilter(authorizations));
    }
    return iterator;
  }

  public void delete(final DataIndexReaderParams readerParams) {
    final String typeName =
        readerParams.getInternalAdapterStore().getTypeName(readerParams.getAdapterId());
    deleteRowsFromDataIndex(readerParams.getDataIds(), readerParams.getAdapterId(), typeName);
  }

  public void deleteRowsFromDataIndex(
      final byte[][] dataIds,
      final short adapterId,
      final String typeName) {
    final HaloDBDataIndexTable table = client.getDataIndexTable(typeName, adapterId);
    Arrays.stream(dataIds).forEach(d -> table.delete(d));
  }

  @Override
  public void close() {
    try {
      HaloDBCache.getInstance().close(directory);
    } catch (final IOException e) {
      LOGGER.warn("unable to close HaloDB at directory '" + directory + "'");
    }
  }
}
