/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.operations;

import java.util.Arrays;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemClient;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemIndexTable;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemRow;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class FileSystemRowDeleter implements RowDeleter {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemRowDeleter.class);

  private static class CacheKey {
    private final String tableName;
    private final short adapterId;
    private final byte[] partition;
    private final String format;

    public CacheKey(
        final String tableName,
        final short adapterId,
        final byte[] partition,
        final String format) {
      this.tableName = tableName;
      this.adapterId = adapterId;
      this.partition = partition;
      this.format = format;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + adapterId;
      result = (prime * result) + ((format == null) ? 0 : format.hashCode());
      result = (prime * result) + Arrays.hashCode(partition);
      result = (prime * result) + ((tableName == null) ? 0 : tableName.hashCode());
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final CacheKey other = (CacheKey) obj;
      if (adapterId != other.adapterId) {
        return false;
      }
      if (format == null) {
        if (other.format != null) {
          return false;
        }
      } else if (!format.equals(other.format)) {
        return false;
      }
      if (!Arrays.equals(partition, other.partition)) {
        return false;
      }
      if (tableName == null) {
        if (other.tableName != null) {
          return false;
        }
      } else if (!tableName.equals(other.tableName)) {
        return false;
      }
      return true;
    }
  }

  private final LoadingCache<CacheKey, FileSystemIndexTable> tableCache =
      Caffeine.newBuilder().build(nameAndAdapterId -> getIndexTable(nameAndAdapterId));
  private final FileSystemClient client;
  private final PersistentAdapterStore adapterStore;
  private final InternalAdapterStore internalAdapterStore;
  private final String indexName;
  private final String format;

  public FileSystemRowDeleter(
      final FileSystemClient client,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final String indexName,
      final String format) {
    this.client = client;
    this.adapterStore = adapterStore;
    this.internalAdapterStore = internalAdapterStore;
    this.indexName = indexName;
    this.format = format;
  }

  @Override
  public void close() {
    tableCache.invalidateAll();
  }

  private FileSystemIndexTable getIndexTable(final CacheKey cacheKey) {
    return FileSystemUtils.getIndexTable(
        client,
        cacheKey.tableName,
        cacheKey.adapterId,
        cacheKey.partition,
        cacheKey.format,
        FileSystemUtils.isSortByTime(adapterStore.getAdapter(cacheKey.adapterId)));
  }

  @Override
  public void delete(final GeoWaveRow row) {
    final FileSystemIndexTable table =
        tableCache.get(
            new CacheKey(
                FileSystemUtils.getTableName(
                    internalAdapterStore.getTypeName(row.getAdapterId()),
                    indexName,
                    row.getPartitionKey()),
                row.getAdapterId(),
                row.getPartitionKey(),
                format));
    if (row instanceof GeoWaveRowImpl) {
      final GeoWaveKey key = ((GeoWaveRowImpl) row).getKey();
      if (key instanceof FileSystemRow) {
        deleteRow(table, (FileSystemRow) key);
      } else {
        LOGGER.info(
            "Unable to convert scanned row into RocksDBRow for deletion.  Row is of type GeoWaveRowImpl.");
        table.delete(key.getSortKey(), key.getDataId());
      }
    } else if (row instanceof FileSystemRow) {
      deleteRow(table, (FileSystemRow) row);
    } else {
      LOGGER.info(
          "Unable to convert scanned row into RocksDBRow for deletion. Row is of type "
              + row.getClass());
      table.delete(row.getSortKey(), row.getDataId());
    }
  }

  private static void deleteRow(final FileSystemIndexTable table, final FileSystemRow row) {
    Arrays.stream(row.getKeys()).forEach(k -> table.delete(k));
  }

  @Override
  public void flush() {}
}
