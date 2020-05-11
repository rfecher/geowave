/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map.Entry;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class FileSystemClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemClient.class);

  private static class CacheKey {
    protected final String directory;
    protected final boolean requiresTimestamp;

    public CacheKey(final String directory, final boolean requiresTimestamp) {
      this.directory = directory;
      this.requiresTimestamp = requiresTimestamp;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((directory == null) ? 0 : directory.hashCode());
      result = (prime * result) + (requiresTimestamp ? 1231 : 1237);
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
      if (directory == null) {
        if (other.directory != null) {
          return false;
        }
      } else if (!directory.equals(other.directory)) {
        return false;
      }
      if (requiresTimestamp != other.requiresTimestamp) {
        return false;
      }
      return true;
    }
  }

  private static class IndexCacheKey extends DataIndexCacheKey {
    protected final byte[] partition;

    public IndexCacheKey(
        final String directory,
        final short adapterId,
        final byte[] partition,
        final String format,
        final boolean requiresTimestamp) {
      super(directory, requiresTimestamp, adapterId, format);
      this.partition = partition;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = (prime * result) + Arrays.hashCode(partition);
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!super.equals(obj)) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final IndexCacheKey other = (IndexCacheKey) obj;
      if (!Arrays.equals(partition, other.partition)) {
        return false;
      }
      return true;
    }

  }
  private static class DataIndexCacheKey extends CacheKey {
    protected final short adapterId;
    protected final String format;

    public DataIndexCacheKey(final String directory, final short adapterId, final String format) {
      super(directory, false);
      this.adapterId = adapterId;
      this.format = format;
    }

    private DataIndexCacheKey(
        final String directory,
        final boolean requiresTimestamp,
        final short adapterId,
        final String format) {
      super(directory, requiresTimestamp);
      this.adapterId = adapterId;
      this.format = format;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = (prime * result) + adapterId;
      result = (prime * result) + ((format == null) ? 0 : format.hashCode());
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!super.equals(obj)) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final DataIndexCacheKey other = (DataIndexCacheKey) obj;
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
      return true;
    }

  }

  private final Cache<String, CacheKey> keyCache = Caffeine.newBuilder().build();
  private final LoadingCache<IndexCacheKey, FileSystemIndexTable> indexTableCache =
      Caffeine.newBuilder().build(key -> loadIndexTable(key));

  private final LoadingCache<DataIndexCacheKey, FileSystemDataIndexTable> dataIndexTableCache =
      Caffeine.newBuilder().build(key -> loadDataIndexTable(key));
  private final LoadingCache<CacheKey, FileSystemMetadataTable> metadataTableCache =
      Caffeine.newBuilder().build(key -> loadMetadataTable(key));
  private final String subDirectory;
  private final boolean visibilityEnabled;

  public FileSystemClient(final String subDirectory, final boolean visibilityEnabled) {
    this.subDirectory = subDirectory;
    this.visibilityEnabled = visibilityEnabled;
  }

  private FileSystemMetadataTable loadMetadataTable(final CacheKey key) throws IOException {
    Path dir = Paths.get(key.directory);
    if (!Files.exists(dir)) {
      dir = Files.createDirectories(dir);
    }
    return new FileSystemMetadataTable(
        Paths.get(key.directory),
        key.requiresTimestamp,
        visibilityEnabled);
  }

  private FileSystemIndexTable loadIndexTable(final IndexCacheKey key) throws IOException {
    return new FileSystemIndexTable(
        key.directory,
        key.adapterId,
        key.partition,
        key.format,
        key.requiresTimestamp,
        visibilityEnabled);
  }

  private FileSystemDataIndexTable loadDataIndexTable(final DataIndexCacheKey key)
      throws IOException {
    return new FileSystemDataIndexTable(
        key.directory,
        key.adapterId,
        visibilityEnabled,
        key.format);
  }

  public String getSubDirectory() {
    return subDirectory;
  }

  public synchronized FileSystemIndexTable getIndexTable(
      final String tableName,
      final short adapterId,
      final byte[] partition,
      final String format,
      final boolean requiresTimestamp) {
    final String directory = subDirectory + "/" + tableName;
    return indexTableCache.get(
        (IndexCacheKey) keyCache.get(
            directory,
            d -> new IndexCacheKey(d, adapterId, partition, format, requiresTimestamp)));
  }

  public synchronized FileSystemDataIndexTable getDataIndexTable(
      final String tableName,
      final short adapterId,
      final String format) {
    final String directory = subDirectory + "/" + tableName;
    return dataIndexTableCache.get(
        (DataIndexCacheKey) keyCache.get(
            directory,
            d -> new DataIndexCacheKey(d, adapterId, format)));
  }

  public synchronized FileSystemMetadataTable getMetadataTable(final MetadataType type) {
    final String directory = subDirectory + "/" + type.name();
    return metadataTableCache.get(
        keyCache.get(directory, d -> new CacheKey(d, type.equals(MetadataType.STATS))));
  }

  public boolean indexTableExists(final String indexName) {
    // then look for prefixes of this index directory in which case there is
    // a partition key
    for (final String key : keyCache.asMap().keySet()) {
      if (key.substring(subDirectory.length()).contains(indexName)) {
        return true;
      }
    }
    // this could have been created by a different process so check the
    // directory listing
    try {
      return Files.list(Paths.get(subDirectory)).anyMatch(p -> p.toString().contains(indexName));
    } catch (final IOException e) {
      LOGGER.warn("Unable to list directory", e);
    }
    return false;
  }

  public boolean metadataTableExists(final MetadataType type) {
    // this could have been created by a different process so check the
    // directory listing
    return (keyCache.getIfPresent(subDirectory + "/" + type.name()) != null)
        || Files.exists(Paths.get(subDirectory + "/" + type.name()));
  }

  public void close(final String indexName, final String typeName) {
    final String prefix = FileSystemUtils.getTablePrefix(typeName, indexName);
    for (final Entry<String, CacheKey> e : keyCache.asMap().entrySet()) {
      final String key = e.getKey();
      if (key.substring(subDirectory.length() + 1).startsWith(prefix)) {
        keyCache.invalidate(key);
        AbstractFileSystemTable indexTable = indexTableCache.getIfPresent(e.getValue());
        if (indexTable == null) {
          indexTable = dataIndexTableCache.getIfPresent(e.getValue());
        }
        if (indexTable != null) {
          indexTableCache.invalidate(e.getValue());
          dataIndexTableCache.invalidate(e.getValue());
        }
      }
    }
  }

  public boolean isVisibilityEnabled() {
    return visibilityEnabled;
  }

}
