package org.locationtech.geowave.datastore.halodb.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import org.locationtech.geowave.datastore.halodb.HaloDBOptions;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.oath.halodb.HaloDBException;

public class HaloDBClient implements Closeable {
  private static class CacheKey {
    private final short adapterId;
    private final String directory;

    public CacheKey(final String directory, final short adapterId) {
      this.directory = directory;
      this.adapterId = adapterId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + adapterId;
      result = (prime * result) + ((directory == null) ? 0 : directory.hashCode());
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
      if (directory == null) {
        if (other.directory != null) {
          return false;
        }
      } else if (!directory.equals(other.directory)) {
        return false;
      }
      return true;
    }

  }

  private final Cache<String, CacheKey> keyCache = Caffeine.newBuilder().build();
  private final LoadingCache<CacheKey, HaloDBDataIndexTable> dataIndexTableCache =
      Caffeine.newBuilder().build(key -> loadDataIndexTable(key));

  private final boolean visibilityEnabled;
  private final String subDirectory;

  private final com.oath.halodb.HaloDBOptions nativeOptions;

  public HaloDBClient(
      final HaloDBOptions options,
      final String namespace,
      final boolean visibilityEnabled) {
    subDirectory = options.getDirectory() + File.separator + namespace;
    nativeOptions = options.getNativeOptions();
    this.visibilityEnabled = visibilityEnabled;
  }


  private HaloDBDataIndexTable loadDataIndexTable(final CacheKey key) throws HaloDBException {
    return new HaloDBDataIndexTable(key.directory, nativeOptions, visibilityEnabled, key.adapterId);
  }

  public synchronized HaloDBDataIndexTable getDataIndexTable(
      final String typeName,
      final short adapterId) {
    final String directory = subDirectory + "/" + HaloDBUtils.getTableName(typeName);
    return dataIndexTableCache.get(keyCache.get(directory, d -> new CacheKey(d, adapterId)));
  }

  public void close(final String typeName) throws IOException {
    final String directory = subDirectory + "/" + HaloDBUtils.getTableName(typeName);
    final CacheKey key = keyCache.getIfPresent(directory);
    if (key != null) {
      final HaloDBDataIndexTable table = dataIndexTableCache.getIfPresent(key);
      if (table != null) {
        table.close();
        dataIndexTableCache.invalidate(key);
      }
      keyCache.invalidate(directory);
    }
  }

  @Override
  public void close() throws IOException {
    keyCache.invalidateAll();
    for (final HaloDBDataIndexTable table : dataIndexTableCache.asMap().values()) {
      table.close();
    }
    dataIndexTableCache.invalidateAll();
  }
}
