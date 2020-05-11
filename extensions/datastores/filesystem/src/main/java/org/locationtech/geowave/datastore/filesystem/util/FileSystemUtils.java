package org.locationtech.geowave.datastore.filesystem.util;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.RangeReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Streams;
import com.google.common.primitives.UnsignedBytes;

public class FileSystemUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemUtils.class);

  public static int FILESYSTEM_DEFAULT_MAX_RANGE_DECOMPOSITION = 250;
  public static int FILESYSTEM_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION = 250;

  public static String keyToFileName(final byte[] key) {
    return ByteArrayUtils.byteArrayToString(key) + ".row";
  }

  public static byte[] fileNameToKey(final String key) {
    if (key.length() < 5) {
      return new byte[0];
    }
    return ByteArrayUtils.byteArrayFromString(key.substring(0, key.length() - 4));
  }

  public static SortedSet<Pair<byte[], Path>> getSortedSet(final Path subDirectory) {
    return getSortedSet(subDirectory, null, null);
  }

  public static SortedSet<Pair<byte[], Path>> getSortedSet(
      final Path subDirectory,
      final byte[] startKeyInclusive,
      final byte[] endKeyExclusive) {
    try {
      final Supplier<SortedSet<Pair<byte[], Path>>> sortedSetFactory =
          () -> new TreeSet<>(
              (p1, p2) -> UnsignedBytes.lexicographicalComparator().compare(
                  p1.getLeft(),
                  p2.getLeft()));
      SortedSet<Pair<byte[], Path>> sortedSet =
          Files.walk(subDirectory).filter(Files::isRegularFile).map(
              path -> Pair.of(
                  FileSystemUtils.fileNameToKey(path.getFileName().toString()),
                  path)).collect(Collectors.toCollection(sortedSetFactory));
      if (startKeyInclusive != null) {
        sortedSet = sortedSet.tailSet(Pair.of(startKeyInclusive, null));
      }
      if (endKeyExclusive != null) {
        sortedSet = sortedSet.headSet(Pair.of(endKeyExclusive, null));
      }
      return sortedSet;
    } catch (final IOException e) {
      LOGGER.warn("Unable to iterate through file system", e);
    }
    return new TreeSet<>();
  }

  public static void visit(
      final Path subDirectory,
      final byte[] startKeyInclusive,
      final byte[] endKeyExclusive,
      final Consumer<Path> pathVisitor) {
    getSortedSet(subDirectory, startKeyInclusive, endKeyExclusive).stream().map(
        Pair::getRight).forEach(pathVisitor);
  }

  public static String getTablePrefix(final String typeName, final String indexName) {
    return typeName + "_" + indexName;
  }

  public static FileSystemDataIndexTable getDataIndexTable(
      final FileSystemClient client,
      final String typeName,
      final short adapterId) {
    return client.getDataIndexTable(
        getTablePrefix(typeName, DataIndexUtils.DATA_ID_INDEX.getName()),
        adapterId);
  }

  public static FileSystemIndexTable getIndexTable(
      final FileSystemClient client,
      final String tableName,
      final short adapterId,
      final byte[] partitionKey,
      final boolean requiresTimestamp) {
    return client.getIndexTable(tableName, adapterId, partitionKey, requiresTimestamp);
  }

  public static boolean isSortByTime(final InternalDataAdapter<?> adapter) {
    return adapter.getAdapter() instanceof RowMergingDataAdapter;
  }

  public static boolean isSortByKeyRequired(final RangeReaderParams<?> params) {
    // subsampling needs to be sorted by sort key to work properly
    return (params.getMaxResolutionSubsamplingPerDimension() != null)
        && (params.getMaxResolutionSubsamplingPerDimension().length > 0);
  }

  public static Pair<Boolean, Boolean> isGroupByRowAndIsSortByTime(
      final RangeReaderParams<?> readerParams,
      final short adapterId) {
    final boolean sortByTime = isSortByTime(readerParams.getAdapterStore().getAdapter(adapterId));
    return Pair.of(readerParams.isMixedVisibility() || sortByTime, sortByTime);
  }

  public static Iterator<GeoWaveRow> sortBySortKey(final Iterator<GeoWaveRow> it) {
    return Streams.stream(it).sorted(SortKeyOrder.SINGLETON).iterator();
  }

  public static FileSystemMetadataTable getMetadataTable(
      final FileSystemClient client,
      final MetadataType metadataType) {
    // stats also store a timestamp because stats can be the exact same but
    // need to still be unique (consider multiple count statistics that are
    // exactly the same count, but need to be merged)
    return client.getMetadataTable(metadataType);
  }

  public static String getTableName(
      final String typeName,
      final String indexName,
      final byte[] partitionKey) {
    return getTableName(getTablePrefix(typeName, indexName), partitionKey);
  }

  public static String getTableName(final String setNamePrefix, final byte[] partitionKey) {
    String partitionStr;
    if ((partitionKey != null) && (partitionKey.length > 0)) {
      partitionStr = "_" + ByteArrayUtils.byteArrayToString(partitionKey);
    } else {
      partitionStr = "";
    }
    return setNamePrefix + partitionStr;
  }

  public static FileSystemIndexTable getIndexTableFromPrefix(
      final FileSystemClient client,
      final String namePrefix,
      final short adapterId,
      final byte[] partitionKey,
      final boolean requiresTimestamp) {
    return getIndexTable(
        client,
        getTableName(namePrefix, partitionKey),
        adapterId,
        partitionKey,
        requiresTimestamp);
  }

  public static Set<ByteArray> getPartitions(final String directory, final String tableNamePrefix) {
    return Arrays.stream(
        new File(directory).list((dir, name) -> name.startsWith(tableNamePrefix))).map(
            str -> str.length() > (tableNamePrefix.length() + 1)
                ? new ByteArray(
                    ByteArrayUtils.byteArrayFromString(str.substring(tableNamePrefix.length() + 1)))
                : new ByteArray()).collect(Collectors.toSet());
  }

  private static class SortKeyOrder implements Comparator<GeoWaveRow>, Serializable {
    private static SortKeyOrder SINGLETON = new SortKeyOrder();
    private static final long serialVersionUID = 23275155231L;

    @Override
    public int compare(final GeoWaveRow o1, final GeoWaveRow o2) {
      if (o1 == o2) {
        return 0;
      }
      if (o1 == null) {
        return 1;
      }
      if (o2 == null) {
        return -1;
      }
      byte[] otherComp = o2.getSortKey() == null ? new byte[0] : o2.getSortKey();
      byte[] thisComp = o1.getSortKey() == null ? new byte[0] : o1.getSortKey();

      int comp = UnsignedBytes.lexicographicalComparator().compare(thisComp, otherComp);
      if (comp != 0) {
        return comp;
      }
      otherComp = o2.getPartitionKey() == null ? new byte[0] : o2.getPartitionKey();
      thisComp = o1.getPartitionKey() == null ? new byte[0] : o1.getPartitionKey();

      comp = UnsignedBytes.lexicographicalComparator().compare(thisComp, otherComp);
      if (comp != 0) {
        return comp;
      }
      comp = Short.compare(o1.getAdapterId(), o2.getAdapterId());
      if (comp != 0) {
        return comp;
      }
      otherComp = o2.getDataId() == null ? new byte[0] : o2.getDataId();
      thisComp = o1.getDataId() == null ? new byte[0] : o1.getDataId();

      comp = UnsignedBytes.lexicographicalComparator().compare(thisComp, otherComp);

      if (comp != 0) {
        return comp;
      }
      return Integer.compare(o1.getNumberOfDuplicates(), o2.getNumberOfDuplicates());
    }
  }
}
