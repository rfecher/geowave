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
import java.util.Collection;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

public class FileSystemIndexTable extends AbstractFileSystemTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemIndexTable.class);
  private long prevTime = Long.MAX_VALUE;
  private final boolean requiresTimestamp;
  private final byte[] partition;

  public FileSystemIndexTable(
      final String subDirectory,
      final short adapterId,
      final byte[] partition,
      final boolean requiresTimestamp,
      final boolean visibilityEnabled) throws IOException {
    super(subDirectory, adapterId, visibilityEnabled);
    this.requiresTimestamp = requiresTimestamp;
    this.partition = partition;
  }

  public void delete(final byte[] sortKey, final byte[] dataId) {
    final byte[] prefix = Bytes.concat(sortKey, dataId);
    FileSystemUtils.visit(subDirectory, prefix, ByteArrayUtils.getNextPrefix(prefix), p -> {
      try {
        Files.deleteIfExists(p);
      } catch (final IOException e) {
        LOGGER.warn("", e);
      }
    });
  }

  public synchronized void add(
      final byte[] sortKey,
      final byte[] dataId,
      final short numDuplicates,
      final GeoWaveValue value) {
    byte[] key;
    byte[] endBytes;
    if (visibilityEnabled) {
      endBytes =
          Bytes.concat(
              value.getVisibility(),
              ByteArrayUtils.shortToByteArray(numDuplicates),
              new byte[] {
                  (byte) value.getVisibility().length,
                  (byte) sortKey.length,
                  (byte) value.getFieldMask().length});
    } else {
      endBytes =
          Bytes.concat(
              ByteArrayUtils.shortToByteArray(numDuplicates),
              new byte[] {(byte) sortKey.length, (byte) value.getFieldMask().length});
    }
    if (requiresTimestamp) {
      // sometimes rows can be written so quickly that they are the exact
      // same millisecond - while Java does offer nanosecond precision,
      // support is OS-dependent. Instead this check is done to ensure
      // subsequent millis are written at least within this ingest
      // process.
      long time = Long.MAX_VALUE - System.currentTimeMillis();
      if (time >= prevTime) {
        time = prevTime - 1;
      }
      prevTime = time;
      key = Bytes.concat(sortKey, dataId, Longs.toByteArray(time), value.getFieldMask(), endBytes);
    } else {
      key = Bytes.concat(sortKey, dataId, value.getFieldMask(), endBytes);
    }
    put(key, value.getValue());
  }


  public CloseableIterator<GeoWaveRow> iterator() {
    return new FileSystemRowIterator(
        subDirectory,
        null,
        null,
        adapterId,
        partition,
        requiresTimestamp,
        visibilityEnabled);
  }

  public CloseableIterator<GeoWaveRow> iterator(final Collection<ByteArrayRange> ranges) {
    return new FileSystemRowIterator(
        subDirectory,
        ranges,
        adapterId,
        partition,
        requiresTimestamp,
        visibilityEnabled);
  }
}
