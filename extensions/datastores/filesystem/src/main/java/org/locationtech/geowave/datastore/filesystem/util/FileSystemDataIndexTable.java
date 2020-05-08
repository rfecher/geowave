/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemDataIndexTable extends AbstractFileSystemTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemDataIndexTable.class);

  public FileSystemDataIndexTable(
      final String subDirectory,
      final short adapterId,
      final boolean visibilityEnabled) throws IOException {
    super(subDirectory, adapterId, visibilityEnabled);
  }

  public synchronized void add(final byte[] dataId, final GeoWaveValue value) {
    put(dataId, DataIndexUtils.serializeDataIndexValue(value, visibilityEnabled));
  }

  public CloseableIterator<GeoWaveRow> dataIndexIterator(final byte[][] dataIds) {
    final List<byte[]> dataIdsList = Arrays.asList(dataIds);
    return new CloseableIterator.Wrapper(
        Arrays.stream(dataIds).map(
            // convert to pari with path so the path is only instantiated once (depending on
            // filesystem (such as S3 or HDFS) can be modestly expensive
            dataId -> Pair.of(
                dataId,
                subDirectory.resolve(Paths.get(FileSystemUtils.keyToFileName(dataId))))).filter(
                    p -> Files.exists(p.getRight())).map(pair -> {
                      try {
                        return DataIndexUtils.deserializeDataIndexRow(
                            pair.getLeft(),
                            adapterId,
                            Files.readAllBytes(pair.getRight()),
                            visibilityEnabled);
                      } catch (final IOException e) {
                        LOGGER.error(
                            "Unable to read value by data ID for file '" + pair.getRight() + "'",
                            e);
                        return null;
                      }
                    }).filter(Objects::nonNull).iterator());
  }

  public CloseableIterator<GeoWaveRow> dataIndexIterator(
      final byte[] startDataId,
      final byte[] endDataId) {
    return new DataIndexRowIterator(
        subDirectory,
        startDataId,
        endDataId,
        adapterId,
        visibilityEnabled);
  }
}
