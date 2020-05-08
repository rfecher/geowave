/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.nio.file.Path;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public class FileSystemRowIterator extends AbstractFileSystemIterator<GeoWaveRow> {
  private final short adapterId;
  private final byte[] partition;
  private final boolean containsTimestamp;
  private final boolean visibilityEnabled;

  public FileSystemRowIterator(
      final Path subDirectory,
      final byte[] startKey,
      final byte[] endKey,
      final short adapterId,
      final byte[] partition,
      final boolean containsTimestamp,
      final boolean visiblityEnabled) {
    super(subDirectory, startKey, endKey);
    this.adapterId = adapterId;
    this.partition = partition;
    this.containsTimestamp = containsTimestamp;
    visibilityEnabled = visiblityEnabled;
  }

  @Override
  protected GeoWaveRow readRow(final byte[] key, final byte[] value) {
    return new FileSystemRow(
        adapterId,
        partition,
        key,
        value,
        containsTimestamp,
        visibilityEnabled);
  }
}
