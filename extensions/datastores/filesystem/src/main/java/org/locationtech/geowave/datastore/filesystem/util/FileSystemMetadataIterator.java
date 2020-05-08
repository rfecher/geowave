/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

public class FileSystemMetadataIterator extends AbstractFileSystemIterator<GeoWaveMetadata> {
  private final boolean containsTimestamp;
  private final boolean visibilityEnabled;

  public FileSystemMetadataIterator(
      final Path subDirectory,
      final byte[] startKey,
      final byte[] endKey,
      final boolean containsTimestamp,
      final boolean visibilityEnabled) {
    super(subDirectory, startKey, endKey);
    this.containsTimestamp = containsTimestamp;
    this.visibilityEnabled = visibilityEnabled;
  }

  @Override
  protected GeoWaveMetadata readRow(final byte[] key, final byte[] value) {
    final ByteBuffer buf = ByteBuffer.wrap(key);
    final byte[] primaryId = new byte[Byte.toUnsignedInt(key[key.length - 1])];
    final byte[] visibility;

    if (visibilityEnabled) {
      visibility = new byte[Byte.toUnsignedInt(key[key.length - 2])];
    } else {
      visibility = new byte[0];
    }
    int secondaryIdLength = key.length - primaryId.length - visibility.length - 1;
    if (containsTimestamp) {
      secondaryIdLength -= 8;
    }
    if (visibilityEnabled) {
      secondaryIdLength--;
    }
    final byte[] secondaryId = new byte[secondaryIdLength];
    buf.get(primaryId);
    buf.get(secondaryId);
    if (containsTimestamp) {
      // just skip 8 bytes - we don't care to parse out the timestamp but
      // its there for key uniqueness and to maintain expected sort order
      buf.position(buf.position() + 8);
    }
    if (visibilityEnabled) {
      buf.get(visibility);
    }

    return new FileSystemGeoWaveMetadata(primaryId, secondaryId, visibility, value, key);
  }
}
