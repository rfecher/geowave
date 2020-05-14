package org.locationtech.geowave.datastore.filesystem.util;

import com.google.common.primitives.UnsignedBytes;

interface FileSystemKey extends Comparable<FileSystemKey> {
  byte[] getSortOrderKey();

  String getFileName();

  @Override
  default int compareTo(final FileSystemKey o) {
    return UnsignedBytes.lexicographicalComparator().compare(
        getSortOrderKey(),
        o.getSortOrderKey());
  }

}
