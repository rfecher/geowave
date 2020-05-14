package org.locationtech.geowave.datastore.filesystem.util;

import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.FileSystemIndexKey;

class FileSystemIndexKeyWrapper implements FileSystemKey {
  private final byte[] sortOrderKey;
  private final FileSystemIndexKey key;
  private final String fileName;

  public FileSystemIndexKeyWrapper(final FileSystemIndexKey key, final String fileName) {
    super();
    sortOrderKey = key.getSortOrderKey();
    this.key = key;
    this.fileName = fileName;
  }

  @Override
  public byte[] getSortOrderKey() {
    return sortOrderKey;
  }

  @Override
  public String getFileName() {
    return fileName;
  }

  public FileSystemIndexKey getOriginalKey() {
    return key;
  }

}
