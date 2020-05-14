package org.locationtech.geowave.datastore.filesystem.util;

class BasicFileSystemKey implements FileSystemKey {
  // this can be more than just a sort key, as it may include a timestamp
  private final byte[] sortOrderKey;
  private final String fileName;

  public BasicFileSystemKey(final byte[] sortOrderKey) {
    this(sortOrderKey, null);
  }

  public BasicFileSystemKey(final byte[] sortOrderKey, final String fileName) {
    super();
    this.sortOrderKey = sortOrderKey;
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


}
