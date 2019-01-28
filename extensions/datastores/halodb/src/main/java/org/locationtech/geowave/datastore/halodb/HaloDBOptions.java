package org.locationtech.geowave.datastore.halodb;

import com.beust.jcommander.Parameter;

public class HaloDBOptions {
  @Parameter(
      names = "--halodb-dir",
      description = "The directory to read/write for the data index.  Defaults to \"halodb\" in the working directory.")
  private final String directory = "halodb";
  @Parameter(
      names = "--halodb-maxfilesize",
      description = "The max file size in bytes.  Defaults to 1 GB.")
  private final int maxFileSize = 1024 * 1024 * 1024;

  @Parameter(
      names = "--halodb-flushdatasize",
      description = "The threshold at which page cache is synced to disk.  Defaults to 10 MB.")
  private final int flushDataSize = 10 * 1024 * 1024;

  @Parameter(
      names = "--halodb-compactionthreshold",
      description = "The percentage of stale data in a data file at which the file will be compacted.  Defaults to 70%.")
  private final double compactionThreshold = 0.7;

  @Parameter(
      names = "--halodb-compactionjobrate",
      description = " This is the amount of data which will be copied by the compaction thread per second.  Defaults to 50 MB.")
  private final int compactionJobRate = 50 * 1024 * 1024;

  @Parameter(
      names = "--halodb-expectedrecords",
      description = "Setting this value is important as it helps to preallocate enough memory for the off-heap cache..  Defaults to 250 million.")
  private final int expectedTotalRecords = 250_000_000;

  @Parameter(
      names = "--halodb-maxkeysize",
      description = "Using a memory pool requires us to declare the size of keys in advance. Any write request with key length greater than the declared value will fail, but it is still possible to store keys smaller than this declared size. Defaults to 4 bytes.")
  private final int maxKeySize = 4;

  public String getDirectory() {
    return directory;
  }

  public com.oath.halodb.HaloDBOptions getNativeOptions() {
    // Open a db with default options.
    final com.oath.halodb.HaloDBOptions options = new com.oath.halodb.HaloDBOptions();

    // size of each data file will be 1GB.
    options.setMaxFileSize(maxFileSize);

    // the threshold at which page cache is synced to disk.
    // data will be durable only if it is flushed to disk, therefore
    // more data will be lost if this value is set too high. Setting
    // this value too low might interfere with read and write performance.
    options.setFlushDataSizeBytes(flushDataSize);

    // The percentage of stale data in a data file at which the file will be compacted.
    // This value helps control write and space amplification. Increasing this value will
    // reduce write amplification but will increase space amplification.
    // This along with the compactionJobRate below is the most important setting
    // for tuning HaloDB performance. If this is set to x then write amplification
    // will be approximately 1/x.
    options.setCompactionThresholdPerFile(compactionThreshold);

    // Controls how fast the compaction job should run.
    // This is the amount of data which will be copied by the compaction thread per second.
    // Optimal value depends on the compactionThresholdPerFile option.
    options.setCompactionJobRate(compactionJobRate);

    // Setting this value is important as it helps to preallocate enough
    // memory for the off-heap cache. If the value is too low the db might
    // need to rehash the cache. For a db of size n set this value to 2*n.
    options.setNumberOfRecords(expectedTotalRecords);

    // Delete operation for a key will write a tombstone record to a tombstone file.
    // the tombstone record can be removed only when all previous version of that key
    // has been deleted by the compaction job.
    // enabling this option will delete during startup all tombstone records whose previous
    // versions were removed from the data file.
    options.setCleanUpTombstonesDuringOpen(true);

    // HaloDB does native memory allocation for the in-memory index.
    // Enabling this option will release all allocated memory back to the kernel when the db is
    // closed.
    // This option is not necessary if the JVM is shutdown when the db is closed, as in that case
    // allocated memory is released automatically by the kernel.
    // If using in-memory index without memory pool this option,
    // depending on the number of records in the database,
    // could be a slow as we need to call _free_ for each record.
    options.setCleanUpInMemoryIndexOnClose(false);

    // ** settings for memory pool **
    options.setUseMemoryPool(true);

    // Hash table implementation in HaloDB is similar to that of ConcurrentHashMap in Java 7.
    // Hash table is divided into segments and each segment manages its own native memory.
    // The number of segments is twice the number of cores in the machine.
    // A segment's memory is further divided into chunks whose size can be configured here.
    options.setMemoryPoolChunkSize(2 * 1024 * 1024);

    // using a memory pool requires us to declare the size of keys in advance.
    // Any write request with key length greater than the declared value will fail, but it
    // is still possible to store keys smaller than this declared size.
    options.setFixedKeySize(maxKeySize);
    return options;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + compactionJobRate;
    long temp;
    temp = Double.doubleToLongBits(compactionThreshold);
    result = (prime * result) + (int) (temp ^ (temp >>> 32));
    result = (prime * result) + ((directory == null) ? 0 : directory.hashCode());
    result = (prime * result) + expectedTotalRecords;
    result = (prime * result) + flushDataSize;
    result = (prime * result) + maxFileSize;
    result = (prime * result) + maxKeySize;
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
    final HaloDBOptions other = (HaloDBOptions) obj;
    if (compactionJobRate != other.compactionJobRate) {
      return false;
    }
    if (Double.doubleToLongBits(compactionThreshold) != Double.doubleToLongBits(
        other.compactionThreshold)) {
      return false;
    }
    if (directory == null) {
      if (other.directory != null) {
        return false;
      }
    } else if (!directory.equals(other.directory)) {
      return false;
    }
    if (expectedTotalRecords != other.expectedTotalRecords) {
      return false;
    }
    if (flushDataSize != other.flushDataSize) {
      return false;
    }
    if (maxFileSize != other.maxFileSize) {
      return false;
    }
    if (maxKeySize != other.maxKeySize) {
      return false;
    }
    return true;
  }
}
