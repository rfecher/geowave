package org.locationtech.geowave.datastore.halodb.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.oath.halodb.HaloDB;
import com.oath.halodb.HaloDBException;
import com.oath.halodb.HaloDBOptions;

public class HaloDBDataIndexTable implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HaloDBDataIndexTable.class);
  private final HaloDB db;
  private final boolean visibilityEnabled;
  private final short adapterId;

  public HaloDBDataIndexTable(
      final String directory,
      final HaloDBOptions dbOptions,
      final boolean visibilityEnabled,
      final short adapterId) throws HaloDBException {
    // Open the database. Directory will be created if it doesn't exist.
    // If we are opening an existing database HaloDB needs to scan all the
    // index files to create the in-memory index, which, depending on the db size, might take a few
    // minutes.
    this(HaloDB.open(directory, dbOptions), visibilityEnabled, adapterId);
  }

  private HaloDBDataIndexTable(
      final HaloDB db,
      final boolean visibilityEnabled,
      final short adapterId) {
    super();
    this.db = db;
    this.visibilityEnabled = visibilityEnabled;
    this.adapterId = adapterId;
  }

  public void add(final byte[] dataId, final GeoWaveValue value) {
    try {
      db.put(dataId, DataIndexUtils.serializeDataIndexValue(value, visibilityEnabled));
    } catch (final HaloDBException e) {
      LOGGER.error(
          "Unable to add to data index ID: '"
              + ByteArrayUtils.getHexString(dataId)
              + "' (hex), '"
              + StringUtils.stringFromBinary(dataId)
              + "' ("
              + StringUtils.getGeoWaveCharset().name()
              + ")",
          e);
    }
  }

  public void delete(final byte[] dataId) {
    try {
      db.delete(dataId);
    } catch (final HaloDBException e) {
      LOGGER.error(
          "Unable to delete from data index ID: '"
              + ByteArrayUtils.getHexString(dataId)
              + "' (hex), '"
              + StringUtils.stringFromBinary(dataId)
              + "' ("
              + StringUtils.getGeoWaveCharset().name()
              + ")",
          e);
    }
  }

  public Iterator<GeoWaveRow> dataIndexIterator(final byte[][] dataIds) {
    return Arrays.stream(dataIds).map(
        dataId -> DataIndexUtils.deserializeDataIndexRow(
            dataId,
            adapterId,
            getValue(dataId),
            visibilityEnabled)).iterator();
  }

  private byte[] getValue(final byte[] dataId) {
    try {
      return db.get(dataId);
    } catch (final HaloDBException e) {
      LOGGER.error(
          "Unable to get value by data ID '"
              + ByteArrayUtils.getHexString(dataId)
              + "' (hex), '"
              + StringUtils.stringFromBinary(dataId)
              + "' ("
              + StringUtils.getGeoWaveCharset().name()
              + ")",
          e);
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    try {
      db.close();
    } catch (final HaloDBException e) {
      LOGGER.warn("unable to close halodb");
      throw new IOException(e);
    }
  }
}
