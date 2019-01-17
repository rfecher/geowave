package org.locationtech.geowave.core.store.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.DefaultDataIndexRowWriterWrapper.GeoWaveRowWrapper;

public class DefaultDataIndexRowDeleterWrapper implements RowDeleter {
  private final RowDeleter delegateDeleter;

  public DefaultDataIndexRowDeleterWrapper(final RowDeleter delegateDeleter) {
    this.delegateDeleter = delegateDeleter;
  }

  @Override
  public void delete(final GeoWaveRow row) {
    delegateDeleter.delete(new GeoWaveRowWrapper(row));
  }

  @Override
  public void flush() {
    delegateDeleter.flush();
  }

  @Override
  public void close() {
    delegateDeleter.close();
  }
}
