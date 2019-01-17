package org.locationtech.geowave.core.store.base;

import java.util.function.BiConsumer;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParams;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.Deleter;
import org.locationtech.geowave.core.store.operations.RowReader;

public class BaseDataIndexDeleter<T extends DataStoreOperations> implements Deleter<GeoWaveRow> {

  private boolean closed = false;
  private final RowReader<GeoWaveRow> readerDelegate;
  private final T operations;
  private final DataIndexReaderParams readerParams;
  private final BiConsumer<DataIndexReaderParams, T> deleter;

  public BaseDataIndexDeleter(
      final DataIndexReaderParams readerParams,
      final T operations,
      final BiConsumer<DataIndexReaderParams, T> deleter) {
    readerDelegate = operations.createReader(readerParams);
    this.operations = operations;
    this.deleter = deleter;
    this.readerParams = readerParams;
  }

  @Override
  public boolean hasNext() {
    return readerDelegate.hasNext();
  }

  @Override
  public GeoWaveRow next() {
    return readerDelegate.next();
  }

  @Override
  public void close() {
    if (!closed) {
      deleter.accept(readerParams, operations);

      closed = true;
    }
    readerDelegate.close();
  }

  @Override
  public void entryScanned(final GeoWaveRow entry, final GeoWaveRow row) {}

}

