package org.locationtech.geowave.core.store.base;

import java.util.concurrent.CompletableFuture;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public interface BatchDataIndexRetrieval extends DataIndexRetrieval {
  CompletableFuture<GeoWaveValue[]> getDataAsync(byte[] dataId, short adapterId);

  void flush();

  void incrementOutstandingIterators();

  void decrementOutstandingIterators();
}
