package org.locationtech.geowave.core.store.base;

import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public interface DataIndexRetrieval {
  GeoWaveValue[] getData(byte[] dataId, short adapterId);

  void setParams(DataIndexRetrievalParams params);
}
