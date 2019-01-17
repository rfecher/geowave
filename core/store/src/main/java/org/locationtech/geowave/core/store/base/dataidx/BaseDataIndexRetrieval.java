package org.locationtech.geowave.core.store.base.dataidx;

import java.util.function.BiFunction;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.util.TriFunction;

public class BaseDataIndexRetrieval implements DataIndexRetrieval {
  private final TriFunction<byte[], Short, DataIndexRetrievalParams, GeoWaveValue[]> function;
  private BiFunction<byte[], Short, GeoWaveValue[]> reducedFunction;

  public BaseDataIndexRetrieval(
      final TriFunction<byte[], Short, DataIndexRetrievalParams, GeoWaveValue[]> function) {
    this.function = function;
  }

  @Override
  public GeoWaveValue[] getData(final byte[] dataId, final short adapterId) {
    return reducedFunction.apply(dataId, adapterId);
  }

  @Override
  public void setParams(final DataIndexRetrievalParams params) {
    reducedFunction = (dataId, adapterId) -> function.apply(dataId, adapterId, params);
  }
}
