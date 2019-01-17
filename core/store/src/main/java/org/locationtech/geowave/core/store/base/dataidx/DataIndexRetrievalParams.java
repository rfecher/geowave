package org.locationtech.geowave.core.store.base.dataidx;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Aggregation;

public class DataIndexRetrievalParams {
  private final Pair<String[], InternalDataAdapter<?>> fieldSubsets;
  private final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation;

  public DataIndexRetrievalParams(
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation) {
    this.fieldSubsets = fieldSubsets;
    this.aggregation = aggregation;
  }

  public Pair<String[], InternalDataAdapter<?>> getFieldSubsets() {
    return fieldSubsets;
  }

  public Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
    return aggregation;
  }
}
