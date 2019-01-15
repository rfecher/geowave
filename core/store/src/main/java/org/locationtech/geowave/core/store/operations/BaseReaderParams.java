package org.locationtech.geowave.core.store.operations;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;

public abstract class BaseReaderParams<T> {
  private final PersistentAdapterStore adapterStore;
  private final InternalAdapterStore internalAdapterStore;
  private final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation;
  private final Pair<String[], InternalDataAdapter<?>> fieldSubsets;

  public BaseReaderParams(
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets) {
    this.adapterStore = adapterStore;
    this.internalAdapterStore = internalAdapterStore;
    this.aggregation = aggregation;
    this.fieldSubsets = fieldSubsets;
  }


  public PersistentAdapterStore getAdapterStore() {
    return adapterStore;
  }

  public InternalAdapterStore getInternalAdapterStore() {
    return internalAdapterStore;
  }

  public Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
    return aggregation;
  }

  public Pair<String[], InternalDataAdapter<?>> getFieldSubsets() {
    return fieldSubsets;
  }

  public boolean isAggregation() {
    return ((aggregation != null) && (aggregation.getRight() != null));
  }
}
