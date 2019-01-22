package org.locationtech.geowave.core.store.operations;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;

public abstract class BaseReaderParamsBuilder<T, R extends BaseReaderParamsBuilder<T, R>> {
  protected final PersistentAdapterStore adapterStore;
  protected final InternalAdapterStore internalAdapterStore;
  protected Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation = null;
  protected Pair<String[], InternalDataAdapter<?>> fieldSubsets = null;
  protected boolean isAuthorizationsLimiting = true;
  protected String[] additionalAuthorizations;

  public BaseReaderParamsBuilder(
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore) {
    this.adapterStore = adapterStore;
    this.internalAdapterStore = internalAdapterStore;
  }

  protected abstract R builder();

  public R aggregation(final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation) {
    this.aggregation = aggregation;
    return builder();
  }

  public R fieldSubsets(final Pair<String[], InternalDataAdapter<?>> fieldSubsets) {
    this.fieldSubsets = fieldSubsets;
    return builder();
  }

  public R additionalAuthorizations(final String... authorizations) {
    this.additionalAuthorizations = authorizations;
    return builder();
  }

  public R isAuthorizationsLimiting(final boolean isAuthorizationsLimiting) {
    this.isAuthorizationsLimiting = isAuthorizationsLimiting;
    return builder();
  }
}
