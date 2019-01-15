package org.locationtech.geowave.core.store.util;

import java.util.Iterator;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BatchDataIndexRetrieval;
import org.locationtech.geowave.core.store.base.DataIndexRetrieval;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class GeoWaveRowIteratorFactory {
  public static <T> Iterator<T> iterator(
      final PersistentAdapterStore adapterStore,
      final Index index,
      final Iterator<GeoWaveRow> rowIter,
      final QueryFilter[] clientFilters,
      final ScanCallback<T, ? extends GeoWaveRow> scanCallback,
      final byte[] fieldSubsetBitmask,
      final double[] maxResolutionSubsamplingPerDimension,
      final boolean decodePersistenceEncoding,
      final DataIndexRetrieval dataIndexRetrieval) {
    if (dataIndexRetrieval instanceof BatchDataIndexRetrieval) {
      return new AsyncNativeEntryIteratorWrapper<>(
          adapterStore,
          index,
          rowIter,
          clientFilters,
          scanCallback,
          fieldSubsetBitmask,
          maxResolutionSubsamplingPerDimension,
          decodePersistenceEncoding,
          (BatchDataIndexRetrieval)dataIndexRetrieval);
    }
    return new NativeEntryIteratorWrapper<>(
        adapterStore,
        index,
        rowIter,
        clientFilters,
        scanCallback,
        fieldSubsetBitmask,
        maxResolutionSubsamplingPerDimension,
        decodePersistenceEncoding,
        dataIndexRetrieval);
  }
}
