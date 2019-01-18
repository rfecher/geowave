package org.locationtech.geowave.mapreduce.input;

import java.util.Map.Entry;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.dataidx.BatchDataIndexRetrieval;
import org.locationtech.geowave.core.store.base.dataidx.BatchDataIndexRetrievalIteratorHelper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class AsyncInputFormatIteratorWrapper<T> extends InputFormatIteratorWrapper<T> {
  private final BatchDataIndexRetrievalIteratorHelper<T, Entry<GeoWaveInputKey, T>> batchHelper;

  public AsyncInputFormatIteratorWrapper(
      final RowReader<GeoWaveRow> reader,
      final QueryFilter[] queryFilters,
      final TransientAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final Index index,
      final boolean isOutputWritable,
      final BatchDataIndexRetrieval dataIndexRetrieval) {
    super(
        reader,
        queryFilters,
        adapterStore,
        internalAdapterStore,
        index,
        isOutputWritable,
        dataIndexRetrieval);
    batchHelper = new BatchDataIndexRetrievalIteratorHelper<>(dataIndexRetrieval);
  }

  @Override
  protected void findNext() {
    super.findNext();

    final boolean hasNextValue = (nextEntry != null);
    final Entry<GeoWaveInputKey, T> batchNextValue =
        batchHelper.postFindNext(hasNextValue, reader.hasNext());
    if (!hasNextValue) {
      nextEntry = batchNextValue;
    }
  }


  @Override
  public boolean hasNext() {
    batchHelper.preHasNext();
    return super.hasNext();
  }

  @Override
  protected Entry<GeoWaveInputKey, T> decodeRowToEntry(
      final GeoWaveRow row,
      final QueryFilter[] clientFilters,
      final InternalDataAdapter<T> adapter,
      final Index index) {
    Object value = decodeRowToValue(row, clientFilters, adapter, index);
    if (value == null) {
      return null;
    }
    value = batchHelper.postDecodeRow((T) value, v -> valueToEntry(row, v));
    if (value == null) {
      return null;
    }
    return valueToEntry(row, value);
  }
}
