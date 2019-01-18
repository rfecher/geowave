package org.locationtech.geowave.core.store.base.dataidx;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParams;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParamsBuilder;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataIndexUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataIndexUtils.class);
  public static final Index DATA_ID_INDEX = new NullIndex("DATA");

  public static boolean isDataIndex(final String indexName) {
    return DATA_ID_INDEX.getName().equals(indexName);
  }

  public static GeoWaveRow getDataIndexRow(
      final byte[] dataId,
      final short adapterId,
      final byte[] value) {
    return new GeoWaveRowImpl(
        new GeoWaveKeyImpl(dataId, adapterId, new byte[0], new byte[0], 0),
        new GeoWaveValue[] {new GeoWaveValueImpl(new byte[0], new byte[0], value)});
  }

  public static DataIndexRetrieval getDataIndexRetrieval(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final Index index,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final int dataIndexBatchSize) {
    if ((dataIndexBatchSize > 0) && !isDataIndex(index.getName())) {
      // this implies that this index merely contains a reference by data ID and a second lookup
      // must be done
      if (dataIndexBatchSize > 1) {
        return new BatchIndexRetrievalImpl(
            operations,
            adapterStore,
            internalAdapterStore,
            fieldSubsets,
            aggregation,
            dataIndexBatchSize);
      }
      return new DataIndexRetrievalImpl(
          operations,
          adapterStore,
          internalAdapterStore,
          fieldSubsets,
          aggregation);
    }
    return null;
  }

  protected static GeoWaveValue[] getFieldValuesFromDataIdIndex(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final Short adapterId,
      final byte[] dataId) {
    try (final RowReader<GeoWaveRow> reader =
        getRowReader(
            operations,
            adapterStore,
            internalAdapterStore,
            fieldSubsets,
            aggregation,
            adapterId,
            dataId)) {
      if (reader.hasNext()) {
        return reader.next().getFieldValues();
      } else {
        LOGGER.warn(
            "Unable to find data ID '"
                + StringUtils.stringFromBinary(dataId)
                + " (hex:"
                + ByteArrayUtils.getHexString(dataId)
                + ")' with adapter ID "
                + adapterId
                + " in data table");
      }
    } catch (final Exception e) {
      LOGGER.warn("Unable to close reader", e);
    }
    return null;
  }

  protected static RowReader<GeoWaveRow> getRowReader(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final short adapterId,
      final byte[]... dataIds) {
    final DataIndexReaderParams readerParams =
        new DataIndexReaderParamsBuilder<>(adapterStore, internalAdapterStore).adapterId(
            adapterId).dataIds(dataIds).fieldSubsets(fieldSubsets).aggregation(aggregation).build();
    return operations.createReader(readerParams);
  }
}
