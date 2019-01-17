package org.locationtech.geowave.core.store.base.dataidx;

import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
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
import com.google.common.collect.Iterators;

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
      final DataStoreOptions options,
      final Index index) {
    if (options.isSecondaryIndexing() && !(index instanceof NullIndex)) {
      // this implies that this index merely contains a reference by data ID and a second lookup
      // must be done
      final int batchSize = options.getDataIndexBatchSize();
      if (batchSize > 1) {
        return new BaseBatchIndexRetrieval((dataIds, adapterId, params) -> {
          final RowReader<GeoWaveRow> rowReader =
              getRowReader(
                  operations,
                  adapterStore,
                  internalAdapterStore,
                  adapterId,
                  new DataIndexRetrievalParams(params.getFieldSubsets(), params.getAggregation()),
                  dataIds);
          return new CloseableIteratorWrapper<>(
              rowReader,
              Iterators.transform(rowReader, r -> r.getFieldValues()));
        }, batchSize);
      }
      return new BaseDataIndexRetrieval(((dataId, adapterId, params) -> {
        return getFieldValuesFromDataIdIndex(
            operations,
            adapterStore,
            internalAdapterStore,
            dataId,
            adapterId,
            new DataIndexRetrievalParams(params.getFieldSubsets(), params.getAggregation()));
      }));
    }
    return null;
  }

  private static GeoWaveValue[] getFieldValuesFromDataIdIndex(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final byte[] dataId,
      final Short adapterId,
      final DataIndexRetrievalParams params) {
    try (final RowReader<GeoWaveRow> reader =
        getRowReader(operations, adapterStore, internalAdapterStore, adapterId, params, dataId)) {
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

  private static RowReader<GeoWaveRow> getRowReader(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final short adapterId,
      final DataIndexRetrievalParams params,
      final byte[]... dataIds) {
    final DataIndexReaderParams readerParams =
        new DataIndexReaderParamsBuilder<>(adapterStore, internalAdapterStore).adapterId(
            adapterId).dataIds(dataIds).fieldSubsets(params.getFieldSubsets()).aggregation(
                params.getAggregation()).build();
    return operations.createReader(readerParams);
  }
}
