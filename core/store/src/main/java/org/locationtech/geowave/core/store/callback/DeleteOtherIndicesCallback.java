package org.locationtech.geowave.core.store.callback;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.base.BaseDataStore;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.query.constraints.InsertionIdQuery;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteOtherIndicesCallback<T> implements DeleteCallback<T, GeoWaveRow>, Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseDataStore.class);
  private final BaseDataStore dataStore;
  private final InternalDataAdapter<?> adapter;
  private final List<Index> indices;

  public DeleteOtherIndicesCallback(
      final BaseDataStore store,
      final InternalDataAdapter<?> adapter,
      final List<Index> indices) {
    this.adapter = adapter;
    this.indices = indices;
    dataStore = store;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void entryDeleted(final T entry, final GeoWaveRow... rows) {
    if (rows.length > 0) {
      for (final Index index : indices) {
        final InsertionIds ids = DataStoreUtils.getInsertionIdsForEntry(entry, adapter, index);
        for (final SinglePartitionInsertionIds partitionId : ids.getPartitionKeys()) {
          for (final byte[] sortKey : partitionId.getSortKeys()) {
            final InsertionIdQuery constraint =
                new InsertionIdQuery(partitionId.getPartitionKey(), sortKey, rows[0].getDataId());

            final Query<T> query =
                (Query) QueryBuilder.newBuilder().indexName(index.getName()).addTypeName(
                    adapter.getTypeName()).constraints(constraint).build();
            LOGGER.warn("deleting sort key " + ByteArrayUtils.getHexString(sortKey) +" from " +index.getName());
            dataStore.delete(query, false);
            LOGGER.warn("finished delete from " +index.getName());
          }
        }
      }
    }
  }
}
