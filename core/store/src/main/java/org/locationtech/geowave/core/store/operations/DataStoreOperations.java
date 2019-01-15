/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Bytes;

public interface DataStoreOperations {

  boolean indexExists(String indexName) throws IOException;

  boolean metadataExists(MetadataType type) throws IOException;

  void deleteAll() throws Exception;

  boolean deleteAll(
      String indexName,
      String typeName,
      Short adapterId,
      String... additionalAuthorizations);

  boolean ensureAuthorizations(String clientUser, String... authorizations);

  RowWriter createWriter(Index index, InternalDataAdapter<?> adapter);

  default RowWriter createDataIndexWriter(final InternalDataAdapter<?> adapter) {
    return new DefaultDataIndexRowWriterWrapper(
        createWriter(BaseDataStoreUtils.DATA_ID_INDEX, adapter));
  }

  MetadataWriter createMetadataWriter(MetadataType metadataType);

  MetadataReader createMetadataReader(MetadataType metadataType);

  MetadataDeleter createMetadataDeleter(MetadataType metadataType);

  <T> RowReader<T> createReader(ReaderParams<T> readerParams);

  default <T> RowReader<T> createReader(final DataIndexReaderParams readerParams) {
    final List<RowReader<GeoWaveRow>> readers =
        Arrays.stream(readerParams.getDataIds()).map(dataId -> {
          final byte[] sortKey = Bytes.concat(new byte[] {(byte) dataId.length}, dataId);
          return createReader(
              new ReaderParams<>(
                  BaseDataStoreUtils.DATA_ID_INDEX,
                  readerParams.getAdapterStore(),
                  readerParams.getInternalAdapterStore(),
                  new short[] {readerParams.getAdapterId()},
                  null,
                  readerParams.getAggregation(),
                  readerParams.getFieldSubsets(),
                  false,
                  false,
                  false,
                  false,
                  new QueryRanges(new ByteArrayRange(sortKey, sortKey, false)),
                  null,
                  1,
                  null,
                  null,
                  null,
                  GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
                  new String[0]));
        }).collect(Collectors.toList());
    return new RowReaderWrapper<>(new CloseableIteratorWrapper(new Closeable() {
      @Override
      public void close() {
        for (final RowReader<GeoWaveRow> r : readers) {
          r.close();
        }
      }
    }, Iterators.concat(readers.iterator())));
  }

  <T> Deleter<T> createDeleter(ReaderParams<T> readerParams);

  RowDeleter createRowDeleter(
      String indexName,
      PersistentAdapterStore adapterStore,
      InternalAdapterStore internalAdapterStore,
      String... authorizations);

  boolean mergeData(
      final Index index,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final AdapterIndexMappingStore adapterIndexMappingStore);

  boolean mergeStats(DataStatisticsStore statsStore, InternalAdapterStore internalAdapterStore);
}
