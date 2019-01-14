/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.operations;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingIterator;
import org.locationtech.geowave.core.store.operations.BaseReaderParams;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParams;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.filter.ClientVisibilityFilter;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBClient;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBUtils;
import org.locationtech.geowave.mapreduce.splits.GeoWaveRowRange;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

public class RocksDBReader<T> implements RowReader<T> {
  private final CloseableIterator<T> iterator;

  public RocksDBReader(
      final RocksDBClient client,
      final ReaderParams<T> readerParams,
      final boolean async) {
    this.iterator = createIteratorForReader(client, readerParams, false);
  }

  public RocksDBReader(final RocksDBClient client, final RecordReaderParams<T> recordReaderParams) {
    this.iterator = createIteratorForRecordReader(client, recordReaderParams);
  }

  public RocksDBReader(
      final RocksDBClient client,
      final DataIndexReaderParams<T> dataIndexReaderParams) {
    this.iterator = createIteratorForDataIndexReader(client, dataIndexReaderParams, false);
  }

  private CloseableIterator<T> createIteratorForReader(
      final RocksDBClient client,
      final ReaderParams<T> readerParams,
      final boolean async) {
    final Collection<SinglePartitionQueryRanges> ranges =
        readerParams.getQueryRanges().getPartitionQueryRanges();

    final Set<String> authorizations = Sets.newHashSet(readerParams.getAdditionalAuthorizations());
    if ((ranges != null) && !ranges.isEmpty()) {
      return createIterator(client, readerParams, ranges, authorizations, async);
    } else {
      final List<CloseableIterator<GeoWaveRow>> iterators = new ArrayList<>();
      for (final short adapterId : readerParams.getAdapterIds()) {
        final Pair<Boolean, Boolean> groupByRowAndSortByTime =
            RocksDBUtils.isGroupByRowAndIsSortByTime(readerParams, adapterId);
        final String indexNamePrefix =
            RocksDBUtils.getTablePrefix(
                readerParams.getInternalAdapterStore().getTypeName(adapterId),
                readerParams.getIndex().getName());
        final Stream<CloseableIterator<GeoWaveRow>> streamIt =
            RocksDBUtils.getPartitions(client.getSubDirectory(), indexNamePrefix).stream().map(
                p -> {
                  return RocksDBUtils.getIndexTableFromPrefix(
                      client,
                      indexNamePrefix,
                      adapterId,
                      p.getBytes(),
                      groupByRowAndSortByTime.getRight()).iterator();
                });
        iterators.addAll(streamIt.collect(Collectors.toList()));
      }
      return wrapResults(new Closeable() {
        AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public void close() throws IOException {
          if (!closed.getAndSet(true)) {
            iterators.forEach(it -> it.close());
          }
        }
      }, Iterators.concat(iterators.iterator()), readerParams, authorizations);
    }
  }

  private CloseableIterator<T> createIterator(
      final RocksDBClient client,
      final BaseReaderParams<T> readerParams,
      final Consumer<BatchedRangeRead> rangeOrDataIdInitializer,
      final Set<String> authorizations,
      final boolean async) {
    final Iterator<CloseableIterator> it =
        Arrays.stream(ArrayUtils.toObject(readerParams.getAdapterIds())).map(adapterId -> {
          final BatchedRangeRead read =
              new BatchedRangeRead(
                  client,
                  RocksDBUtils.getTablePrefix(
                      readerParams.getInternalAdapterStore().getTypeName(adapterId),
                      readerParams.getIndex().getName()),
                  adapterId,
                  readerParams.getRowTransformer(),
                  new ClientVisibilityFilter(authorizations),
                  readerParams.isClientsideRowMerging(),
                  async,
                  RocksDBUtils.isGroupByRowAndIsSortByTime(readerParams, adapterId),
                  RocksDBUtils.isSortByKeyRequired(readerParams));
          rangeOrDataIdInitializer.accept(read);
          return read.results();
        }).iterator();
    final CloseableIterator<T>[] itArray = Iterators.toArray(it, CloseableIterator.class);
    return new CloseableIteratorWrapper<>(new Closeable() {
      AtomicBoolean closed = new AtomicBoolean(false);

      @Override
      public void close() throws IOException {
        if (!closed.getAndSet(true)) {
          Arrays.stream(itArray).forEach(it -> it.close());
        }
      }
    }, Iterators.concat(itArray));
  }

  private CloseableIterator<T> createIterator(
      final RocksDBClient client,
      final BaseReaderParams<T> readerParams,
      final Collection<SinglePartitionQueryRanges> ranges,
      final Set<String> authorizations,
      final boolean async) {
    return createIterator(
        client,
        readerParams,
        (final BatchedRangeRead read) -> read.setRanges(ranges),
        authorizations,
        async);
  }

  private CloseableIterator<T> createIteratorForRecordReader(
      final RocksDBClient client,
      final RecordReaderParams<T> recordReaderParams) {
    final GeoWaveRowRange range = recordReaderParams.getRowRange();
    final byte[] startKey = range.isInfiniteStartSortKey() ? null : range.getStartSortKey();
    final byte[] stopKey = range.isInfiniteStopSortKey() ? null : range.getEndSortKey();
    final SinglePartitionQueryRanges partitionRange =
        new SinglePartitionQueryRanges(
            range.getPartitionKey(),
            Collections.singleton(new ByteArrayRange(startKey, stopKey)));
    final Set<String> authorizations =
        Sets.newHashSet(recordReaderParams.getAdditionalAuthorizations());
    return createIterator(
        client,
        recordReaderParams,
        Collections.singleton(partitionRange),
        authorizations,
        // there should already be sufficient parallelism created by
        // input splits for record reader use cases
        false);
  }

  private CloseableIterator<T> createIteratorForDataIndexReader(
      final RocksDBClient client,
      final DataIndexReaderParams<T> dataIndexReaderParams,
      final boolean async) {
    final byte[][] dataIds = dataIndexReaderParams.getDataIds();
    final Set<String> authorizations =
        Sets.newHashSet(dataIndexReaderParams.getAdditionalAuthorizations());

    return createIterator(
        client,
        dataIndexReaderParams,
        (final BatchedRangeRead read) -> read.setDataIds(dataIds),
        authorizations,
        async);
  }

  @SuppressWarnings("unchecked")
  private CloseableIterator<T> wrapResults(
      final Closeable closeable,
      final Iterator<GeoWaveRow> results,
      final BaseReaderParams<T> params,
      final Set<String> authorizations) {
    final Iterator<GeoWaveRow> iterator =
        Iterators.filter(results, new ClientVisibilityFilter(authorizations));
    return new CloseableIteratorWrapper<>(
        closeable,
        params.getRowTransformer().apply(
            sortBySortKeyIfRequired(
                params,
                params.isClientsideRowMerging() ? new GeoWaveRowMergingIterator(iterator)
                    : iterator)));
  }

  private static Iterator<GeoWaveRow> sortBySortKeyIfRequired(
      final BaseReaderParams<?> params,
      final Iterator<GeoWaveRow> it) {
    if (RocksDBUtils.isSortByKeyRequired(params)) {
      return RocksDBUtils.sortBySortKey(it);
    }
    return it;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    return iterator.next();
  }

  @Override
  public void close() {
    iterator.close();
  }
}
