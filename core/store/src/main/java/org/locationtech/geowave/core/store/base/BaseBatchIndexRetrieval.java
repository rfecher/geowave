package org.locationtech.geowave.core.store.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.util.TriFunction;

public class BaseBatchIndexRetrieval implements BatchDataIndexRetrieval {
  private final TriFunction<byte[][], Short, DataIndexRetrievalParams, CloseableIterator<GeoWaveValue[]>> function;
  private BiFunction<byte[][], Short, CloseableIterator<GeoWaveValue[]>> reducedFunction;
  private final int batchSize;
  private final Map<Short, List<Pair<byte[], CompletableFuture<GeoWaveValue[]>>>> currentBatchesPerAdapter =
      new HashMap<>();
  private int outstandingIterators = 0;

  public BaseBatchIndexRetrieval(
      final TriFunction<byte[][], Short, DataIndexRetrievalParams, CloseableIterator<GeoWaveValue[]>> function,
      final int batchSize) {
    this.function = function;
    this.batchSize = batchSize;
  }

  @Override
  public GeoWaveValue[] getData(final byte[] dataId, final short adapterId) {
    try (CloseableIterator<GeoWaveValue[]> it =
        reducedFunction.apply(new byte[][] {dataId}, adapterId)) {
      if (it.hasNext()) {
        return it.next();
      }
    }
    return null;
  }

  @Override
  public void setParams(final DataIndexRetrievalParams params) {
    reducedFunction = (dataIds, adapterId) -> function.apply(dataIds, adapterId, params);
  }

  @Override
  public CompletableFuture<GeoWaveValue[]> getDataAsync(
      final byte[] dataId,
      final short adapterId) {
    List<Pair<byte[], CompletableFuture<GeoWaveValue[]>>> batch =
        currentBatchesPerAdapter.get(adapterId);
    if (batch == null) {
      batch = new ArrayList<>();
      currentBatchesPerAdapter.put(adapterId, batch);
    }

    final CompletableFuture<GeoWaveValue[]> retVal = new CompletableFuture<>();
    synchronized (batch) {
      batch.add(Pair.of(dataId, retVal));
      if (batch.size() >= batchSize) {
        flush(adapterId, batch);
      }
    }
    return retVal;
  }

  private void flush(
      final Short adapterId,
      final List<Pair<byte[], CompletableFuture<GeoWaveValue[]>>> batch) {
    final byte[][] internalDataIds;
    final CompletableFuture<GeoWaveValue[]>[] internalSuppliers;
    synchronized (batch) {
      internalDataIds = new byte[batch.size()][];
      internalSuppliers =
        new CompletableFuture[batch.size()];
    final Iterator<Pair<byte[], CompletableFuture<GeoWaveValue[]>>> it = batch.iterator();
    for (int i = 0; i < internalDataIds.length; i++) {
      final Pair<byte[], CompletableFuture<GeoWaveValue[]>> pair = it.next();
      internalDataIds[i] = pair.getLeft();
      internalSuppliers[i] = pair.getRight();
    }
    batch.clear();
    }
    CompletableFuture.supplyAsync(
        () -> reducedFunction.apply(internalDataIds, adapterId)).whenComplete((values, ex) -> {
          if (values != null) {
            int i = 0;
            while (values.hasNext() && (i < internalSuppliers.length)) {
              // the iterator has to be in order
              internalSuppliers[i++].complete(values.next());
            }
            while (i < internalSuppliers.length) {
              // there should be exactly as many results as suppliers so this shouldn't happen
              internalSuppliers[i++].complete(null);
            }
          } else if (ex != null) {
            Arrays.stream(internalSuppliers).forEach(s -> s.completeExceptionally(ex));
          }
        });

  }

  @Override
  public void flush() {
    if (!currentBatchesPerAdapter.isEmpty()) {
      currentBatchesPerAdapter.forEach((k, v) -> flush(k, v));
    }
  }

  @Override
  public void incrementOutstandingIterators() {
    outstandingIterators++;
  }

  @Override
  public void decrementOutstandingIterators() {
    outstandingIterators--;
    if (outstandingIterators <= 0) {
      flush();
    }
  }
}
