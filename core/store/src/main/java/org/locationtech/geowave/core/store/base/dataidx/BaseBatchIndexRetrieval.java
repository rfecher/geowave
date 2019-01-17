package org.locationtech.geowave.core.store.base.dataidx;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.util.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseBatchIndexRetrieval implements BatchDataIndexRetrieval {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseBatchIndexRetrieval.class);
  private final TriFunction<byte[][], Short, DataIndexRetrievalParams, CloseableIterator<GeoWaveValue[]>> function;
  private BiFunction<byte[][], Short, CloseableIterator<GeoWaveValue[]>> reducedFunction;
  private final int batchSize;
  private final Map<Short, Map<ByteArray, CompletableFuture<GeoWaveValue[]>>> currentBatchesPerAdapter =
      new HashMap<>();
  private final AtomicInteger outstandingIterators = new AtomicInteger(0);

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
  public synchronized CompletableFuture<GeoWaveValue[]> getDataAsync(
      final byte[] dataId,
      final short adapterId) {
    Map<ByteArray, CompletableFuture<GeoWaveValue[]>> batch =
        currentBatchesPerAdapter.get(adapterId);
    if (batch == null) {
      batch = new HashMap<>();
      currentBatchesPerAdapter.put(adapterId, batch);
    }
    final ByteArray dataIdKey = new ByteArray(dataId);
    CompletableFuture<GeoWaveValue[]> retVal = batch.get(dataIdKey);
    if (retVal == null) {
      retVal = new CompletableFuture<>();
      retVal = retVal.exceptionally(e -> {
        LOGGER.error("Unable to retrieve from data index", e);
        return null;
      });
      batch.put(dataIdKey, retVal);
      if (batch.size() >= batchSize) {
        flush(adapterId, batch);
      }
    }
    return retVal;
  }

  private void flush(
      final Short adapterId,
      final Map<ByteArray, CompletableFuture<GeoWaveValue[]>> batch) {
    final byte[][] internalDataIds;
    final CompletableFuture<GeoWaveValue[]>[] internalSuppliers;
    internalDataIds = new byte[batch.size()][];
    internalSuppliers = new CompletableFuture[batch.size()];
    final Iterator<Entry<ByteArray, CompletableFuture<GeoWaveValue[]>>> it =
        batch.entrySet().iterator();
    for (int i = 0; i < internalDataIds.length; i++) {
      final Entry<ByteArray, CompletableFuture<GeoWaveValue[]>> entry = it.next();
      internalDataIds[i] = entry.getKey().getBytes();
      internalSuppliers[i] = entry.getValue();
    }
    batch.clear();
    if (internalSuppliers.length > 0) {
      CompletableFuture.supplyAsync(
          () -> reducedFunction.apply(internalDataIds, adapterId)).whenComplete((values, ex) -> {
            if (values != null) {
              int i = 0;
              while (values.hasNext() && (i < internalSuppliers.length)) {
                // the iterator has to be in order
                internalSuppliers[i++].complete(values.next());
              }
              if (values.hasNext()) {
                LOGGER.warn("There are more data index results than expected");
              } else if (i < internalSuppliers.length) {
                LOGGER.warn("There are less data index results than expected");
                while (i < internalSuppliers.length) {
                  // there should be exactly as many results as suppliers so this shouldn't happen
                  internalSuppliers[i++].complete(null);
                }
              }
            } else if (ex != null) {
              LOGGER.warn("Unable to retrieve from data index", ex);
              Arrays.stream(internalSuppliers).forEach(s -> s.completeExceptionally(ex));
            }
          });
    }
  }

  @Override
  public synchronized void flush() {
    if (!currentBatchesPerAdapter.isEmpty()) {
      currentBatchesPerAdapter.forEach((k, v) -> flush(k, v));
    }
  }

  @Override
  public void notifyIteratorInitiated() {
    outstandingIterators.incrementAndGet();
  }

  @Override
  public void notifyIteratorExhausted() {
    if (outstandingIterators.decrementAndGet() <= 0) {
      flush();
    }
  }
}
