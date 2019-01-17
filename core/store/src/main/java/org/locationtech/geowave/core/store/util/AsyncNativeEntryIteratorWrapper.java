/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.util;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.dataidx.BatchDataIndexRetrieval;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncNativeEntryIteratorWrapper<T> extends NativeEntryIteratorWrapper<T> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AsyncNativeEntryIteratorWrapper.class);
  private static final int MAX_COMPLETED_OBJECT_CAPACITY = 1000000;
  private final BlockingQueue<Object> completedObjects =
      new LinkedBlockingDeque<>(MAX_COMPLETED_OBJECT_CAPACITY);
  private final AtomicInteger outstandingFutures = new AtomicInteger(0);
  private static final Object POISON = new Object();
  private final AtomicBoolean scannedResultsExhausted = new AtomicBoolean(false);

  private final AtomicBoolean scannedResultsStarted = new AtomicBoolean(false);

  public AsyncNativeEntryIteratorWrapper(
      final PersistentAdapterStore adapterStore,
      final Index index,
      final Iterator<GeoWaveRow> scannerIt,
      final QueryFilter[] clientFilters,
      final ScanCallback<T, ? extends GeoWaveRow> scanCallback,
      final byte[] fieldSubsetBitmask,
      final double[] maxResolutionSubsamplingPerDimension,
      final boolean decodePersistenceEncoding,
      final BatchDataIndexRetrieval dataIndexRetrieval) {
    super(
        adapterStore,
        index,
        scannerIt,
        clientFilters,
        scanCallback,
        fieldSubsetBitmask,
        maxResolutionSubsamplingPerDimension,
        decodePersistenceEncoding,
        dataIndexRetrieval);
  }

  @Override
  protected T decodeRow(
      final GeoWaveRow row,
      final QueryFilter[] clientFilters,
      final Index index) {
    final T retVal = super.decodeRow(row, clientFilters, index);
    if (retVal instanceof CompletableFuture) {
      if (((CompletableFuture) retVal).isDone()) {
        try {
          return (T) ((CompletableFuture) retVal).get();
        } catch (InterruptedException | ExecutionException e) {
          LOGGER.warn("unable to get results", e);
        }
      } else {
        outstandingFutures.incrementAndGet();
        ((CompletableFuture) retVal).whenComplete((decodedValue, exception) -> {
          if (decodedValue != null) {
            try {
              completedObjects.put(decodedValue);
            } catch (final InterruptedException e) {
              LOGGER.error("Unable to put value in blocking queue", e);
            }
          }

          if ((outstandingFutures.decrementAndGet() == 0) && scannedResultsExhausted.get()) {
            try {
              completedObjects.put(POISON);
            } catch (final InterruptedException e) {
              LOGGER.error("Unable to put poison in blocking queue", e);
            }
          }
        });
      }
      return null;
    }
    return retVal;
  }

  @Override
  public boolean hasNext() {
    if (!scannedResultsStarted.getAndSet(true)) {
      ((BatchDataIndexRetrieval) dataIndexRetrieval).notifyIteratorInitiated();
    }
    return super.hasNext();
  }

  @Override
  protected void findNext() {
    super.findNext();
    if (!hasNextScannedResult() && !scannedResultsExhausted.getAndSet(true)) {
      ((BatchDataIndexRetrieval) dataIndexRetrieval).notifyIteratorExhausted();
    }
    if ((nextValue == null) && ((outstandingFutures.get() > 0) || !completedObjects.isEmpty())) {
      try {
        final Object completedObj = completedObjects.take();
        if (completedObj == POISON) {
          nextValue = null;
        } else {
          nextValue = (T) completedObj;
        }
      } catch (final InterruptedException e) {
        LOGGER.error("Unable to take value from blocking queue", e);
      }
    }
  }
}
