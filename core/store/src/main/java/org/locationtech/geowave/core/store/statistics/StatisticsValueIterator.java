/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import java.util.Arrays;
import java.util.Iterator;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;

/**
 * Iterates over the values of a set of statistics.
 */
public class StatisticsValueIterator implements CloseableIterator<StatisticValue<?>> {

  private final DataStatisticsStore statisticsStore;
  private final Iterator<? extends Statistic<? extends StatisticValue<?>>> statistics;
  private final ByteArray[] bins;
  private final String[] authorizations;

  private CloseableIterator<? extends StatisticValue<?>> current = null;

  private StatisticValue<?> next = null;

  public StatisticsValueIterator(
      final DataStatisticsStore statisticsStore,
      final Iterator<? extends Statistic<? extends StatisticValue<?>>> statistics,
      final ByteArray[] bins,
      String... authorizations) {
    this.statisticsStore = statisticsStore;
    this.statistics = statistics;
    this.bins = bins;
    this.authorizations = authorizations;
  }

  @SuppressWarnings("unchecked")
  private void computeNext() {
    if (next == null) {
      while ((current == null || !current.hasNext()) && statistics.hasNext()) {
        if (current != null) {
          current.close();
          current = null;
        }
        Statistic<StatisticValue<Object>> nextStat =
            (Statistic<StatisticValue<Object>>) statistics.next();
        if (nextStat.getBinningStrategy() != null && bins != null && bins.length > 0) {
          current =
              new CloseableIterator.Wrapper<>(
                  Arrays.stream(bins).map(
                      bin -> statisticsStore.getStatisticValue(
                          nextStat,
                          bin,
                          authorizations)).iterator());
        } else {
          current = statisticsStore.getStatisticValues(nextStat, authorizations);
        }
      }
      if (current != null && current.hasNext()) {
        next = current.next();
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (next == null) {
      computeNext();
    }
    return next != null;
  }

  @Override
  public StatisticValue<?> next() {
    if (next == null) {
      computeNext();
    }
    StatisticValue<?> retVal = next;
    next = null;
    return retVal;
  }

  @Override
  public void close() {
    if (current != null) {
      current.close();
      current = null;
    }
  }

}
