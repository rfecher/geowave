/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.statistics.adapter.DataTypeStatisticType;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticType;
import org.locationtech.geowave.core.store.statistics.index.IndexStatisticType;
import org.locationtech.geowave.core.store.statistics.query.DataTypeStatisticQueryBuilder;
import org.locationtech.geowave.core.store.statistics.query.FieldStatisticQueryBuilder;
import org.locationtech.geowave.core.store.statistics.query.IndexStatisticQueryBuilder;

/**
 * Base interface for constructing statistic queries.
 *
 * @param <V> the statistic value type
 * @param <R> the return type of the statistic value
 * @param <B> the builder type
 */
public interface StatisticQueryBuilder<V extends StatisticValue<R>, R, B extends StatisticQueryBuilder<V, R, B>> {

  /**
   * Set the tag for the query. Only statistics that match the given tag will be queried.
   * 
   * @param tag the tag to use
   * @return {@code this}
   */
  public B tag(final String tag);

  /**
   * Set the tag filter to internal statistics. If this is set, only internal statistics willb e
   * queried.
   * 
   * @return {@code this}
   */
  public B internal();

  /**
   * Add an authorization to the query.
   * 
   * @param authorization the authorization to add
   * @return {@code this}
   */
  public B addAuthorization(final String authorization);

  /**
   * Set the query authorizations to the given set.
   * 
   * @param authorizations the authorizations to use
   * @return {@code this}
   */
  public B authorizations(final String[] authorizations);

  /**
   * Add a bin to the query. If a queried statistic uses a binning strategy, only values contained
   * in one of the given bins will be returned.
   * 
   * @param bin the bin to add
   * @return {@code this}
   */
  public B addBin(final ByteArray bin);

  /**
   * Sets the bins of the query. If a queried statistic uses a binning strategy, only values
   * contained in one of the given bins will be returned.
   * 
   * @param bin the bins to use
   * @return {@code this}
   */
  public B bins(final ByteArray... bins);

  /**
   * Build the statistic query.
   * 
   * @return the statistic query
   */
  public StatisticQuery<V, R> build();

  /**
   * Create a new index statistic query builder for the given statistic type.
   * 
   * @param statisticType the index statistic type to query
   * @return the index statistic query builder
   */
  public static <V extends StatisticValue<R>, R> IndexStatisticQueryBuilder<V, R> newBuilder(
      IndexStatisticType<V> statisticType) {
    return new IndexStatisticQueryBuilder<>(statisticType);
  }

  /**
   * Create a new data type statistic query builder for the given statistic type.
   * 
   * @param statisticType the data type statistic type to query
   * @return the data type statistic query builder
   */
  public static <V extends StatisticValue<R>, R> DataTypeStatisticQueryBuilder<V, R> newBuilder(
      DataTypeStatisticType<V> statisticType) {
    return new DataTypeStatisticQueryBuilder<>(statisticType);
  }

  /**
   * Create a new field statistic query builder for the given statistic type.
   * 
   * @param statisticType the field statistic type to query
   * @return the field statistic query builder
   */
  public static <V extends StatisticValue<R>, R> FieldStatisticQueryBuilder<V, R> newBuilder(
      FieldStatisticType<V> statisticType) {
    return new FieldStatisticQueryBuilder<>(statisticType);
  }

}
