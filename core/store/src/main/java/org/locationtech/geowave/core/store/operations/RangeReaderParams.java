/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;

public abstract class RangeReaderParams<T> extends BaseReaderParams<T> {
  private final Index index;
  private final short[] adapterIds;
  private final double[] maxResolutionSubsamplingPerDimension;
  private final boolean isMixedVisibility;
  private final boolean isAuthorizationsLimiting;
  private final boolean isClientsideRowMerging;
  private final Integer limit;
  private final Integer maxRangeDecomposition;
  private final String[] additionalAuthorizations;
  private final GeoWaveRowIteratorTransformer<T> rowTransformer;

  public RangeReaderParams(
      final Index index,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final short[] adapterIds,
      final double[] maxResolutionSubsamplingPerDimension,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final boolean isMixedVisibility,
      final boolean isAuthorizationsLimiting,
      final boolean isClientsideRowMerging,
      final Integer limit,
      final Integer maxRangeDecomposition,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final String[] additionalAuthorizations) {
    super(adapterStore, internalAdapterStore, aggregation, fieldSubsets);
    this.index = index;
    this.adapterIds = adapterIds;
    this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
    this.isMixedVisibility = isMixedVisibility;
    this.isAuthorizationsLimiting = isAuthorizationsLimiting;
    this.isClientsideRowMerging = isClientsideRowMerging;
    this.limit = limit;
    this.maxRangeDecomposition = maxRangeDecomposition;
    this.additionalAuthorizations = additionalAuthorizations;
    this.rowTransformer = rowTransformer;
  }

  public Index getIndex() {
    return index;
  }

  public short[] getAdapterIds() {
    return adapterIds;
  }

  public double[] getMaxResolutionSubsamplingPerDimension() {
    return maxResolutionSubsamplingPerDimension;
  }

  public boolean isAuthorizationsLimiting() {
    return isAuthorizationsLimiting;
  }

  public boolean isMixedVisibility() {
    return isMixedVisibility;
  }

  public Integer getLimit() {
    return limit;
  }

  public Integer getMaxRangeDecomposition() {
    return maxRangeDecomposition;
  }

  public String[] getAdditionalAuthorizations() {
    return additionalAuthorizations;
  }

  public boolean isClientsideRowMerging() {
    return isClientsideRowMerging;
  }

  public GeoWaveRowIteratorTransformer<T> getRowTransformer() {
    return rowTransformer;
  }
}
