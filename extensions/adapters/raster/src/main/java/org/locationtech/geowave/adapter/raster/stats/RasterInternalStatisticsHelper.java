/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.stats;

import org.locationtech.geowave.adapter.raster.stats.RasterBoundingBoxStatistic.RasterBoundingBoxValue;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.adapter.DataTypeStatistic;
import org.locationtech.geowave.core.store.statistics.field.FieldStatistic;

public class RasterInternalStatisticsHelper {

  public static RasterBoundingBoxValue getBbox(
      final DataStatisticsStore statisticsStore,
      final String typeName,
      final String... authorizations) {
    Statistic<RasterBoundingBoxValue> statistic =
        statisticsStore.getStatisticById(
            DataTypeStatistic.generateStatisticId(
                typeName,
                RasterBoundingBoxStatistic.STATS_TYPE,
                Statistic.INTERNAL_TAG));
    if (statistic != null) {
      return statisticsStore.getStatisticValue(statistic, authorizations);
    }
    return null;
  }

}
