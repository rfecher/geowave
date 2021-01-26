/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.StatisticType;
import org.locationtech.geowave.core.store.statistics.StatisticsRegistry;
import org.locationtech.geowave.core.store.statistics.adapter.DataTypeStatisticType;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticType;
import org.locationtech.geowave.core.store.statistics.index.IndexStatisticType;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.internal.Console;
import com.clearspring.analytics.util.Lists;

@GeowaveOperation(name = "recalc", parentOperation = StatsSection.class)
@Parameters(commandDescription = "Recalculate statistics in a given data store")
public class RecalculateStatsCommand extends AbstractStatsCommand<Void> {

  @Parameter(description = "<store name> [<stat type>]")
  private final List<String> parameters = new ArrayList<>();

  @Parameter(
      names = "--all",
      description = "If specified, all matching statistics will be recalculated.")
  private boolean all = false;

  private String statType = null;

  @Override
  public void execute(final OperationParams params) {
    computeResults(params);
  }

  @Override
  protected boolean performStatsCommand(
      final DataStorePluginOptions storeOptions,
      final StatsCommandLineOptions statsOptions,
      final Console console) throws IOException {

    final DataStore dataStore = storeOptions.createDataStore();
    final DataStatisticsStore statStore = storeOptions.createDataStatisticsStore();

    List<Statistic<? extends StatisticValue<?>>> toRecalculate = Lists.newArrayList();

    if (statType != null) {
      StatisticType<StatisticValue<Object>> statisticType =
          StatisticsRegistry.instance().getStatisticType(statType);

      if (statisticType == null) {
        throw new ParameterException("Unrecognized statistic type: " + statType);
      }

      if (statisticType instanceof IndexStatisticType) {
        if (statsOptions.getIndexName() == null) {
          throw new ParameterException(
              "An index name must be specified when removing an index statistic.");
        }
        final IndexStore indexStore = storeOptions.createIndexStore();
        final Index index = indexStore.getIndex(statsOptions.getIndexName());
        if (index == null) {
          throw new ParameterException(
              "Unable to find an index named: " + statsOptions.getIndexName());
        }
        try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
            statStore.getIndexStatistics(index, statisticType, statsOptions.getTag())) {
          stats.forEachRemaining(stat -> toRecalculate.add(stat));
        }
      } else if (statisticType instanceof DataTypeStatisticType) {
        if (statsOptions.getTypeName() == null) {
          throw new ParameterException(
              "A type name must be specified when removing an adapter statistic.");
        }
        DataTypeAdapter<?> adapter = dataStore.getType(statsOptions.getTypeName());
        if (adapter == null) {
          throw new ParameterException(
              "Unable to find an type named: " + statsOptions.getTypeName());
        }
        try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
            statStore.getDataTypeStatistics(adapter, statisticType, statsOptions.getTag())) {
          stats.forEachRemaining(stat -> toRecalculate.add(stat));
        }
      } else if (statisticType instanceof FieldStatisticType) {
        if (statsOptions.getTypeName() == null) {
          throw new ParameterException(
              "A type name must be specified when removing a field statistic.");
        }
        DataTypeAdapter<?> adapter = dataStore.getType(statsOptions.getTypeName());
        if (adapter == null) {
          throw new ParameterException(
              "Unable to find an type named: " + statsOptions.getTypeName());
        }
        try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
            statStore.getFieldStatistics(
                adapter,
                statisticType,
                statsOptions.getFieldName(),
                statsOptions.getTag())) {
          stats.forEachRemaining(stat -> toRecalculate.add(stat));
        }
      }
    } else {
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
          statStore.getAllStatistics(null)) {
        // STATS_TODO: Filter stats by stat options (tag, type name, index name, field name)
        stats.forEachRemaining(stat -> toRecalculate.add(stat));
      }
    }



    if (toRecalculate.isEmpty()) {
      throw new ParameterException("A matching statistic could not be found");
    } else if (toRecalculate.size() > 1 && !all) {
      throw new ParameterException(
          "Multiple statistics matched the given parameters.  If this is intentional, "
              + "supply the --all option, otherwise provide additional parameters to "
              + "specify which statistic to recalculate.");
    }

    dataStore.removeStatistics(toRecalculate.iterator());
    dataStore.addStatistics(toRecalculate.iterator(), true);

    return true;
  }

  @Override
  public Void computeResults(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() != 1 && parameters.size() != 2) {
      throw new ParameterException("Requires one or two arguments: <store name> [<stat type>]");
    }

    if (parameters.size() == 2) {
      statType = parameters.get(1);
    }

    super.run(params, parameters);
    return null;
  }
}
