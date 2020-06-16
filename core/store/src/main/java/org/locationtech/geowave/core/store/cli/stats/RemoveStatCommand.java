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
import com.clearspring.analytics.util.Lists;

@GeowaveOperation(name = "rm", parentOperation = StatsSection.class)
@Parameters(commandDescription = "Remove a statistic from a data store")
public class RemoveStatCommand extends AbstractStatsCommand<Void> {

  @Parameter(description = "<store name> <stat type>")
  private final List<String> parameters = new ArrayList<>();

  @Parameter(
      names = "--all",
      description = "If specified, all matching statistics will be removed.")
  private boolean all = false;

  @Parameter(
      names = "--force",
      description = "Force the statistic to be removed.  IMPORTANT: Removing statistics "
          + "that are marked as \"internal\" can have a detrimental impact on performance!")
  private boolean force = false;

  private String statType = null;

  @Override
  public void execute(final OperationParams params) {
    computeResults(params);
  }

  @Override
  protected boolean performStatsCommand(
      final DataStorePluginOptions storeOptions,
      final StatsCommandLineOptions statsOptions) throws IOException {

    // Remove the stat
    final DataStatisticsStore statStore = storeOptions.createDataStatisticsStore();

    StatisticType<StatisticValue<Object>> statisticType =
        StatisticsRegistry.instance().getStatisticType(statType);

    if (statisticType == null) {
      throw new ParameterException("Unrecognized statistic type: " + statType);
    }

    List<Statistic<? extends StatisticValue<?>>> toRemove = Lists.newArrayList();

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
        markForRemoval(stats, toRemove);
      }
    } else if (statisticType instanceof DataTypeStatisticType) {
      if (statsOptions.getTypeName() == null) {
        throw new ParameterException(
            "A type name must be specified when removing an adapter statistic.");
      }
      final DataStore dataStore = storeOptions.createDataStore();
      dataStore.getTypes();
      dataStore.getIndices();
      DataTypeAdapter<?> adapter = dataStore.getType(statsOptions.getTypeName());
      if (adapter == null) {
        throw new ParameterException("Unable to find an type named: " + statsOptions.getTypeName());
      }
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
          statStore.getDataTypeStatistics(adapter, statisticType, statsOptions.getTag())) {
        markForRemoval(stats, toRemove);
      }
    } else if (statisticType instanceof FieldStatisticType) {
      if (statsOptions.getTypeName() == null) {
        throw new ParameterException(
            "A type name must be specified when removing a field statistic.");
      }
      final DataStore dataStore = storeOptions.createDataStore();
      DataTypeAdapter<?> adapter = dataStore.getType(statsOptions.getTypeName());
      if (adapter == null) {
        throw new ParameterException("Unable to find an type named: " + statsOptions.getTypeName());
      }
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
          statStore.getFieldStatistics(
              adapter,
              statisticType,
              statsOptions.getFieldName(),
              statsOptions.getTag())) {
        markForRemoval(stats, toRemove);
      }
    }


    if (toRemove.isEmpty()) {
      throw new ParameterException("A matching statistic could not be found");
    } else if (toRemove.size() > 1 && !all) {
      throw new ParameterException(
          "Multiple statistics matched the given parameters.  If this is intentional, "
              + "supply the --all option, otherwise provide additional parameters to "
              + "specify which statistic to delete.");
    }

    if (!statStore.removeStatistics(toRemove.iterator())) {
      throw new RuntimeException("Unable to remove statistics of type: " + statType);
    }

    return true;
  }

  private void markForRemoval(
      CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats,
      List<Statistic<? extends StatisticValue<?>>> toRemove) {
    while (stats.hasNext()) {
      Statistic<? extends StatisticValue<?>> stat = stats.next();
      if (!force && stat.isInternal()) {
        throw new ParameterException(
            "Unable to remove an internal statistic without specifying the --force option. "
                + "Removing an internal statistic can have a detrimental impact on performance.");
      }
      toRemove.add(stat);
    }
  }

  @Override
  public Void computeResults(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <store name> <stat type>");
    }

    statType = parameters.get(1);

    super.run(params, parameters);
    return null;
  }
}
