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
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.StatisticType;
import org.locationtech.geowave.core.store.statistics.StatisticsRegistry;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.internal.Console;

@GeowaveOperation(name = "recalc", parentOperation = StatsSection.class)
@Parameters(commandDescription = "Recalculate statistics in a given data store")
public class RecalculateStatsCommand extends AbstractStatsCommand<Void> {

  @Parameter(description = "<store name>")
  private final List<String> parameters = new ArrayList<>();

  @Parameter(
      names = "--all",
      description = "If specified, all matching statistics will be recalculated.")
  private final boolean all = false;

  @Parameter(
      names = "--statType",
      description = "If specified, only statistics of the given type will be recalculated.")
  private final String statType = null;

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
    final IndexStore indexStore = storeOptions.createIndexStore();

    StatisticType<StatisticValue<Object>> statisticType = null;

    if (statType != null) {
      statisticType = StatisticsRegistry.instance().getStatisticType(statType);

      if (statisticType == null) {
        throw new ParameterException("Unrecognized statistic type: " + statType);
      }
    }

    final List<Statistic<? extends StatisticValue<?>>> toRecalculate =
        statsOptions.resolveMatchingStatistics(statisticType, dataStore, statStore, indexStore);

    if (toRecalculate.isEmpty()) {
      throw new ParameterException("A matching statistic could not be found");
    } else if ((toRecalculate.size() > 1) && !all) {
      throw new ParameterException(
          "Multiple statistics matched the given parameters.  If this is intentional, "
              + "supply the --all option, otherwise provide additional parameters to "
              + "specify which statistic to recalculate.");
    }
    final Statistic<?>[] toRecalcArray =
        toRecalculate.toArray(new Statistic<?>[toRecalculate.size()]);
    dataStore.recalcStatistic(toRecalcArray);

    return true;
  }

  @Override
  public Void computeResults(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <store name>");
    }

    super.run(params, parameters);
    return null;
  }
}
