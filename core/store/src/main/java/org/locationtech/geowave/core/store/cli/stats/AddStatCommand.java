/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.stats;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreLoader;
import org.locationtech.geowave.core.store.statistics.BaseStatistic;
import org.locationtech.geowave.core.store.statistics.StatisticsRegistry;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "add", parentOperation = StatsSection.class)
@Parameters(commandDescription = "Add a statistic to a data store")
public class AddStatCommand extends ServiceEnabledCommand<Void> {

  @Parameter(description = "<store name> <stat type>")
  private final List<String> parameters = new ArrayList<>();

  @Parameter(
      names = "--binningStrategy",
      description = "If specified, statistics will be binned using the given strategy.")
  private final String binningStrategyName = null;

  @Parameter(
      names = "--skipCalculation",
      description = "If specified, the initial value of the statistic will not be calculated.")
  private final boolean skipCalculation = false;

  private String statType = null;

  @ParametersDelegate
  Statistic<?> statOptions;

  @ParametersDelegate
  StatisticBinningStrategy binningStrategy = null;

  @Override
  public boolean prepare(final OperationParams params) {
    super.prepare(params);

    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <store name> <stat type>");
    }

    statType = parameters.get(1);

    statOptions = StatisticsRegistry.instance().getStatistic(statType);
    if (statOptions == null) {
      throw new ParameterException("Unrecognized stat type: " + statType);
    }

    if (binningStrategyName != null) {
      binningStrategy = StatisticsRegistry.instance().getBinningStrategy(binningStrategyName);
      if (binningStrategy == null) {
        throw new ParameterException("Unrecognized binning strategy: " + binningStrategyName);
      }
      if (binningStrategy instanceof CompositeBinningStrategy) {
        throw new ParameterException(
            "Statistics with composite binning strategies are currently unable to be added through the CLI.");
      }
    }

    return true;
  }

  @Override
  public void execute(final OperationParams params) {
    computeResults(params);
  }

  @Override
  public Void computeResults(final OperationParams params) {
    final String storeName = parameters.get(0);

    // Attempt to load store.
    final File configFile = getGeoWaveConfigFile(params);

    final StoreLoader inputStoreLoader = new StoreLoader(storeName);
    if (!inputStoreLoader.loadFromConfig(configFile)) {
      throw new ParameterException("Cannot find store name: " + storeName);
    }
    final DataStorePluginOptions storeOptions = inputStoreLoader.getDataStorePlugin();

    final DataStore dataStore = storeOptions.createDataStore();

    if ((binningStrategy != null) && (statOptions instanceof BaseStatistic<?>)) {
      ((BaseStatistic<?>) statOptions).setBinningStrategy(binningStrategy);
    }
    if (skipCalculation) {
      dataStore.addEmptyStatistic(statOptions);
    } else {
      dataStore.addStatistic(statOptions);
    }
    return null;
  }
}
