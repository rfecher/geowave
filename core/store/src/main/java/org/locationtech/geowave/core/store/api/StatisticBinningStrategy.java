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
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

/**
 * Base interface for statistic binning strategies. These strategies allow a statistic's values to
 * be split up by an arbitrary strategy. This allows a simple statistic to be used in many different
 * ways.
 */
public interface StatisticBinningStrategy extends Persistable {

  /**
   * Get the name of the binning strategy.
   * 
   * @return the binning strategy name
   */
  public String getStrategyName();

  /**
   * Get a human-readable description of the binning strategy.
   * 
   * @return a description of the binning strategy
   */
  public String getDescription();

  /**
   * Get the bins used by the given entry. Each bin will have a separate statistic value.
   * 
   * @param type the data type
   * @param entry the entry
   * @param rows the rows created for the entry
   * @return a set of bins used by the given entry
   */
  public <T> ByteArray[] getBins(DataTypeAdapter<T> type, T entry, GeoWaveRow... rows);

  /**
   * Get a human-readable string of a bin.
   * 
   * @param bin the bin
   * @return the string value of the bin
   */
  public String binToString(final ByteArray bin);
}
