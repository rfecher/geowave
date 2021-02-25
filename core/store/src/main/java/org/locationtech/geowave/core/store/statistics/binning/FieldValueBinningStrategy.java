/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.binning;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import com.beust.jcommander.Parameter;

/**
 * Statistic binning strategy that bins statistic values by the string representation of the value
 * of a given field.
 */
public class FieldValueBinningStrategy implements StatisticBinningStrategy {
  public static final String NAME = "FIELD_VALUE";

  @Parameter(
      names = "--binField",
      description = "Field that contains the bin value.",
      required = true)
  protected String fieldName;

  public FieldValueBinningStrategy() {
    fieldName = null;
  }

  public FieldValueBinningStrategy(final String fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public String getStrategyName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Bin the statistic by the value of a specified field.";
  }

  @Override
  public <T> ByteArray[] getBins(
      final DataTypeAdapter<T> adapter,
      final T entry,
      final GeoWaveRow... rows) {
    return new ByteArray[] {getBin(adapter.getFieldValue(entry, fieldName))};
  }

  public static ByteArray getBin(final Object value) {
    if (value == null) {
      return new ByteArray();
    }
    return new ByteArray(value.toString());
  }

  @Override
  public byte[] toBinary() {
    return StringUtils.stringToBinary(fieldName);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    fieldName = StringUtils.stringFromBinary(bytes);
  }

  @Override
  public String binToString(final ByteArray bin) {
    return bin.getString();
  }
}
