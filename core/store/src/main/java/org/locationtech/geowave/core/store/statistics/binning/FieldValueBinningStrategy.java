/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.binning;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import com.beust.jcommander.Parameter;
import com.clearspring.analytics.util.Lists;

/**
 * Statistic binning strategy that bins statistic values by the string representation of the value
 * of one or more fields.
 */
public class FieldValueBinningStrategy implements StatisticBinningStrategy {
  public static final String NAME = "FIELD_VALUE";

  @Parameter(
      names = "--binField",
      description = "Field that contains the bin value. This can be specified multiple times to bin on a combination of fields.",
      required = true)
  private List<String> fields;

  public FieldValueBinningStrategy() {
    fields = Lists.newArrayList();
  }

  public FieldValueBinningStrategy(final String... fields) {
    this.fields = Arrays.asList(fields);
  }

  @Override
  public String getStrategyName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Bin the statistic by the value of one or more fields.";
  }

  @Override
  public <T> ByteArray[] getBins(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
    if (fields.size() == 1) {
      return new ByteArray[] {getSingleBin(adapter.getFieldValue(entry, fields.get(0)))};
    }
    Object[] fieldValues =
        fields.stream().map(field -> adapter.getFieldValue(entry, field)).toArray();
    return new ByteArray[] {getBin(fieldValues)};
  }

  public static ByteArray getBin(final Object... values) {
    if (values == null) {
      return new ByteArray();
    }
    return new ByteArray(
        Arrays.stream(values).map(value -> value == null ? "" : value.toString()).collect(
            Collectors.joining("|")));
  }
  
  private static ByteArray getSingleBin(final Object value) {
    if (value == null) {
      return new ByteArray();
    }
    return new ByteArray(value.toString());
  }

  @Override
  public byte[] toBinary() {
    return StringUtils.stringsToBinary(fields.toArray(new String[fields.size()]));
  }

  @Override
  public void fromBinary(byte[] bytes) {
    fields = Arrays.asList(StringUtils.stringsFromBinary(bytes));
  }

  @Override
  public String binToString(final ByteArray bin) {
    return bin.getString();
  }
}
