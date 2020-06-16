/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.binning;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import com.google.common.collect.Lists;

/**
 * Statistic binning strategy that combines two other binning strategies.
 */
public class CompositeBinningStrategy implements StatisticBinningStrategy {

  public static final String NAME = "COMPOSITE";
  public static final byte[] WILDCARD_BYTES = new byte[0];

  private StatisticBinningStrategy left;
  private StatisticBinningStrategy right;

  public CompositeBinningStrategy() {
    this.left = null;
    this.right = null;
  }

  public CompositeBinningStrategy(
      final StatisticBinningStrategy left,
      final StatisticBinningStrategy right) {
    this.left = left;
    this.right = right;
  }

  @Override
  public String getStrategyName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Bin the statistic using multiple strategies.";
  }

  @Override
  public <T> ByteArray[] getBins(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
    ByteArray[] leftBins = left.getBins(adapter, entry, rows);
    ByteArray[] rightBins = right.getBins(adapter, entry, rows);
    ByteArray[] bins = new ByteArray[leftBins.length * rightBins.length];
    int binIndex = 0;
    for (ByteArray leftBin : leftBins) {
      for (ByteArray rightBin : rightBins) {
        bins[binIndex++] = getBin(leftBin, rightBin);
      }
    }
    return bins;
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(Lists.newArrayList(left, right));
  }

  @Override
  public void fromBinary(byte[] bytes) {
    List<Persistable> strategies = PersistenceUtils.fromBinaryAsList(bytes);
    if (strategies.size() == 2) {
      left = (StatisticBinningStrategy) strategies.get(0);
      right = (StatisticBinningStrategy) strategies.get(1);
    }
  }

  @Override
  public String binToString(final ByteArray bin) {
    ByteBuffer buffer = ByteBuffer.wrap(bin.getBytes());
    byte[] leftBin = new byte[buffer.getShort()];
    buffer.get(leftBin);
    byte[] rightBin = new byte[buffer.remaining()];
    buffer.get(rightBin);
    return left.binToString(new ByteArray(leftBin))
        + "|"
        + right.binToString(new ByteArray(rightBin));
  }

  public boolean binMatches(
      Class<? extends StatisticBinningStrategy> binningStrategyClass,
      ByteArray bin,
      ByteArray subBin) {
    ByteBuffer buffer = ByteBuffer.wrap(bin.getBytes());
    byte[] leftBin = new byte[buffer.getShort()];
    buffer.get(leftBin);
    if (binningStrategyClass.isAssignableFrom(left.getClass())) {
      return Arrays.equals(leftBin, subBin.getBytes());
    } else if (binningStrategyClass.isAssignableFrom(right.getClass())) {
      byte[] rightBin = new byte[buffer.remaining()];
      buffer.get(rightBin);
      return Arrays.equals(rightBin, subBin.getBytes());
    } else if (left instanceof CompositeBinningStrategy
        && ((CompositeBinningStrategy) left).usesStrategy(binningStrategyClass)) {
      return ((CompositeBinningStrategy) left).binMatches(
          binningStrategyClass,
          new ByteArray(leftBin),
          subBin);
    } else if (right instanceof CompositeBinningStrategy
        && ((CompositeBinningStrategy) right).usesStrategy(binningStrategyClass)) {
      byte[] rightBin = new byte[buffer.remaining()];
      buffer.get(rightBin);
      return ((CompositeBinningStrategy) right).binMatches(
          binningStrategyClass,
          new ByteArray(rightBin),
          subBin);
    }
    return false;
  }

  public boolean usesStrategy(Class<? extends StatisticBinningStrategy> binningStrategyClass) {
    return binningStrategyClass.isAssignableFrom(left.getClass())
        || binningStrategyClass.isAssignableFrom(right.getClass())
        || (left instanceof CompositeBinningStrategy
            && ((CompositeBinningStrategy) left).usesStrategy(binningStrategyClass))
        || (right instanceof CompositeBinningStrategy
            && ((CompositeBinningStrategy) right).usesStrategy(binningStrategyClass));
  }

  public boolean isOfType(
      Class<? extends StatisticBinningStrategy> leftStrategy,
      Class<? extends StatisticBinningStrategy> rightStrategy) {
    return leftStrategy.isAssignableFrom(left.getClass())
        && rightStrategy.isAssignableFrom(right.getClass());
  }

  public static ByteArray getBin(ByteArray left, ByteArray right) {
    ByteBuffer bytes = ByteBuffer.allocate(2 + left.getBytes().length + right.getBytes().length);
    bytes.putShort((short) left.getBytes().length);
    bytes.put(left.getBytes());
    bytes.put(right.getBytes());
    return new ByteArray(bytes.array());
  }
}
