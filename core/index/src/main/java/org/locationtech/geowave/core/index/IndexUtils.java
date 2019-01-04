/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.bin.BinRange;
import org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;

public class IndexUtils {
  public static MultiDimensionalNumericData getFullBounds(
      final NumericIndexStrategy indexStrategy) {
    return getFullBounds(indexStrategy.getOrderedDimensionDefinitions());
  }

  /**
   * Constraints that are empty indicate full table scan. A full table scan occurs if ANY one
   * dimension is unbounded.
   *
   * @param constraints
   * @return true if any one dimension is unbounded
   */
  public static final boolean isFullTableScan(final List<MultiDimensionalNumericData> constraints) {
    for (final MultiDimensionalNumericData constraint : constraints) {
      if (constraint.isEmpty()) {
        return false;
      }
    }
    return constraints.isEmpty();
  }

  public static MultiDimensionalNumericData getFullBounds(
      final NumericDimensionDefinition[] dimensionDefinitions) {
    final NumericRange[] boundsPerDimension = new NumericRange[dimensionDefinitions.length];
    for (int d = 0; d < dimensionDefinitions.length; d++) {
      boundsPerDimension[d] = dimensionDefinitions[d].getBounds();
    }
    return new BasicNumericDataset(boundsPerDimension);
  }

  public static final double getDimensionalBitsUsed(
      final NumericIndexStrategy indexStrategy,
      final double[] dataRangePerDimension) {
    double result = Long.MAX_VALUE;
    if (dataRangePerDimension.length == 0) {
      return 0;
    }
    final double cellRangePerDimension[] = indexStrategy.getHighestPrecisionIdRangePerDimension();
    final double inflatedRangePerDimension[] =
        inflateRange(cellRangePerDimension, dataRangePerDimension);
    final double bitsPerDimension[] = getBitsPerDimension(indexStrategy, cellRangePerDimension);

    final BinRange[][] binsPerDimension =
        getBinsPerDimension(indexStrategy, inflatedRangePerDimension);
    final double[][] bitsFromTheRightPerDimension =
        getBitsFromTheRightPerDimension(binsPerDimension, cellRangePerDimension);

    // This ALWAYS chooses the index who dimension
    // cells cover the widest range thus fewest cells. In temporal, YEAR is
    // always chosen.
    // However, this is not necessarily bad. A smaller bin size may result
    // in more bins searched.
    // When searching across multiple bins associated with a dimension, The
    // first and last bin are
    // partial searches. The inner bins are 'full' scans over the bin.
    // Thus, smaller bin sizes could result more in more rows scanned.
    // On the flip, fewer larger less-granular bins can also have the same
    // result.
    // Bottom line: this is not straight forward
    // Example: YEAR
    // d[ 3600000.0]
    // cellRangePerDimension[30157.470702171326]
    // inflatedRangePerDimension[3618896.484260559]
    // bitsFromTheRightPerDimension[6.906890595608519]]
    // Example: DAY
    // cellRangePerDimension[ 2554.3212881088257]
    // inflatedRangePerDimension[ 3601593.016233444]
    // bitsFromTheRightPerDimension[ 10.461479447286157]]
    for (final double[] binnedBitsPerFromTheRightDimension : bitsFromTheRightPerDimension) {
      for (int d = 0; d < binnedBitsPerFromTheRightDimension.length; d++) {
        final double totalBitsUsed = (bitsPerDimension[d] - binnedBitsPerFromTheRightDimension[d]);
        if (totalBitsUsed < 0) {
          return 0;
        }
        result = Math.min(totalBitsUsed, result);
      }
    }

    // The least constraining dimension uses the least amount of bits of
    // fixed bits from the left.
    // For example, half of the world latitude is 1 bit, 1/4 of the world is
    // 2 bits etc.
    // Use the least constraining dimension, but multiply by the
    // # of dimensions.
    return result * cellRangePerDimension.length;
  }

  public static double[] inflateRange(
      final double[] cellRangePerDimension,
      final double[] dataRangePerDimension) {
    final double[] result = new double[cellRangePerDimension.length];
    for (int d = 0; d < result.length; d++) {
      result[d] =
          Math.ceil(dataRangePerDimension[d] / cellRangePerDimension[d]) * cellRangePerDimension[d];
    }
    return result;
  }

  public static double[][] getBitsFromTheRightPerDimension(
      final BinRange[][] binsPerDimension,
      final double[] cellRangePerDimension) {
    int numBinnedQueries = 1;
    for (int d = 0; d < binsPerDimension.length; d++) {
      numBinnedQueries *= binsPerDimension[d].length;
    }
    // now we need to combine all permutations of bin ranges into
    // BinnedQuery objects
    final double[][] binnedQueries = new double[numBinnedQueries][];
    for (int d = 0; d < binsPerDimension.length; d++) {
      for (int b = 0; b < binsPerDimension[d].length; b++) {
        for (int i = b; i < numBinnedQueries; i += binsPerDimension[d].length) {
          if (binnedQueries[i] == null) {
            binnedQueries[i] = new double[binsPerDimension.length];
          }
          if ((binsPerDimension[d][b].getNormalizedMax()
              - binsPerDimension[d][b].getNormalizedMin()) <= 0.000000001) {
            binnedQueries[i][d] = 0;
          } else {
            binnedQueries[i][d] =
                log2(
                    Math.ceil(
                        (binsPerDimension[d][b].getNormalizedMax()
                            - binsPerDimension[d][b].getNormalizedMin())
                            / cellRangePerDimension[d]));
          }
        }
      }
    }
    return binnedQueries;
  }

  public static int getBitPositionOnSortKeyFromSubsamplingArray(
      final NumericIndexStrategy indexStrategy,
      final double[] maxResolutionSubsamplingPerDimension) {
    return (int) Math.round(
        getDimensionalBitsUsed(indexStrategy, maxResolutionSubsamplingPerDimension));
  }

  public static int getBitPositionFromSubsamplingArray(
      final NumericIndexStrategy indexStrategy,
      final double[] maxResolutionSubsamplingPerDimension) {
    return getBitPositionOnSortKeyFromSubsamplingArray(
        indexStrategy,
        maxResolutionSubsamplingPerDimension) + (8 * indexStrategy.getPartitionKeyLength());
  }

  public static byte[] getNextRowForSkip(final byte[] row, final int bitPosition) {
    if (bitPosition <= 0) {
      return new byte[0];
    }
    // Calculate the number of full bytes affected by the bit position
    int numBytes = (bitPosition + 1) / 8;

    // Calculate the number of bits used in the last byte
    final int extraBits = (bitPosition + 1) % 8;

    // If there was a remainder, add 1 to the number of bytes
    if (extraBits > 0) {
      numBytes++;
    }
    // Copy affected bytes

    final byte[] rowCopy = Arrays.copyOf(row, numBytes);

    final int lastByte = rowCopy.length - 1;

    // Turn on all bits after the bit position
    rowCopy[lastByte] |= 0xFF >> (extraBits);

    // Increment the bit represented by the bit position
    for (int i = lastByte; i >= 0; i--) {
      rowCopy[i]++;
      if (rowCopy[i] != 0) {
        // Turn on all bits after the bit position
        rowCopy[lastByte] |= 0xFF >> (extraBits);
        return rowCopy;
      }
    }
    return null;
  }

  private static final double[] getBitsPerDimension(
      final NumericIndexStrategy indexStrategy,
      final double[] rangePerDimension) {
    final NumericDimensionDefinition dim[] = indexStrategy.getOrderedDimensionDefinitions();
    final double result[] = new double[rangePerDimension.length];
    for (int d = 0; d < rangePerDimension.length; d++) {
      result[d] += Math.ceil(log2((dim[d].getRange() / rangePerDimension[d])));
    }
    return result;
  }

  private static final BinRange[][] getBinsPerDimension(
      final NumericIndexStrategy indexStrategy,
      final double[] rangePerDimension) {

    final NumericDimensionDefinition dim[] = indexStrategy.getOrderedDimensionDefinitions();
    final BinRange[][] result = new BinRange[rangePerDimension.length][];
    for (int d = 0; d < rangePerDimension.length; d++) {
      final BinRange[] ranges =
          dim[d].getNormalizedRanges(new NumericRange(0, rangePerDimension[d]));
      result[d] = ranges;
    }
    return result;
  }

  private static double log2(final double v) {
    return Math.log(v) / Math.log(2);
  }

  public static Set<ByteArray> getQueryPartitionKeys(
      final NumericIndexStrategy strategy,
      final MultiDimensionalNumericData queryData,
      final IndexMetaData... hints) {
    final QueryRanges queryRanges = strategy.getQueryRanges(queryData, hints);
    return Sets.newHashSet(
        Collections2.transform(
            queryRanges.getPartitionQueryRanges(),
            new Function<SinglePartitionQueryRanges, ByteArray>() {
              @Override
              public ByteArray apply(@Nonnull final SinglePartitionQueryRanges input) {
                return input.getPartitionKey();
              }
            }));
  }

  public static Set<ByteArray> getInsertionPartitionKeys(
      final NumericIndexStrategy strategy,
      final MultiDimensionalNumericData insertionData) {
    final InsertionIds insertionIds = strategy.getInsertionIds(insertionData);
    return Sets.newHashSet(
        Collections2.transform(
            insertionIds.getPartitionKeys(),
            new Function<SinglePartitionInsertionIds, ByteArray>() {
              @Override
              public ByteArray apply(@Nonnull final SinglePartitionInsertionIds input) {
                return input.getPartitionKey();
              }
            }));
  }
}
