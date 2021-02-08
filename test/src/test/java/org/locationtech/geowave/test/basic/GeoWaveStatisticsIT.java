/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.List;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic.NumericRangeValue;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveStatisticsIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveStatisticsIT.class);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM})
  protected DataStorePluginOptions dataStore;

  private static long startMillis;

  @BeforeClass
  public static void reportTestStart() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-------------------------------");
    LOGGER.warn("*                             *");
    LOGGER.warn("* RUNNING GeoWaveStatisticsIT *");
    LOGGER.warn("*                             *");
    LOGGER.warn("-------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("--------------------------------");
    LOGGER.warn("*                              *");
    LOGGER.warn("* FINISHED GeoWaveStatisticsIT *");
    LOGGER.warn(
        "*        "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.          *");
    LOGGER.warn("*                              *");
    LOGGER.warn("--------------------------------");
  }

  @Before
  public void initialize() throws MismatchedIndexToAdapterMapping, IOException {
    final DataStore ds = dataStore.createDataStore();
    final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
    final Index idx = SimpleIngest.createSpatialIndex();
    final GeotoolsFeatureDataAdapter<SimpleFeature> fda = SimpleIngest.createDataAdapter(sft);
    final List<SimpleFeature> features =
        SimpleIngest.getGriddedFeatures(new SimpleFeatureBuilder(sft), 8675309);
    LOGGER.info(
        String.format("Beginning to ingest a uniform grid of %d features", features.size()));
    int ingestedFeatures = 0;
    final int featuresPer5Percent = features.size() / 20;
    ds.addType(fda, idx);

    try (Writer<Object> writer = ds.createWriter(fda.getTypeName())) {
      for (final SimpleFeature feat : features) {
        ingestedFeatures++;
        if ((ingestedFeatures % featuresPer5Percent) == 0) {
          // just write 5 percent of the grid
          writer.write(feat);
        }
      }
    }
  }

  @After
  public void cleanupWorkspace() {
    TestUtils.deleteAll(dataStore);
  }

  @Test
  public void testAddStatistic() {
    final DataStore ds = dataStore.createDataStore();

    final NumericRangeStatistic longitudeRange =
        new NumericRangeStatistic(SimpleIngest.FEATURE_NAME, "Longitude");
    final NumericRangeStatistic latitudeRange =
        new NumericRangeStatistic(SimpleIngest.FEATURE_NAME, "Latitude");
    ds.addStatistic(longitudeRange);
    ds.addEmptyStatistic(latitudeRange);

    try (CloseableIterator<NumericRangeValue> iterator =
        ds.queryStatistics(
            StatisticQueryBuilder.newBuilder(NumericRangeStatistic.STATS_TYPE).typeName(
                SimpleIngest.FEATURE_NAME).fieldName("Longitude").build())) {
      assertTrue(iterator.hasNext());
      final NumericRangeValue value = iterator.next();
      assertEquals(-165.0, value.getMin(), 0.1);
      assertEquals(180.0, value.getMax(), 0.1);
      assertFalse(iterator.hasNext());
    }

    try (CloseableIterator<NumericRangeValue> iterator =
        ds.queryStatistics(
            StatisticQueryBuilder.newBuilder(NumericRangeStatistic.STATS_TYPE).typeName(
                SimpleIngest.FEATURE_NAME).fieldName("Latitude").build())) {
      // We only calculated stats for Longitude
      assertFalse(iterator.hasNext());
    }

  }

  @Test
  public void testRemoveStatistic() {
    final DataStore ds = dataStore.createDataStore();
    // STATS_TODO: Add more statistics endpoints to the data store interface for getStatistics?
    // getDataTypeStatistic(typeName, statsType, tag)
    // getIndexStatistic(indexName, statsType, tag)
    // getFieldStatistic(typeName, fieldName, statsType, tag)

    // Verify count statistic exists
    CountStatistic count = null;
    Statistic<?>[] typeStats = ds.getDataTypeStatistics(SimpleIngest.FEATURE_NAME);
    for (Statistic<?> stat : typeStats) {
      if (stat instanceof CountStatistic) {
        count = (CountStatistic) stat;
        break;
      }
    }
    assertNotNull(count);

    // Verify value exists
    try (CloseableIterator<CountValue> iterator =
        ds.queryStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                SimpleIngest.FEATURE_NAME).build())) {
      assertTrue(iterator.hasNext());
      final CountValue value = iterator.next();
      assertEquals(new Long(20), value.getValue());
      assertFalse(iterator.hasNext());
    }

    ds.removeStatistic(count);

    // Verify statistic value was removed
    try (CloseableIterator<CountValue> iterator =
        ds.queryStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                SimpleIngest.FEATURE_NAME).build())) {
      assertFalse(iterator.hasNext());
    }

    // Verify statistic is no longer present
    typeStats = ds.getDataTypeStatistics(SimpleIngest.FEATURE_NAME);
    for (Statistic<?> stat : typeStats) {
      assertFalse(stat instanceof CountStatistic);
    }
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }
}
