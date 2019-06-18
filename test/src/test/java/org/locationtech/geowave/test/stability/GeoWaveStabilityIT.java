/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.stability;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.ExplicitCQLQuery;
import org.locationtech.geowave.core.geotime.store.query.OptimalCQLQuery;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.core.store.operations.ReaderParamsBuilder;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveStabilityIT extends AbstractGeoWaveBasicVectorIT {
  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB})
  protected DataStorePluginOptions dataStore;
  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB},
      namespace = "badDataStore")
  protected DataStorePluginOptions badDataStore;

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveStabilityIT.class);
  private static long startMillis;
  private static final int NUM_THREADS = 4;

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      RUNNING GeoWaveStabilityIT       *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      FINISHED GeoWaveStabilityIT      *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @Test
  public void testBadMetadataStability() throws Exception {
    TestUtils.deleteAll(badDataStore);
    TestUtils.testLocalIngest(
        dataStore,
        DimensionalityType.SPATIAL_TEMPORAL,
        HAIL_SHAPEFILE_FILE,
        NUM_THREADS);

    copyBadData(true, false);

    queryBadData(true);
    queryGoodData();
  }

  @Test
  public void testBadDataStability() throws Exception {
    TestUtils.deleteAll(badDataStore);
    TestUtils.testLocalIngest(
        dataStore,
        DimensionalityType.SPATIAL_TEMPORAL,
        HAIL_SHAPEFILE_FILE,
        NUM_THREADS);

    copyBadData(false, true);

    queryBadData(false);
    queryGoodData();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void copyBadData(boolean badMetadata, boolean badData) throws Exception {
    DataStoreOperations badStoreOperations = badDataStore.createDataStoreOperations();
    DataStoreOperations storeOperations = dataStore.createDataStoreOperations();
    PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
    InternalAdapterStore internalAdapterStore = dataStore.createInternalAdapterStore();
    AdapterIndexMappingStore indexMappingStore = dataStore.createAdapterIndexMappingStore();
    IndexStore indexStore = dataStore.createIndexStore();
    for (final MetadataType metadataType : MetadataType.values()) {
      try (MetadataWriter writer = badStoreOperations.createMetadataWriter(metadataType)) {
        final MetadataReader reader = storeOperations.createMetadataReader(metadataType);
        try (CloseableIterator<GeoWaveMetadata> it = reader.query(new MetadataQuery(null, null))) {
          while (it.hasNext()) {
            if (badMetadata) {
              writer.write(new BadGeoWaveMetadata(it.next()));
            } else {
              writer.write(it.next());
            }
          }
        }
      } catch (final Exception e) {
        LOGGER.error("Unable to write metadata on copy", e);
      }
    }
    try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
      while (it.hasNext()) {
        final InternalDataAdapter<?> adapter = it.next();
        for (final Index index : indexMappingStore.getIndicesForAdapter(
            adapter.getAdapterId()).getIndices(indexStore)) {
          final boolean rowMerging = BaseDataStoreUtils.isRowMerging(adapter);
          final ReaderParamsBuilder bldr =
              new ReaderParamsBuilder(
                  index,
                  adapterStore,
                  internalAdapterStore,
                  GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER);
          bldr.adapterIds(new short[] {adapter.getAdapterId()});
          bldr.isClientsideRowMerging(rowMerging);
          try (RowReader<GeoWaveRow> reader = storeOperations.createReader(bldr.build())) {
            try (RowWriter writer = badStoreOperations.createWriter(index, adapter)) {
              while (reader.hasNext()) {
                if (badData) {
                  writer.write(new BadGeoWaveRow(reader.next()));
                } else {
                  writer.write(reader.next());
                }
              }
            }
          } catch (final Exception e) {
            LOGGER.error("Unable to write metadata on copy", e);
          }
        }
      }
    }
    try {
      badStoreOperations.mergeStats(
          badDataStore.createDataStatisticsStore(),
          badDataStore.createInternalAdapterStore());
    } catch (Exception e) {
      LOGGER.info("Caught exception while merging bad stats.");
    }

  }

  private void queryBadData(boolean badMetadata) throws Exception {
    PersistentAdapterStore badAdapterStore = badDataStore.createAdapterStore();
    try (CloseableIterator<InternalDataAdapter<?>> dataAdapters = badAdapterStore.getAdapters()) {
      InternalDataAdapter<?> adapter = dataAdapters.next();
      Assert.assertTrue(adapter.getAdapter() instanceof GeotoolsFeatureDataAdapter);
      final QueryConstraints constraints =
          (ExplicitCQLQuery) OptimalCQLQuery.createOptimalQuery(
              "BBOX(geom,-105,28,-87,44) and STATE = 'IL'",
              (GeotoolsFeatureDataAdapter) adapter.getAdapter(),
              null,
              null);
      QueryBuilder<?, ?> bldr = QueryBuilder.newBuilder();

      try (final CloseableIterator<?> actualResults =
          badDataStore.createDataStore().query(bldr.constraints(constraints).build())) {
        int size = Iterators.size(actualResults);
        LOGGER.error(String.format("Found %d results, expected exception...", size));
        Assert.fail();
      } catch (Exception e) {
        // Expected exception
      }
    } catch (Exception e) {
      if (badMetadata) {
        // expected
      } else {
        Assert.fail();
      }
    }
  }

  private void queryGoodData() {
    try {
      URL[] expectedResultsUrls =
          new URL[] {new File(HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()};

      testQuery(
          new File(TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
          expectedResultsUrls,
          "bounding box and time range");
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while testing a bounding box and time range query of spatial temporal index: '"
              + e.getLocalizedMessage()
              + "'");
    }
  }

  private static class BadGeoWaveMetadata extends GeoWaveMetadata {

    public BadGeoWaveMetadata(GeoWaveMetadata source) {
      super(
          reverse(source.getPrimaryId()),
          reverse(source.getSecondaryId()),
          reverse(source.getVisibility()),
          reverse(source.getValue()));
    }

    private static byte[] reverse(byte[] source) {
      ArrayUtils.reverse(source);
      return source;
    }

  }

  private static class BadGeoWaveRow implements GeoWaveRow {

    private final GeoWaveRow source;

    public BadGeoWaveRow(GeoWaveRow source) {
      this.source = source;
    }

    @Override
    public byte[] getDataId() {
      return source.getDataId();
    }

    @Override
    public short getAdapterId() {
      return source.getAdapterId();
    }

    @Override
    public byte[] getSortKey() {
      return source.getSortKey();
    }

    @Override
    public byte[] getPartitionKey() {
      return source.getPartitionKey();
    }

    @Override
    public int getNumberOfDuplicates() {
      return source.getNumberOfDuplicates();
    }

    @Override
    public GeoWaveValue[] getFieldValues() {
      return Arrays.stream(source.getFieldValues()).map(BadGeoWaveValue::new).toArray(
          BadGeoWaveValue[]::new);
    }

    private static class BadGeoWaveValue implements GeoWaveValue {

      private final GeoWaveValue source;

      public BadGeoWaveValue(GeoWaveValue source) {
        this.source = source;
      }

      @Override
      public byte[] getFieldMask() {
        return source.getFieldMask();
      }

      @Override
      public byte[] getVisibility() {
        return source.getVisibility();
      }

      @Override
      public byte[] getValue() {
        byte[] value = source.getValue();
        ArrayUtils.reverse(value);
        return value;
      }

      @Override
      public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + Arrays.hashCode(getFieldMask());
        result = (prime * result) + Arrays.hashCode(getValue());
        result = (prime * result) + Arrays.hashCode(getVisibility());
        return result;
      }

      @Override
      public boolean equals(final Object obj) {
        if (this == obj) {
          return true;
        }
        if (obj == null) {
          return false;
        }
        if (getClass() != obj.getClass()) {
          return false;
        }
        final GeoWaveValueImpl other = (GeoWaveValueImpl) obj;
        if (!Arrays.equals(getFieldMask(), other.getFieldMask())) {
          return false;
        }
        if (!Arrays.equals(getValue(), other.getValue())) {
          return false;
        }
        if (!Arrays.equals(getVisibility(), other.getVisibility())) {
          return false;
        }
        return true;
      }

    }

  }

}
