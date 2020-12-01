/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.secondary;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Pair;
import org.bouncycastle.util.Arrays;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorAggregationQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.BinaryDataAdapter;
import org.locationtech.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.base.BaseDataStore;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.index.CustomIndexStrategy;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Lists;

@RunWith(GeoWaveITRunner.class)
public class DataIndexOnlyIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataIndexOnlyIT.class);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM},
      options = {"enableSecondaryIndexing=true"})
  protected DataStorePluginOptions dataStoreOptions;

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM},
      options = {"enableSecondaryIndexing=true"},
      namespace = "BasicSecondaryIndexIT_dataIdxOnly")
  protected DataStorePluginOptions dataIdxOnlyDataStoreOptions;
  private static long startMillis;
  private static final String testName = "DataIndexOnlyIT";

  private static class TestCustomIndexStrategy implements
      CustomIndexStrategy<Pair<byte[], byte[]>, TestCustomConstraints> {

    public TestCustomIndexStrategy() {}

    @Override
    public byte[] toBinary() {
      return new byte[0];
    }

    @Override
    public void fromBinary(final byte[] bytes) {}

    @Override
    public InsertionIds getInsertionIds(final Pair<byte[], byte[]> entry) {
      return new InsertionIds(Lists.newArrayList(entry.getValue()));
    }

    @Override
    public QueryRanges getQueryRanges(final TestCustomConstraints constraints) {
      final byte[] sortKey = StringUtils.stringToBinary(constraints.matchText());
      return new QueryRanges(new ByteArrayRange(sortKey, sortKey));
    }

  }

  /**
   * This class serves as constraints for our UUID index strategy. Since we only need to query for
   * exact UUIDs, the constraints class is fairly straightforward. We only need a single UUID String
   * to use as our constraint.
   */
  public static class TestCustomConstraints implements Persistable {
    private String matchText;

    public TestCustomConstraints() {}

    public TestCustomConstraints(final String matchText) {
      this.matchText = matchText;
    }

    public String matchText() {
      return matchText;
    }

    /**
     * Serialize any data needed to persist this constraint.
     */
    @Override
    public byte[] toBinary() {
      return StringUtils.stringToBinary(matchText);
    }

    /**
     * Load the UUID constraint from binary.
     */
    @Override
    public void fromBinary(final byte[] bytes) {
      matchText = StringUtils.stringFromBinary(bytes);
    }

  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    TestUtils.printStartOfTest(LOGGER, testName);
  }

  @AfterClass
  public static void reportTest() {
    TestUtils.printEndOfTest(LOGGER, testName, startMillis);
  }

  @Test
  public void testDataIndexOnly() throws Exception {
    TestUtils.testLocalIngest(
        getDataStorePluginOptions(),
        DimensionalityType.SPATIAL,
        HAIL_SHAPEFILE_FILE,
        1);

    final DataStore store = dataStoreOptions.createDataStore();
    final DataStore dataIdxStore = dataIdxOnlyDataStoreOptions.createDataStore();
    final FeatureDataAdapter adapter = (FeatureDataAdapter) store.getTypes()[0];
    dataIdxStore.addType(adapter);
    try (Writer<SimpleFeature> writer = dataIdxStore.createWriter(adapter.getTypeName())) {
      try (CloseableIterator<SimpleFeature> it =
          store.query(VectorQueryBuilder.newBuilder().build())) {
        while (it.hasNext()) {
          writer.write(it.next());
        }
      }
    }
    Long count =
        (Long) dataIdxStore.aggregate(
            VectorAggregationQueryBuilder.newBuilder().count(adapter.getTypeName()).build());
    final Long originalCount =
        (Long) store.aggregate(
            VectorAggregationQueryBuilder.newBuilder().count(adapter.getTypeName()).build());
    Assert.assertTrue(count > 0);
    Assert.assertEquals(originalCount, count);
    final VectorStatisticsQueryBuilder bldr = VectorStatisticsQueryBuilder.newBuilder();
    count = dataIdxStore.aggregateStatistics(bldr.factory().count().build());
    Assert.assertEquals(originalCount, count);
    count = 0L;
    final String[] idsToRemove = new String[3];
    int idsToRemoveIdx = 0;
    try (CloseableIterator<SimpleFeature> it =
        store.query(VectorQueryBuilder.newBuilder().build())) {
      while (it.hasNext()) {
        if (idsToRemoveIdx < 3) {
          idsToRemove[idsToRemoveIdx++] = it.next().getID();
        } else {
          it.next();
        }
        count++;
      }
    }
    Assert.assertEquals(originalCount, count);
    for (final String id : idsToRemove) {
      final VectorQueryBuilder idBldr = VectorQueryBuilder.newBuilder();
      Assert.assertTrue(
          dataIdxStore.delete(
              idBldr.constraints(
                  idBldr.constraintsFactory().dataIds(StringUtils.stringToBinary(id))).build()));
    }
    count = dataIdxStore.aggregateStatistics(bldr.factory().count().build());
    Assert.assertEquals(originalCount - 3, (long) count);

    TestUtils.deleteAll(dataStoreOptions);
    TestUtils.deleteAll(dataIdxOnlyDataStoreOptions);
  }

  @Test
  public void testDataIndexOnlyOnBinaryType() throws Exception {
    final DataStore dataStore = dataIdxOnlyDataStoreOptions.createDataStore();
    final BinaryDataAdapter adapter = new BinaryDataAdapter("testDataIndexOnlyOnBinaryType");
    final String customIndexName = "MatchTextIdx";
    dataStore.addType(adapter, new CustomIndex<>(new TestCustomIndexStrategy(), customIndexName));
    try (Writer<Pair<byte[], byte[]>> writer = dataStore.createWriter(adapter.getTypeName())) {
      for (int i = 0; i < 9; i++) {
        writer.write(
            Pair.of(
                StringUtils.stringToBinary("abcdefghijk" + i),
                StringUtils.stringToBinary("abcdefghijk" + i)));
      }
    }

    for (int i = 0; i < 9; i++) {
      final String matchText = "abcdefghijk" + i;
      final byte[] id = StringUtils.stringToBinary(matchText);
      try (CloseableIterator<Pair<byte[], byte[]>> it =
          (CloseableIterator) dataStore.query(
              QueryBuilder.newBuilder().constraints(
                  QueryBuilder.newBuilder().constraintsFactory().dataIds(id)).build())) {
        Assert.assertTrue(it.hasNext());
        Assert.assertTrue(Arrays.areEqual(id, it.next().getRight()));
        Assert.assertFalse(it.hasNext());
      }
      try (CloseableIterator<Pair<byte[], byte[]>> it =
          (CloseableIterator) dataStore.query(
              QueryBuilder.newBuilder().constraints(
                  QueryBuilder.newBuilder().constraintsFactory().dataIdsByRange(id, id)).build())) {
        Assert.assertTrue(it.hasNext());
        Assert.assertTrue(Arrays.areEqual(id, it.next().getRight()));
        Assert.assertFalse(it.hasNext());
      }
      try (CloseableIterator<Pair<byte[], byte[]>> it =
          (CloseableIterator) dataStore.query(
              QueryBuilder.newBuilder().indexName(customIndexName).constraints(
                  QueryBuilder.newBuilder().constraintsFactory().customConstraints(
                      new TestCustomConstraints(matchText))).build())) {
        Assert.assertTrue(it.hasNext());
        Assert.assertTrue(Arrays.areEqual(id, it.next().getRight()));
        Assert.assertFalse(it.hasNext());
      }
    }
    TestUtils.deleteAll(dataIdxOnlyDataStoreOptions);
  }

  @Test
  public void testDataIndexOnlyOnCustomType() throws Exception {
    final DataStore dataStore = dataIdxOnlyDataStoreOptions.createDataStore();
    final LatLonTimeAdapter adapter = new LatLonTimeAdapter();
    dataStore.addType(adapter);
    try (Writer<LatLonTime> writer = dataStore.createWriter(adapter.getTypeName())) {
      for (int i = 0; i < 10; i++) {
        writer.write(new LatLonTime(i, 100 * i, 0.25f * i, -0.5f * i));
      }
    }

    final Set<Integer> expectedIntIds =
        IntStream.rangeClosed(0, 9).boxed().collect(Collectors.toSet());
    try (CloseableIterator<LatLonTime> it =
        (CloseableIterator) dataStore.query(QueryBuilder.newBuilder().build())) {
      while (it.hasNext()) {
        Assert.assertTrue(expectedIntIds.remove(it.next().getId()));
      }
    }
    Assert.assertTrue(expectedIntIds.isEmpty());
    try {
      List<Integer> expectedReversedIntIds =
          IntStream.rangeClosed(0, 2).boxed().collect(Collectors.toList());
      ListIterator<Integer> expectedReversedIntIdsIterator =
          expectedReversedIntIds.listIterator(expectedReversedIntIds.size());
      try (CloseableIterator<LatLonTime> it =
          (CloseableIterator) dataStore.query(
              QueryBuilder.newBuilder().constraints(
                  QueryBuilder.newBuilder().constraintsFactory().dataIdsByRangeReverse(
                      null,
                      Lexicoders.LONG.toByteArray(200L))).build())) {
        while (it.hasNext()) {
          Assert.assertEquals(
              Integer.valueOf(expectedReversedIntIdsIterator.previous()),
              Integer.valueOf(it.next().getId()));
        }
        Assert.assertTrue(!expectedReversedIntIdsIterator.hasPrevious());
      }
      expectedReversedIntIds = IntStream.rangeClosed(7, 9).boxed().collect(Collectors.toList());
      expectedReversedIntIdsIterator =
          expectedReversedIntIds.listIterator(expectedReversedIntIds.size());
      try (CloseableIterator<LatLonTime> it =
          (CloseableIterator) dataStore.query(
              QueryBuilder.newBuilder().constraints(
                  QueryBuilder.newBuilder().constraintsFactory().dataIdsByRangeReverse(
                      Lexicoders.LONG.toByteArray(650L),
                      null)).build())) {
        while (it.hasNext()) {
          Assert.assertEquals(
              Integer.valueOf(expectedReversedIntIdsIterator.previous()),
              Integer.valueOf(it.next().getId()));
        }
        Assert.assertTrue(!expectedReversedIntIdsIterator.hasPrevious());
      }
      expectedReversedIntIds = IntStream.rangeClosed(4, 8).boxed().collect(Collectors.toList());
      expectedReversedIntIdsIterator =
          expectedReversedIntIds.listIterator(expectedReversedIntIds.size());
      try (CloseableIterator<LatLonTime> it =
          (CloseableIterator) dataStore.query(
              QueryBuilder.newBuilder().constraints(
                  QueryBuilder.newBuilder().constraintsFactory().dataIdsByRangeReverse(
                      Lexicoders.LONG.toByteArray(400L),
                      Lexicoders.LONG.toByteArray(800L))).build())) {
        while (it.hasNext()) {
          Assert.assertEquals(
              Integer.valueOf(expectedReversedIntIdsIterator.previous()),
              Integer.valueOf(it.next().getId()));
        }
        Assert.assertTrue(!expectedReversedIntIdsIterator.hasPrevious());
      }
    } catch (final UnsupportedOperationException e) {
      if (((BaseDataStore) dataStore).isReverseIterationSupported()) {
        Assert.fail(e.getMessage());
      }
    }
    TestUtils.deleteAll(dataIdxOnlyDataStoreOptions);
  }

  public static class LatLonTime {
    private transient int id;
    private long time;
    private float lat;
    private float lon;

    public LatLonTime() {}

    public LatLonTime(final int id, final long time, final float lat, final float lon) {
      this.id = id;
      this.time = time;
      this.lat = lat;
      this.lon = lon;
    }

    public void setId(final int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }

    public long getTime() {
      return time;
    }

    public float getLat() {
      return lat;
    }

    public float getLon() {
      return lon;
    }

    public byte[] toBinary() {
      // ID can be set from the adapter so no need to persist
      final ByteBuffer buf = ByteBuffer.allocate(12);
      buf.putInt((int) time);
      buf.putFloat(lat);
      buf.putFloat(lon);
      return buf.array();
    }

    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      time = buf.getInt();
      lat = buf.getFloat();
      lon = buf.getFloat();
    }
  }
  public static class LatLonTimeAdapter implements DataTypeAdapter<LatLonTime> {
    private static final FieldReader READER = new LatLonTimeReader();
    private static final FieldWriter WRITER = new LatLonTimeWriter();
    private static final String SINGLETON_FIELD = "LLT";

    @Override
    public FieldReader<Object> getReader(final String fieldName) {
      return READER;
    }

    @Override
    public FieldWriter<LatLonTime, Object> getWriter(final String fieldName) {
      return WRITER;
    }

    @Override
    public byte[] toBinary() {
      return new byte[0];
    }

    @Override
    public void fromBinary(final byte[] bytes) {}

    @Override
    public String getTypeName() {
      return "LLT";
    }

    @Override
    public byte[] getDataId(final LatLonTime entry) {
      final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
      buf.put(Lexicoders.LONG.toByteArray(entry.time));
      buf.put(Lexicoders.INT.toByteArray(entry.getId()));
      return buf.array();
    }

    @Override
    public LatLonTime decode(final IndexedAdapterPersistenceEncoding data, final Index index) {
      final LatLonTime retVal =
          (LatLonTime) data.getAdapterExtendedData().getValue(SINGLETON_FIELD);
      final ByteBuffer buf = ByteBuffer.wrap(data.getDataId());
      buf.position(8);
      final byte[] idBytes = new byte[4];
      buf.get(idBytes);
      final int id = Lexicoders.INT.fromByteArray(idBytes);
      retVal.setId(id);
      return retVal;
    }


    @Override
    public AdapterPersistenceEncoding encode(
        final LatLonTime entry,
        final CommonIndexModel indexModel) {
      return new AdapterPersistenceEncoding(
          getDataId(entry),
          new MultiFieldPersistentDataset<>(),
          new MultiFieldPersistentDataset<>(Collections.singletonMap(SINGLETON_FIELD, entry)));
    }

    @Override
    public int getPositionOfOrderedField(final CommonIndexModel model, final String fieldName) {
      return 0;
    }

    @Override
    public String getFieldNameForPosition(final CommonIndexModel model, final int position) {
      return "LLT";
    }

    private static class LatLonTimeReader implements FieldReader<LatLonTime> {
      @Override
      public LatLonTime readField(final byte[] fieldData) {
        final LatLonTime retVal = new LatLonTime();
        retVal.fromBinary(fieldData);
        return retVal;
      }
    }
    private static class LatLonTimeWriter implements FieldWriter<LatLonTime, LatLonTime> {
      @Override
      public byte[] writeField(final LatLonTime fieldValue) {
        final byte[] bytes = fieldValue.toBinary();
        return bytes;
      }
    }
  }
}
