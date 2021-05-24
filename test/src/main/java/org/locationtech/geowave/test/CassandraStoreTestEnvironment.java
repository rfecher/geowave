/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexStatistic;
import org.locationtech.geowave.core.store.api.IngestOptions;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticQuery;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.StatisticType;
import org.locationtech.geowave.datastore.cassandra.CassandraStoreFactoryFamily;
import org.locationtech.geowave.datastore.cassandra.cli.CassandraServer;
import org.locationtech.geowave.datastore.cassandra.config.CassandraOptions;
import org.locationtech.geowave.datastore.cassandra.config.CassandraRequiredOptions;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CassandraStoreTestEnvironment extends StoreTestEnvironment {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraStoreTestEnvironment.class);

  private static final GenericStoreFactory<DataStore> STORE_FACTORY =
      new CassandraStoreFactoryFamily().getDataStoreFactory();
  private static CassandraStoreTestEnvironment singletonInstance = null;
  protected static final File TEMP_DIR =
      new File(System.getProperty("user.dir") + File.separator + "target", "cassandra_temp");
  protected static final File DATA_DIR =
      new File(TEMP_DIR.getAbsolutePath() + File.separator + "cassandra", "data");
  protected static final String NODE_DIRECTORY_PREFIX = "cassandra";

  public static synchronized CassandraStoreTestEnvironment getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new CassandraStoreTestEnvironment();
    }
    return singletonInstance;
  }

  private boolean running = false;
  CassandraServer s = new CassandraServer();

  private CassandraStoreTestEnvironment() {}

  @Override
  protected void initOptions(final StoreFactoryOptions options) {
    final CassandraRequiredOptions cassandraOpts = (CassandraRequiredOptions) options;
    cassandraOpts.getAdditionalOptions().setReplicationFactor(1);
    cassandraOpts.getAdditionalOptions().setDurableWrites(false);
    cassandraOpts.getAdditionalOptions().setGcGraceSeconds(0);

    try {
      cassandraOpts.getAdditionalOptions().setTableOptions(
          Collections.singletonMap(
              "compaction",
              new ObjectMapper().writeValueAsString(
                  SchemaBuilder.sizeTieredCompactionStrategy().withMinSSTableSizeInBytes(
                      500000L).withMinThreshold(2).withUncheckedTombstoneCompaction(
                          true).getOptions())));
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    cassandraOpts.setContactPoints("127.0.0.1");
    ((CassandraOptions) cassandraOpts.getStoreOptions()).setBatchWriteSize(5);
  }

  @Override
  protected GenericStoreFactory<DataStore> getDataStoreFactory() {
    return STORE_FACTORY;
  }

  @Override
  public void setup() {
    if (!running) {
      if (TEMP_DIR.exists()) {
        cleanTempDir();
      }
      if (!TEMP_DIR.mkdirs()) {
        LOGGER.warn("Unable to create temporary cassandra directory");
      }
      s.start();
      running = true;
    }
  }

  @Override
  public void tearDown() {
    if (running) {
      s.stop();
      running = false;
    }
    try {
      // it seems sometimes one of the nodes processes is still holding
      // onto a file, so wait a short time to be able to reliably clean up
      Thread.sleep(1500);
    } catch (final InterruptedException e) {
      LOGGER.warn("Unable to sleep waiting to delete directory", e);
    }
    cleanTempDir();
  }

  private static void cleanTempDir() {
    try {
      FileUtils.deleteDirectory(TEMP_DIR);
    } catch (final IOException e) {
      LOGGER.warn("Unable to delete temp cassandra directory", e);
    }
  }

  @Override
  protected GeoWaveStoreType getStoreType() {
    return GeoWaveStoreType.CASSANDRA;
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[] {};
  }

  @Override
  public int getMaxCellSize() {
    return 64 * 1024;
  }

  @Override
  public DataStorePluginOptions getDataStoreOptions(
      final GeoWaveTestStore store,
      final String[] profileOptions) {
    // because Cassandra leaves tombstone rows around (even when dropping the entire keyspace, which
    // seems to make little sense), we need to setup this delegation so that we can trigger manual
    // file system cleanup on "deleteAll" when the keyspace is dropped anyways
    return new DataStorePluginOptionsWrapper(super.getDataStoreOptions(store, profileOptions));
  }

  private static class DataStorePluginOptionsWrapper extends DataStorePluginOptions {
    DataStorePluginOptions delegate;

    public DataStorePluginOptionsWrapper(final DataStorePluginOptions delegate) {
      super();
      this.delegate = delegate;
    }

    @Override
    public void save(final Properties properties, final String namespace) {
      delegate.save(properties, namespace);
    }

    @Override
    public boolean load(final Properties properties, final String namespace) {
      return delegate.load(properties, namespace);
    }

    @Override
    public int hashCode() {
      return delegate.hashCode();
    }

    @Override
    public void selectPlugin(final String qualifier) {
      delegate.selectPlugin(qualifier);
    }

    @Override
    public Map<String, String> getOptionsAsMap() {
      return delegate.getOptionsAsMap();
    }

    @Override
    public void setFactoryOptions(final StoreFactoryOptions factoryOptions) {
      delegate.setFactoryOptions(factoryOptions);
    }

    @Override
    public void setFactoryFamily(final StoreFactoryFamilySpi factoryPlugin) {
      delegate.setFactoryFamily(factoryPlugin);
    }

    @Override
    public StoreFactoryFamilySpi getFactoryFamily() {
      return delegate.getFactoryFamily();
    }

    @Override
    public StoreFactoryOptions getFactoryOptions() {
      return delegate.getFactoryOptions();
    }

    @Override
    public DataStore createDataStore() {
      return new DataStoreWrapper(delegate.createDataStore());
    }

    @Override
    public boolean equals(final Object obj) {
      return delegate.equals(obj);
    }

    @Override
    public PersistentAdapterStore createAdapterStore() {
      return delegate.createAdapterStore();
    }

    @Override
    public IndexStore createIndexStore() {
      return delegate.createIndexStore();
    }

    @Override
    public DataStatisticsStore createDataStatisticsStore() {
      return delegate.createDataStatisticsStore();
    }

    @Override
    public AdapterIndexMappingStore createAdapterIndexMappingStore() {
      return delegate.createAdapterIndexMappingStore();
    }

    @Override
    public InternalAdapterStore createInternalAdapterStore() {
      return delegate.createInternalAdapterStore();
    }

    @Override
    public DataStoreOperations createDataStoreOperations() {
      return delegate.createDataStoreOperations();
    }

    @Override
    public String getType() {
      return delegate.getType();
    }

    @Override
    public String getGeoWaveNamespace() {
      return delegate.getGeoWaveNamespace();
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
  }
  private static class DataStoreWrapper implements DataStore {
    DataStore delegate;


    public DataStoreWrapper(final DataStore delegate) {
      super();
      this.delegate = delegate;
    }

    @Override
    public <T> void ingest(final String inputPath, final Index... index) {
      delegate.ingest(inputPath, index);
    }

    @Override
    public <T> void ingest(
        final String inputPath,
        final IngestOptions<T> options,
        final Index... index) {
      delegate.ingest(inputPath, options, index);
    }

    @Override
    public <T> CloseableIterator<T> query(final Query<T> query) {
      return delegate.query(query);
    }

    @Override
    public <P extends Persistable, R, T> R aggregate(final AggregationQuery<P, R, T> query) {
      return delegate.aggregate(query);
    }

    @Override
    public DataTypeAdapter<?> getType(final String typeName) {
      return delegate.getType(typeName);
    }

    @Override
    public DataTypeAdapter<?>[] getTypes() {
      return delegate.getTypes();
    }

    @Override
    public void addEmptyStatistic(final Statistic<?>... statistic) {
      delegate.addEmptyStatistic(statistic);
    }

    @Override
    public void addStatistic(final Statistic<?>... statistic) {
      delegate.addStatistic(statistic);
    }

    @Override
    public void removeStatistic(final Statistic<?>... statistic) {
      delegate.removeStatistic(statistic);
    }

    @Override
    public void recalcStatistic(final Statistic<?>... statistic) {
      delegate.recalcStatistic(statistic);
    }

    @Override
    public DataTypeStatistic<?>[] getDataTypeStatistics(final String typeName) {
      return delegate.getDataTypeStatistics(typeName);
    }

    @Override
    public <V extends StatisticValue<R>, R> DataTypeStatistic<V> getDataTypeStatistic(
        final StatisticType<V> statisticType,
        final String typeName,
        final String tag) {
      return delegate.getDataTypeStatistic(statisticType, typeName, tag);
    }

    @Override
    public IndexStatistic<?>[] getIndexStatistics(final String indexName) {
      return delegate.getIndexStatistics(indexName);
    }

    @Override
    public <V extends StatisticValue<R>, R> IndexStatistic<V> getIndexStatistic(
        final StatisticType<V> statisticType,
        final String indexName,
        final String tag) {
      return delegate.getIndexStatistic(statisticType, indexName, tag);
    }

    @Override
    public FieldStatistic<?>[] getFieldStatistics(final String typeName, final String fieldName) {
      return delegate.getFieldStatistics(typeName, fieldName);
    }

    @Override
    public <V extends StatisticValue<R>, R> FieldStatistic<V> getFieldStatistic(
        final StatisticType<V> statisticType,
        final String typeName,
        final String fieldName,
        final String tag) {
      return delegate.getFieldStatistic(statisticType, typeName, fieldName, tag);
    }

    @Override
    public <V extends StatisticValue<R>, R> R getStatisticValue(final Statistic<V> stat) {
      return delegate.getStatisticValue(stat);
    }

    @Override
    public <V extends StatisticValue<R>, R> R getStatisticValue(
        final Statistic<V> stat,
        final BinConstraints binConstraints) {
      return delegate.getStatisticValue(stat, binConstraints);
    }

    @Override
    public <V extends StatisticValue<R>, R> CloseableIterator<Pair<ByteArray, R>> getBinnedStatisticValues(
        final Statistic<V> stat) {
      return delegate.getBinnedStatisticValues(stat);
    }

    @Override
    public <V extends StatisticValue<R>, R> CloseableIterator<Pair<ByteArray, R>> getBinnedStatisticValues(
        final Statistic<V> stat,
        final BinConstraints binConstraints) {
      return delegate.getBinnedStatisticValues(stat, binConstraints);
    }

    @Override
    public <V extends StatisticValue<R>, R> CloseableIterator<V> queryStatistics(
        final StatisticQuery<V, R> query) {
      return delegate.queryStatistics(query);
    }

    @Override
    public <V extends StatisticValue<R>, R> V aggregateStatistics(
        final StatisticQuery<V, R> query) {
      return delegate.aggregateStatistics(query);
    }

    @Override
    public void addIndex(final Index index) {
      delegate.addIndex(index);
    }

    @Override
    public Index[] getIndices() {
      return delegate.getIndices();
    }

    @Override
    public Index[] getIndices(final String typeName) {
      return delegate.getIndices(typeName);
    }

    @Override
    public Index getIndex(final String indexName) {
      return delegate.getIndex(indexName);
    }

    @Override
    public void copyTo(final DataStore other) {
      delegate.copyTo(other);
    }

    @Override
    public void copyTo(final DataStore other, final Query<?> query) {
      delegate.copyTo(other, query);
    }

    @Override
    public void addIndex(final String typeName, final Index... indices) {
      delegate.addIndex(typeName, indices);
    }

    @Override
    public void removeIndex(final String indexName) throws IllegalStateException {
      delegate.removeIndex(indexName);
    }

    @Override
    public void removeIndex(final String typeName, final String indexName)
        throws IllegalStateException {
      delegate.removeIndex(typeName, indexName);
    }

    @Override
    public void removeType(final String typeName) {
      delegate.removeType(typeName);
    }

    @Override
    public <T> boolean delete(final Query<T> query) {
      return delegate.delete(query);
    }

    @Override
    public void deleteAll() {
      delegate.deleteAll();
      try {
        for (final File dataDir : DATA_DIR.listFiles(
            f -> f.isDirectory() && !f.getName().toLowerCase().contains("system"))) {
          FileUtils.deleteDirectory(dataDir);
        }
      } catch (final IOException e) {
        LOGGER.warn("Unable to delete cassandra data directory", e);
      }
    }

    @Override
    public <T> void addType(
        final DataTypeAdapter<T> dataTypeAdapter,
        final Index... initialIndices) {
      delegate.addType(dataTypeAdapter, initialIndices);
    }

    @Override
    public <T> void addType(
        final DataTypeAdapter<T> dataTypeAdapter,
        final List<Statistic<?>> statistics,
        final Index... initialIndices) {
      delegate.addType(dataTypeAdapter, statistics, initialIndices);
    }

    @Override
    public <T> Writer<T> createWriter(final String typeName) {
      return delegate.createWriter(typeName);
    }
  }
}
