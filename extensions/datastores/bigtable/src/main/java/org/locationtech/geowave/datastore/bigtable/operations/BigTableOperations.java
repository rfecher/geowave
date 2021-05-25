/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.bigtable.operations;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.bigtable.BigTableConnectionPool;
import org.locationtech.geowave.datastore.bigtable.config.BigTableOptions;
import org.locationtech.geowave.datastore.hbase.operations.GeoWaveColumnFamily;
import org.locationtech.geowave.datastore.hbase.operations.GeoWaveColumnFamily.GeoWaveColumnFamilyFactory;
import org.locationtech.geowave.datastore.hbase.operations.GeoWaveColumnFamily.StringColumnFamily;
import org.locationtech.geowave.datastore.hbase.operations.HBaseOperations;
import com.google.cloud.bigtable.hbase.BigtableRegionLocator;
import com.google.common.collect.Sets;

public class BigTableOperations extends HBaseOperations {

  // max versions on bigtable throws an NPE with a fix provided on April 14, 2021, not currently
  // in a release though
//  @SuppressWarnings("unchecked")
//  public static final Pair<GeoWaveColumnFamily, Boolean>[] METADATA_CFS_VERSIONING =
//      new Pair[] {
//          ImmutablePair.of(new StringColumnFamily(MetadataType.AIM.id()), true),
//          ImmutablePair.of(new StringColumnFamily(MetadataType.ADAPTER.id()), true),
//          ImmutablePair.of(new StringColumnFamily(MetadataType.STATISTICS.id()), true),
//          ImmutablePair.of(new StringColumnFamily(MetadataType.STATISTIC_VALUES.id()), true),
//          ImmutablePair.of(new StringColumnFamily(MetadataType.LEGACY_STATISTICS.id()), true),
//          ImmutablePair.of(new StringColumnFamily(MetadataType.INDEX.id()), true),
//          ImmutablePair.of(new StringColumnFamily(MetadataType.INTERNAL_ADAPTER.id()), true),};

  private final HashSet<String> tableCache = Sets.newHashSet();

  public BigTableOperations(final BigTableOptions options) throws IOException {
    super(
        BigTableConnectionPool.getInstance().getConnection(
            options.getProjectId(),
            options.getInstanceId()),
        options.getGeoWaveNamespace(),
        options.getHBaseOptions());
  }

//  @Override
//  public boolean verifyColumnFamily(
//      final short columnFamily,
//      final boolean enableVersioning,
//      final String tableNameStr,
//      final boolean addIfNotExist) {
//    // max versions on bigtable throws an NPE with a fix provided on April 14, 2021, not currently
//    // in a release though
//    return super.verifyColumnFamily(columnFamily, true, tableNameStr, addIfNotExist);
//  }

//  @Override
//  protected boolean verifyColumnFamilies(
//      final GeoWaveColumnFamily[] columnFamilies,
//      final GeoWaveColumnFamilyFactory columnFamilyFactory,
//      final boolean enableVersioning,
//      final TableName tableName,
//      final boolean addIfNotExist) throws IOException {
//    // TODO Auto-generated method stub
//    return super.verifyColumnFamilies(
//        columnFamilies,
//        columnFamilyFactory,
//        true,
//        tableName,
//        addIfNotExist);
//  }
//
//  @Override
//  protected void createTable(
//      final byte[][] preSplits,
//      final GeoWaveColumnFamily[] columnFamilies,
//      final GeoWaveColumnFamilyFactory columnFamilyFactory,
//      final boolean enableVersioning,
//      final TableName tableName) throws IOException {
//    super.createTable(preSplits, columnFamilies, columnFamilyFactory, true, tableName);
//  }
//
//  @Override
//  public void createTable(
//      final byte[][] preSplits,
//      final String indexName,
//      final boolean enableVersioning,
//      final short internalAdapterId) {
//    super.createTable(preSplits, indexName, true, internalAdapterId);
//  }
//
//  @Override
//  protected Pair<GeoWaveColumnFamily, Boolean>[] getMetadataCFAndVersioning() {
//    return METADATA_CFS_VERSIONING;
//  }

  @Override
  protected int getMaxVersions() {
    return super.getMaxVersions() - 1;
  }

  @Override
  public RegionLocator getRegionLocator(final String tableName) throws IOException {
    final BigtableRegionLocator regionLocator =
        (BigtableRegionLocator) super.getRegionLocator(tableName);

    if (regionLocator != null) {
      // Force region update
      if (regionLocator.getAllRegionLocations().size() <= 1) {
        regionLocator.getRegionLocation(HConstants.EMPTY_BYTE_ARRAY, true);
      }
    }

    return regionLocator;
  }

  @Override
  protected String getMetadataTableName(final MetadataType type) {
    return AbstractGeoWavePersistence.METADATA_TABLE + "_" + type.id();
  }

  @Override
  public boolean parallelDecodeEnabled() {
    // TODO: Rows that should be merged are ending up in different regions
    // which causes parallel decode to return incorrect results.
    return false;
  }

  protected void forceRegionUpdate(final BigtableRegionLocator regionLocator) {}

  @Override
  public Iterable<Result> getScannedResults(final Scan scanner, final String tableName)
      throws IOException {

    // Check the local cache
    boolean tableAvailable = tableCache.contains(tableName);

    // No local cache. Check the server and update cache
    if (!tableAvailable) {
      if (indexExists(tableName)) {
        tableAvailable = true;

        tableCache.add(tableName);
      }
    }

    // Get the results if available
    if (tableAvailable) {
      return super.getScannedResults(scanner, tableName);
    }

    // Otherwise, return empty results
    return Collections.emptyList();
  }

  public static BigTableOperations createOperations(final BigTableOptions options)
      throws IOException {
    return new BigTableOperations(options);
  }
}
