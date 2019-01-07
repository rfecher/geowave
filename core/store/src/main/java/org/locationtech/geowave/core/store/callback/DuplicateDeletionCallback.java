/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.callback;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.base.BaseDataStore;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.query.constraints.InsertionIdQuery;
import org.locationtech.geowave.core.store.util.DataStoreUtils;

/** This callback finds the duplicates for each scanned entry, and deletes them by insertion ID */
public class DuplicateDeletionCallback<T> implements DeleteCallback<T, GeoWaveRow>, Closeable {
  private final BaseDataStore dataStore;
  private final InternalDataAdapter adapter;
  private final Index index;
  private final HashMap<ByteArray, InsertionIdData> duplicateInsertionIds;
  private final List<InsertionIdData> origRows;

  private boolean closed = false;

  public DuplicateDeletionCallback(
      final BaseDataStore store,
      final InternalDataAdapter adapter,
      final Index index) {
    this.adapter = adapter;
    this.index = index;
    dataStore = store;
    duplicateInsertionIds = new HashMap<>();
    // we need to keep track of the original rows visited so we don't
    // mis-identify them as duplicates
    origRows = new ArrayList<>();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    } else {
      closed = true;
    }
    // make sure we take out the original rows first if they accidentally
    // got added to the duplicates
    for (int i = 0; i < origRows.size(); i++) {
      final InsertionIdData rowId = origRows.get(i);
      if (duplicateInsertionIds.containsKey(new ByteArray(rowId.partitionKey))) {
        duplicateInsertionIds.remove(new ByteArray(rowId.partitionKey));
      }
    }

    for (final Map.Entry<ByteArray, InsertionIdData> entry : duplicateInsertionIds.entrySet()) {
      final InsertionIdData insertionId = entry.getValue();
      final InsertionIdQuery constraint =
          new InsertionIdQuery(insertionId.partitionKey, insertionId.sortKey, insertionId.dataId);
      final Query<T> query =
          (Query) QueryBuilder.newBuilder().indexName(index.getName()).addTypeName(
              adapter.getTypeName()).constraints(constraint).build();

      // we don't want the duplicates to try to delete one another
      // recursively over and over so we pass false for this deletion
      dataStore.delete(query, false);
    }
  }

  @Override
  public void entryDeleted(final T entry, final GeoWaveRow... rows) {
    closed = false;
    for (final GeoWaveRow row : rows) {
      final int rowDups = row.getNumberOfDuplicates();
      if (rowDups > 0) {
        final InsertionIds ids = DataStoreUtils.getInsertionIdsForEntry(entry, adapter, index);
        // keep track of the original deleted rows
        origRows.add(new InsertionIdData(row.getPartitionKey(), row.getSortKey(), row.getDataId()));
        for (final SinglePartitionInsertionIds insertId : ids.getPartitionKeys()) {
          final ByteArray partitionKeyObj = new ByteArray(insertId.getPartitionKey());
          if (!Arrays.equals(insertId.getPartitionKey(), row.getPartitionKey())) {
            for (final byte[] sortKey : insertId.getSortKeys()) {
              final InsertionIdData insertionId =
                  new InsertionIdData(insertId.getPartitionKey(), sortKey, row.getDataId());
              if (!duplicateInsertionIds.containsKey(partitionKeyObj)) {
                duplicateInsertionIds.put(partitionKeyObj, insertionId);
              }
            }
          }
        }
      }
    }
  }

  private class InsertionIdData {
    public final byte[] partitionKey;
    public final byte[] sortKey;
    public final byte[] dataId;

    public InsertionIdData(final byte[] partitionKey, final byte[] sortKey, final byte[] dataId) {
      this.partitionKey = partitionKey;
      this.sortKey = sortKey;
      this.dataId = dataId;
    }
  }
}
