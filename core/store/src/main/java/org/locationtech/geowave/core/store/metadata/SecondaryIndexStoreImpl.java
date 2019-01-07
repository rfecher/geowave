/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.metadata;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.index.SecondaryIndexDataStore;
import org.locationtech.geowave.core.store.index.SecondaryIndexImpl;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;

public class SecondaryIndexStoreImpl implements SecondaryIndexDataStore {

  @Override
  public void setDataStore(final DataStore dataStore) {
    // TODO Auto-generated method stub

  }

  @Override
  public <T> CloseableIterator<T> query(
      final SecondaryIndexImpl<T> secondaryIndex,
      final String indexedAttributeFieldName,
      final InternalDataAdapter<T> adapter,
      final Index primaryIndex,
      final QueryConstraints query,
      final String... authorizations) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void storeJoinEntry(
      String secondaryIndexName,
      byte[] indexedAttributeValue,
      String typeName,
      String indexedAttributeFieldName,
      String primaryIndexName,
      byte[] primaryIndexPartitionKey,
      byte[] primaryIndexSortKey,
      byte[] attributeVisibility) {
    // TODO Auto-generated method stub

  }

  @Override
  public void storeEntry(
      String secondaryIndexName,
      byte[] indexedAttributeValue,
      String typeName,
      String indexedAttributeFieldName,
      byte[] dataId,
      GeoWaveValue... originalFields) {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteJoinEntry(
      String secondaryIndexName,
      byte[] indexedAttributeValue,
      String typeName,
      String indexedAttributeFieldName,
      String primaryIndexName,
      byte[] primaryIndexPartitionKey,
      byte[] primaryIndexSortKey,
      byte[] attributeVisibility) {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteEntry(
      String secondaryIndexName,
      byte[] indexedAttributeValue,
      String typeName,
      String indexedAttributeFieldName,
      byte[] dataId,
      GeoWaveValue... originalFields) {
    // TODO Auto-generated method stub

  }

  @Override
  public void flush() {
    // TODO Auto-generated method stub

  }

  @Override
  public void removeAll() {
    // TODO Auto-generated method stub

  }
}
