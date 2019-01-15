/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.redisson.api.RBatch;
import org.redisson.api.RMap;
import org.redisson.api.RMapAsync;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;

public class RedisMapWrapper
    extends AbstractRedisSetWrapper<RMapAsync<byte[], byte[]>, RMap<byte[], byte[]>> {

  public RedisMapWrapper(final RedissonClient client, final String setName, final Codec codec) {
    super(client, setName, codec);
  }

  public boolean remove(final byte[] dataId) {
    return getCurrentSyncCollection().remove(dataId) != null;
  }

  public void add(final byte[] dataId, final byte[] value) {
    preAdd();
    getCurrentAsyncCollection().putAsync(dataId, value);
  }


  public Iterator<GeoWaveRow> getRows(final byte[][] dataIds, final short adapterId) {
    final Map<byte[], byte[]> results =
        getCurrentSyncCollection().getAll(new HashSet<>(Arrays.asList(dataIds)));
    return Arrays.stream(dataIds).map(
        dataId -> BaseDataStoreUtils.getDataIndexRow(
            dataId,
            adapterId,
            results.get(dataId))).iterator();
  }

  @Override
  protected RMapAsync<byte[], byte[]> initAsyncCollection(
      final RBatch batch,
      final String setName,
      final Codec codec) {
    return batch.getMap(setName, codec);
  }

  @Override
  protected RMap<byte[], byte[]> initSyncCollection(
      final RedissonClient client,
      final String setName,
      final Codec codec) {
    return client.getMap(setName, codec);
  }
}
