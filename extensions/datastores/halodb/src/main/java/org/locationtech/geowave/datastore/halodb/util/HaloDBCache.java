/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.halodb.util;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Triple;
import org.locationtech.geowave.datastore.halodb.HaloDBOptions;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class HaloDBCache {
  private static HaloDBCache singletonInstance;

  public static synchronized HaloDBCache getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new HaloDBCache();
    }
    return singletonInstance;
  }

  private final LoadingCache<Triple<HaloDBOptions, String, Boolean>, HaloDBClient> clientCache =
      Caffeine.newBuilder().build(constructorArgs -> {
        return new HaloDBClient(
            constructorArgs.getLeft(),
            constructorArgs.getMiddle(),
            constructorArgs.getRight());
      });

  protected HaloDBCache() {}

  public HaloDBClient getClient(
      final HaloDBOptions options,
      final String namespace,
      final boolean visibilityEnabled) {
    return clientCache.get(Triple.of(options, namespace, visibilityEnabled));
  }

  public synchronized void close(final String namespace) throws IOException {
    final Set<Triple<HaloDBOptions, String, Boolean>> matchingKeys =
        clientCache.asMap().entrySet().stream().filter(
            e -> e.getKey().getMiddle().equals(namespace)).map(e -> e.getKey()).collect(
                Collectors.toSet());
    for (final Triple<HaloDBOptions, String, Boolean> key : matchingKeys) {
      final HaloDBClient client = clientCache.getIfPresent(key);
      if (client != null) {
        clientCache.invalidate(key);
        client.close();
      }
    }
  }

  public synchronized void closeAll() throws IOException {
    for (final HaloDBClient c : clientCache.asMap().values()) {
      c.close();
    }
    clientCache.invalidateAll();
  }
}
