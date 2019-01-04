/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter.statistics;

public class PartitionStatisticsType<R>
    extends StatisticsType<R, PartitionStatisticsQueryBuilder<R>> {
  private static final long serialVersionUID = 1L;

  public PartitionStatisticsType() {
    super();
  }

  public PartitionStatisticsType(final byte[] id) {
    super(id);
  }

  public PartitionStatisticsType(final String id) {
    super(id);
  }

  @Override
  public PartitionStatisticsQueryBuilder<R> newBuilder() {
    return new PartitionStatisticsQueryBuilder<>(this);
  }
}
