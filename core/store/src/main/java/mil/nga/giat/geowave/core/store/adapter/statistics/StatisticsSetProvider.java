/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

/**
 * This interface defines a set of statistics types where each also represents a
 * set of individual statistics
 * 
 * @param <T>
 *            The set of statistics
 */
public interface StatisticsSetProvider<T>
{
	public ByteArrayId[] getSupportedStatisticsSetTypes();

	public DataStatisticsSet<T> createDataStatisticsSets(
			ByteArrayId statisticsSetId );

	public EntryVisibilityHandler<T> getVisibilityHandler(
			CommonIndexModel indexModel,
			DataAdapter<T> adapter,
			ByteArrayId statisticsSetId );
}
