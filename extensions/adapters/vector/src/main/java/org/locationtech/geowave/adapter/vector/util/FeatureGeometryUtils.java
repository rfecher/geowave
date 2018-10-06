/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.adapter.vector.util;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.ows.bindings.UnitBinding;
import org.geotools.referencing.GeodeticCalculator;
import org.locationtech.geowave.core.geotime.store.statistics.FeatureBoundingBoxStatistics;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.cs.CoordinateSystemAxis;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.uzaygezen.core.BitSetMath;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class FeatureGeometryUtils
{

	public static Envelope getGeoBounds(
			final DataStorePluginOptions dataStorePlugin,
			final String typeName,
			final String geomField ) {
		final DataStatisticsStore statisticsStore = dataStorePlugin.createDataStatisticsStore();
		final ByteArrayId geoStatId = FeatureBoundingBoxStatistics.composeId(geomField);
		final InternalAdapterStore internalAdapterStore = dataStorePlugin.createInternalAdapterStore();
		short adapterId = internalAdapterStore.getAdapterId(typeName);
		final InternalDataStatistics<?,?,?> geoStat = statisticsStore.getDataStatistics(
				adapterId,
				geoStatId,
				null);

		if (geoStat != null) {
			if (geoStat instanceof FeatureBoundingBoxStatistics) {
				final FeatureBoundingBoxStatistics bbStats = (FeatureBoundingBoxStatistics) geoStat;
				return new Envelope(
						bbStats.getMinX(),
						bbStats.getMaxX(),
						bbStats.getMinY(),
						bbStats.getMaxY());
			}
		}

		return null;
	}


}
