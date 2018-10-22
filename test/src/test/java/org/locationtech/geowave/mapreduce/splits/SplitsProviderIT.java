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
package org.locationtech.geowave.mapreduce.splits;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.FilterByTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.QuerySingleIndex;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.mapreduce.MapReduceMemoryDataStore;
import org.locationtech.geowave.mapreduce.MapReduceMemoryOperations;
import org.locationtech.geowave.service.rest.GeoWaveOperationServiceWrapper;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveIT;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.MAP_REDUCE
})
public class SplitsProviderIT extends
		AbstractGeoWaveIT
{

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE,
		GeoWaveStoreType.DYNAMODB,
		GeoWaveStoreType.CASSANDRA
	})
	protected DataStorePluginOptions dataStorePluginOptions;

	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveOperationServiceWrapper.class);
	private static long startMillis;
	private final static String testName = "SplitsProviderIT";

	private static MapReduceMemoryOperations mapReduceMemoryOps;
	private static DataStoreInfo uniformDataStore;
	private static DataStoreInfo bimodalDataStore;
	private static DataStoreInfo skewedDataStore;

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStorePluginOptions;
	}

	enum Distribution {
		UNIFORM,
		BIMODAL,
		SKEWED
	}

	private static class DataStoreInfo
	{
		final public MapReduceMemoryDataStore mapReduceMemoryDataStore;
		final public Index index;
		final public GeotoolsFeatureDataAdapter adapter;

		public DataStoreInfo(
				final MapReduceMemoryDataStore mapReduceMemoryDataStore,
				final Index index,
				final GeotoolsFeatureDataAdapter adapter ) {
			this.mapReduceMemoryDataStore = mapReduceMemoryDataStore;
			this.index = index;
			this.adapter = adapter;
		}
	}

	@BeforeClass
	public static void setup() {
		startMillis = System.currentTimeMillis();
		TestUtils.printStartOfTest(
				LOGGER,
				testName);

		mapReduceMemoryOps = new MapReduceMemoryOperations();
		uniformDataStore = createDataStore(Distribution.UNIFORM);
		bimodalDataStore = createDataStore(Distribution.BIMODAL);
		skewedDataStore = createDataStore(Distribution.SKEWED);
	}

	@AfterClass
	public static void reportTest() {
		TestUtils.printEndOfTest(
				LOGGER,
				testName,
				startMillis);
	}

	@Test
	public void testUniform() {
		final QueryConstraints query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-180,
						180,
						-90,
						90)));
		assertTrue(getSplitsMSE(
				uniformDataStore,
				query,
				12,
				12) < 0.1);
	}

	@Test
	public void testBimodal() {
		QueryConstraints query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-180,
						180,
						-90,
						90)));
		assertTrue(getSplitsMSE(
				bimodalDataStore,
				query,
				12,
				12) < 0.1);

		query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-120,
						-60,
						-90,
						90)));
		assertTrue(getSplitsMSE(
				bimodalDataStore,
				query,
				12,
				12) < 0.1);

		query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-20,
						20,
						-90,
						90)));
		assertTrue(getSplitsMSE(
				bimodalDataStore,
				query,
				12,
				12) < 0.1);
	}

	@Test
	public void testSkewed() {
		QueryConstraints query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-180,
						180,
						-90,
						90)));
		assertTrue(getSplitsMSE(
				skewedDataStore,
				query,
				12,
				12) < 0.1);

		query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-180,
						-140,
						-90,
						90)));
		assertTrue(getSplitsMSE(
				skewedDataStore,
				query,
				12,
				12) < 0.1);

		query = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						0,
						180,
						-90,
						90)));
		assertTrue(getSplitsMSE(
				skewedDataStore,
				query,
				12,
				12) < 0.1);
	}

	private static DataStoreInfo createDataStore(
			final Distribution distr ) {

		final MapReduceMemoryDataStore dataStore = new MapReduceMemoryDataStore(
				mapReduceMemoryOps);
		final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
		final Index idx = SimpleIngest.createSpatialIndex();
		final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);
		dataStore.addType(fda, idx);
		try (final Writer<SimpleFeature> writer = dataStore.createWriter(fda.getTypeName())) {

			switch (distr) {
				case UNIFORM:
					createUniformFeatures(
							new SimpleFeatureBuilder(
									sft),
							writer,
							100000);
					break;
				case BIMODAL:
					createBimodalFeatures(
							new SimpleFeatureBuilder(
									sft),
							writer,
							400000);
					break;
				case SKEWED:
					createSkewedFeatures(
							new SimpleFeatureBuilder(
									sft),
							writer,
							700000);
					break;
				default:
					LOGGER.error("Invalid Distribution");
					throw new Exception();
			}
		}
		catch (final MismatchedIndexToAdapterMapping e) {
			LOGGER.error(
					"MismathcedIndexToAdapterMapping exception thrown when creating data store writer",
					e);
		}
		catch (final IOException e) {
			LOGGER.error(
					"IOException thrown when creating data store writer",
					e);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception thrown when creating data store writer",
					e);
		}

		return new DataStoreInfo(
				dataStore,
				idx,
				fda);
	}

	private double getSplitsMSE(
			final DataStoreInfo dataStoreInfo,
			final QueryConstraints query,
			final int minSplits,
			final int maxSplits ) {

		// get splits and create reader for each RangeLocationPair, then summing
		// up the rows for each split

		List<InputSplit> splits = null;
		try {
			splits = dataStoreInfo.mapReduceMemoryDataStore.getSplits(
					null,
					new FilterByTypeQueryOptions<>(
							new String[] {
								dataStoreInfo.adapter.getTypeName()
							}),
					new QuerySingleIndex(
							dataStoreInfo.index.getName()),
					null,
					null,
					null,
					null,
					null,
					null,
					null,
					minSplits,
					maxSplits);
		}
		catch (final IOException e) {
			LOGGER.error(
					"IOException thrown when calling getSplits",
					e);
		}
		catch (final InterruptedException e) {
			LOGGER.error(
					"InterruptedException thrown when calling getSplits",
					e);
		}

		final double[] observed = new double[splits.size()];

		int totalCount = 0;
		int currentSplit = 0;

		for (final InputSplit split : splits) {
			int countPerSplit = 0;
			if (GeoWaveInputSplit.class.isAssignableFrom(split.getClass())) {
				final GeoWaveInputSplit gwSplit = (GeoWaveInputSplit) split;
				for (final String indexName : gwSplit.getIndexNames()) {
					final SplitInfo splitInfo = gwSplit.getInfo(indexName);
					for (final RangeLocationPair p : splitInfo.getRangeLocationPairs()) {
						final RecordReaderParams<?> readerParams = new RecordReaderParams(
								splitInfo.getIndex(),
								dataStoreInfo.mapReduceMemoryDataStore.getAdapterStore(),
								dataStoreInfo.mapReduceMemoryDataStore.getInternalAdapterStore(),
								new short[] {
									dataStorePluginOptions.createInternalAdapterStore().getAdapterId(
											dataStoreInfo.adapter.getTypeName())
								},
								null,
								null,
								null,
								splitInfo.isMixedVisibility(),
								splitInfo.isAuthorizationsLimiting(),
								p.getRange(),
								null,
								null,
								GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
								null);
						try (RowReader<?> reader = mapReduceMemoryOps.createReader(readerParams)) {
							while (reader.hasNext()) {
								reader.next();
								countPerSplit++;
							}
						}
						catch (final Exception e) {
							LOGGER.error(
									"Exception thrown when calling createReader",
									e);
						}
					}
				}
			}
			totalCount += countPerSplit;
			observed[currentSplit] = countPerSplit;
			currentSplit++;
		}

		final double expected = 1.0 / splits.size();

		double sum = 0;

		for (int i = 0; i < observed.length; i++) {
			sum += Math.pow(
					(observed[i] / totalCount) - expected,
					2);
		}

		return sum / splits.size();
	}

	public static void createUniformFeatures(
			final SimpleFeatureBuilder pointBuilder,
			final Writer<SimpleFeature> writer,
			final int firstFeatureId ) {

		int featureId = firstFeatureId;
		for (int longitude = -180; longitude <= 180; longitude += 1) {
			for (int latitude = -90; latitude <= 90; latitude += 1) {
				pointBuilder.set(
						"geometry",
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								longitude,
								latitude)));
				pointBuilder.set(
						"TimeStamp",
						new Date());
				pointBuilder.set(
						"Latitude",
						latitude);
				pointBuilder.set(
						"Longitude",
						longitude);
				// Note since trajectoryID and comment are marked as nillable we
				// don't need to set them (they default ot null).

				final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
				writer.write(sft);
				featureId++;
			}
		}
	}

	public static void createBimodalFeatures(
			final SimpleFeatureBuilder pointBuilder,
			final Writer<SimpleFeature> writer,
			final int firstFeatureId ) {

		int featureId = firstFeatureId;
		for (double longitude = -180.0; longitude <= 0.0; longitude += 1.0) {
			if (longitude == -90) {
				continue;
			}
			for (double latitude = -180.0; latitude <= 0.0; latitude += (Math.abs(-90.0 - longitude) / 10.0)) {
				pointBuilder.set(
						"geometry",
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								longitude,
								latitude)));
				pointBuilder.set(
						"TimeStamp",
						new Date());
				pointBuilder.set(
						"Latitude",
						latitude);
				pointBuilder.set(
						"Longitude",
						longitude);
				// Note since trajectoryID and comment are marked as nillable we
				// don't need to set them (they default ot null).

				final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
				writer.write(sft);
				featureId++;
			}
		}

		for (double longitude = 0.0; longitude <= 180.0; longitude += 1.0) {
			if (longitude == 90) {
				continue;
			}
			for (double latitude = 0.0; latitude <= 180.0; latitude += (Math.abs(90.0 - longitude) / 10.0)) {
				pointBuilder.set(
						"geometry",
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								longitude,
								latitude)));
				pointBuilder.set(
						"TimeStamp",
						new Date());
				pointBuilder.set(
						"Latitude",
						latitude);
				pointBuilder.set(
						"Longitude",
						longitude);
				// Note since trajectoryID and comment are marked as nillable we
				// don't need to set them (they default ot null).

				final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
				writer.write(sft);
				featureId++;
			}
		}
	}

	public static void createSkewedFeatures(
			final SimpleFeatureBuilder pointBuilder,
			final Writer<SimpleFeature> writer,
			final int firstFeatureId ) {

		int featureId = firstFeatureId;
		for (double longitude = -180.0; longitude <= 180.0; longitude += 1.0) {
			for (double latitude = -90.0; latitude <= 90.0; latitude += ((longitude + 181.0) / 10.0)) {
				pointBuilder.set(
						"geometry",
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								longitude,
								latitude)));
				pointBuilder.set(
						"TimeStamp",
						new Date());
				pointBuilder.set(
						"Latitude",
						latitude);
				pointBuilder.set(
						"Longitude",
						longitude);
				// Note since trajectoryID and comment are marked as nillable we
				// don't need to set them (they default ot null).

				final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
				writer.write(sft);
				featureId++;
			}
		}
	}
}
