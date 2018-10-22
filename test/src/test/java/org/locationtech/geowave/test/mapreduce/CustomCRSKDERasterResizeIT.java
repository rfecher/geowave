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
package org.locationtech.geowave.test.mapreduce;

import java.awt.Color;
import java.awt.Rectangle;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import javax.media.jai.Interpolation;

import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.referencing.CRS;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.raster.operations.ResizeCommand;
import org.locationtech.geowave.adapter.raster.plugin.GeoWaveRasterConfig;
import org.locationtech.geowave.adapter.raster.plugin.GeoWaveRasterReader;
import org.locationtech.geowave.adapter.raster.util.ZipUtils;
import org.locationtech.geowave.analytic.mapreduce.operations.KdeCommand;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Statistics;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;
import org.locationtech.geowave.core.store.cli.config.AddIndexCommand;
import org.locationtech.geowave.core.store.cli.config.AddStoreCommand;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.IndexPluginOptions;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.NamespaceOverride;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.mapreduce.MapReduceTestEnvironment;
import org.locationtech.geowave.test.mapreduce.MapReduceTestUtils;
import org.opengis.coverage.grid.GridCoverage;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.MAP_REDUCE
})
@GeoWaveTestStore({
	GeoWaveStoreType.ACCUMULO,
	GeoWaveStoreType.BIGTABLE,
	GeoWaveStoreType.HBASE
})
public class CustomCRSKDERasterResizeIT
{
	private static final String TEST_COVERAGE_NAME_PREFIX = "TEST_COVERAGE";
	private static final String TEST_RESIZE_COVERAGE_NAME_PREFIX = "TEST_RESIZE";
	private static final String TEST_COVERAGE_NAMESPACE = "mil_nga_giat_geowave_test_coverage";
	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TestUtils.TEST_RESOURCE_PACKAGE + "kde-testdata.zip";
	protected static final String KDE_INPUT_DIR = TestUtils.TEST_CASE_BASE + "kde_test_case/";
	private static final String KDE_SHAPEFILE_FILE = KDE_INPUT_DIR + "kde-test.shp";
	private static final double TARGET_MIN_LON = 155;
	private static final double TARGET_MIN_LAT = 16;
	private static final double TARGET_DECIMAL_DEGREES_SIZE = 0.132;
	private static final String KDE_FEATURE_TYPE_NAME = "kde-test";
	private static final int MIN_TILE_SIZE_POWER_OF_2 = 0;
	private static final int MAX_TILE_SIZE_POWER_OF_2 = 4;
	private static final int INCREMENT = 4;
	private static final int BASE_MIN_LEVEL = 15;
	private static final int BASE_MAX_LEVEL = 17;

	@NamespaceOverride(TEST_COVERAGE_NAMESPACE)
	protected DataStorePluginOptions outputDataStorePluginOptions;

	protected DataStorePluginOptions inputDataStorePluginOptions;

	private final static Logger LOGGER = LoggerFactory.getLogger(CustomCRSKDERasterResizeIT.class);
	private static long startMillis;

	@BeforeClass
	public static void extractTestFiles()
			throws URISyntaxException {
		ZipUtils.unZipFile(
				new File(
						CustomCRSKDERasterResizeIT.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TestUtils.TEST_CASE_BASE);
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-------------------------------------------------");
		LOGGER.warn("*                                               *");
		LOGGER.warn("*         RUNNING CustomCRSKDERasterResizeIT    *");
		LOGGER.warn("*                                               *");
		LOGGER.warn("-------------------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("------------------------------------------------");
		LOGGER.warn("*                                              *");
		LOGGER.warn("*      FINISHED CustomCRSKDERasterResizeIT     *");
		LOGGER.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
				+ "s elapsed.                               *");
		LOGGER.warn("*                                              *");
		LOGGER.warn("------------------------------------------------");
	}

	@After
	public void clean()
			throws IOException {
		TestUtils.deleteAll(inputDataStorePluginOptions);
		TestUtils.deleteAll(outputDataStorePluginOptions);
	}

	@Test
	public void testKDEAndRasterResize()
			throws Exception {
		TestUtils.deleteAll(inputDataStorePluginOptions);
		TestUtils.testLocalIngest(
				inputDataStorePluginOptions,
				DimensionalityType.SPATIAL,
				"EPSG:4901",
				KDE_SHAPEFILE_FILE,
				"geotools-vector",
				1);

		File configFile = File.createTempFile(
				"test_export",
				null);
		ManualOperationParams params = new ManualOperationParams();

		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);
		AddStoreCommand addStore = new AddStoreCommand();
		addStore.setParameters("test-in");
		addStore.setPluginOptions(inputDataStorePluginOptions);
		addStore.execute(params);
		addStore.setParameters("raster-spatial");
		addStore.setPluginOptions(outputDataStorePluginOptions);
		addStore.execute(params);

		String outputIndex = "raster-spatial";
		final IndexPluginOptions outputIndexOptions = new IndexPluginOptions();
		outputIndexOptions.selectPlugin("spatial");
		((SpatialOptions) outputIndexOptions.getDimensionalityOptions()).setCrs("EPSG:4240");

		AddIndexCommand addIndex = new AddIndexCommand();
		addIndex.setParameters("raster-spatial");
		addIndex.setPluginOptions(outputIndexOptions);
		addIndex.execute(params);
		// use the min level to define the request boundary because it is the
		// most coarse grain
		final double decimalDegreesPerCellMinLevel = 180.0 / Math.pow(
				2,
				BASE_MIN_LEVEL);
		final double cellOriginXMinLevel = Math.round(TARGET_MIN_LON / decimalDegreesPerCellMinLevel);
		final double cellOriginYMinLevel = Math.round(TARGET_MIN_LAT / decimalDegreesPerCellMinLevel);
		final double numCellsMinLevel = Math.round(TARGET_DECIMAL_DEGREES_SIZE / decimalDegreesPerCellMinLevel);
		final GeneralEnvelope queryEnvelope = new GeneralEnvelope(
				new double[] {
					// this is exactly on a tile boundary, so there will be no
					// scaling on the tile composition/rendering
					decimalDegreesPerCellMinLevel * cellOriginXMinLevel,
					decimalDegreesPerCellMinLevel * cellOriginYMinLevel
				},
				new double[] {
					// these values are also on a tile boundary, to avoid
					// scaling
					decimalDegreesPerCellMinLevel * (cellOriginXMinLevel + numCellsMinLevel),
					decimalDegreesPerCellMinLevel * (cellOriginYMinLevel + numCellsMinLevel)
				});

		final MapReduceTestEnvironment env = MapReduceTestEnvironment.getInstance();
		for (int i = MIN_TILE_SIZE_POWER_OF_2; i <= MAX_TILE_SIZE_POWER_OF_2; i += INCREMENT) {
			final String tileSizeCoverageName = TEST_COVERAGE_NAME_PREFIX + i;

			final KdeCommand command = new KdeCommand();

			// We're going to override these anyway.
			command.setParameters(
					"test-in",
					"raster-spatial");

			command.getKdeOptions().setOutputIndex(
					outputIndex);
			command.getKdeOptions().setFeatureType(
					KDE_FEATURE_TYPE_NAME);
			command.getKdeOptions().setMinLevel(
					BASE_MIN_LEVEL);
			command.getKdeOptions().setMaxLevel(
					BASE_MAX_LEVEL);
			command.getKdeOptions().setMinSplits(
					MapReduceTestUtils.MIN_INPUT_SPLITS);
			command.getKdeOptions().setMaxSplits(
					MapReduceTestUtils.MAX_INPUT_SPLITS);
			command.getKdeOptions().setCoverageName(
					tileSizeCoverageName);
			command.getKdeOptions().setHdfsHostPort(
					env.getHdfs());
			command.getKdeOptions().setJobTrackerOrResourceManHostPort(
					env.getJobtracker());
			command.getKdeOptions().setTileSize(
					(int) Math.pow(
							2,
							i));
			command.setOutputIndexOptions(Collections.singletonList(outputIndexOptions));

			ToolRunner.run(
					command.createRunner(params),
					new String[] {});
		}

		final int numLevels = (BASE_MAX_LEVEL - BASE_MIN_LEVEL) + 1;
		final double[][][][] initialSampleValuesPerRequestSize = new double[numLevels][][][];
		try(CloseableIterator<Object> objIt = outputDataStorePluginOptions.createDataStore().query(QueryBuilder.newBuilder().build())) {
			int c = 0;
			while (objIt.hasNext()) {
				objIt.next();
				c++;
			}
			System.err.println("number of rows: " + c);
			Statistics<?> [] s=outputDataStorePluginOptions.createDataStore().queryStatistics(StatisticsQueryBuilder.newBuilder().build());
			for (Statistics<?> a : s) {
				System.err.println("data type: "+a.getDataTypeName() + ";  type: "+a.getType() +"; id:" + a.getId() + "; result:" + a.getResult()); 
			}
		}
		for (int l = 0; l < numLevels; l++) {
			initialSampleValuesPerRequestSize[l] = testSamplesMatch(
					TEST_COVERAGE_NAME_PREFIX,
					((MAX_TILE_SIZE_POWER_OF_2 - MIN_TILE_SIZE_POWER_OF_2) / INCREMENT) + 1,
					queryEnvelope,
					new Rectangle(
							(int) (numCellsMinLevel * Math.pow(
									2,
									l)),
							(int) (numCellsMinLevel * Math.pow(
									2,
									l))),
					null);
		}

		for (int i = MIN_TILE_SIZE_POWER_OF_2; i <= MAX_TILE_SIZE_POWER_OF_2; i += INCREMENT) {
			final String originalTileSizeCoverageName = TEST_COVERAGE_NAME_PREFIX + i;
			final String resizeTileSizeCoverageName = TEST_RESIZE_COVERAGE_NAME_PREFIX + i;

			final ResizeCommand command = new ResizeCommand();

			// We're going to override these anyway.
			command.setParameters(
					"raster-spatial",
					"raster-spatial");

			command.getOptions().setInputCoverageName(
					originalTileSizeCoverageName);
			command.getOptions().setMinSplits(
					MapReduceTestUtils.MIN_INPUT_SPLITS);
			command.getOptions().setMaxSplits(
					MapReduceTestUtils.MAX_INPUT_SPLITS);
			command.getOptions().setHdfsHostPort(
					env.getHdfs());
			command.getOptions().setJobTrackerOrResourceManHostPort(
					env.getJobtracker());
			command.getOptions().setOutputCoverageName(
					resizeTileSizeCoverageName);
			command.getOptions().setIndexName(
					TestUtils.createCustomCRSPrimaryIndex().getName());

			// due to time considerations when running the test, downsample to
			// at most 2 powers of 2 lower
			int targetRes = (MAX_TILE_SIZE_POWER_OF_2 - i);
			if ((i - targetRes) > 2) {
				targetRes = i - 2;
			}
			command.getOptions().setOutputTileSize(
					(int) Math.pow(
							2,
							targetRes));

			ToolRunner.run(
					command.createRunner(params),
					new String[] {});
		}

		for (int l = 0; l < numLevels; l++) {
			testSamplesMatch(
					TEST_RESIZE_COVERAGE_NAME_PREFIX,
					((MAX_TILE_SIZE_POWER_OF_2 - MIN_TILE_SIZE_POWER_OF_2) / INCREMENT) + 1,
					queryEnvelope,
					new Rectangle(
							(int) (numCellsMinLevel * Math.pow(
									2,
									l)),
							(int) (numCellsMinLevel * Math.pow(
									2,
									l))),
					initialSampleValuesPerRequestSize[l]);
		}
	}

	private double[][][] testSamplesMatch(
			final String coverageNamePrefix,
			final int numCoverages,
			final GeneralEnvelope queryEnvelope,
			final Rectangle pixelDimensions,
			double[][][] expectedResults )
			throws Exception {
		final StringBuilder str = new StringBuilder(
				StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION).append(
				"=").append(
				TEST_COVERAGE_NAMESPACE).append(
				";equalizeHistogramOverride=false;scaleTo8Bit=false;interpolationOverride=").append(
				Interpolation.INTERP_NEAREST);

		str.append(
				";").append(
				GeoWaveStoreFinder.STORE_HINT_KEY).append(
				"=").append(
				outputDataStorePluginOptions.getType());

		final Map<String, String> options = outputDataStorePluginOptions.getOptionsAsMap();

		for (final Entry<String, String> entry : options.entrySet()) {
			if (!entry.getKey().equals(
					StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION)) {
				str.append(
						";").append(
						entry.getKey()).append(
						"=").append(
						entry.getValue());
			}
		}

		final GeoWaveRasterReader reader = new GeoWaveRasterReader(
				GeoWaveRasterConfig.readFromConfigParams(str.toString()));

		queryEnvelope.setCoordinateReferenceSystem(CRS.decode(
				"EPSG:4166",
				true));
		final Raster[] rasters = new Raster[numCoverages];
		int coverageCount = 0;
		for (int i = MIN_TILE_SIZE_POWER_OF_2; i <= MAX_TILE_SIZE_POWER_OF_2; i += INCREMENT) {
			final String tileSizeCoverageName = coverageNamePrefix + i;
			final GridCoverage gridCoverage = reader.renderGridCoverage(
					tileSizeCoverageName,
					pixelDimensions,
					queryEnvelope,
					Color.BLACK,
					null,
					null);
			final RenderedImage image = gridCoverage.getRenderedImage();
			final Raster raster = image.getData();
			rasters[coverageCount++] = raster;
		}
		boolean atLeastOneResult = expectedResults != null;
		for (int i = 0; i < numCoverages; i++) {
			final boolean initialResults = expectedResults == null;
			if (initialResults) {
				expectedResults = new double[rasters[i].getWidth()][rasters[i].getHeight()][rasters[i].getNumBands()];
			}
			else {
				Assert.assertEquals(
						"The expected width does not match the expected width for the coverage " + i,
						expectedResults.length,
						rasters[i].getWidth());
				Assert.assertEquals(
						"The expected height does not match the expected height for the coverage " + i,
						expectedResults[0].length,
						rasters[i].getHeight());
				Assert.assertEquals(
						"The expected number of bands does not match the expected bands for the coverage " + i,
						expectedResults[0][0].length,
						rasters[i].getNumBands());
			}
			for (int x = 0; x < rasters[i].getWidth(); x++) {
				for (int y = 0; y < rasters[i].getHeight(); y++) {
					for (int b = 0; b < rasters[i].getNumBands(); b++) {
						final double sample = rasters[i].getSampleDouble(
								x,
								y,
								b);
						if (initialResults) {
							expectedResults[x][y][b] = sample;
							if (!atLeastOneResult && sample !=0) {
								atLeastOneResult = true;
							}
						}
						else {
							Assert.assertEquals(
									"The sample does not match the expected sample value for the coverage " + i
											+ " at x=" + x + ",y=" + y + ",b=" + b,
									new Double(
											expectedResults[x][y][b]),
									new Double(
											sample));
						}
					}
				}
			}
		}
		Assert.assertTrue("There should be at least one value that is not black", atLeastOneResult);
		return expectedResults;
	}
}
