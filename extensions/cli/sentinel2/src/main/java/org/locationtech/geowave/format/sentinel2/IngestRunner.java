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
package org.locationtech.geowave.format.sentinel2;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.IndexLoader;
import org.locationtech.geowave.core.store.cli.remote.options.IndexPluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;

public class IngestRunner extends
		RasterIngestRunner
{
	private final static Logger LOGGER = LoggerFactory.getLogger(IngestRunner.class);

	private Writer<SimpleFeature> bandWriter;
	private Writer<SimpleFeature> sceneWriter;
	private final VectorOverrideCommandLineOptions vectorOverrideOptions;
	private SimpleFeatureType sceneType;

	public IngestRunner(
			final Sentinel2BasicCommandLineOptions analyzeOptions,
			final Sentinel2DownloadCommandLineOptions downloadOptions,
			final Sentinel2RasterIngestCommandLineOptions ingestOptions,
			final VectorOverrideCommandLineOptions vectorOverrideOptions,
			final List<String> parameters ) {
		super(
				analyzeOptions,
				downloadOptions,
				ingestOptions,
				parameters);
		this.vectorOverrideOptions = vectorOverrideOptions;
	}

	@Override
	protected void processParameters(
			final OperationParams params )
			throws Exception { // Ensure we have all the required arguments
		super.processParameters(params);

		final DataStore vectorStore;
		final Index[] vectorIndices;

		// Config file
		final File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		if ((vectorOverrideOptions.getVectorStore() != null)
				&& !vectorOverrideOptions.getVectorStore().trim().isEmpty()) {
			final String vectorStoreName = vectorOverrideOptions.getVectorStore();

			final StoreLoader vectorStoreLoader = new StoreLoader(
					vectorStoreName);
			if (!vectorStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find vector store name: " + vectorStoreLoader.getStoreName());
			}

			final DataStorePluginOptions vectorStoreOptions = vectorStoreLoader.getDataStorePlugin();
			vectorStore = vectorStoreOptions.createDataStore();
		}
		else {
			vectorStore = store;
		}

		if ((vectorOverrideOptions.getVectorIndex() != null)
				&& !vectorOverrideOptions.getVectorIndex().trim().isEmpty()) {
			final String vectorIndexList = vectorOverrideOptions.getVectorIndex();

			// Load the Indices
			final IndexLoader indexLoader = new IndexLoader(
					vectorIndexList);
			if (!indexLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find index(s) by name: " + vectorIndexList);
			}

			final List<IndexPluginOptions> indexOptions = indexLoader.getLoadedIndexes();
			vectorIndices = new Index[indexOptions.size()];
			int i = 0;
			for (final IndexPluginOptions dimensionType : indexOptions) {
				final Index primaryIndex = dimensionType.createIndex();
				if (primaryIndex == null) {
					LOGGER.error("Could not get index instance, getIndex() returned null;");
					throw new IOException(
							"Could not get index instance, getIndex() returned null");
				}
				vectorIndices[i++] = primaryIndex;
			}
		}
		else {
			vectorIndices = indices;
		}

		sceneType = provider.sceneFeatureTypeBuilder().buildFeatureType();
		final FeatureDataAdapter sceneAdapter = new FeatureDataAdapter(
				sceneType);
		vectorStore.addType(sceneAdapter);
		vectorStore.addIndex(
				sceneAdapter.getTypeName(),
				vectorIndices);
		sceneWriter = vectorStore.createWriter(sceneAdapter.getTypeName());

		final SimpleFeatureType bandType = provider.bandFeatureTypeBuilder().buildFeatureType();
		final FeatureDataAdapter bandAdapter = new FeatureDataAdapter(
				bandType);

		vectorStore.addType(bandAdapter);
		vectorStore.addIndex(
				bandAdapter.getTypeName(),
				vectorIndices);
		bandWriter = vectorStore.createWriter(bandAdapter.getTypeName());
	}

	@Override
	protected void nextBand(
			final SimpleFeature band,
			final AnalysisInfo analysisInfo ) {
		bandWriter.write(band);
		super.nextBand(
				band,
				analysisInfo);
	}

	@Override
	protected void nextScene(
			final SimpleFeature firstBandOfScene,
			final AnalysisInfo analysisInfo ) {
		VectorIngestRunner.writeScene(
				sceneType,
				firstBandOfScene,
				sceneWriter);
		super.nextScene(
				firstBandOfScene,
				analysisInfo);
	}

	@Override
	protected void runInternal(
			final OperationParams params )
			throws Exception {
		try {
			super.runInternal(params);
		}
		finally {
			if (sceneWriter != null) {
				sceneWriter.close();
			}
			if (bandWriter != null) {
				bandWriter.close();
			}
		}
	}
}
