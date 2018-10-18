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
package org.locationtech.geowave.format.landsat8;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.geotools.feature.simple.SimpleFeatureBuilder;
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
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;

public class VectorIngestRunner extends
		AnalyzeRunner
{

	private final static Logger LOGGER = LoggerFactory.getLogger(RasterIngestRunner.class);
	protected final List<String> parameters;
	private Writer bandWriter;
	private Writer sceneWriter;

	private SimpleFeatureType sceneType;

	public VectorIngestRunner(
			final Landsat8BasicCommandLineOptions analyzeOptions,
			final List<String> parameters ) {
		super(
				analyzeOptions);
		this.parameters = parameters;
	}

	@Override
	protected void runInternal(
			final OperationParams params )
			throws Exception {
		try {
			// Ensure we have all the required arguments
			if (parameters.size() != 2) {
				throw new ParameterException(
						"Requires arguments: <storename> <comma delimited index/group list>");
			}
			final String inputStoreName = parameters.get(0);
			final String indexList = parameters.get(1);

			// Config file
			final File configFile = (File) params.getContext().get(
					ConfigOptions.PROPERTIES_FILE_CONTEXT);

			// Attempt to load input store.
			final StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			final DataStorePluginOptions storeOptions = inputStoreLoader.getDataStorePlugin();
			final DataStore store = storeOptions.createDataStore();

			// Load the Indices
			final IndexLoader indexLoader = new IndexLoader(
					indexList);
			if (!indexLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find index(s) by name: " + indexList);
			}
			final List<IndexPluginOptions> indexOptions = indexLoader.getLoadedIndexes();

			final Index[] indices = new Index[indexOptions.size()];
			int i = 0;
			for (final IndexPluginOptions dimensionType : indexOptions) {
				final Index primaryIndex = dimensionType.createIndex();
				if (primaryIndex == null) {
					LOGGER.error("Could not get index instance, getIndex() returned null;");
					throw new IOException(
							"Could not get index instance, getIndex() returned null");
				}
				indices[i++] = primaryIndex;
			}
			sceneType = SceneFeatureIterator.createFeatureType();
			final FeatureDataAdapter sceneAdapter = new FeatureDataAdapter(
					sceneType);
			store.addType(sceneAdapter);
			store.addIndex(
					sceneAdapter.getTypeName(),
					indices);
			sceneWriter = store.createWriter(sceneAdapter.getTypeName());
			final SimpleFeatureType bandType = BandFeatureIterator.createFeatureType(sceneType);
			final FeatureDataAdapter bandAdapter = new FeatureDataAdapter(
					bandType);
			store.addType(bandAdapter);
			store.addIndex(
					bandAdapter.getTypeName(),
					indices);
			bandWriter = store.createWriter(bandAdapter.getTypeName());
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
		writeScene(
				sceneType,
				firstBandOfScene,
				sceneWriter);
		super.nextScene(
				firstBandOfScene,
				analysisInfo);
	}

	public static void writeScene(
			final SimpleFeatureType sceneType,
			final SimpleFeature firstBandOfScene,
			final Writer sceneWriter ) {
		final SimpleFeatureBuilder bldr = new SimpleFeatureBuilder(
				sceneType);
		String fid = null;
		for (int i = 0; i < sceneType.getAttributeCount(); i++) {
			final AttributeDescriptor attr = sceneType.getDescriptor(i);
			final String attrName = attr.getLocalName();
			final Object attrValue = firstBandOfScene.getAttribute(attrName);
			if (attrValue != null) {
				bldr.set(
						i,
						attrValue);
				if (attrName.equals(SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME)) {
					fid = attrValue.toString();
				}
			}
		}
		if (fid != null) {
			sceneWriter.write(bldr.buildFeature(fid));
		}
	}
}
