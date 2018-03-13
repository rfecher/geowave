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
package mil.nga.giat.geowave.analytic.mapreduce.operations;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.analytic.mapreduce.kde.KDECommandLineOptions;
import mil.nga.giat.geowave.analytic.mapreduce.kde.KDEJobRunner;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.StoreLoader;

@GeowaveOperation(name = "kde", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "Kernel Density Estimate")
public class KdeCommand extends
		DefaultOperation implements
		Command
{
	private final static Logger LOGGER = LoggerFactory.getLogger(KdeCommand.class);
	@Parameter(description = "<input storename> <output storename>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private KDECommandLineOptions kdeOptions = new KDECommandLineOptions();

	private DataStorePluginOptions inputStoreOptions = null;

	private DataStorePluginOptions outputStoreOptions = null;

	private List<IndexPluginOptions> outputIndexOptions = null;

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		KDEJobRunner runner = createRunner(params);
		int status = runner.runJob();
		if (status != 0) {
			throw new RuntimeException(
					"Failed to execute: " + status);
		}
	}

	public KDEJobRunner createRunner(
			final OperationParams params )
			throws IOException {
		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <input storename> <output storename>");
		}

		String inputStore = parameters.get(0);
		String outputStore = parameters.get(1);
		// Config file
		File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		PrimaryIndex outputPrimaryIndex = null;

		// Attempt to load input store.
		if (inputStoreOptions == null) {
			StoreLoader inputStoreLoader = new StoreLoader(
					inputStore);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		// Attempt to load output store.
		if (outputStoreOptions == null) {
			StoreLoader outputStoreLoader = new StoreLoader(
					outputStore);
			if (!outputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + outputStoreLoader.getStoreName());
			}
			outputStoreOptions = outputStoreLoader.getDataStorePlugin();
		}
		if ((kdeOptions.getOutputIndex() != null) && !kdeOptions.getOutputIndex().trim().isEmpty()) {
			if (outputIndexOptions == null) {
				String outputIndex = kdeOptions.getOutputIndex();

				// Load the Indices
				final IndexLoader indexLoader = new IndexLoader(
						outputIndex);
				if (!indexLoader.loadFromConfig(configFile)) {
					throw new ParameterException(
							"Cannot find index(s) by name: " + outputIndex);
				}
				outputIndexOptions = indexLoader.getLoadedIndexes();
			}

			for (final IndexPluginOptions dimensionType : outputIndexOptions) {
				if (dimensionType.getType().equals(
						"spatial")) {
					final PrimaryIndex primaryIndex = dimensionType.createPrimaryIndex();
					if (primaryIndex == null) {
						LOGGER.error("Could not get index instance, getIndex() returned null;");
						throw new IOException(
								"Could not get index instance, getIndex() returned null");
					}
					outputPrimaryIndex = primaryIndex;
				}
				else {
					LOGGER
							.error("spatial temporal is not supported for output index. Only spatial index is supported.");
					throw new IOException(
							"spatial temporal is not supported for output index. Only spatial index is supported.");
				}
			}
		}

		final KDEJobRunner runner = new KDEJobRunner(
				kdeOptions,
				inputStoreOptions,
				outputStoreOptions,
				configFile,
				outputPrimaryIndex);
		return runner;
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String inputStore,
			String outputStore ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(inputStore);
		this.parameters.add(outputStore);
	}

	public KDECommandLineOptions getKdeOptions() {
		return kdeOptions;
	}

	public void setKdeOptions(
			KDECommandLineOptions kdeOptions ) {
		this.kdeOptions = kdeOptions;
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}

	public DataStorePluginOptions getOutputStoreOptions() {
		return outputStoreOptions;
	}

	public void setOutputStoreOptions(
			DataStorePluginOptions outputStoreOptions ) {
		this.outputStoreOptions = outputStoreOptions;
	}

	public List<IndexPluginOptions> getOutputIndexOptions() {
		return outputIndexOptions;
	}

	public void setOutputIndexOptions(
			List<IndexPluginOptions> outputIndexOptions ) {
		this.outputIndexOptions = outputIndexOptions;
	}

	@Override
	public Void computeResults(
			final OperationParams params )
			throws Exception {
		final KDEJobRunner runner = createRunner(params);
		final int status = runner.runJob();
		if (status != 0) {
			throw new RuntimeException(
					"Failed to execute: " + status);
		}
		return null;
	}
}
