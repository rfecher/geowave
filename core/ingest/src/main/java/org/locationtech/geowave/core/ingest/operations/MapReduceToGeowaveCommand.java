/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.operations;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsDriver;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.MapReduceCommandLineOptions;
import org.locationtech.geowave.core.ingest.local.LocalInputCommandLineOptions;
import org.locationtech.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.IndexLoader;
import org.locationtech.geowave.core.store.cli.remote.options.IndexPluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import org.locationtech.geowave.core.store.cli.remote.options.VisibilityOptions;
import org.locationtech.geowave.mapreduce.operations.ConfigHDFSCommand;

@GeowaveOperation(name = "mrToGW", parentOperation = IngestSection.class)
@Parameters(commandDescription = "Ingest supported files that already exist in HDFS")
public class MapReduceToGeowaveCommand extends ServiceEnabledCommand<Void> {

  @Parameter(
      description = "<path to base directory to write to> <store name> <comma delimited index/group list>")
  private List<String> parameters = new ArrayList<String>();

  @ParametersDelegate
  private VisibilityOptions ingestOptions = new VisibilityOptions();

  @ParametersDelegate
  private MapReduceCommandLineOptions mapReduceOptions = new MapReduceCommandLineOptions();

  @ParametersDelegate
  private LocalInputCommandLineOptions localInputOptions = new LocalInputCommandLineOptions();

  // This helper is used to load the list of format SPI plugins that will be
  // used
  @ParametersDelegate
  private IngestFormatPluginOptions pluginFormats = new IngestFormatPluginOptions();

  private DataStorePluginOptions inputStoreOptions = null;

  private List<IndexPluginOptions> inputIndexOptions = null;

  @Override
  public boolean prepare(final OperationParams params) {

    // TODO: localInputOptions has 'extensions' which doesn't mean
    // anything for MapReduce to GeoWave.

    // Based on the selected formats, select the format plugins
    pluginFormats.selectPlugin(localInputOptions.getFormats());

    return true;
  }

  /**
   * Prep the driver & run the operation.
   *
   * @throws Exception
   */
  @Override
  public void execute(final OperationParams params) throws Exception {

    // Ensure we have all the required arguments
    if (parameters.size() != 3) {
      throw new ParameterException(
          "Requires arguments: <path to base directory to write to> <store name> <comma delimited index/group list>");
    }

    computeResults(params);
  }

  @Override
  public boolean runAsync() {
    return true;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(
      final String hdfsPath,
      final String storeName,
      final String commaSeparatedIndexes) {
    parameters = new ArrayList<String>();
    parameters.add(hdfsPath);
    parameters.add(storeName);
    parameters.add(commaSeparatedIndexes);
  }

  public VisibilityOptions getIngestOptions() {
    return ingestOptions;
  }

  public void setIngestOptions(final VisibilityOptions ingestOptions) {
    this.ingestOptions = ingestOptions;
  }

  public MapReduceCommandLineOptions getMapReduceOptions() {
    return mapReduceOptions;
  }

  public void setMapReduceOptions(final MapReduceCommandLineOptions mapReduceOptions) {
    this.mapReduceOptions = mapReduceOptions;
  }

  public LocalInputCommandLineOptions getLocalInputOptions() {
    return localInputOptions;
  }

  public void setLocalInputOptions(final LocalInputCommandLineOptions localInputOptions) {
    this.localInputOptions = localInputOptions;
  }

  public IngestFormatPluginOptions getPluginFormats() {
    return pluginFormats;
  }

  public void setPluginFormats(final IngestFormatPluginOptions pluginFormats) {
    this.pluginFormats = pluginFormats;
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }

  public List<IndexPluginOptions> getInputIndexOptions() {
    return inputIndexOptions;
  }

  @Override
  public Void computeResults(final OperationParams params) throws Exception {
    if (mapReduceOptions.getJobTrackerOrResourceManagerHostPort() == null) {
      throw new ParameterException(
          "Requires job tracker or resource manager option (try geowave help <command>...)");
    }

    final String basePath = parameters.get(0);
    final String inputStoreName = parameters.get(1);
    final String indexList = parameters.get(2);
    // Config file
    File configFile = getGeoWaveConfigFile(params);

    Properties configProperties = ConfigOptions.loadProperties(configFile);
    String hdfsHostPort = ConfigHDFSCommand.getHdfsUrl(configProperties);

    final StoreLoader inputStoreLoader = new StoreLoader(inputStoreName);
    if (!inputStoreLoader.loadFromConfig(configFile)) {
      throw new ParameterException("Cannot find store name: " + inputStoreLoader.getStoreName());
    }
    inputStoreOptions = inputStoreLoader.getDataStorePlugin();

    final IndexLoader indexLoader = new IndexLoader(indexList);
    if (!indexLoader.loadFromConfig(configFile)) {
      throw new ParameterException("Cannot find index(s) by name: " + indexList);
    }
    inputIndexOptions = indexLoader.getLoadedIndexes();

    // Ingest Plugins
    final Map<String, IngestFromHdfsPlugin<?, ?>> ingestPlugins =
        pluginFormats.createHdfsIngestPlugins();

    // Driver
    final IngestFromHdfsDriver driver =
        new IngestFromHdfsDriver(
            inputStoreOptions,
            inputIndexOptions,
            ingestOptions,
            mapReduceOptions,
            ingestPlugins,
            hdfsHostPort,
            basePath);

    // Execute
    if (!driver.runOperation()) {
      throw new RuntimeException("Ingest failed to execute");
    }
    return null;
  }
}
