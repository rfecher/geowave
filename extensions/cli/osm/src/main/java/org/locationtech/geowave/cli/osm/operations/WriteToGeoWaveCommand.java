/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.osm.operations;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.cli.osm.parser.GeoWaveOsmWriter;
import org.locationtech.geowave.cli.osm.parser.OsmPbfParser;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreLoader;
import org.openstreetmap.osmosis.osmbinary.file.BlockInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@GeowaveOperation(name = "stage", parentOperation = OSMSection.class)
@Parameters(commandDescription = "Stage OSM data to HDFS")
public class WriteToGeoWaveCommand extends DefaultOperation implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteToGeoWaveCommand.class);

  @Parameter(description = "<file or directory> <store name>")
  private final List<String> parameters = new ArrayList<>();

  @Parameter(
      names = {"-m", "--mappingFile"},
      required = false,
      description = "Mapping file, imposm3 form")
  private final String mappingFile = null;

  @Parameter(
      names = {"-i", "--index"},
      required = false,
      description = "comma delimited index list for features")
  private final String indexList = null;

  private DataStorePluginOptions storeOptions = null;

  @Override
  public void execute(final OperationParams params) throws Exception {

    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <file or directory>  <store name>");
    }

    final String inputPath = parameters.get(0);
    final String storeName = parameters.get(1);

    // Config file
    final File configFile = getGeoWaveConfigFile(params);

    // These are set as main parameter arguments, to keep consistency with
    final StoreLoader inputStoreLoader = new StoreLoader(storeName);
    if (!inputStoreLoader.loadFromConfig(configFile)) {
      throw new ParameterException("Cannot find store name: " + inputStoreLoader.getStoreName());
    }
    storeOptions = inputStoreLoader.getDataStorePlugin();

    //TODO force this to use secondary indexing
    final OsmPbfParser osmPbfParser =
        new OsmPbfParser(new GeoWaveOsmWriter(storeOptions.createDataStore()));
    final File f = new File(inputPath);
    if (f.isDirectory()) {
      Files.walkFileTree(Paths.get(inputPath), new SimpleFileVisitor<java.nio.file.Path>() {
        @Override
        @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
        public FileVisitResult visitFile(
            final java.nio.file.Path file,
            final BasicFileAttributes attrs) throws IOException {
          if (file.getFileName().toString().toLowerCase().endsWith(".pbf")) {
            readFile(file.toFile(), osmPbfParser);
          }
          return FileVisitResult.CONTINUE;
        }
      });
    } else if (f.getName().toLowerCase().endsWith(".pbf")) {
      readFile(f, osmPbfParser);
    } else {
      JCommander.getConsole().println("Input must be PBF file or a directory");
    }
  }

  private void readFile(final File file, final OsmPbfParser parser) {

    try (InputStream is = new FileInputStream(file)) {
      new BlockInputStream(is, parser).process();
    } catch (final FileNotFoundException e) {
      LOGGER.error("Unable to load file: " + file.toString(), e);
    } catch (final IOException e1) {
      LOGGER.error("Unable to process file: " + file.toString(), e1);
    }
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String fileOrDirectory, final String storeName) {
    parameters.clear();

    parameters.add(fileOrDirectory);
    parameters.add(storeName);
  }
}
