/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.sentinel2;

import static org.junit.Assert.*;
import com.beust.jcommander.ParameterException;
import it.geosolutions.jaiext.JAIExt;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import org.apache.commons.lang.SystemUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.geowave.adapter.raster.plugin.gdal.InstallGdal;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;

public class RasterIngestRunnerTest {
  @BeforeClass
  public static void setup() throws IOException {

    // Skip this test if we're on a Mac
    org.junit.Assume.assumeTrue(isNotMac());

    GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put("memory",
        new MemoryStoreFactoryFamily());

    InstallGdal.main(new String[] {System.getenv("GDAL_DIR")});
  }

  private static boolean isNotMac() {
    return !SystemUtils.IS_OS_MAC;
  }

  @Test
  public void testIngestProviders() throws Exception {
    for (Sentinel2ImageryProvider provider : Sentinel2ImageryProvider.getProviders()) {
      testIngest(provider.providerName());
    }
  }

  public void testIngest(String providerName) throws Exception {
    JAIExt.initJAIEXT();

    if (providerName == "AWS" && !Tests.jp2ecwPluginIsWorking()) {
      System.out.println(
          "Unable to ingest Sentinel2 products with JP2 files, JP2ECW plugin is not working.");
      return;
    }

    Sentinel2ImageryProvider provider = Sentinel2ImageryProvider.getProvider(providerName);
    if (provider == null) {
      throw new RuntimeException("Unable to find '" + providerName + "' Sentinel2 provider");
    }

    if (!Tests.authenticationSettingsAreValid(providerName))
      return;

    Date[] timePeriodSettings = Tests.timePeriodSettings(providerName);
    Date startDate = timePeriodSettings[0];
    Date endDate = timePeriodSettings[1];

    Sentinel2BasicCommandLineOptions analyzeOptions = new Sentinel2BasicCommandLineOptions();
    analyzeOptions.setWorkspaceDir(Tests.WORKSPACE_DIR);
    analyzeOptions.setProviderName(providerName);
    analyzeOptions.setCollection(provider.collections()[0]);
    analyzeOptions.setLocation("T30TXN");
    analyzeOptions.setStartDate(startDate);
    analyzeOptions.setEndDate(endDate);
    analyzeOptions.setCqlFilter(
        "BBOX(shape,-1.8274,42.3253,-1.6256,42.4735) AND location='T30TXN' AND (band='B4' OR band='B8')");

    String[] settings = Tests.authenticationSettings(providerName);
    String iden = settings[0];
    String pass = settings[1];

    Sentinel2DownloadCommandLineOptions downloadOptions = new Sentinel2DownloadCommandLineOptions();
    downloadOptions.setOverwriteIfExists(false);
    downloadOptions.setUserIdent(iden);
    downloadOptions.setPassword(pass);

    Sentinel2RasterIngestCommandLineOptions ingestOptions =
        new Sentinel2RasterIngestCommandLineOptions();
    ingestOptions.setRetainImages(true);
    ingestOptions.setCreatePyramid(false);
    ingestOptions.setScale(10);
    ingestOptions.setCreateHistogram(true);

    RasterIngestRunner runner = new RasterIngestRunner(analyzeOptions, downloadOptions,
        ingestOptions, Arrays.asList("memorystore", "spatialindex"));

    ManualOperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT,
        new File(RasterIngestRunnerTest.class.getClassLoader()
            .getResource("geowave-config.properties").toURI()));

    runner.runInternal(params);

    try (CloseableIterator<Object> results =
        getStore(params).query(QueryBuilder.newBuilder().build())) {
      assertTrue("Store is not empty", results.hasNext());
    }

    // Not sure what assertions can be made about the indexes.
  }

  private DataStore getStore(OperationParams params) {
    File configFile = (File) params.getContext().get(ConfigOptions.PROPERTIES_FILE_CONTEXT);

    StoreLoader inputStoreLoader = new StoreLoader("memorystore");
    if (!inputStoreLoader.loadFromConfig(configFile)) {
      throw new ParameterException("Cannot find store name: " + inputStoreLoader.getStoreName());
    }

    DataStorePluginOptions storeOptions = inputStoreLoader.getDataStorePlugin();
    return storeOptions.createDataStore();
  }
}
