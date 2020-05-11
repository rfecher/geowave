/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.config;

import java.io.File;
import java.util.Properties;
import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.filesystem.FileSystemStoreFactoryFamily;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemUtils;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

public class FileSystemOptions extends StoreFactoryOptions {
  public static final String DEFAULT_BINARY_FORMATTER = "binary";
  @Parameter(
      names = "--dir",
      description = "The directory to read/write to.  Defaults to \"filesystem\" in the working directory.")
  private String dir = "filesystem";

  @Parameter(
      names = "--format",
      description = "Optionally use a formatter configured with Java SPI of type org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatterSpi.  Defaults to \""
          + DEFAULT_BINARY_FORMATTER
          + "\" which is a compact geowave serialization.")
  private String format = "binary";
  @ParametersDelegate
  protected BaseDataStoreOptions baseOptions = new BaseDataStoreOptions() {
    @Override
    public boolean isServerSideLibraryEnabled() {
      return false;
    }

    @Override
    protected int defaultMaxRangeDecomposition() {
      return FileSystemUtils.FILESYSTEM_DEFAULT_MAX_RANGE_DECOMPOSITION;
    }

    @Override
    protected int defaultAggregationMaxRangeDecomposition() {
      return FileSystemUtils.FILESYSTEM_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION;
    }

    @Override
    protected boolean defaultEnableVisibility() {
      return false;
    }
  };

  public String getFormat() {
    return format;
  }

  public void setFormat(final String format) {
    this.format = format;
  }

  @Override
  public void validatePluginOptions() throws ParameterException {
    // Set the directory to be absolute
    dir = new File(dir).getAbsolutePath();
    super.validatePluginOptions();
  }

  @Override
  public void validatePluginOptions(final Properties properties) throws ParameterException {
    // Set the directory to be absolute
    dir = new File(dir).getAbsolutePath();
    super.validatePluginOptions(properties);
  }

  public FileSystemOptions() {
    super();
  }

  public FileSystemOptions(final String geowaveNamespace) {
    super(geowaveNamespace);
  }

  public void setDirectory(final String dir) {
    this.dir = dir;
  }

  public String getDirectory() {
    return dir;
  }

  @Override
  public StoreFactoryFamilySpi getStoreFactory() {
    return new FileSystemStoreFactoryFamily();
  }

  @Override
  public DataStoreOptions getStoreOptions() {
    return baseOptions;
  }
}
