/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.operations.options;

import com.beust.jcommander.Parameter;
import org.locationtech.geowave.mapreduce.operations.HdfsHostPortConverter;

public class RasterTileResizeCommandLineOptions {
  @Parameter(names = "--inputCoverageName", description = "The name of the input raster coverage",
      required = true)
  private String inputCoverageName;

  @Parameter(names = "--outputCoverageName", description = "The out output raster coverage name",
      required = true)
  private String outputCoverageName;

  @Parameter(names = "--minSplits", description = "The min partitions for the input data")
  private Integer minSplits;

  @Parameter(names = "--maxSplits", description = "The max partitions for the input data")
  private Integer maxSplits;

  @Parameter(names = "--hdfsHostPort", description = "he hdfs host port",
      converter = HdfsHostPortConverter.class)
  private String hdfsHostPort;

  @Parameter(names = "--jobSubmissionHostPort", description = "The job submission tracker",
      required = true)
  private String jobTrackerOrResourceManHostPort;

  @Parameter(names = "--outputTileSize", description = "The tile size to output", required = true)
  private Integer outputTileSize;

  @Parameter(names = "--indexName", description = "The index that the input raster is stored in")
  private String indexName;

  // Default constructor
  public RasterTileResizeCommandLineOptions() {}

  public RasterTileResizeCommandLineOptions(final String inputCoverageName,
      final String outputCoverageName, final Integer minSplits, final Integer maxSplits,
      final String hdfsHostPort, final String jobTrackerOrResourceManHostPort,
      final Integer outputTileSize, final String indexName) {
    this.inputCoverageName = inputCoverageName;
    this.outputCoverageName = outputCoverageName;
    this.minSplits = minSplits;
    this.maxSplits = maxSplits;
    this.hdfsHostPort = hdfsHostPort;
    this.jobTrackerOrResourceManHostPort = jobTrackerOrResourceManHostPort;
    this.outputTileSize = outputTileSize;
    this.indexName = indexName;
  }

  public String getInputCoverageName() {
    return inputCoverageName;
  }

  public String getOutputCoverageName() {
    return outputCoverageName;
  }

  public Integer getMinSplits() {
    return minSplits;
  }

  public Integer getMaxSplits() {
    return maxSplits;
  }

  public String getHdfsHostPort() {
    return hdfsHostPort;
  }

  public String getJobTrackerOrResourceManHostPort() {
    return jobTrackerOrResourceManHostPort;
  }

  public Integer getOutputTileSize() {
    return outputTileSize;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setInputCoverageName(final String inputCoverageName) {
    this.inputCoverageName = inputCoverageName;
  }

  public void setOutputCoverageName(final String outputCoverageName) {
    this.outputCoverageName = outputCoverageName;
  }

  public void setMinSplits(final Integer minSplits) {
    this.minSplits = minSplits;
  }

  public void setMaxSplits(final Integer maxSplits) {
    this.maxSplits = maxSplits;
  }

  public void setHdfsHostPort(final String hdfsHostPort) {
    this.hdfsHostPort = hdfsHostPort;
  }

  public void setJobTrackerOrResourceManHostPort(final String jobTrackerOrResourceManHostPort) {
    this.jobTrackerOrResourceManHostPort = jobTrackerOrResourceManHostPort;
  }

  public void setOutputTileSize(final Integer outputTileSize) {
    this.outputTileSize = outputTileSize;
  }

  public void setIndexName(final String indexName) {
    this.indexName = indexName;
  }
}
