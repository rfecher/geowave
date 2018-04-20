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
package mil.nga.giat.geowave.format.stanag4676;

import java.util.Map;

import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.avro.WholeFile;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;

public class Stanag4676IngestFormat implements
		IngestFormatPluginProviderSpi<WholeFile, Object>
{
	private static Stanag4676IngestPlugin singletonInstance;
	private static Stanag4676IngestPlugin singletonInstanceNoImageChips;

	private static Stanag4676IngestPlugin getSingletonInstance(
			IngestFormatOptionProvider options ) {
		if (options instanceof Stanag4676Options && ((Stanag4676Options) options).isDisableImageChips()) {
			return getSingletonInstanceNoImageChips();
		}
		else {
			return getSingletonInstance();
		}
	}

	private static synchronized Stanag4676IngestPlugin getSingletonInstance() {
		if (singletonInstance == null) {
			singletonInstance = new Stanag4676IngestPlugin(
					false);
		}
		return singletonInstance;
	}

	private static synchronized Stanag4676IngestPlugin getSingletonInstanceNoImageChips() {
		if (singletonInstanceNoImageChips == null) {
			singletonInstanceNoImageChips = new Stanag4676IngestPlugin(
					true);
		}
		return singletonInstanceNoImageChips;

	}

	@Override
	public AvroFormatPlugin<WholeFile, Object> createAvroFormatPlugin(
			IngestFormatOptionProvider options )
			throws UnsupportedOperationException {
		return getSingletonInstance(options);
	}

	@Override
	public IngestFromHdfsPlugin<WholeFile, Object> createIngestFromHdfsPlugin(
			IngestFormatOptionProvider options )
			throws UnsupportedOperationException {
		return getSingletonInstance(options);
	}

	@Override
	public LocalFileIngestPlugin<Object> createLocalFileIngestPlugin(
			IngestFormatOptionProvider options )
			throws UnsupportedOperationException {
		return getSingletonInstance(options);
	}

	@Override
	public String getIngestFormatName() {
		return "stanag4676";
	}

	@Override
	public String getIngestFormatDescription() {
		return "xml files representing track data that adheres to the schema defined by STANAG-4676";
	}

	@Override
	public IngestFormatOptionProvider createOptionsInstances() {
		return new Stanag4676Options();
	}

}
