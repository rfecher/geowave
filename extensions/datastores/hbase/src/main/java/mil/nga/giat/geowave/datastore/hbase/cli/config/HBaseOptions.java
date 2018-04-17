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
package mil.nga.giat.geowave.datastore.hbase.cli.config;

import org.apache.hadoop.hbase.HConstants;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.cli.VersionUtils;
import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;

public class HBaseOptions extends
		BaseDataStoreOptions
{
	public static final String COPROCESSOR_JAR_KEY = "coprocessorJar";

	@Parameter(names = "--scanCacheSize")
	protected int scanCacheSize = HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING;

	@Parameter(names = "--disableVerifyCoprocessors")
	protected boolean disableVerifyCoprocessors = false;

	protected boolean bigTable = false;

	@Parameter(names = {
		"--" + COPROCESSOR_JAR_KEY
	}, description = "Path (HDFS URL) to the jar containing coprocessor classes")
	private String coprocessorJar;

	public HBaseOptions() {
		super();
		setServerSideLibraryEnabled(isDefaultServerSide());
	}

	private static boolean isDefaultServerSide() {
		final String buildArgs = (String) VersionUtils.getBuildProperties().get(
				"project.build.args");
		if ((buildArgs != null) && !buildArgs.isEmpty()) {
			if (buildArgs.toLowerCase().contains(
					"cdh")
					|| buildArgs.toLowerCase().contains(
							"hdp")
					|| buildArgs.toLowerCase().contains(
							"cloudera")
					|| buildArgs.toLowerCase().contains(
							"hortonworks")) {
				// for now let's assume if its CDH or HDP it doesn't use
				// server-side
				// by default, because GeoWave serversideoperations is only
				// available for hbase 1.4.x
				return false;
			}
		}
		return true;
	}

	public void setBigTable(
			final boolean bigTable ) {
		this.bigTable = bigTable;
		if (bigTable) {
			enableServerSideLibrary = false;
		}
	}

	public boolean isBigTable() {
		return bigTable;
	}

	public int getScanCacheSize() {
		return scanCacheSize;
	}

	public void setScanCacheSize(
			final int scanCacheSize ) {
		this.scanCacheSize = scanCacheSize;
	}

	public boolean isVerifyCoprocessors() {
		return !disableVerifyCoprocessors && enableServerSideLibrary;
	}

	public void setVerifyCoprocessors(
			final boolean verifyCoprocessors ) {
		disableVerifyCoprocessors = !verifyCoprocessors;
	}

	public String getCoprocessorJar() {
		return coprocessorJar;
	}

	public void setCoprocessorJar(
			final String coprocessorJar ) {
		this.coprocessorJar = coprocessorJar;
	}
}
