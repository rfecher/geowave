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
package mil.nga.giat.geowave.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;

public class ZookeeperTestEnvironment implements
		TestEnvironment
{

	private static ZookeeperTestEnvironment singletonInstance = null;

	public static synchronized ZookeeperTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new ZookeeperTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(ZookeeperTestEnvironment.class);
	protected String zookeeper;

	private Object zookeeperLocalCluster;

	public static final String ZK_PROPERTY_NAME = "zookeeperUrl";
	public static final String DEFAULT_ZK_TEMP_DIR = "./target/zk_temp";

	private ZookeeperTestEnvironment() {}

	@Override
	public void setup()
			throws Exception {
		if (!TestUtils.isSet(zookeeper)) {
			zookeeper = System.getProperty(ZK_PROPERTY_NAME);

			if (!TestUtils.isSet(zookeeper)) {
//				Thread.currentThread().setContextClassLoader(
//						HBaseStoreTestEnvironment.newCl);
				try {
					System.setProperty(
							"test.build.data.basedirectory",
							DEFAULT_ZK_TEMP_DIR);
					zookeeperLocalCluster = Class.forName(
							"org.apache.hadoop.hbase.HBaseTestingUtility",
							true,
							HBaseStoreTestEnvironment.newCl).newInstance();
					// zookeeperLocalCluster = new HBaseTestingUtility();
					Object conf = zookeeperLocalCluster.getClass().getMethod(
							"getConfiguration").invoke(
							zookeeperLocalCluster);
					conf.getClass().getMethod(
							"setInt",
							String.class,
							Integer.TYPE).invoke(
							conf,
							"test.hbase.zookeeper.property.clientPort",
							2181);
					zookeeperLocalCluster.getClass().getMethod(
							"startMiniZKCluster").invoke(
							zookeeperLocalCluster);
				}
				catch (final Exception e) {
					LOGGER.error(
							"Exception starting zookeeperLocalCluster: " + e,
							e);
					Assert.fail();
				}

				zookeeper = "127.0.0.1:2181";// +
												// zookeeperLocalCluster.getZkCluster().getClientPort();
			}
		}
	}

	@Override
	public void tearDown()
			throws Exception {
		try {
			zookeeperLocalCluster.getClass().getMethod(
					"shutdownMiniZKCluster").invoke(
					zookeeperLocalCluster);
			if (!(Boolean) zookeeperLocalCluster.getClass().getMethod(
					"cleanupTestDir").invoke(
					zookeeperLocalCluster)) {
				LOGGER.warn("Unable to delete mini zookeeper temporary directory");
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to delete or shutdown mini zookeeper temporary directory",
					e);
		}

		zookeeper = null;
	}

	public String getZookeeper() {
		return zookeeper;
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}

}