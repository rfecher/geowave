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

import org.apache.hadoop.hbase.client.Connection;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.hbase.HBaseStoreFactoryFamily;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseRequiredOptions;
import mil.nga.giat.geowave.datastore.hbase.util.ConnectionPool;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

public class HBaseStoreTestEnvironment extends
		StoreTestEnvironment
{
	private static final GenericStoreFactory<DataStore> STORE_FACTORY = new HBaseStoreFactoryFamily()
			.getDataStoreFactory();

	private final static int NUM_REGION_SERVERS = 2;

	private static HBaseStoreTestEnvironment singletonInstance = null;

	private static boolean enableVisibility = true;

	public static synchronized HBaseStoreTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new HBaseStoreTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseStoreTestEnvironment.class);

	public static final String DEFAULT_HBASE_TEMP_DIR = "./target/hbase_temp";
	protected String zookeeper;

	private Object hbaseLocalCluster;

	public HBaseStoreTestEnvironment() {}

	// VisibilityTest valid authorizations
	private static String[] auths = new String[] {
		"a",
		"b",
		"c",
		"z"
	};

	// protected User SUPERUSER;

	@Override
	protected void initOptions(
			final StoreFactoryOptions options ) {
		HBaseRequiredOptions hbaseRequiredOptions = (HBaseRequiredOptions) options;
		hbaseRequiredOptions.setZookeeper(zookeeper);
	}

	public static ClassLoader newCl = new HBaseClassloader(Thread.currentThread().getContextClassLoader());

	@Override
	protected GenericStoreFactory<DataStore> getDataStoreFactory() {
		return STORE_FACTORY;
	}

	@Override
	public void setup() {
		if (hbaseLocalCluster == null) {

			if (!TestUtils.isSet(zookeeper)) {
				zookeeper = System.getProperty(ZookeeperTestEnvironment.ZK_PROPERTY_NAME);

				if (!TestUtils.isSet(zookeeper)) {
					zookeeper = ZookeeperTestEnvironment.getInstance().getZookeeper();
					LOGGER.debug("Using local zookeeper URL: " + zookeeper);
				}
			}
			// DirectoryBasedParentLastClassLoader newCl =
			ClassLoader prev = Thread.currentThread().getContextClassLoader();
			Thread.currentThread().setContextClassLoader(
					newCl);
			// try {
			// Class.forName(
			// "org.apache.hadoop.hbase.HBaseTestingUtility",
			// true,
			// newCl);
			// Class.forName(
			// "org.apache.hadoop.hbase.util.JVMClusterUtil",
			// true,
			// newCl);
			// Class.forName(
			// "org.apache.hadoop.hbase.master.HMaster",
			// true,
			// newCl);
			// Class.forName(
			// "org.apache.hadoop.hbase.test.MetricsAssertHelper",
			// true,
			// newCl);
			// Class.forName(
			// "org.apache.hadoop.hbase.test.MetricsAssertHelperImpl",
			// true,
			// newCl);
			// Class.forName(
			// "org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry",
			// true,
			// newCl);
			// //
			// System.err.println(newCl.loadClass("org.apache.hadoop.hbase.test.MetricsAssertHelper").getClassLoader());
			// // System.err.println(
			// //
			// newCl.loadClass("org.apache.hadoop.hbase.test.MetricsAssertHelperImpl").getClassLoader());
			// }
			// catch (ClassNotFoundException e1) {
			// // TODO Auto-generated catch block
			// e1.printStackTrace();
			// }
			// File[] files = new File(
			// "target/hbase/lib").listFiles();
			// URL[] urls = new URL[files.length];
			// for (int i = 0; i < files.length; i++) {
			// try {
			// urls[i] = files[i].toURI().toURL();
			// }
			// catch (MalformedURLException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			// }
			// ClassLoader newCl = new DirectoryBasedParentLastClassLoader(
			// "target/hbase/lib");
			// Thread.currentThread().setContextClassLoader(
			// newCl);
			try {
				Class.forName(
						"org.apache.hadoop.hbase.HBaseTestingUtility",
						true,
						newCl);
				Class.forName(
						"org.apache.hadoop.hbase.util.JVMClusterUtil",
						true,
						newCl);
				Class.forName(
						"org.apache.hadoop.hbase.master.HMaster",
						true,
						newCl);
				Class.forName(
						"org.apache.hadoop.hbase.CoordinatedStateManager",
						true,
						newCl);
				Class.forName(
						"org.apache.hadoop.hbase.coordination.ZkCoordinatedStateManager",
						true,
						newCl);
			}
			catch (ClassNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			if (!TestUtils.isSet(System.getProperty(ZookeeperTestEnvironment.ZK_PROPERTY_NAME))) {
				try {
					// hbaseLocalCluster = (HBaseTestingUtility)
					// newCl.loadClass(
					// HBaseTestingUtility.class.getName()).newInstance();
					hbaseLocalCluster = Class.forName(
							"org.apache.hadoop.hbase.HBaseTestingUtility",
							true,
							newCl).newInstance();
					System.err.println(hbaseLocalCluster.getClass().getClassLoader());
					final Object conf = hbaseLocalCluster.getClass().getMethod(
							"getConfiguration").invoke(
							hbaseLocalCluster);
					System.setProperty(
							// HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY,
							"test.build.data.basedirectory",
							DEFAULT_HBASE_TEMP_DIR);
					conf.getClass().getMethod(
							"set",
							String.class,
							String.class).invoke(
							conf,
							"hbase.online.schema.update.enable",
							"true");

					if (enableVisibility) {
						conf.getClass().getMethod(
								"set",
								String.class,
								String.class).invoke(
								conf,
								"hbase.superuser",
								"admin");

						conf.getClass().getMethod(
								"setBoolean",
								String.class,
								Boolean.TYPE).invoke(
								conf,
								"hbase.security.authorization",
								true);

						conf.getClass().getMethod(
								"setBoolean",
								String.class,
								Boolean.TYPE).invoke(
								conf,
								"hbase.security.visibility.mutations.checkauths",
								true);

						// // setup vis IT configuration
						// conf.getClass().getMethod(
						// "setClass",
						// String.class,
						// Class.class,
						// Class.class).invoke(
						// conf,
						// VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS,
						// SimpleScanLabelGenerator.class,
						// ScanLabelGenerator.class);
						//
						// conf.getClass().getMethod(
						// "setClass",
						// String.class,
						// Class.class,
						// Class.class).invoke(
						// conf,
						// VisibilityLabelServiceManager.VISIBILITY_LABEL_SERVICE_CLASS,
						// // DefaultVisibilityLabelServiceImpl.class,
						// HBaseTestVisibilityLabelServiceImpl.class,
						// VisibilityLabelService.class);

						// Install the VisibilityController as a system
						// processor
						// VisibilityTestUtil.enableVisiblityLabels(conf);
					}
					// Start the cluster
					// hbaseLocalCluster = new HBaseTestingUtility(
					// conf);
					hbaseLocalCluster.getClass().getMethod(
							"startMiniHBaseCluster",
							Integer.TYPE,
							Integer.TYPE).invoke(
							hbaseLocalCluster,
							1,
							NUM_REGION_SERVERS);
					// 1,
					// NUM_REGION_SERVERS);

					if (enableVisibility) {

						// Set valid visibilities for the vis IT
						final Connection conn = ConnectionPool.getInstance().getConnection(
								zookeeper);
						try {
							// SUPERUSER = User.createUserForTesting(
							// conf,
							// "admin",
							// new String[] {
							// "supergroup"
							// });

							// Set up valid visibilities for the user
							// addLabels(
							// conn.getConfiguration(),
							// auths,
							// User.getCurrent().getName());

							// Verify hfile version
							final String hfileVersionStr = conn.getAdmin().getConfiguration().get(
									"hfile.format.version");
							Assert.assertTrue(
									"HFile version is incorrect",
									hfileVersionStr.equals("3"));
						}
						catch (final Throwable e) {
							LOGGER.error(
									"Error creating test user",
									e);
						}
					}
				}
				catch (final Exception e) {
					LOGGER.error(
							"Exception starting hbaseLocalCluster",
							e);
					Assert.fail();
				}
			}
			Thread.currentThread().setContextClassLoader(
					prev);
		}
	}

	// private void addLabels(
	// final Configuration conf,
	// final String[] labels,
	// final String user )
	// throws Exception {
	// final PrivilegedExceptionAction<VisibilityLabelsResponse> action = new
	// PrivilegedExceptionAction<VisibilityLabelsResponse>() {
	// @Override
	// public VisibilityLabelsResponse run()
	// throws Exception {
	// try {
	// VisibilityClient.addLabels(
	// conf,
	// labels);
	//
	// VisibilityClient.setAuths(
	// conf,
	// labels,
	// user);
	// }
	// catch (final Throwable t) {
	// throw new IOException(
	// t);
	// }
	// return null;
	// }
	// };
	//
	// SUPERUSER.runAs(action);
	// }

	@Override
	public void tearDown() {
		try {
			hbaseLocalCluster.getClass().getMethod(
					"shutdownMiniCluster").invoke(
					hbaseLocalCluster);
			// if (!hbaseLocalCluster.cleanupTestDir()) {
			if (!(Boolean) hbaseLocalCluster.getClass().getMethod(
					"shutdownMiniCluster").invoke(
					hbaseLocalCluster)) {
				LOGGER.warn("Unable to delete mini hbase temporary directory");
			}
			hbaseLocalCluster = null;
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to shutdown and delete mini hbase temporary directory",
					e);
		}
	}

	@Override
	protected GeoWaveStoreType getStoreType() {
		return GeoWaveStoreType.HBASE;
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {
			ZookeeperTestEnvironment.getInstance()
		};
	}
}