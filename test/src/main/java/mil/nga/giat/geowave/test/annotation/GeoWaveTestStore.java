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
package mil.nga.giat.geowave.test.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


import mil.nga.giat.geowave.test.AccumuloStoreTestEnvironment;
import mil.nga.giat.geowave.test.DynamoDBTestEnvironment;
import mil.nga.giat.geowave.test.HBaseClassloader;
import mil.nga.giat.geowave.test.BigtableStoreTestEnvironment;
import mil.nga.giat.geowave.test.CassandraStoreTestEnvironment;
import mil.nga.giat.geowave.test.DirectoryBasedParentLastClassLoader;
import mil.nga.giat.geowave.test.StoreTestEnvironment;
import mil.nga.giat.geowave.test.TestUtils;

/**
 * The <code>DataStores</code> annotation specifies the GeoWave DataStore to be
 * run when a class annotated with <code>@RunWith(GeoWaveIT.class)</code> is
 * run.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({
	ElementType.FIELD,
	ElementType.TYPE
})
public @interface GeoWaveTestStore {
	/**
	 * @return the data stores to run with
	 */
	public GeoWaveStoreType[] value();

	/**
	 * @return the namespace to associate the store with
	 */
	public String namespace() default TestUtils.TEST_NAMESPACE;

	/**
	 * @return a "key=value" pair that will override default options for the
	 *         client-side configuration of this datastore
	 */
	public String[] options() default "";

	public static enum GeoWaveStoreType {
		DYNAMODB(
				DynamoDBTestEnvironment.getInstance()),
		ACCUMULO(
				AccumuloStoreTestEnvironment.getInstance()),
		BIGTABLE(
				BigtableStoreTestEnvironment.getInstance()),
		CASSANDRA(
				CassandraStoreTestEnvironment.getInstance()),
		HBASE(
				newHBaseEnvironment());
		private final StoreTestEnvironment testEnvironment;

		private GeoWaveStoreType(
				final StoreTestEnvironment testEnvironment ) {
			this.testEnvironment = testEnvironment;
		}

		public StoreTestEnvironment getTestEnvironment() {
			return testEnvironment;
		}

		private static StoreTestEnvironment newHBaseEnvironment() {
			DirectoryBasedParentLastClassLoader newCl = new DirectoryBasedParentLastClassLoader(
					"target/hbase/lib");
//			HBaseClassloader newCl = new HBaseClassloader(Thread.currentThread().getContextClassLoader());
			try {
//				try {
//					Class.forName(
//							"org.apache.hadoop.hbase.HBaseTestingUtility",
//							true,
//							newCl);
//					Class.forName(
//							"org.apache.hadoop.hbase.util.JVMClusterUtil",
//							true,
//							newCl);
//					Class.forName(
//							"org.apache.hadoop.hbase.master.HMaster",
//							true,
//							newCl);
//				}
//				catch (ClassNotFoundException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				}
				Class.forName("com.google.common.hash.HashFunction", true,newCl);
				Class.forName("com.google.common.hash.Hashing", true,newCl);
				Thread.currentThread().setContextClassLoader(
						newCl);
				return (StoreTestEnvironment) Class.forName(
						"mil.nga.giat.geowave.test.HBaseStoreTestEnvironment",
						true,
						newCl).newInstance();
			}
			catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}
	}
}
