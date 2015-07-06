package mil.nga.giat.geowave.test.query;

import java.io.IOException;
import java.util.Date;
import java.util.Map.Entry;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.VectorDataStore;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.NumericSecondaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.TemporalSecondaryIndex;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;

/**
 * This class is currently a dirty test harness used to sanity check changes as
 * I go. It will likely be rewritten/replaced by a much more sophisticated
 * integration test for secondary indexing once the capability matures
 * 
 * @author yeagerdc
 *
 */
public class SecondaryIndexingDriver extends
		GeoWaveTestEnvironment

{
	private static SimpleFeatureType schema;

	@BeforeClass
	public static void init()
			throws IOException,
			SchemaException,
			AccumuloException,
			AccumuloSecurityException {

		GeoWaveTestEnvironment.setup();

		// see https://github.com/ngageoint/geowave/wiki/Secondary-Indexing
		schema = DataUtilities.createType(
				"cannedData",
				"location:Geometry,persons:Double,record_date:Date,income_category:String,affiliation:String");

		// mark numeric attribute for secondary indexing
		schema.getDescriptor(
				"persons").getUserData().put(
				"index",
				Boolean.TRUE);

		// mark temporal attribute for secondary indexing
		schema.getDescriptor(
				"record_date").getUserData().put(
				"index",
				Boolean.TRUE);

		// mark text attribute for secondary indexing
		// schema.getDescriptor(
		// "affiliation").getUserData().put(
		// "index",
		// Boolean.TRUE);

		FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema);

		VectorDataStore dataStore = new VectorDataStore(
				accumuloOperations);

		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

		dataStore.ingest(
				dataAdapter,
				index,
				buildSimpleFeature());

		System.out.println("Feature ingested");

		// final CloseableIterator<?> results = dataStore.query(new
		// SpatialQuery(
		// GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
		// -102,
		// 41))));
		//
		// while (results.hasNext()) {
		// SimpleFeature sf = (SimpleFeature) results.next();
		// System.out.println("Result: " + sf.getID());
		// }
		//
		// results.close();

	}

	@Test
	public void test()
			throws AccumuloException,
			AccumuloSecurityException,
			TableNotFoundException {

		int numNumericEntries = countNumberOfEntriesInIndexTable(NumericSecondaryIndex.TABLE_NAME);
		Assert.assertTrue(numNumericEntries == 1);

		int numTemporalEntries = countNumberOfEntriesInIndexTable(TemporalSecondaryIndex.TABLE_NAME);
		Assert.assertTrue(numTemporalEntries == 1);
	}

	private int countNumberOfEntriesInIndexTable(
			final String tableName )
			throws TableNotFoundException {
		Scanner scanner = accumuloOperations.createScanner(tableName);
		int numEntries = 0;
		for (Entry<Key, Value> kv : scanner) {
			numEntries++;
		}
		scanner.close();
		return numEntries;
	}

	private static SimpleFeature buildSimpleFeature() {

		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				schema);

		builder.set(
				"location",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						-102,
						41)));
		builder.set(
				"persons",
				1500000);
		builder.set(
				"record_date",
				new Date());
		builder.set(
				"income_category",
				"10-15");
		builder.set(
				"affiliation",
				"blahblah");

		return builder.buildFeature(UUID.randomUUID().toString());
	}

}
