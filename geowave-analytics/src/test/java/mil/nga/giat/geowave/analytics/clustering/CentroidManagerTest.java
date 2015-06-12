package mil.nga.giat.geowave.analytics.clustering;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.analytics.clustering.CentroidManager.CentroidProcessingFn;
import mil.nga.giat.geowave.analytics.tools.AnalyticFeature;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.io.FileUtils;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class CentroidManagerTest
{
	final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
			"centroid",
			new String[] {
				"extra1"
			},
			BasicFeatureTypes.DEFAULT_NAMESPACE,
			ClusteringUtils.CLUSTERING_CRS).getType();

	final MockInstance mockDataInstance = new MockInstance(
			UUID.randomUUID().toString());

	final GeometryFactory factory = new GeometryFactory();

	@Test
	public void testSampleRecall()
			throws Exception {

		final Connector mockDataConnector = mockDataInstance.getConnector(
				"root",
				new PasswordToken(
						new byte[0]));

		final BasicAccumuloOperations dataOps = new BasicAccumuloOperations(
				mockDataConnector);

		final String grp1 = "g1";
		final String grp2 = "g2";
		SimpleFeature feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"123",
				"fred",
				grp1,
				20.30203,
				factory.createPoint(new Coordinate(
						02.33,
						0.23)),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);

		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				ftype);

		final AccumuloDataStore dataStore = new AccumuloDataStore(
				dataOps);
		dataStore.ingest(
				adapter,
				index,
				feature);

		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"231",
				"flood",
				grp1,
				20.30203,
				factory.createPoint(new Coordinate(
						02.33,
						0.23)),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);
		dataStore.ingest(
				adapter,
				index,
				feature);

		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"321",
				"flou",
				grp2,
				20.30203,
				factory.createPoint(new Coordinate(
						02.33,
						0.23)),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);
		dataStore.ingest(
				adapter,
				index,
				feature);

		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b2",
				"312",
				"flapper",
				grp2,
				20.30203,
				factory.createPoint(new Coordinate(
						02.33,
						0.23)),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);
		dataStore.ingest(
				adapter,
				index,
				feature);

		// and one feature with a different zoom level
		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b2",
				"312",
				"flapper",
				grp2,
				20.30203,
				factory.createPoint(new Coordinate(
						02.33,
						0.23)),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				2,
				1,
				0);
		dataStore.ingest(
				adapter,
				index,
				feature);

		CentroidManagerGeoWave<SimpleFeature> mananger = new CentroidManagerGeoWave<SimpleFeature>(
				dataOps,
				new SimpleFeatureItemWrapperFactory(),
				StringUtils.stringFromBinary(adapter.getAdapterId().getBytes()),
				StringUtils.stringFromBinary(index.getId().getBytes()),
				"b1",
				1);
		List<AnalyticItemWrapper<SimpleFeature>> centroids = mananger.getCentroidsForGroup(null);

		assertEquals(
				3,
				centroids.size());
		feature = centroids.get(
				0).getWrappedItem();
		assertEquals(
				0.022,
				(Double) feature.getAttribute("extra1"),
				0.001);

		centroids = mananger.getCentroidsForGroup(grp1);
		assertEquals(
				2,
				centroids.size());
		centroids = mananger.getCentroidsForGroup(grp2);
		assertEquals(
				1,
				centroids.size());
		feature = centroids.get(
				0).getWrappedItem();
		assertEquals(
				0.022,
				(Double) feature.getAttribute("extra1"),
				0.001);

		mananger = new CentroidManagerGeoWave<SimpleFeature>(
				dataOps,
				new SimpleFeatureItemWrapperFactory(),
				StringUtils.stringFromBinary(adapter.getAdapterId().getBytes()),
				StringUtils.stringFromBinary(index.getId().getBytes()),
				"b1",
				1);

		mananger.processForAllGroups(new CentroidProcessingFn<SimpleFeature>() {

			@Override
			public int processGroup(
					final String groupID,
					final List<AnalyticItemWrapper<SimpleFeature>> centroids ) {
				if (groupID.equals(grp1)) {
					assertEquals(
							2,
							centroids.size());
				}
				else if (groupID.equals(grp2)) {
					assertEquals(
							1,
							centroids.size());
				}
				else {
					assertTrue(
							"what group is this : " + groupID,
							false);
				}
				return 0;
			}

		});

		FileUtils.deleteDirectory(new File(
				"./testdata"));

		assertTrue(new File(
				"./testdata").mkdirs());

		mananger.toShapeFile(
				"./testdata",
				Point.class);

		assertEquals(
				3,
				checkShapeFile("./testdata/b1"));

		// FileUtils.deleteDirectory(new File("./testdata"));

	}

	private int checkShapeFile(
			final String location )
			throws Exception {
		final File file = new File(
				location + ".shp");
		final Map<String, Serializable> map = new HashMap<>();
		map.put(
				"url",
				file.toURI().toURL());

		final DataStore dataStore = DataStoreFinder.getDataStore(map);
		final String typeName = dataStore.getTypeNames()[0];

		final FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore.getFeatureSource(typeName);

		final FeatureCollection<?, SimpleFeature> collection = source.getFeatures();

		int count = 0;
		try (FeatureIterator<SimpleFeature> results = collection.features()) {
			while (results.hasNext()) {
				final SimpleFeature feature = results.next();
				count += (feature.getDefaultGeometry() != null ? 1 : 0);
			}
		}
		return count;
	}
}
