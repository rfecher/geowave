package mil.nga.giat.geowave.analytics.mapreduce.nn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.analytics.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytics.distance.DistanceFn;
import mil.nga.giat.geowave.analytics.distance.FeatureDistanceFn;
import mil.nga.giat.geowave.analytics.kmeans.mapreduce.SimpleFeatureImplSerialization;
import mil.nga.giat.geowave.analytics.mapreduce.nn.NNMapReduce.PartitionDataWritable;
import mil.nga.giat.geowave.analytics.parameters.ClusteringParameters;
import mil.nga.giat.geowave.analytics.parameters.CommonParameters;
import mil.nga.giat.geowave.analytics.parameters.HullParameters;
import mil.nga.giat.geowave.analytics.parameters.PartitionParameters;
import mil.nga.giat.geowave.analytics.tools.AdapterWithObjectWritable;
import mil.nga.giat.geowave.analytics.tools.AnalyticFeature;
import mil.nga.giat.geowave.analytics.tools.Projection;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureProjection;
import mil.nga.giat.geowave.analytics.tools.partitioners.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.analytics.tools.partitioners.Partitioner.PartitionData;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.vector.adapter.FeatureWritable;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class DBScanMapReduceTest
{

	MapDriver<GeoWaveInputKey, Object, PartitionDataWritable, AdapterWithObjectWritable> mapDriver;
	ReduceDriver<PartitionDataWritable, AdapterWithObjectWritable, GeoWaveInputKey, ObjectWritable> reduceDriver;
	SimpleFeatureType ftype;
	final GeometryFactory factory = new GeometryFactory();
	final Key accumuloKey = null;

	@Before
	public void setUp()
			throws IOException,
			AccumuloException,
			AccumuloSecurityException {
		final NNMapReduce.NNMapper<SimpleFeature> nnMapper = new NNMapReduce.NNMapper<SimpleFeature>();
		final NNMapReduce.NNReducer<SimpleFeature, GeoWaveInputKey, ObjectWritable, Map<ByteArrayId, Cluster<SimpleFeature>>> nnReducer = new DBScanMapReduce.DBScanMapHullReducer();

		mapDriver = MapDriver.newMapDriver(nnMapper);
		reduceDriver = ReduceDriver.newReduceDriver(nnReducer);

		mapDriver.getConfiguration().set(
				GeoWaveConfiguratorBase.enumToConfKey(
						OrthodromicDistancePartitioner.class,
						ClusteringParameters.Clustering.DISTANCE_THRESHOLDS),
				"10,10");

		reduceDriver.getConfiguration().setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						NNMapReduce.class,
						CommonParameters.Common.DISTANCE_FUNCTION_CLASS),
				FeatureDistanceFn.class,
				DistanceFn.class);

		reduceDriver.getConfiguration().setDouble(
				GeoWaveConfiguratorBase.enumToConfKey(
						NNMapReduce.class,
						PartitionParameters.Partition.PARTITION_DISTANCE),
				10);

		ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroid",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getType();

		reduceDriver.getConfiguration().setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						DBScanMapReduce.class,
						HullParameters.Hull.PROJECTION_CLASS),
				SimpleFeatureProjection.class,
				Projection.class);

		reduceDriver.getConfiguration().setInt(
				GeoWaveConfiguratorBase.enumToConfKey(
						DBScanMapReduce.class,
						ClusteringParameters.Clustering.MINIMUM_SIZE),
				2);

		JobContextAdapterStore.addDataAdapter(
				mapDriver.getConfiguration(),
				new FeatureDataAdapter(
						ftype));

		JobContextAdapterStore.addDataAdapter(
				reduceDriver.getConfiguration(),
				new FeatureDataAdapter(
						ftype));

		serializations();
	}

	private SimpleFeature createTestFeature(
			final String name,
			final Coordinate coord ) {
		return AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				name,
				name,
				"NA",
				20.30203,
				factory.createPoint(coord),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);

	}

	private SimpleFeature createTestGeometry(
			final String name,
			final Coordinate[] coord ) {
		return AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				name,
				name,
				"NA",
				20.30203,
				factory.createPolygon(coord),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);

	}

	private void serializations() {
		final String[] strings = reduceDriver.getConfiguration().getStrings(
				"io.serializations");
		final String[] newStrings = new String[strings.length + 1];
		System.arraycopy(
				strings,
				0,
				newStrings,
				0,
				strings.length);
		newStrings[newStrings.length - 1] = SimpleFeatureImplSerialization.class.getName();
		reduceDriver.getConfiguration().setStrings(
				"io.serializations",
				newStrings);

		mapDriver.getConfiguration().setStrings(
				"io.serializations",
				newStrings);
	}

	@Test
	public void testReducer()
			throws IOException,
			AccumuloException,
			AccumuloSecurityException {

		final ByteArrayId adapterId = new ByteArrayId(
				ftype.getTypeName());

		final SimpleFeature feature1 = createTestFeature(
				"f1",
				new Coordinate(
						30.0,
						30.00000001));
		final SimpleFeature feature2 = createTestFeature(
				"f2",
				new Coordinate(
						50.001,
						50.001));
		final SimpleFeature feature3 = createTestFeature(
				"f3",
				new Coordinate(
						30.00000001,
						30.00000001));
		final SimpleFeature feature4 = createTestFeature(
				"f4",
				new Coordinate(
						50.0011,
						50.00105));
		final SimpleFeature feature5 = createTestFeature(
				"f5",
				new Coordinate(
						50.00112,
						50.00111));
		final SimpleFeature feature6 = createTestFeature(
				"f6",
				new Coordinate(
						30.00000001,
						30.00000002));
		final SimpleFeature feature7 = createTestFeature(
				"f7",
				new Coordinate(
						50.00113,
						50.00114));
		final SimpleFeature feature8 = createTestFeature(
				"f8",
				new Coordinate(
						40.00000001,
						40.000000002));

		mapDriver.addInput(
				new GeoWaveInputKey(
						adapterId,
						new ByteArrayId(
								feature1.getID())),
				feature1);
		mapDriver.addInput(
				new GeoWaveInputKey(
						adapterId,
						new ByteArrayId(
								feature2.getID())),
				feature2);
		mapDriver.addInput(
				new GeoWaveInputKey(
						adapterId,
						new ByteArrayId(
								feature3.getID())),
				feature3);
		mapDriver.addInput(
				new GeoWaveInputKey(
						adapterId,
						new ByteArrayId(
								feature4.getID())),
				feature4);
		mapDriver.addInput(
				new GeoWaveInputKey(
						adapterId,
						new ByteArrayId(
								feature5.getID())),
				feature5);
		mapDriver.addInput(
				new GeoWaveInputKey(
						adapterId,
						new ByteArrayId(
								feature6.getID())),
				feature6);
		mapDriver.addInput(
				new GeoWaveInputKey(
						adapterId,
						new ByteArrayId(
								feature7.getID())),
				feature7);
		mapDriver.addInput(
				new GeoWaveInputKey(
						adapterId,
						new ByteArrayId(
								feature8.getID())),
				feature8);

		final List<Pair<PartitionDataWritable, AdapterWithObjectWritable>> mapperResults = mapDriver.run();
		assertNotNull(getPartitionDataFor(
				mapperResults,
				feature1.getID(),
				true));
		assertNotNull(getPartitionDataFor(
				mapperResults,
				feature2.getID(),
				true));
		assertNotNull(getPartitionDataFor(
				mapperResults,
				feature2.getID(),
				true));
		assertNotNull(getPartitionDataFor(
				mapperResults,
				feature3.getID(),
				true));

		assertEquals(
				getPartitionDataFor(
						mapperResults,
						feature1.getID(),
						true).getId(),
				getPartitionDataFor(
						mapperResults,
						feature3.getID(),
						true).getId());

		assertEquals(
				getPartitionDataFor(
						mapperResults,
						feature6.getID(),
						true).getId(),
				getPartitionDataFor(
						mapperResults,
						feature3.getID(),
						true).getId());

		assertEquals(
				getPartitionDataFor(
						mapperResults,
						feature5.getID(),
						true).getId(),
				getPartitionDataFor(
						mapperResults,
						feature7.getID(),
						true).getId());

		assertEquals(
				getPartitionDataFor(
						mapperResults,
						feature5.getID(),
						true).getId(),
				getPartitionDataFor(
						mapperResults,
						feature4.getID(),
						true).getId());

		final List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> partitions = getReducerDataFromMapperInput(mapperResults);

		reduceDriver.addAll(partitions);

		final List<Pair<GeoWaveInputKey, ObjectWritable>> reduceResults = reduceDriver.run();

		assertEquals(
				2,
				reduceResults.size());

		/*
		 * assertEquals( feature3.getID(), find( reduceResults,
		 * feature1.getID()).toString());
		 * 
		 * assertEquals( feature1.getID(), find( reduceResults,
		 * feature3.getID()).toString());
		 * 
		 * assertEquals( feature4.getID(), find( reduceResults,
		 * feature2.getID()).toString());
		 * 
		 * assertEquals( feature2.getID(), find( reduceResults,
		 * feature4.getID()).toString());
		 */
	}

	private List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> getReducerDataFromMapperInput(
			final List<Pair<PartitionDataWritable, AdapterWithObjectWritable>> mapperResults ) {
		final List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> reducerInputSet = new ArrayList<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>>();
		for (final Pair<PartitionDataWritable, AdapterWithObjectWritable> pair : mapperResults) {
			getListFor(
					pair.getFirst(),
					reducerInputSet).add(
					pair.getSecond());

		}
		return reducerInputSet;
	}

	private List<AdapterWithObjectWritable> getListFor(
			final PartitionDataWritable pd,
			final List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> reducerInputSet ) {
		for (final Pair<PartitionDataWritable, List<AdapterWithObjectWritable>> pair : reducerInputSet) {
			if (pair.getFirst().compareTo(
					pd) == 0) {
				return pair.getSecond();
			}
		}
		final List<AdapterWithObjectWritable> newPairList = new ArrayList<AdapterWithObjectWritable>();
		reducerInputSet.add(new Pair(
				pd,
				newPairList));
		return newPairList;
	}

	private PartitionData getPartitionDataFor(
			final List<Pair<PartitionDataWritable, AdapterWithObjectWritable>> mapperResults,
			final String id,
			final boolean primary ) {
		for (final Pair<PartitionDataWritable, AdapterWithObjectWritable> pair : mapperResults) {
			if (((FeatureWritable) pair.getSecond().getObjectWritable().get()).getFeature().getID().equals(
					id) && (pair.getFirst().partitionData.isPrimary() == primary)) {
				return pair.getFirst().partitionData;
			}
		}
		return null;
	}

	private static boolean coversPoints(
			final Geometry coverer,
			final Geometry pointsToCover ) {
		for (final Coordinate coordinate : pointsToCover.getCoordinates()) {
			if (!coverer.covers(coverer.getFactory().createPoint(
					coordinate))) {
				return false;
			}
		}
		return true;
	}
}
