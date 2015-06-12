package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import mil.nga.giat.geowave.accumulo.mapreduce.HadoopWritableSerializer;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.analytics.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytics.distance.CoordinateCircleDistanceFn;
import mil.nga.giat.geowave.analytics.distance.DistanceFn;
import mil.nga.giat.geowave.analytics.mapreduce.nn.NNMapReduce.NNReducer;
import mil.nga.giat.geowave.analytics.mapreduce.nn.NNMapReduce.PartitionDataWritable;
import mil.nga.giat.geowave.analytics.parameters.ClusteringParameters;
import mil.nga.giat.geowave.analytics.parameters.CommonParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.HullParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.tools.AdapterWithObjectWritable;
import mil.nga.giat.geowave.analytics.tools.AnalyticFeature;
import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.analytics.tools.GeometryHullTool;
import mil.nga.giat.geowave.analytics.tools.Projection;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureProjection;
import mil.nga.giat.geowave.analytics.tools.mapreduce.JobContextConfigurationWrapper;
import mil.nga.giat.geowave.analytics.tools.partitioners.Partitioner.PartitionData;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.vector.adapter.FeatureWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.geotools.feature.type.BasicFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class DBScanMapReduce
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(DBScanMapReduce.class);

	public abstract static class DBScanMapReducer<VALUEIN, KEYOUT, VALUEOUT> extends
			NNReducer<VALUEIN, KEYOUT, VALUEOUT, Map<ByteArrayId, Cluster<VALUEIN>>>
	{
		protected int minOwners = 0;
		protected ClusterMemberSize<VALUEIN> memberSizeFn = new ClusterMemberSize<VALUEIN>() {
			@Override
			public long getCount(
					final Cluster<VALUEIN> cluster ) {
				return cluster.getSize();
			}
		};

		protected HullBuilder<VALUEIN> hullBuilder;

		@Override
		public List<Map.Entry<ByteArrayId, VALUEIN>> createNeighborsList() {
			// one less neighbor so that the NN component does not clip on our
			// behalf
			return new ClippedList<VALUEIN>(
					super.maxNeighbors - 1);
		}

		@Override
		protected Map<ByteArrayId, Cluster<VALUEIN>> createSummary() {
			return new HashMap<ByteArrayId, Cluster<VALUEIN>>();
		}

		@Override
		protected void processNeighbors(
				final PartitionData partitionData,
				final ByteArrayId primaryId,
				final VALUEIN primary,
				final List<Map.Entry<ByteArrayId, VALUEIN>> neighbors,
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context,
				final Map<ByteArrayId, Cluster<VALUEIN>> summary )
				throws IOException,
				InterruptedException {

			if (neighbors.size() < minOwners) {
				return;
			}
			Cluster.mergeClusters(
					summary,
					new Cluster<VALUEIN>(
							memberSizeFn,
							primaryId,
							primary,
							(ClippedList<VALUEIN>) neighbors));

		}

		@Override
		protected void setup(
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);
			final ConfigurationWrapper config = new JobContextConfigurationWrapper(
					context);

			// first run must at least form a triangle
			minOwners = config.getInt(
					ClusteringParameters.Clustering.MINIMUM_SIZE,
					NNMapReduce.class,
					2);

			LOGGER.info(
					"Minumum owners = {}",
					minOwners);

			try {
				hullBuilder = config.getInstance(
						HullParameters.Hull.HULL_BUILDER,
						NNMapReduce.class,
						HullBuilder.class,
						PointMergeBuilder.class);

				hullBuilder.initialize(config);
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}
		}
	}

	public static class DBScanMapHullReducer<VALUEIN> extends
			DBScanMapReducer<VALUEIN, GeoWaveInputKey, ObjectWritable>
	{
		private String batchID;
		private int zoomLevel = 1;
		private int iteration = 1;
		private FeatureDataAdapter outputAdapter;

		private final ObjectWritable output = new ObjectWritable();

		@Override
		protected void processSummary(
				final PartitionData partitionData,
				final Map<ByteArrayId, Cluster<VALUEIN>> summary,
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
				throws IOException,
				InterruptedException {
			final HadoopWritableSerializer<SimpleFeature, FeatureWritable> serializer = outputAdapter.createWritableSerializer();
			final Set<Cluster<VALUEIN>> processed = new HashSet<Cluster<VALUEIN>>();
			for (final Map.Entry<ByteArrayId, Cluster<VALUEIN>> entry : summary.entrySet()) {
				final Cluster<VALUEIN> cluster = entry.getValue();
				if (!processed.contains(cluster)) {
					processed.add(cluster);
					final SimpleFeature newPolygonFeature = AnalyticFeature.createGeometryFeature(
							outputAdapter.getType(),
							batchID,
							cluster.id.getString(),
							cluster.id.getString(), // name
							partitionData.getGroupId() != null ? partitionData.getGroupId().toString() : entry.getKey().getString(), // group
							0.0,
							hullBuilder.getProjection(cluster),
							new String[0],
							new double[0],
							zoomLevel,
							iteration,
							cluster.size);
					output.set(serializer.toWritable(newPolygonFeature));
					// ShapefileTool.writeShape(
					// cluster.center.getId().getString() + iteration,
					// / new File(
					// "./target/testdb_" + cluster.center.getId().getString() +
					// iteration),
					// new Geometry[] {
					// (Geometry) newPolygonFeature.getDefaultGeometry()
					// });
					context.write(
							new GeoWaveInputKey(
									outputAdapter.getAdapterId(),
									new ByteArrayId(
											newPolygonFeature.getID())),
							output);
				}
			}
		}

		@Override
		protected void setup(
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
				throws IOException,
				InterruptedException {

			final ConfigurationWrapper config = new JobContextConfigurationWrapper(
					context);

			super.setup(context);

			batchID = config.getString(
					GlobalParameters.Global.BATCH_ID,
					NNMapReduce.class,
					UUID.randomUUID().toString());

			zoomLevel = config.getInt(
					HullParameters.Hull.ZOOM_LEVEL,
					NNMapReduce.class,
					1);

			iteration = config.getInt(
					HullParameters.Hull.ITERATION,
					NNMapReduce.class,
					1);

			final String polygonDataTypeId = config.getString(
					HullParameters.Hull.DATA_TYPE_ID,
					NNMapReduce.class,
					"concave_hull");

			outputAdapter = AnalyticFeature.createGeometryFeatureAdapter(
					polygonDataTypeId,
					new String[0],
					config.getString(
							HullParameters.Hull.DATA_NAMESPACE_URI,
							NNMapReduce.class,
							BasicFeatureTypes.DEFAULT_NAMESPACE),
					ClusteringUtils.CLUSTERING_CRS);

			memberSizeFn = new ClusterMemberSize<VALUEIN>() {
				@Override
				public long getCount(
						final Cluster<VALUEIN> cluster ) {
					long count = cluster.members.size();
					if (cluster.center instanceof SimpleFeature) {
						final SimpleFeature sf = (SimpleFeature) cluster.center;
						// this occurs in the case of multiple iterations of DB
						// SCAN
						if (sf.getFeatureType().getName().getLocalPart().equals(
								outputAdapter.getType().getName().getLocalPart())) {
							count = (Long) sf.getAttribute(AnalyticFeature.ClusterFeatureAttribute.COUNT.attrName());
							for (final Map.Entry<ByteArrayId, VALUEIN> member : cluster.members) {
								final SimpleFeature sfm = (SimpleFeature) member.getValue();
								count += (Long) sfm.getAttribute(AnalyticFeature.ClusterFeatureAttribute.COUNT.attrName());
							}
						}
					}
					return count;
				}
			};

		}
	}

	public static interface HullBuilder<VALUEIN> extends
			Projection<Cluster<VALUEIN>>
	{
	}

	public static class PointMergeBuilder<VALUEIN> implements
			HullBuilder<VALUEIN>
	{
		Projection<VALUEIN> projectionFunction;
		DistanceFn<Coordinate> distanceFunction;
		GeometryHullTool connectGeometryTool = new GeometryHullTool();

		@SuppressWarnings("unchecked")
		@Override
		public void initialize(
				final ConfigurationWrapper context )
				throws IOException {
			try {
				projectionFunction = context.getInstance(
						HullParameters.Hull.PROJECTION_CLASS,
						HullBuilder.class,
						Projection.class,
						SimpleFeatureProjection.class);
			}
			catch (InstantiationException | IllegalAccessException e) {
				throw new IOException(
						e);
			}

			try {
				distanceFunction = context.getInstance(
						CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
						HullBuilder.class,
						DistanceFn.class,
						CoordinateCircleDistanceFn.class);
			}
			catch (InstantiationException | IllegalAccessException e) {
				throw new IOException(
						e);
			}

			connectGeometryTool.setDistanceFnForCoordinate(distanceFunction);
		}

		@Override
		public void setup(
				final PropertyManagement runTimeProperties,
				final Configuration configuration ) {
			RunnerUtils.setParameter(
					configuration,
					HullBuilder.class,
					runTimeProperties,
					new ParameterEnum[] {
						CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
						HullParameters.Hull.PROJECTION_CLASS
					});

			projectionFunction.setup(
					runTimeProperties,
					configuration);
		}

		@Override
		public Geometry getProjection(
				final Cluster<VALUEIN> cluster ) {
			if (cluster.members.isEmpty()) {
				return projectionFunction.getProjection(cluster.center);
			}
			final Set<Coordinate> batchCoords = new HashSet<Coordinate>();
			for (final Coordinate coordinate : projectionFunction.getProjection(
					cluster.center).getCoordinates()) {
				batchCoords.add(coordinate);
			}
			for (final Map.Entry<ByteArrayId, VALUEIN> member : cluster.members) {
				for (final Coordinate coordinate : projectionFunction.getProjection(
						member.getValue()).getCoordinates()) {
					batchCoords.add(coordinate);
				}
			}
			LOGGER.info(
					"Cluster {} with size {}",
					cluster.id.toString(),
					batchCoords.size());

			final GeometryFactory factory = new GeometryFactory();
			if (batchCoords.size() == 1) {
				return factory.createPoint(batchCoords.iterator().next());
			}

			final Coordinate[] actualCoords = batchCoords.toArray(new Coordinate[batchCoords.size()]);

			if (batchCoords.size() == 2) {
				return factory.createLineString(actualCoords);
			}

			try {
				// generate convex hull for current batch of points
				final ConvexHull convexHull = new ConvexHull(
						actualCoords,
						factory);

				final Geometry hull = convexHull.getConvexHull();
				if (batchCoords.size() > 5) {
					return connectGeometryTool.concaveHull(
							hull,
							batchCoords);
				}
				else {
					return hull;
				}
			}
			catch (final Exception ex) {
				LOGGER.error(
						"Failed to compute hull",
						ex);
				LOGGER.warn(cluster.center.toString());
				for (final Map.Entry<ByteArrayId, VALUEIN> member : cluster.members) {
					LOGGER.warn(member.getValue().toString());
				}
				throw ex;
			}

		}
	}

	public static class HullMergeBuilder extends
			PointMergeBuilder<SimpleFeature> implements
			HullBuilder<SimpleFeature>
	{

		private Geometry addToHull(
				final Geometry hull,
				final Geometry hulToUnion ) {
			final ConvexHull convexHull1 = new ConvexHull(
					hull);
			final ConvexHull convexHull2 = new ConvexHull(
					hulToUnion);

			return convexHull1.getConvexHull().union(
					convexHull2.getConvexHull());

		}

		@Override
		public Geometry getProjection(
				final Cluster<SimpleFeature> cluster ) {
			if (cluster.members.isEmpty()) {
				return projectionFunction.getProjection(cluster.center);
			}
			Geometry hull = (Geometry) cluster.center.getDefaultGeometry();

			for (final Map.Entry<ByteArrayId, SimpleFeature> member : cluster.members) {
				final Geometry hulltoUnion = (Geometry) member.getValue().getDefaultGeometry();
				try {
					hull = hull.union(hulltoUnion);
				}
				catch (final com.vividsolutions.jts.geom.TopologyException ex) {
					hull = addToHull(
							hull,
							hulltoUnion);
					LOGGER.warn(
							"Exception occurred merging concave hulls",
							ex);
				}
			}
			return hull;
		}
	}

	public static class Cluster<VALUE>
	{
		final protected VALUE center;
		final protected ClippedList<VALUE> members;
		protected long size = 0;
		protected double density = 0;
		final private ClusterMemberSize<VALUE> memberSizeFn;
		final private ByteArrayId id;
		final private boolean isHull = false;

		public Cluster(
				final ClusterMemberSize<VALUE> memberSizeFn,
				final ByteArrayId centerId,
				final VALUE center,
				final ClippedList<VALUE> members ) {
			super();
			this.id = centerId;
			this.center = center;
			this.memberSizeFn = memberSizeFn;
			this.members = members;
			this.size = memberSizeFn.getCount(this);
			density = size;
		}

		public Cluster(
				final ByteArrayId centerId,
				final VALUE center,
				final ClippedList<VALUE> members ) {
			super();
			this.id = centerId;
			this.center = center;
			this.memberSizeFn = new ClusterMemberSize<VALUE>() {

				@Override
				public long getCount(
						final Cluster<VALUE> cluster ) {
					return cluster.members.addCount();
				}
			};
			this.members = members;
			this.size = memberSizeFn.getCount(this);
			density = size;
		}

		public static <VALUE> void mergeClusters(
				final Map<ByteArrayId, Cluster<VALUE>> index,
				final Cluster<VALUE> newCluster ) {

			// if already clustered
			if (index.containsKey(newCluster.id)) {
				index.get(
						newCluster.id).merge(
						newCluster,
						index);

				return;
			}

			// if a member is clustered (reachable) from another cluster
			for (final Map.Entry<ByteArrayId, VALUE> member : newCluster.members) {
				if (index.containsKey(member.getKey())) {
					final Cluster<VALUE> cluster = index.get(member.getKey());
					cluster.merge(
							newCluster,
							index);
					return;
				}
			}

			index.put(
					newCluster.id,
					newCluster);

			for (final Map.Entry<ByteArrayId, VALUE> neighbor : newCluster.members) {
				index.put(
						neighbor.getKey(),
						newCluster);
			}
		}

		public void merge(
				final Cluster<VALUE> clusterToMerge,
				final Map<ByteArrayId, Cluster<VALUE>> index ) {
			// drop off
			final double drop = (clusterToMerge.density / this.density);
			if ((drop < 0.1) || (drop > 10)) {
				return;
			}
			density = Math.min(
					clusterToMerge.density,
					this.density);
			index.put(
					clusterToMerge.id,
					this);

			// if (members.size() + clusterToMerge.members.size() >
			// members.getMaxSize()) {
			// Geometry thisHull = hullBuilder.getProjection(this);
			// Geometry otherHull = hullBuilder.getProjection(clusterToMerge);
			// }

			if (index.get(clusterToMerge.id) != this) {
				members.add(new AbstractMap.SimpleEntry<ByteArrayId, VALUE>(
						clusterToMerge.id,
						clusterToMerge.center));
			}

			for (final Map.Entry<ByteArrayId, VALUE> neighbor : clusterToMerge.members) {
				if (index.get(neighbor.getKey()) != this) {
					members.add(neighbor);
					index.put(
							neighbor.getKey(),
							this);
				}
			}
			clusterToMerge.members.clear();
			// update the merged count
			this.size = memberSizeFn.getCount(this);
		}

		public long getSize() {
			return size;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((id == null) ? 0 : id.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final Cluster other = (Cluster) obj;
			if (id == null) {
				if (other.id != null) {
					return false;
				}
			}
			else if (!id.equals(other.id)) {
				return false;
			}
			return true;
		}
	}

	public interface ClusterMemberSize<VALUE>
	{
		long getCount(
				Cluster<VALUE> cluster );
	}

}
