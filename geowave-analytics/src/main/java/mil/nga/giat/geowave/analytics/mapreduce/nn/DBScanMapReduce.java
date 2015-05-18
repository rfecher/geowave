package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
import mil.nga.giat.geowave.analytics.tools.NeighborData;
import mil.nga.giat.geowave.analytics.tools.Projection;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.ShapefileTool;
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
					Cluster<VALUEIN> cluster ) {
				return cluster.members.size();
			}
		};

		@Override
		protected Map<ByteArrayId, Cluster<VALUEIN>> createSummary() {
			return new HashMap<ByteArrayId, Cluster<VALUEIN>>();
		}

		@Override
		protected void processNeighbors(
				final PartitionData partitionData,
				final NeighborData<VALUEIN> primary,
				final Set<NeighborData<VALUEIN>> neighbors,
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
							primary,
							neighbors));

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
		}
	}

	public static class DBScanMapHullReducer<VALUEIN> extends
			DBScanMapReducer<VALUEIN, GeoWaveInputKey, ObjectWritable>
	{
		private String batchID;
		private int zoomLevel = 1;
		private int iteration = 1;
		private FeatureDataAdapter outputAdapter;
		private HullBuilder<VALUEIN> hullBuilder;

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
							cluster.center.getId().getString(),
							cluster.center.getId().getString(), // name
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
						Cluster<VALUEIN> cluster ) {
					long count = cluster.members.size();
					if (cluster.center.getElement() instanceof SimpleFeature) {
						final SimpleFeature sf = (SimpleFeature) cluster.center.getElement();
						// this occurs in the case of multiple iterations of DB
						// SCAN
						if (sf.getFeatureType().getName().getLocalPart().equals(
								outputAdapter.getType().getName().getLocalPart())) {
							count = (Long) sf.getAttribute(AnalyticFeature.ClusterFeatureAttribute.COUNT.attrName());
							for (final NeighborData<VALUEIN> members : cluster.members) {
								final SimpleFeature sfm = (SimpleFeature) members.getElement();
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
			if (cluster.members.isEmpty()) return projectionFunction.getProjection(cluster.center.getElement());
			final List<Coordinate> batchCoords = new ArrayList<Coordinate>();
			for (final Coordinate coordinate : projectionFunction.getProjection(
					cluster.center.getElement()).getCoordinates()) {
				batchCoords.add(coordinate);
			}
			for (final NeighborData<VALUEIN> member : cluster.members) {
				for (final Coordinate coordinate : projectionFunction.getProjection(
						member.getElement()).getCoordinates()) {
					batchCoords.add(coordinate);
				}
			}
			final Coordinate[] actualCoords = batchCoords.toArray(new Coordinate[batchCoords.size()]);

			// generate convex hull for current batch of points
			final ConvexHull convexHull = new ConvexHull(
					actualCoords,
					new GeometryFactory());

			final Geometry hull = convexHull.getConvexHull();
			return connectGeometryTool.concaveHull(
					hull,
					batchCoords);
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
			if (cluster.members.isEmpty()) return projectionFunction.getProjection(cluster.center.getElement());
			Geometry hull = (Geometry) cluster.center.getElement().getDefaultGeometry();

			for (final NeighborData<SimpleFeature> member : cluster.members) {
				Geometry hulltoUnion = (Geometry) member.getElement().getDefaultGeometry();
				try {
					hull = hull.union(hulltoUnion);
				}
				catch (com.vividsolutions.jts.geom.TopologyException ex) {
					hull = addToHull(
							hull,
							hulltoUnion);
					LOGGER.warn(
							"Exception occurred mergeing concave hulls",
							ex);
				}
			}
			return hull;
		}
	}

	public static class Cluster<VALUE>
	{
		final protected NeighborData<VALUE> center;
		final protected Set<NeighborData<VALUE>> members;
		protected long size = 0;
		protected double density = 0;
		final private ClusterMemberSize<VALUE> memberSizeFn;

		public Cluster(
				final ClusterMemberSize<VALUE> memberSizeFn,
				final NeighborData<VALUE> center,
				final Set<NeighborData<VALUE>> members ) {
			super();
			this.center = center;
			this.memberSizeFn = memberSizeFn;
			this.members = new HashSet<NeighborData<VALUE>>(
					members);
			this.size = memberSizeFn.getCount(this);
			density = size;
		}

		public Cluster(
				final NeighborData<VALUE> center,
				final Set<NeighborData<VALUE>> members ) {
			super();
			this.center = center;
			this.memberSizeFn = new ClusterMemberSize<VALUE>() {

				@Override
				public long getCount(
						Cluster<VALUE> cluster ) {
					return cluster.members.size();
				}
			};
			this.members = new HashSet<NeighborData<VALUE>>(
					members);
			this.size = memberSizeFn.getCount(this);
			density = size;
		}

		public static <VALUE> void mergeClusters(

				final Map<ByteArrayId, Cluster<VALUE>> index,
				final Cluster<VALUE> newCluster ) {
			if (index.containsKey(newCluster.center.getId())) {
				index.get(
						newCluster.center.getId()).merge(
						newCluster,
						index);

				return;
			}

			for (final NeighborData<VALUE> member : newCluster.members) {
				if (index.containsKey(member.getId())) {
					Cluster<VALUE> cluster = index.get(member.getId());
					cluster.merge(
							newCluster,
							index);
					return;
				}
			}

			index.put(
					newCluster.center.getId(),
					newCluster);

			for (final NeighborData<VALUE> neighbor : newCluster.members) {
				index.put(
						neighbor.getId(),
						newCluster);
			}
			index.put(
					newCluster.center.getId(),
					newCluster);
		}

		public void merge(
				final Cluster<VALUE> clusterToMerge,
				final Map<ByteArrayId, Cluster<VALUE>> index ) {
			// drop off
			final double drop = (clusterToMerge.density / this.density);
			if (drop < 0.1 || drop > 10) {
				return;
			}
			density = Math.min(
					clusterToMerge.density,
					this.density);

			index.put(
					clusterToMerge.center.getId(),
					this);
			members.add(clusterToMerge.center);
			for (final NeighborData<VALUE> neighbor : clusterToMerge.members) {
				members.add(neighbor);
				index.put(
						neighbor.getId(),
						this);
			}
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
			result = prime * result + ((center.getId() == null) ? 0 : center.getId().hashCode());
			return result;
		}

		@Override
		public boolean equals(
				Object obj ) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			Cluster other = (Cluster) obj;
			if (center == null) {
				if (other.center != null) return false;
			}
			else if (!center.getId().equals(
					other.center.getId())) return false;
			return true;
		}
	}

	public interface ClusterMemberSize<VALUE>
	{
		long getCount(
				Cluster<VALUE> cluster );
	}
}
