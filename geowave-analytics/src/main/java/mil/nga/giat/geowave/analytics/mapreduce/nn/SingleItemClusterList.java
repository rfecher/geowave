package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.analytics.distance.DistanceFn;
import mil.nga.giat.geowave.analytics.tools.GeometryHullTool;
import mil.nga.giat.geowave.index.ByteArrayId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.operation.distance.DistanceOp;

/**
 * 
 * Maintains a single hull around a set of points.
 * 
 * Intended to run in a single thread. Not Thread Safe.
 * 
 * 
 * TODO: connectGeometryTool.connect(
 */
public class SingleItemClusterList extends
		DBScanClusterList implements
		CompressingCluster<ClusterItem, Geometry>
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(SingleItemClusterList.class);

	// internal state
	private Geometry clusterGeo;
	private final Set<Coordinate> clusterPoints = new HashSet<Coordinate>();

	private final GeometryHullTool connectGeometryTool = new GeometryHullTool();

	public SingleItemClusterList(
			final DistanceFn<Coordinate> distanceFnForCoordinate,
			final ByteArrayId centerId,
			final ClusterItem center,
			final Map<ByteArrayId, Cluster<ClusterItem>> index ) {
		super(
				centerId,
				index);

		this.connectGeometryTool.setDistanceFnForCoordinate(distanceFnForCoordinate);

		final Geometry clusterGeo = center.getGeometry();

		// start with the center. TODO Should change to a buffer!!!
		this.clusterGeo = clusterGeo.getCentroid();

		for (Coordinate coordinate : this.clusterGeo.getCoordinates()) {
			clusterPoints.add(coordinate);
		}

		add(
				centerId,
				center);
	}

	@Override
	protected Long addAndFetchCount(
			final ByteArrayId id,
			final ClusterItem newInstance ) {
		Geometry newGeo = newInstance.getGeometry();

		newGeo = clip(
				clusterGeo,
				newGeo);

		if (!clusterGeo.covers(newGeo)) {
			for (final Coordinate coordinate : newGeo.getCoordinates())
				this.clusterPoints.add(coordinate);
			checkForCompression();
		}

		return ONE;
	}

	private Geometry clip(
			final Geometry centerGeo,
			final Geometry geometry ) {
		if (geometry instanceof Point) {
			return geometry;
		}
		final DistanceOp op = new DistanceOp(
				centerGeo,
				geometry);
		// TODO : buffer distance
		return centerGeo.getFactory().createPoint(
				op.nearestPoints()[1]);
	}

	@Override
	public void merge(
			Cluster<ClusterItem> cluster ) {
		super.merge(cluster);
		this.clusterPoints.addAll(((SingleItemClusterList) cluster).clusterPoints);
		checkForCompression();
	}

	private void checkForCompression() {
		if (clusterPoints.size() > 20) {
			clusterGeo = compress();
			clusterPoints.clear();
		}
	}

	@Override
	protected Geometry compress() {

		if (clusterPoints.isEmpty()) return clusterGeo;
		final Set<Coordinate> batchCoords = new HashSet<Coordinate>();

		for (final Coordinate coordinate : clusterGeo.getCoordinates()) {
			batchCoords.add(coordinate);
		}
		for (final Coordinate coordinate : clusterPoints) {
			batchCoords.add(coordinate);
		}

		final Coordinate[] actualCoords = batchCoords.toArray(new Coordinate[batchCoords.size()]);

		if (batchCoords.size() == 2) {
			return clusterGeo.getFactory().createLineString(
					actualCoords);
		}

		final ConvexHull convexHull = new ConvexHull(
				actualCoords,
				clusterGeo.getFactory());

		final Geometry hull = convexHull.getConvexHull();

		try {
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
			LOGGER.warn(getId().toString());

			return hull;
		}

	}

	public static class SingleItemClusterListFactory implements
			NeighborListFactory<ClusterItem>
	{
		private final DistanceFn<Coordinate> distanceFnForCoordinate;
		private final Map<ByteArrayId, Cluster<ClusterItem>> index;

		public SingleItemClusterListFactory(
				final DistanceFn<Coordinate> distanceFnForCoordinate,
				final Map<ByteArrayId, Cluster<ClusterItem>> index ) {
			super();
			this.distanceFnForCoordinate = distanceFnForCoordinate;
			this.index = index;
		}

		public NeighborList<ClusterItem> buildNeighborList(
				final ByteArrayId centerId,
				final ClusterItem center ) {
			return new SingleItemClusterList(
					distanceFnForCoordinate,
					centerId,
					center,
					index);
		}
	}
}
