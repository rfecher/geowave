package mil.nga.giat.geowave.analytics.mapreduce.nn;

import mil.nga.giat.geowave.analytics.distance.CoordinateCircleDistanceFn;
import mil.nga.giat.geowave.analytics.distance.DistanceFn;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.operation.distance.DistanceOp;

/**
 * Calculate distance between two SimpleFeatures, assuming has a Geometry.
 * 
 * @see org.opengis.feature.simple.SimpleFeature
 * 
 */
public class ClusterItemDistanceFn implements
		DistanceFn<ClusterItem>
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3824608959408031752L;
	private DistanceFn<Coordinate> coordinateDistanceFunction = new CoordinateCircleDistanceFn();

	public ClusterItemDistanceFn() {}

	public ClusterItemDistanceFn(
			final DistanceFn<Coordinate> coordinateDistanceFunction ) {
		super();
		this.coordinateDistanceFunction = coordinateDistanceFunction;
	}

	public DistanceFn<Coordinate> getCoordinateDistanceFunction() {
		return coordinateDistanceFunction;
	}

	public void setCoordinateDistanceFunction(
			final DistanceFn<Coordinate> coordinateDistanceFunction ) {
		this.coordinateDistanceFunction = coordinateDistanceFunction;
	}

	@Override
	public double measure(
			final ClusterItem x,
			final ClusterItem y ) {

		final DistanceOp op = new DistanceOp(
				x.getGeometry(),
				y.getGeometry());
		Coordinate[] points = op.nearestPoints();
		return coordinateDistanceFunction.measure(
				points[0],
				points[1]);
	}
}
