package mil.nga.giat.geowave.analytics.kmeans.mapreduce;

import mil.nga.giat.geowave.analytics.distance.CoordinateEuclideanDistanceFn;
import mil.nga.giat.geowave.analytics.distance.DistanceFn;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class TestObjectDistanceFn implements
		DistanceFn<TestObject>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final DistanceFn<Coordinate> coordinateDistanceFunction = new CoordinateEuclideanDistanceFn();

	private Geometry getGeometry(
			final TestObject x ) {
		return x.geo;
	}

	@Override
	public double measure(
			final TestObject x,
			final TestObject y ) {

		return coordinateDistanceFunction.measure(
				getGeometry(
						x).getCentroid().getCoordinate(),
				getGeometry(
						y).getCentroid().getCoordinate());
	}

}
