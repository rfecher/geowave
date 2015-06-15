package mil.nga.giat.geowave.analytics.tools;

import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class GeometryMergeTest
{

	GeometryFactory factory = new GeometryFactory();

	@Test
	public void test()
			throws NoSuchAuthorityCodeException,
			FactoryException,
			TransformException {

		final Geometry triangle1 = factory.createPolygon(new Coordinate[] {
			new Coordinate(
					10,
					10),
			new Coordinate(
					9.998,
					9.998),
			new Coordinate(
					10.002,
					9.998),
			new Coordinate(
					10,
					10)
		});

		final Geometry triangle2 = factory.createPolygon(new Coordinate[] {
			new Coordinate(
					10,
					9.9998),
			new Coordinate(
					9.998,
					10.002),
			new Coordinate(
					10.002,
					10.002),
			new Coordinate(
					10,
					9.9998)
		});

		final Geometry triangle3 = factory.createPolygon(new Coordinate[] {
			new Coordinate(
					11,
					10.9998),
			new Coordinate(
					10.998,
					11.002),
			new Coordinate(
					11.002,
					11.002),
			new Coordinate(
					11,
					10.9998)
		});

		final Geometry square1 = factory.createPolygon(new Coordinate[] {
			new Coordinate(
					11,
					10.98),
			new Coordinate(
					10.98,
					11.02),
			new Coordinate(
					11.02,
					11.02),
			new Coordinate(
					11.02,
					10.98),
			new Coordinate(
					11,
					10.98)
		});

		final Geometry square2 = factory.createPolygon(new Coordinate[] {
			new Coordinate(
					10.99,
					10.97),
			new Coordinate(
					10.99,
					10.99),
			new Coordinate(
					11.01,
					10.99),
			new Coordinate(
					11.1,
					10.97),
			new Coordinate(
					10.99,
					10.97)
		});

		final Geometry diamond = triangle2.intersection(triangle1);
		final Geometry box = square2.intersection(square1);
		final Geometry diamond2 = triangle2.symDifference(triangle1);
		final Geometry hull = triangle2.union(triangle1);
		final double dist = triangle1.distance(triangle3);

		final Coordinate[] hourGlassCoords = diamond.getCoordinates();
		final Coordinate[] diamondCoords = diamond2.getCoordinates();
		// assertEquals(
		// 18,
		// hourGlassCoords.length);
	}
}
