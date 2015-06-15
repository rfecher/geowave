package mil.nga.giat.geowave.analytics.tools;

import java.util.LinkedList;
import java.util.List;

import javax.measure.quantity.Length;
import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import mil.nga.giat.geowave.analytics.distance.DistanceFn;

import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.GeodeticCalculator;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class GeometryCalculations
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(GeometryCalculations.class);

	final GeometryFactory factory;
	final CoordinateReferenceSystem crs;
	final double xMin, yMin, xMax, yMax;

	public GeometryCalculations(
			final CoordinateReferenceSystem crs ) {
		factory = new GeometryFactory();
		this.crs = crs;
		xMin = crs.getCoordinateSystem().getAxis(
				0).getMinimumValue();
		xMax = crs.getCoordinateSystem().getAxis(
				0).getMaximumValue();
		yMin = crs.getCoordinateSystem().getAxis(
				1).getMinimumValue();
		yMax = crs.getCoordinateSystem().getAxis(
				1).getMaximumValue();
	}

	/**
	 * Build geometries with the provided coordinate at the center. The width of
	 * the geometry is twice the distance provided. More than one geometry is
	 * return when passing the date line.
	 * 
	 * @param distances
	 *            [x,y] = [longitude, latitude]
	 * @param unit
	 * @param coordinate
	 * @return
	 */
	public List<Geometry> buildSurroundingGeometries(
			final double[] distances,
			final Unit<Length> unit,
			final Coordinate coordinate ) {
		final List<Geometry> geos = new LinkedList<Geometry>();
		final GeodeticCalculator geoCalc = new GeodeticCalculator();

		try {
			geoCalc.setStartingGeographicPoint(
					coordinate.x,
					coordinate.y);

			geoCalc.setDirection(
					0,
					unit.getConverterTo(
							SI.METER).convert(
							distances[1]));
			final DirectPosition north = geoCalc.getDestinationPosition();
			geoCalc.setDirection(
					90,
					unit.getConverterTo(
							SI.METER).convert(
							distances[0]));
			final DirectPosition east = geoCalc.getDestinationPosition();
			geoCalc.setStartingGeographicPoint(
					coordinate.x,
					coordinate.y);
			geoCalc.setDirection(
					-90,
					unit.getConverterTo(
							SI.METER).convert(
							distances[0]));
			final DirectPosition west = geoCalc.getDestinationPosition();
			geoCalc.setDirection(
					180,
					unit.getConverterTo(
							SI.METER).convert(
							distances[1]));
			final DirectPosition south = geoCalc.getDestinationPosition();

			final double x1 = west.getOrdinate(0);
			final double x2 = east.getOrdinate(0);
			final double y1 = north.getOrdinate(1);
			final double y2 = south.getOrdinate(1);

			handleBoundaries(
					geos,
					coordinate,
					x1,
					x2,
					y1,
					y2);
		}
		catch (final Exception ex) {
			LOGGER.error(
					"Cannot calculate envelope for {} : {}",
					coordinate.toString(),
					ex.getMessage());
			throw new IllegalArgumentException(
					ex);
		}

		return geos;
	}

	private void handleBoundaries(
			final List<Geometry> geos,
			final Coordinate coordinate,
			final double x1,
			final double x2,
			final double y1,
			final double y2 ) {

		if (Math.signum(x1) > Math.signum(coordinate.x)) {
			ReferencedEnvelope bounds = new ReferencedEnvelope(
					x1,
					xMax,
					Math.max(
							y1,
							yMin),
					Math.min(
							y2,
							yMax),
					crs);
			geos.add(factory.toGeometry(bounds));
			bounds = new ReferencedEnvelope(
					xMin,
					x2,
					Math.max(
							y1,
							yMin),
					Math.min(
							y2,
							yMax),
					crs);
			geos.add(factory.toGeometry(bounds));
		}
		else if (Math.signum(x2) < Math.signum(coordinate.x)) {
			ReferencedEnvelope bounds = new ReferencedEnvelope(
					xMin,
					x2,
					Math.max(
							y1,
							yMin),
					Math.min(
							y2,
							yMax),
					crs);
			geos.add(factory.toGeometry(bounds));
			bounds = new ReferencedEnvelope(
					x1,
					xMax,
					Math.max(
							y1,
							yMin),
					Math.min(
							y2,
							yMax),
					crs);
			geos.add(factory.toGeometry(bounds));
		}
		else {
			final ReferencedEnvelope bounds = new ReferencedEnvelope(
					x1,
					x2,
					Math.max(
							y1,
							yMin),
					Math.min(
							y2,
							yMax),
					crs);
			geos.add(factory.toGeometry(bounds));
		}

	}
	/*
	 * public static double measureDistanceBetweenGeometries( final
	 * DistanceFn<Coordinate> distanceFnForCoordinate, final Geometry g1, final
	 * Geometry g2 ) { if (g1.distance(g2) <= 0.0) { return 0.0; } Coordinate
	 * minG1 = null, minG2 = null;
	 * 
	 * double minDist = Double.MAX_VALUE; final Coordinate[] g1Coords =
	 * g1.getCoordinates(); final Coordinate[] g2Coords = g2.getCoordinates();
	 * int g1Pos, g2Pos; for (int g1i = 0; g1i < g1Coords.length; g1i++) { final
	 * Coordinate g1coord = g1Coords[g1i]; for (int g2i = 0; g2i <
	 * g1Coords.length; g2i++) { final Coordinate g2coord = g2Coords[g2i]; final
	 * double dist = (distanceFnForCoordinate.measure( g1coord, g2coord)); if
	 * (dist < minDist) { minG1 = g1coord; minG2 = g2coord; minDist = dist;
	 * g1Pos = g1i; g2Pos = g2i; } } } if (g1Coords.length == 1) { if
	 * (g2Coords.length == 1) { // one point to one point return minDist; } }
	 * final double base = distanceFnForCoordinate.measure( minG1, lastMinG1);
	 * final double s1 = distanceFnForCoordinate.measure( minG1, minG2); final
	 * double s2 = distanceFnForCoordinate.measure( lastMinG2, minG2); final
	 * double p = (s1 + s2 + base) / 2.0; final double area = Math.sqrt(p * (p -
	 * base) * (p - s1) * (p - s2)); return (2 * area) / base; }
	 */
}
