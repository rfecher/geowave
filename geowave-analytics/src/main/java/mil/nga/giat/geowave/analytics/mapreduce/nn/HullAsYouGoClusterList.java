package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import mil.nga.giat.geowave.analytics.distance.DistanceFn;
import mil.nga.giat.geowave.analytics.tools.AnalyticFeature;
import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.analytics.tools.GeometryHullTool;
import mil.nga.giat.geowave.analytics.tools.Projection;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.index.ByteArrayId;

import org.apache.hadoop.conf.Configuration;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.operation.distance.DistanceOp;

/**
 * 
 * Maintains a single hull.
 * 
 * Intended to run in a single thread. Not Thread Safe.
 * 
 * 
 * @param <VALUEIN>
 */
public class HullAsYouGoClusterList implements
		NeighborList<SimpleFeature>,
		ConvertingList<Geometry>
{
	private int addCount;
	private final int maxSize;
	private Projection<SimpleFeature> projectionFunction;
	private String clusterFeatureTypeName;
	private Geometry clusterGeo;
	private List<Coordinate> clusterPoints = new ArrayList<Coordinate>();
	private GeometryHullTool connectGeometryTool = new GeometryHullTool();
	// maintains the count of a geometry representing a cluster of points
	private HashMap<ByteArrayId, Long> clusteredGeometryCounts = null;

	public HullAsYouGoClusterList(
			final String clusterFeatureTypeName,
			final Projection<SimpleFeature> projectionFunction,
			final DistanceFn<Coordinate> distanceFnForCoordinate,
			final int maxSize,
			final ByteArrayId centerId,
			final SimpleFeature center ) {
		super();

		this.connectGeometryTool.setDistanceFnForCoordinate(distanceFnForCoordinate);

		this.projectionFunction = projectionFunction == null ? new Projection<SimpleFeature>() {

			@Override
			public Geometry getProjection(
					SimpleFeature anItem ) {
				return (Geometry) anItem.getDefaultGeometry();
			}

			@Override
			public void initialize(
					ConfigurationWrapper context )
					throws IOException {

			}

			@Override
			public void setup(
					PropertyManagement runTimeProperties,
					Configuration configuration ) {}
		} : projectionFunction;

		this.maxSize = maxSize;
		this.clusterFeatureTypeName = clusterFeatureTypeName;
		final Geometry clusterGeo = (this.projectionFunction.getProjection(center));

		// start with the center. TODO Should change to a buffer!!!
		this.clusterGeo = (!center.getFeatureType().getName().getLocalPart().equals(
				clusterFeatureTypeName)) ? clusterGeo.getCentroid() : clusterGeo;

		for (Coordinate coordinate : this.clusterGeo.getCoordinates()) {
			clusterPoints.add(coordinate);
		}
		this.add(
				centerId,
				center);
	}

	private static final Long ONE = 1L;
	private static final Long ZERO = 0L;

	@Override
	public boolean add(
			final Entry<ByteArrayId, SimpleFeature> entry ) {
		return this.add(
				entry.getKey(),
				entry.getValue());
	}

	private boolean add(
			final ByteArrayId id,
			final SimpleFeature newInstance ) {
		if (getCount(id) != 0) return false;
		Geometry newGeo = projectionFunction.getProjection(newInstance);
		Long count = ONE;
		if (newInstance.getFeatureType().getName().getLocalPart().equals(
				clusterFeatureTypeName)) {
			count = (Long) newInstance.getAttribute(AnalyticFeature.ClusterFeatureAttribute.COUNT.attrName());
			clusterGeo = clusterGeo.union(newGeo);
		}
		else {
			newGeo = clip(
					clusterGeo,
					newGeo);

			Geometry differentSet = newGeo;
			try {
			 differentSet = newGeo.difference(clusterGeo);
			}
			catch (Exception ex) {
				ex.printStackTrace();
			}
			if (!differentSet.isEmpty()) {

				// not a cluster geometry...shrink it to size of this centroid
				// since it may be big (line, polygon, etc.)

				clusterGeo = GeometryHullTool.addPoints(
						clusterGeo,
						differentSet.getCoordinates());
			}
			else {
				for (final Coordinate coordinate : newGeo.getCoordinates())
					this.clusterPoints.add(coordinate);
			}
		}

		putCount(
				id,
				count);
		addCount += count;

		return true;
	}

	public int getMaxSize() {
		return maxSize;
	}

	@Override
	public void clear() {
		addCount = 0;
		clusteredGeometryCounts = null;
	}

	@Override
	public boolean contains(
			final ByteArrayId obj ) {
		return false;
	}

	@Override
	public Iterator<Entry<ByteArrayId, SimpleFeature>> iterator() {
		return new Iterator<Entry<ByteArrayId, SimpleFeature>>() {

			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public Entry<ByteArrayId, SimpleFeature> next() {
				return null;
			}

			@Override
			public void remove() {}
		};

	}

	@Override
	public int size() {
		return addCount;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public void merge(
			NeighborList<SimpleFeature> otherList,
			mil.nga.giat.geowave.analytics.mapreduce.nn.NeighborList.Callback<SimpleFeature> callback ) {
		for (Entry<ByteArrayId, SimpleFeature> item : otherList) {
			if (this.add(item)) callback.add(item.getKey());
		}
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
	public Iterable<Entry<ByteArrayId, Geometry>> getIterable() {
		return new GeometryIterable(
				this);
	}

	private Long getCount(
			ByteArrayId keyId ) {
		if (clusteredGeometryCounts == null || !clusteredGeometryCounts.containsKey(keyId)) return ZERO;
		return clusteredGeometryCounts.get(keyId);
	}

	private void putCount(
			ByteArrayId keyId,
			Long value ) {
		if (clusteredGeometryCounts == null) clusteredGeometryCounts = new HashMap<ByteArrayId, Long>();
		clusteredGeometryCounts.put(
				keyId,
				value);
	}

	private static class GeometryIterable implements
			Iterable<Entry<ByteArrayId, Geometry>>
	{
		public GeometryIterable(
				HullAsYouGoClusterList geometryMembers ) {
			super();
			this.geometryMembers = geometryMembers;
		}

		final HullAsYouGoClusterList geometryMembers;

		@Override
		public Iterator<Entry<ByteArrayId, Geometry>> iterator() {
			return Collections.singletonList(
					(Entry<ByteArrayId, Geometry>) new AbstractMap.SimpleEntry<ByteArrayId, Geometry>(
							(ByteArrayId) null,
							geometryMembers.connectGeometryTool.concaveHull(
									geometryMembers.clusterGeo,
									geometryMembers.clusterPoints))).iterator();
		}
	}

	public static class HullAsYouGoClusterListFactory implements
			NeighborListFactory<SimpleFeature>
	{
		private final Projection<SimpleFeature> projectionFunction;
		private final int maxSize;
		private final String clusterFeatureTypeName;
		private final double maxDistance;
		private final DistanceFn<Coordinate> distanceFnForCoordinate;

		public HullAsYouGoClusterListFactory(
				final String clusterFeatureTypeName,
				final Projection<SimpleFeature> projectionFunction,
				final DistanceFn<Coordinate> distanceFnForCoordinate,
				final double maxDistance,
				final int maxSize ) {
			super();
			this.clusterFeatureTypeName = clusterFeatureTypeName;
			this.projectionFunction = projectionFunction;
			this.maxSize = maxSize;
			this.maxDistance = maxDistance;
			this.distanceFnForCoordinate = distanceFnForCoordinate;
		}

		public NeighborList<SimpleFeature> buildNeighborList(
				final ByteArrayId centerId,
				final SimpleFeature center ) {
			return new HullAsYouGoClusterList(
					clusterFeatureTypeName,
					projectionFunction,
					distanceFnForCoordinate,
					maxSize,
					centerId,
					center);
		}
	}

}
