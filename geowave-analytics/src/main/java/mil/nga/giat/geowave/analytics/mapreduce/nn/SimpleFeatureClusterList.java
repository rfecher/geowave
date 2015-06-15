package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.UUID;

import mil.nga.giat.geowave.analytics.tools.AnalyticFeature;
import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.analytics.tools.Projection;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.index.ByteArrayId;

import org.apache.hadoop.conf.Configuration;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.operation.distance.DistanceOp;

/**
 * 
 * Maintains a list of entries, clipping entries after reaching a maximum size;
 * Ideally, middle entries are clipped at Random. However, due to performance
 * considerations, the last entry is clipped.
 * 
 * The list maintains the total number of additions (minus removals) since the
 * list size is reflective of the clipped components.
 * 
 * Intended to run in a single thread. Not Thread Safe.
 * 
 * 
 * @param <VALUEIN>
 */
public class SimpleFeatureClusterList implements
		NeighborList<SimpleFeature>
{
	private final HashMap<ByteArrayId, Geometry> clusteredGeometries = new HashMap<ByteArrayId, Geometry>();
	// maintains the count of a geometry representing a cluster of points
	private HashMap<ByteArrayId, Long> clusteredGeometryCounts = null;
	private int addCount;
	private final int maxSize;
	private Projection<SimpleFeature> projectionFunction;
	// wrap with thread local if thread safety is desired
	private SimpleFeature sample = null;
	private String clusterFeatureTypeName;
	private final Geometry center;

	public SimpleFeatureClusterList(
			final String clusterFeatureTypeName,
			final Projection<SimpleFeature> projectionFunction,
			final int maxSize,
			final Geometry center ) {
		super();
		this.center = center;
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
	}

	private static final Long ONE = 1L;
	private static final Long ZERO = 0L;

	@Override
	public boolean add(
			final Entry<ByteArrayId, SimpleFeature> entry ) {
		final SimpleFeature newInstance = entry.getValue();
		if (sample == null) {
			final Object[] defaults = new Object[newInstance.getFeatureType().getAttributeDescriptors().size()];
			int p = 0;
			for (final AttributeDescriptor descriptor : newInstance.getFeatureType().getAttributeDescriptors()) {
				defaults[p++] = descriptor.getDefaultValue();
			}

			// see reason in the iterator
			sample = SimpleFeatureBuilder.build(
					newInstance.getFeatureType(),
					defaults,
					UUID.randomUUID().toString());

		}
		if (!clusteredGeometries.containsKey(entry.getKey())) {
			Long count = ONE;
			Geometry geo = projectionFunction.getProjection(entry.getValue());
			if (newInstance.getFeatureType().getName().getLocalPart().equals(
					clusterFeatureTypeName)) {
				count = (Long) newInstance.getAttribute(AnalyticFeature.ClusterFeatureAttribute.COUNT.attrName());
				putCount(
						entry.getKey(),
						count);
			}
			else {
				// not a cluster geometry...shrink it to size of this centroid
				// since it may be big (line, polygon, etc.)
				geo = clip(geo);
			}
			clusteredGeometries.put(
					entry.getKey(),
					geo);

			addCount += count;
			return true;
		}
		return false;
	}

	private Long getCount(
			ByteArrayId keyId ) {
		if (clusteredGeometryCounts == null) return ZERO;
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

	public int getMaxSize() {
		return maxSize;
	}

	@Override
	public void clear() {
		addCount = 0;
		clusteredGeometries.clear();
	}

	@Override
	public boolean contains(
			final ByteArrayId obj ) {
		return clusteredGeometries.containsKey(obj);
	}

	@Override
	public Iterator<Entry<ByteArrayId, SimpleFeature>> iterator() {
		return new Iterator<Entry<ByteArrayId, SimpleFeature>>() {
			final Iterator<Entry<ByteArrayId, Geometry>> geoIt = clusteredGeometries.entrySet().iterator();

			@Override
			public boolean hasNext() {
				return geoIt.hasNext();
			}

			@Override
			public Entry<ByteArrayId, SimpleFeature> next() {
				final Entry<ByteArrayId, Geometry> nextItem = geoIt.next();

				// just need a wrapper to answer this method correctly
				sample.setDefaultGeometry(geoIt.next());
				return new AbstractMap.SimpleEntry<ByteArrayId, SimpleFeature>(
						nextItem.getKey(),
						sample);
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
		return clusteredGeometries.isEmpty();
	}

	@Override
	public SimpleFeature get(
			ByteArrayId key ) {

		final Geometry geo = clusteredGeometries.get(key);
		if (geo != null) {
			sample.setDefaultGeometry(geo);
			return sample;
		}
		return null;
	}

	@Override
	public void merge(
			NeighborList<SimpleFeature> otherList,
			mil.nga.giat.geowave.analytics.mapreduce.nn.NeighborList.Callback<SimpleFeature> callback ) {
		if (otherList instanceof SimpleFeatureClusterList) {
			for (Entry<ByteArrayId, Geometry> entry : new GeometryIterable(
					((SimpleFeatureClusterList) otherList))) {
				if (clusteredGeometries.put(
						entry.getKey(),
						entry.getValue()) != null) {
					callback.add(entry.getKey());
					Long count = getCount(entry.getKey());
					addCount += (count == null) ? 1 : count;
				}
			}
		}
		else {
			for (Entry<ByteArrayId, SimpleFeature> item : otherList) {
				if (this.add(item)) callback.add(item.getKey());
			}
		}
	}

	private Geometry clip(
			final Geometry geometry ) {
		if (geometry instanceof Point) {
			return geometry;
		}
		final DistanceOp op = new DistanceOp(
				center,
				geometry);
		return center.getFactory().createPoint(
				op.nearestPoints()[1]);
	}

	public static class GeometryIterable implements
			Iterable<Entry<ByteArrayId, Geometry>>
	{
		public GeometryIterable(
				SimpleFeatureClusterList geometryMembers ) {
			super();
			this.geometryMembers = geometryMembers;
		}

		final SimpleFeatureClusterList geometryMembers;

		@Override
		public Iterator<Entry<ByteArrayId, Geometry>> iterator() {
			return geometryMembers.clusteredGeometries.entrySet().iterator();
		}

	}

	public static class SimpleFeatureClusterListFactory implements
			NeighborListFactory<SimpleFeature>
	{
		private final Projection<SimpleFeature> projectionFunction;
		private final int maxSize;
		private final String clusterFeatureTypeName;
		private final double maxDistance;

		public SimpleFeatureClusterListFactory(
				final String clusterFeatureTypeName,
				final Projection<SimpleFeature> projectionFunction,
				final double maxDistance,
				final int maxSize ) {
			super();
			this.clusterFeatureTypeName = clusterFeatureTypeName;
			this.projectionFunction = projectionFunction;
			this.maxSize = maxSize;
			this.maxDistance = maxDistance;
		}

		public NeighborList<SimpleFeature> buildNeighborList(
				SimpleFeature center ) {
			return new SimpleFeatureClusterList(
					clusterFeatureTypeName,
					projectionFunction,
					maxSize,
					(Geometry) center.getDefaultGeometry());
		}
	}
}
