package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.analytics.tools.Projection;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.index.ByteArrayId;

import org.apache.hadoop.conf.Configuration;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

/**
 * 
 * Represents a cluster. Maintains links to other clusters through shared
 * components Maintains counts contributed by components of this cluster.
 * Supports merging with other clusters, incrementing the count by only those
 * components different from the other cluster.
 * 
 * Intended to run in a single thread. Not Thread Safe.
 * 
 */
public abstract class DBScanClusterList implements
		CompressingCluster<SimpleFeature, Geometry>
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(DBScanClusterList.class);

	// internal state
	private int addCount;
	private final Set<Cluster<SimpleFeature>> linkedClusters = new HashSet<Cluster<SimpleFeature>>();
	// maintains the count of a geometry representing a cluster of points
	private HashMap<ByteArrayId, Long> clusteredGeometryCounts = null;

	// configuration
	protected final Projection<SimpleFeature> projectionFunction;
	private final ByteArrayId id;

	// global state
	private final Map<ByteArrayId, Cluster<SimpleFeature>> index;

	public DBScanClusterList(
			final Projection<SimpleFeature> projectionFunction,
			final ByteArrayId centerId,
			final Map<ByteArrayId, Cluster<SimpleFeature>> index ) {
		super();

		this.linkedClusters.add(this);
		this.index = index;

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

		this.id = centerId;
	}

	protected static final Long ONE = 1L;
	protected static final Long ZERO = 0L;

	@Override
	public boolean add(
			final Entry<ByteArrayId, SimpleFeature> entry ) {
		return this.add(
				entry.getKey(),
				entry.getValue());
	}

	private void checkAssignment(
			final ByteArrayId id ) {
		final Cluster<SimpleFeature> cluster = index.get(id);
		if (cluster != null) {
			linkedClusters.add(cluster);
		}
	}

	protected abstract Long addAndFetchCount(
			final ByteArrayId id,
			final SimpleFeature newInstance );

	protected boolean add(
			final ByteArrayId id,
			final SimpleFeature newInstance ) {
		if (getCount(id) != 0) return false;

		checkAssignment(id);

		Long count = addAndFetchCount(
				id,
				newInstance);

		putCount(
				id,
				count);

		return true;
	}

	@Override
	public void clear() {
		addCount = 0;
		clusteredGeometryCounts = null;
	}

	@Override
	public boolean contains(
			final ByteArrayId obj ) {
		return this.clusteredGeometryCounts.containsKey(obj);
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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		DBScanClusterList other = (DBScanClusterList) obj;
		if (id == null) {
			if (other.id != null) return false;
		}
		else if (!id.equals(other.id)) return false;
		return true;
	}

	@Override
	public int size() {
		return addCount;
	}

	@Override
	public boolean isEmpty() {
		return this.clusteredGeometryCounts.isEmpty();
	}

	@Override
	public Geometry get() {
		return this.compress();
	}

	public Iterator<ByteArrayId> clusteredIds() {
		return clusteredGeometryCounts.keySet().iterator();
	}

	@Override
	public void merge(
			Cluster<SimpleFeature> cluster ) {
		if (cluster != this) {
			for (Map.Entry<ByteArrayId, Long> count : ((DBScanClusterList) cluster).clusteredGeometryCounts.entrySet()) {
				if (!clusteredGeometryCounts.containsKey(count.getKey())) {
					putCount(
							count.getKey(),
							count.getValue());
				}
			}
		}
	}

	@Override
	public ByteArrayId getId() {
		return this.id;
	}

	protected abstract Geometry compress();

	@Override
	public Iterator<Cluster<SimpleFeature>> getLinkedClusters() {
		List<Cluster<SimpleFeature>> sortList = new ArrayList<Cluster<SimpleFeature>>(
				linkedClusters);
		Collections.sort(
				sortList,
				new Comparator<Cluster<SimpleFeature>>() {

					@Override
					public int compare(
							Cluster<SimpleFeature> arg0,
							Cluster<SimpleFeature> arg1 ) {
						return ((DBScanClusterList) arg1).clusteredGeometryCounts.size() - ((DBScanClusterList) arg0).clusteredGeometryCounts.size();
					}

				});
		return sortList.iterator();
	}

	protected Long getCount(
			ByteArrayId keyId ) {
		if (clusteredGeometryCounts == null || !clusteredGeometryCounts.containsKey(keyId)) return ZERO;
		return clusteredGeometryCounts.get(keyId);
	}

	protected void putCount(
			ByteArrayId keyId,
			Long value ) {
		if (clusteredGeometryCounts == null) clusteredGeometryCounts = new HashMap<ByteArrayId, Long>();
		clusteredGeometryCounts.put(
				keyId,
				value);
		addCount += value;
	}

}
