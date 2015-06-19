package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import mil.nga.giat.geowave.analytics.tools.ShapefileTool;
import mil.nga.giat.geowave.index.ByteArrayId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

/**
 * 
 * A cluster represented by a hull.
 * 
 * Intended to run in a single thread. Not Thread Safe.
 * 
 * 
 * TODO: connectGeometryTool.connect(
 */
public class ClusterUnionList extends
		DBScanClusterList implements
		CompressingCluster<ClusterItem, Geometry>
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(ClusterUnionList.class);

	// internal state
	private Geometry clusterGeo;

	public ClusterUnionList(
			final ByteArrayId centerId,
			final ClusterItem center,
			final Map<ByteArrayId, Cluster<ClusterItem>> index ) {
		super(
				centerId,
				index);

		clusterGeo = center.getGeometry();

		putCount(
				centerId,
				center.getCount(),
				true);

	}

	protected Long addAndFetchCount(
			final ByteArrayId id,
			final ClusterItem newInstance ) {

		Geometry newGeo = newInstance.getGeometry();
		try {
			clusterGeo = clusterGeo.union(newGeo);
		}
		catch (Exception ex) {
			try {
				ShapefileTool.writeShape(
						"setbaccc1",
						new File(
								"./target/test_setbaccc1"),
						new Geometry[] {
							clusterGeo
						});

				ShapefileTool.writeShape(
						"setbaccc2",
						new File(
								"./target/test_setbaccc2"),
						new Geometry[] {
							newGeo
						});
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return (Long) newInstance.getCount();
	}

	@Override
	public void merge(
			Cluster<ClusterItem> cluster ) {
		interpolateAddCount((DBScanClusterList) cluster);
		if (cluster != this) {
			clusterGeo = clusterGeo.union(((ClusterUnionList) cluster).clusterGeo);
		}
	}

	protected Geometry compress() {

		return clusterGeo;
	}

	public static class ClusterUnionListFactory implements
			NeighborListFactory<ClusterItem>
	{
		private final Map<ByteArrayId, Cluster<ClusterItem>> index;

		public ClusterUnionListFactory(

				final Map<ByteArrayId, Cluster<ClusterItem>> index ) {
			super();
			this.index = index;
		}

		public NeighborList<ClusterItem> buildNeighborList(
				final ByteArrayId centerId,
				final ClusterItem center ) {
			return new ClusterUnionList(
					centerId,
					center,
					index);
		}
	}
}
