package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import mil.nga.giat.geowave.analytics.tools.AnalyticFeature;
import mil.nga.giat.geowave.analytics.tools.Projection;
import mil.nga.giat.geowave.analytics.tools.ShapefileTool;
import mil.nga.giat.geowave.index.ByteArrayId;

import org.opengis.feature.simple.SimpleFeature;
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
		CompressingCluster<SimpleFeature, Geometry>
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(ClusterUnionList.class);

	// internal state
	private Geometry clusterGeo;

	public ClusterUnionList(
			final Projection<SimpleFeature> projectionFunction,
			final ByteArrayId centerId,
			final SimpleFeature center,
			final Map<ByteArrayId, Cluster<SimpleFeature>> index ) {
		super(
				projectionFunction,
				centerId,
				index);

		clusterGeo = (this.projectionFunction.getProjection(center));

		putCount(
				centerId,
				(Long) center.getAttribute(AnalyticFeature.ClusterFeatureAttribute.COUNT.attrName()));

	}

	protected Long addAndFetchCount(
			final ByteArrayId id,
			final SimpleFeature newInstance ) {

		Geometry newGeo = projectionFunction.getProjection(newInstance);
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

		return (Long) newInstance.getAttribute(AnalyticFeature.ClusterFeatureAttribute.COUNT.attrName());
	}

	@Override
	public void merge(
			Cluster<SimpleFeature> cluster ) {
		super.merge(cluster);
		if (cluster != this) {
			this.clusterGeo = this.clusterGeo.union(((ClusterUnionList) cluster).clusterGeo);
		}
	}

	protected Geometry compress() {

		return clusterGeo;
	}

	public static class ClusterUnionListFactory implements
			NeighborListFactory<SimpleFeature>
	{
		private final Projection<SimpleFeature> projectionFunction;
		private final Map<ByteArrayId, Cluster<SimpleFeature>> index;

		public ClusterUnionListFactory(
				final Projection<SimpleFeature> projectionFunction,
				final Map<ByteArrayId, Cluster<SimpleFeature>> index ) {
			super();
			this.projectionFunction = projectionFunction;
			this.index = index;
		}

		public NeighborList<SimpleFeature> buildNeighborList(
				final ByteArrayId centerId,
				final SimpleFeature center ) {
			return new ClusterUnionList(
					projectionFunction,
					centerId,
					center,
					index);
		}
	}
}
