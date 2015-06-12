package mil.nga.giat.geowave.analytics.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Assumes two-dimensional simple feature without time dimensions.
 * 
 */
public class SimpleFeatureProjection implements
		Projection<SimpleFeature>
{

	@Override
	public Geometry getProjection(
			final SimpleFeature anItem ) {
		return (Geometry) anItem.getDefaultGeometry();
	}

	@Override
	public void initialize(
			final ConfigurationWrapper context )
			throws IOException {}

	@Override
	public void setup(
			final PropertyManagement runTimeProperties,
			final Configuration configuration ) {}
}
