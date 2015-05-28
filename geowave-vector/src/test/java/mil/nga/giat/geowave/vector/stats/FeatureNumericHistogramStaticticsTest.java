package mil.nga.giat.geowave.vector.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class FeatureNumericHistogramStaticticsTest
{

	private SimpleFeatureType schema;
	FeatureDataAdapter dataAdapter;
	GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));

	@Before
	public void setup()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			CQLException,
			ParseException {
		schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,somewhere:Polygon,pid:String");
		dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
	}

	private SimpleFeature create(
			final long val ) {
		final List<AttributeDescriptor> descriptors = schema.getAttributeDescriptors();
		final Object[] defaults = new Object[descriptors.size()];
		int p = 0;
		for (final AttributeDescriptor descriptor : descriptors) {
			defaults[p++] = descriptor.getDefaultValue();
		}

		final SimpleFeature newFeature = SimpleFeatureBuilder.build(
				schema,
				defaults,
				UUID.randomUUID().toString());

		newFeature.setAttribute(
				"pop",
				val);
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"when",
				new Date());
		newFeature.setAttribute(
				"whennot",
				new Date());
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						27.25,
						41.25)));
		return newFeature;
	}

	@Test
	public void testPositive() {

		final FeatureNumericHistogramStatistics stat = new FeatureNumericHistogramStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final Random rand = new Random(
				7777);

		stat.entryIngested(
				null,
				create(100));
		stat.entryIngested(
				null,
				create(101));
		stat.entryIngested(
				null,
				create(2));

		long next = 1;
		for (int i = 0; i < 10000; i++) {
			next = next + (Math.round(rand.nextDouble()));
			stat.entryIngested(
					null,
					create(next));
		}

		final FeatureNumericHistogramStatistics stat2 = new FeatureNumericHistogramStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final long start2 = next;

		long max = 0;
		for (long i = 0; i < 10000; i++) {
			final long val = next + (long) (1000 * rand.nextDouble());
			stat2.entryIngested(
					null,
					create(val));
			max = Math.max(
					val,
					max);
		}
		final long skewvalue = next + (long) (1000 * rand.nextDouble());
		SimpleFeature skewedFeature = create(skewvalue);
		for (int i = 0; i < 10000; i++) {
			stat2.entryIngested(
					null,
					skewedFeature);
			// skewedFeature.setAttribute("pop", Long.valueOf(next + (long)
			// (1000 * rand.nextDouble())));
		}

		byte[] b = stat2.toBinary();
		stat2.fromBinary(b);
		assertEquals(
				1.0,
				stat2.cdf(max + 1),
				0.00001);

		stat.merge(stat2);

		assertEquals(
				1.0,
				stat.cdf(max + 1),
				0.00001);

		assertEquals(
				0.33,
				stat.cdf(start2),
				0.01);

		assertEquals(
				30003,
				sum(stat.count(10)));

		final double r = stat.percentPopulationOverRange(
				skewvalue - 1,
				skewvalue + 1);
		assertTrue((r > 0.3) && (r < 0.35));

		System.out.println(stat.toString());

	}

	@Test
	public void testExplosion() {

		final FeatureNumericHistogramStatistics stat1 = new FeatureNumericHistogramStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final Random rand = new Random(
				7777);
		long next = 1;
		for (int i = 0; i < 10000; i++) {
			next = next + (Math.round(rand.nextDouble()));
			stat1.entryIngested(
					null,
					create(next));
		}

		final FeatureNumericHistogramStatistics stat2 = new FeatureNumericHistogramStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		next = 1004839434;
		for (long i = 0; i < 10000; i++) {
			final long val = next + (long) (1000 * rand.nextDouble());
			stat2.entryIngested(
					null,
					create(val));
		}

		byte[] b = stat2.toBinary();
		stat2.fromBinary(b);

		b = stat1.toBinary();
		stat1.fromBinary(b);

		stat1.merge(stat2);

	}

	@Test
	public void testNegative() {

		final FeatureNumericHistogramStatistics stat = new FeatureNumericHistogramStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final Random rand = new Random(
				7777);

		stat.entryIngested(
				null,
				create(-100));
		stat.entryIngested(
				null,
				create(-101));
		stat.entryIngested(
				null,
				create(-2));

		long next = -1;
		for (int i = 0; i < 10000; i++) {
			next = next - (Math.round(rand.nextDouble()));
			stat.entryIngested(
					null,
					create(next));
		}

		final FeatureNumericHistogramStatistics stat2 = new FeatureNumericHistogramStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final long start2 = next;

		long min = 0;
		for (long i = 0; i < 10000; i++) {
			final long val = next - (long) (1000 * rand.nextDouble());
			stat2.entryIngested(
					null,
					create(val));
			min = Math.min(
					val,
					min);
		}
		final long skewvalue = next - (long) (1000 * rand.nextDouble());
		SimpleFeature skewedFeature = create(skewvalue);
		for (int i = 0; i < 10000; i++) {
			stat2.entryIngested(
					null,
					skewedFeature);
		}

		assertEquals(
				1.0,
				stat2.cdf(0),
				0.00001);
		byte[] b = stat2.toBinary();
		stat2.fromBinary(b);

		assertEquals(
				0.0,
				stat2.cdf(min),
				0.00001);

		stat.merge(stat2);

		assertEquals(
				1.0,
				stat.cdf(0),
				0.00001);

		assertEquals(
				0.66,
				stat.cdf(start2),
				0.01);

		assertEquals(
				30003,
				sum(stat.count(10)));

		final double r = stat.percentPopulationOverRange(
				skewvalue - 1,
				skewvalue + 1);
		assertTrue((r > 0.3) && (r < 0.35));

		System.out.println(stat.toString());

	}

	@Test
	public void testMix() {

		final FeatureNumericHistogramStatistics stat = new FeatureNumericHistogramStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final Random rand = new Random(
				7777);

		double min = 0;
		double max = 0;

		long next = 0;
		for (int i = 0; i < 10000; i++) {
			next = next + (Math.round(rand.nextDouble()));
			stat.entryIngested(
					null,
					create(next));
			max = Math.max(
					next,
					max);
		}

		next = 0;
		for (int i = 0; i < 10000; i++) {
			next = next - (Math.round(rand.nextDouble()));
			stat.entryIngested(
					null,
					create(next));
			min = Math.min(
					next,
					min);
		}

		assertEquals(
				0.5,
				stat.cdf(0),
				0.001);

		assertEquals(
				0.0,
				stat.cdf(min),
				0.00001);

		assertEquals(
				1.0,
				stat.cdf(max),
				0.00001);

		assertEquals(
				20000,
				sum(stat.count(10)));

		final double r = stat.percentPopulationOverRange(
				min / 2,
				max / 2);

		assertEquals(
				0.5,
				r,
				0.01);

		System.out.println(stat.toString());

	}

	private long sum(
			final long[] list ) {
		long result = 0;
		for (final long v : list) {
			result += v;
		}
		return result;
	}
}
