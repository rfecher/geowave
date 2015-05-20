package mil.nga.giat.geowave.vector.stats;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.zip.DataFormatException;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.Mergeable;
import mil.nga.giat.geowave.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;

import org.HdrHistogram.DoubleHistogram;
import org.opengis.feature.simple.SimpleFeature;

import com.clearspring.analytics.stream.frequency.CountMinSketch;

public class FeatureNumericHistogramStatistics extends
		AbstractDataStatistics<SimpleFeature> implements
		FeatureStatistic
{
	public static final String STATS_TYPE = "ATT_HISTOGRAM";
	private DoubleHistogram histogram = new DoubleHistogram(
			4);

	protected FeatureNumericHistogramStatistics() {
		super();
	}

	public FeatureNumericHistogramStatistics(
			final ByteArrayId dataAdapterId,
			final String fieldName ) {
		super(
				dataAdapterId,
				composeId(
						STATS_TYPE,
						fieldName));
	}

	public static final ByteArrayId composeId(
			final String fieldName ) {
		return composeId(
				STATS_TYPE,
				fieldName);
	}

	@Override
	public String getFieldName() {
		return decomposeNameFromId(getStatisticsId());
	}

	@Override
	public DataStatistics<SimpleFeature> duplicate() {
		return new FeatureNumericHistogramStatistics(
				dataAdapterId,
				getFieldName());
	}

	public double[] quantile(
			final int bins ) {
		final double[] result = new double[bins];
		final double binSize = 100.0 / bins;
		for (int bin = 0; bin < bins; bin++) {
			result[bin] = histogram.getValueAtPercentile(binSize * (bin + 1));
		}
		return result;
	}

	public double cdf(
			final double val ) {
		return histogram.getPercentileAtOrBelowValue(val) / 100.0;
	}

	public double quantile(
			final double val ) {
		return histogram.getValueAtPercentile(val * 100.0);
	}

	public double percentPopulationOverRange(
			final double start,
			final double stop ) {
		return (histogram.getPercentileAtOrBelowValue(stop) - histogram.getPercentileAtOrBelowValue(start)) / 100.0;
	}

	public long totalSampleSize() {
		return histogram.getTotalCount();
	}

	public long[] count(
			final int bins ) {
		final long[] result = new long[bins];
		final double max = histogram.getMaxValue();
		final double min = histogram.getMinValue();
		final double binSize = (max - min) / (bins);
		long last = 0;
		final long tc = histogram.getTotalCount();
		for (int bin = 0; bin < bins; bin++) {
			final double val = histogram.getPercentileAtOrBelowValue((min + ((bin + 1.0) * binSize))) / 100.0 * tc;
			final long next = (long) val - last;
			result[bin] = next;
			last += next;
		}
		return result;
	}

	@Override
	public void merge(
			final Mergeable mergeable ) {
		if (mergeable instanceof FeatureNumericHistogramStatistics) {
			histogram.add(((FeatureNumericHistogramStatistics) mergeable).histogram);
		}
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = super.binaryBuffer(histogram.getEstimatedFootprintInBytes());
		histogram.encodeIntoCompressedByteBuffer(buffer);
		final byte result[] = new byte[buffer.position() + 1];
		buffer.rewind();
		buffer.get(result);
		return result;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		try {
			histogram = DoubleHistogram.decodeFromCompressedByteBuffer(
					buffer,
					0);
		}
		catch (final DataFormatException e) {
			throw new RuntimeException(
					"Cannot decode statistic",
					e);
		}
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final SimpleFeature entry ) {
		final Object o = entry.getAttribute(getFieldName());
		if (o == null) {
			return;
		}
		final double num = ((Number) o).doubleValue();
		histogram.recordValue(num);
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(
				"histogram[adapter=").append(
				super.getDataAdapterId().getString());
		buffer.append(
				", field=").append(
				getFieldName());
		buffer.append(", bins={");
		final MessageFormat mf = new MessageFormat(
				"{0,number,#.######}");
		for (double v : this.quantile(10)) {
			buffer.append(
					mf.format(new Object[] {
						Double.valueOf(v)
					})).append(
					' ');
		}
		buffer.deleteCharAt(buffer.length() - 1);
		buffer.append(", counts={");
		for (long v : this.count(10)) {
			buffer.append(
					mf.format(new Object[] {
						Long.valueOf(v)
					})).append(
					' ');
		}
		buffer.deleteCharAt(buffer.length() - 1);
		buffer.append("}]");
		return buffer.toString();
	}
}
