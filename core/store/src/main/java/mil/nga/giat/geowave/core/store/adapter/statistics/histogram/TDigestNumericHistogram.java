package mil.nga.giat.geowave.core.store.adapter.statistics.histogram;

import java.nio.ByteBuffer;

import com.tdunning.math.stats.MergingDigest;

public class TDigestNumericHistogram implements
		NumericHistogram
{
	private MergingDigest tdigest;
	private long count;

	@Override
	public void merge(
			final NumericHistogram other ) {
		if (other instanceof TDigestNumericHistogram) {
			tdigest.add(
					((TDigestNumericHistogram) other).tdigest);
			count += ((TDigestNumericHistogram) other).count;
		}
	}

	@Override
	public void add(
			final double v ) {
		tdigest.add(
				v);
		count++;
	}

	@Override
	public double quantile(
			final double q ) {
		return tdigest.quantile(
				q);
	}

	public double sum(
			final double val ) {
		return tdigest.cdf(
				val) * count;
	}

	@Override
	public double cdf(
			final double val ) {
		return tdigest.cdf(
				val);
	}

	@Override
	public int bufferSize() {
		return tdigest.smallByteSize();
	}

	@Override
	public void toBinary(
			final ByteBuffer buffer ) {
		tdigest.asSmallBytes(
				buffer);
	}

	@Override
	public void fromBinary(
			final ByteBuffer buffer ) {
		tdigest = MergingDigest.fromBytes(
				buffer);
	}

	@Override
	public double getMaxValue() {
		return tdigest.getMax();
	}

	@Override
	public double getMinValue() {
		return tdigest.getMin();
	}

	@Override
	public long getTotalCount() {
		return count;
	}

}
