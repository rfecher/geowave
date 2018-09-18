package org.locationtech.geowave.core.store.adapter.statistics;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;

/**
 * This is a marker class extending ByteArrayId that additionally provides type
 * checking with a generic.
 *
 * @param <R>
 *            The type of statistic
 */
abstract public class StatisticsType<R, B extends StatisticsQueryBuilder<R, B>> extends
		ByteArrayId implements
		Persistable
{
	private static final long serialVersionUID = 1L;

	public StatisticsType() {
		super();
	}

	public StatisticsType(
			final byte[] id ) {
		super(
				id);
	}

	public StatisticsType(
			final String id ) {
		super(
				id);
	}

	abstract public B newBuilder();

	@Override
	public byte[] toBinary() {
		return this.id;
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		this.id = bytes;
	}
}
