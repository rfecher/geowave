package org.locationtech.geowave.core.store.adapter.statistics;

import java.util.Arrays;

import org.locationtech.geowave.core.index.ByteArrayId;

public class GenericStatisticsType<R> extends
		BaseStatisticsType<R>
{
	private static final long serialVersionUID = 1L;

	public GenericStatisticsType() {
		super();
	}

	public GenericStatisticsType(
			final byte[] id ) {
		super(
				id);
	}

	public GenericStatisticsType(
			final String id ) {
		super(
				id);
	}

	@Override
	public boolean equals(
			final Object obj ) {
		// If all we know is the name of the stat type,
		// but not the class we need to override equals on
		// the base statistics type so that the
		// class does not need to match
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof ByteArrayId)) {
			return false;
		}
		final ByteArrayId other = (ByteArrayId) obj;
		return Arrays
				.equals(
						id,
						other.getBytes());
	}
}
