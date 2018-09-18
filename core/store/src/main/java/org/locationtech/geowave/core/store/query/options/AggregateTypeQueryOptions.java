package org.locationtech.geowave.core.store.query.options;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.Aggregation;

public class AggregateTypeQueryOptions<P extends Persistable, R, T> implements
		DataTypeQueryOptions<R>
{
	private String[] typeNames;
	private Aggregation<P, R, T> aggregation;

	protected AggregateTypeQueryOptions() {}

	public AggregateTypeQueryOptions(
			final Aggregation<P, R, T> aggregation,
			final String... typeNames ) {
		this.typeNames = typeNames;
		this.aggregation = aggregation;
	}

	@Override
	public String[] getTypeNames() {
		return typeNames;
	}

	public Aggregation<P, R, T> getAggregation() {
		return aggregation;
	}

	@Override
	public byte[] toBinary() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		// TODO Auto-generated method stub

	}
}
