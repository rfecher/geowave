package org.locationtech.geowave.core.store.query.aggregate;

import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.query.BaseQueryBuilderImpl;
import org.locationtech.geowave.core.store.query.options.AggregateTypeQueryOptions;

public class AggregationQueryBuilderImpl<P extends Persistable, R, T, A extends AggregationQueryBuilder<P, R, T, A>>
		extends
		BaseQueryBuilderImpl<R, AggregationQuery<P, R, T>, A> implements
		AggregationQueryBuilder<P, R, T, A>
{
	protected Aggregation<P, R, T> aggregation;
	protected String[] typeNames;

	@Override
	public AggregationQuery<P, R, T> build() {
		return new AggregationQuery<>(
				newCommonQueryOptions(),
				newAggregateTypeQueryOptions(),
				newIndexQueryOptions(),
				constraints);
	}

	@Override
	public A count(
			final String... typeNames ) {
		aggregation = (Aggregation) new CountAggregation();
		return (A) this;
	}

	@Override
	public A aggregate(
			final String typeName,
			final Aggregation<P, R, T> aggregation ) {
		this.aggregation = aggregation;
		return (A) this;
	}

	protected AggregateTypeQueryOptions<P, R, T> newAggregateTypeQueryOptions() {
		return new AggregateTypeQueryOptions<>(
				aggregation,
				typeNames);
	}

}
