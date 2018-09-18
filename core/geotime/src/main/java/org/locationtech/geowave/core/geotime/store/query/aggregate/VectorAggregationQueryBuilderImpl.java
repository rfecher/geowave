package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.geotime.store.query.api.VectorAggregationQueryBuilder;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.query.aggregate.AggregationQueryBuilderImpl;
import org.locationtech.geowave.core.store.query.options.AggregateTypeQueryOptions;
import org.opengis.feature.simple.SimpleFeature;

public class VectorAggregationQueryBuilderImpl<P extends Persistable, R> extends
		AggregationQueryBuilderImpl<P, R, SimpleFeature, VectorAggregationQueryBuilder<P, R>> implements
		VectorAggregationQueryBuilder<P, R>
{

	@Override
	public VectorAggregationQueryBuilder<P, R> bboxOfResults(
			String... typeNames ) {
		return new AggregateTypeQueryOptions(new BoundingBoxDataStatistics<T>(), typeNames);
	}

	@Override
	public VectorAggregationQueryBuilder<P, R> timeRangeOfResults(
			String... typeNames ) {
		return new AggregateTypeQueryOptions(aggregation, typeNames);
	}
}
