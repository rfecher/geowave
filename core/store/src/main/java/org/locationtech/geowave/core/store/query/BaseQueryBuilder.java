package org.locationtech.geowave.core.store.query;

import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraintsFactory;

public interface BaseQueryBuilder<T, Q extends BaseQuery<T, ?>, R extends BaseQueryBuilder<T, Q, R>>
{
	R allIndicies();

	R indexName(
			String indexName );

	R addAuthorization(
			String authorization );

	R setAuthorizations(
			String[] authorizations );

	R noAuthorizations();

	R subsampling(
			double[] maxResolutionPerDimension );

	R noSubsampling();

	R noLimit();

	R limit(
			int limit );

	R maxRanges(
			int maxRangeDecomposition );

	R noMaxRanges();

	R defaultMaxRanges();

	R constraints(
			QueryConstraints constraints );

	default QueryConstraintsFactory constraintsFactory() {
		return QueryConstraintsFactory.SINGLETON_INSTANCE;
	}

	Q build();
}
