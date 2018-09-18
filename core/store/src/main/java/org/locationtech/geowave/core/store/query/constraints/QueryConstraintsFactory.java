package org.locationtech.geowave.core.store.query.constraints;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.Constraints;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;

public class QueryConstraintsFactory
{
	public static final QueryConstraintsFactory SINGLETON_INSTANCE = new QueryConstraintsFactory();

	public static QueryConstraints dataIds(
			final ByteArrayId[] dataIds ) {
		return new DataIdQuery(dataIds);
	}

	public static QueryConstraints prefix(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKeyPrefix ) {
		return new PrefixIdQuery(
				partitionKey,
				sortKeyPrefix);
	}

	public static QueryConstraints coordinateRanges(
			final NumericIndexStrategy indexStrategy,
			final MultiDimensionalCoordinateRangesArray[] coordinateRanges ) {
		return new CoordinateRangeQuery(
				indexStrategy,
				coordinateRanges);
	}

	public static QueryConstraints constraints(
			final Constraints constraints ) {
		return new BasicQuery(
				constraints);
	}

	public static QueryConstraints constraints(
			final Constraints constraints,
			final BasicQueryCompareOperation compareOp ) {
		return new BasicQuery(
				constraints,
				compareOp);
	}

	public static QueryConstraints noConstraints() {
		return new EverythingQuery();
	}

}
