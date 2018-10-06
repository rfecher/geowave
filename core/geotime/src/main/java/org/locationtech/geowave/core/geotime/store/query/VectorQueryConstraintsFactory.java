package org.locationtech.geowave.core.geotime.store.query;

import org.geotools.filter.text.cql2.CQL;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraintsFactory;
import org.opengis.filter.Filter;

public class VectorQueryConstraintsFactory extends
		QueryConstraintsFactory
{

	public static final VectorQueryConstraintsFactory SINGLETON_INSTANCE = new VectorQueryConstraintsFactory();
	private Filter cqlFilter;
	public static SpatialTemporalConstraintsBuilder spatialTemporalConstraints() {

	}

	// these cql expressions should always attempt to use
	// CQLQuery.createOptimalQuery() which requires adapter and index
	public static QueryConstraints cqlConstraints(
			final String cqlExpression ) {		

		cqlFilter = CQL
				.toFilter(cqlExpression);
	}

	QueryConstraints filterConstraints(
			final Filter filter ) {

	}
}
