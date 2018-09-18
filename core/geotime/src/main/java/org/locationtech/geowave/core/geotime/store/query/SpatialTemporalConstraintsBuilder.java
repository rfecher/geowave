package org.locationtech.geowave.core.geotime.store.query;

import java.util.Date;

import javax.annotation.Nullable;

import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import com.vividsolutions.jts.geom.Geometry;

public interface SpatialTemporalConstraintsBuilder
{
	SpatialTemporalConstraintsBuilder spatialConstraint(
			Geometry geometry );

	SpatialTemporalConstraintsBuilder spatialConstraintCrs(
			String crsCode );

	SpatialTemporalConstraintsBuilder spatialConstraintCompareOperation(
			CompareOperation compareOperation );

	// we can always support open-ended time using beginning of epoch as default
	// start and some end of time such as max long as default end
	SpatialTemporalConstraintsBuilder addTimeRange(
			@Nullable Date startTime,
			@Nullable Date endTime );

	SpatialTemporalConstraintsBuilder addTimeRange(
			TemporalConstraints timeRange );

	SpatialTemporalConstraintsBuilder setTimeRanges(
			TemporalConstraints[] timeRanges );
	
	QueryConstraints build();

}
