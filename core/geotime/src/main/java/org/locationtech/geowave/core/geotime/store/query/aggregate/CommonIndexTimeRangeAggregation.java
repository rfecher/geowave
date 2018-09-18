package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.geotime.store.dimension.Time.TimeRange;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.query.aggregate.CommonIndexAggregation;

import com.vividsolutions.jts.geom.Envelope;

public class CommonIndexTimeRangeAggregation extends
		TimeRangeAggregation<P, CommonIndexedPersistenceEncoding> implements
		CommonIndexAggregation<P, TimeRange>
{

}
