package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.geotime.store.dimension.GeometryAdapter;
import org.locationtech.geowave.core.geotime.store.dimension.GeometryWrapper;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.query.aggregate.CommonIndexAggregation;

import com.vividsolutions.jts.geom.Envelope;

public class CommonIndexBoundingBoxAggregation<P extends Persistable> extends
		BoundingBoxAggregation<P, CommonIndexedPersistenceEncoding> implements
		CommonIndexAggregation<P, Envelope>
{

	@Override
	protected Envelope getEnvelope(
			final CommonIndexedPersistenceEncoding entry ) {
		final CommonIndexValue v = entry
				.getCommonData()
				.getValue(
						GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID);
		if ((v != null) && (v instanceof GeometryWrapper)) {
			return ((GeometryWrapper) v).getGeometry().getEnvelopeInternal();
		}
		return null;
	}

}
