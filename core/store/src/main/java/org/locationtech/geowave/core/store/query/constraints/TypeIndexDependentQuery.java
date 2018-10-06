package org.locationtech.geowave.core.store.query.constraints;

import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;

public interface TypeIndexDependentQuery
{
	QueryConstraints createQuery(
			DataTypeAdapter<?> adapter,
			Index index );
}
