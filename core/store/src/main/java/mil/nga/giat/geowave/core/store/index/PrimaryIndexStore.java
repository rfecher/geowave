package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

/**
 * This is responsible for persisting index configuration (either in memory or
 * to disk depending on the implementation).
 */
public interface PrimaryIndexStore extends
		IndexStore<MultiDimensionalNumericData, MultiDimensionalNumericData>
{
}
