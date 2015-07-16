package mil.nga.giat.geowave.core.store.index;

import java.util.List;

import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;

/**
 * This is responsible for persisting index configuration (either in memory or
 * to disk depending on the implementation).
 */
public interface SecondaryIndexStore extends
		IndexStore<CompositeConstraints, List<FieldInfo<?>>>
{
}
