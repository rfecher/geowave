package mil.nga.giat.geowave.analytic.db;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.core.store.index.PrimaryIndexStore;

public interface IndexStoreFactory
{
	public PrimaryIndexStore getIndexStore(
			ConfigurationWrapper context )
			throws InstantiationException;
}
