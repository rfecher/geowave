package mil.nga.giat.geowave.analytic.db;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;

/**
 * Building DB connections. Supports customization for unit tests and caching.
 * 
 */
public interface BasicAccumuloOperationsFactory
{
	BasicAccumuloOperations build(
			final ConfigurationWrapper wrapper )
			throws AccumuloException,
			AccumuloSecurityException;
}
