package mil.nga.giat.geowave.analytic.db;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.analytic.param.DataStoreParameters;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;

public class DirectBasicAccumuloOperationsFactory implements
		BasicAccumuloOperationsFactory
{
	@Override
	public BasicAccumuloOperations build(
			final ConfigurationWrapper config )
			throws AccumuloException,
			AccumuloSecurityException {
		// BasicAccumuloOperations has a built in connection pool
		return new BasicAccumuloOperations(
				config.getString(
						DataStoreParameters.DataStoreParam.ZOOKEEKER,
						"localhost:2181"),
				config.getString(
						DataStoreParameters.DataStoreParam.ACCUMULO_INSTANCE,
						"minInstance"),
				config.getString(
						DataStoreParameters.DataStoreParam.ACCUMULO_USER,
						""),
				config.getString(
						DataStoreParameters.DataStoreParam.ACCUMULO_PASSWORD,
						""),
				config.getString(
						DataStoreParameters.DataStoreParam.ACCUMULO_NAMESPACE,
						""));

	}

}
