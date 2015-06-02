package mil.nga.giat.geowave.analytic.db;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.DataStoreParameters;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloAdapterStoreFactory implements
		AdapterStoreFactory
{
	final static Logger LOGGER = LoggerFactory.getLogger(AccumuloAdapterStoreFactory.class);

	@Override
	public AdapterStore getAdapterStore(
			ConfigurationWrapper context )
			throws InstantiationException {

		final String zookeeper = context.getString(
				DataStoreParameters.DataStoreParam.ZOOKEEKER,
				"localhost:2181");
		final String accumuloInstance = context.getString(
				DataStoreParameters.DataStoreParam.ACCUMULO_INSTANCE,
				"minInstance");

		BasicAccumuloOperations basicAccumuloOperations;
		try {
			basicAccumuloOperations = context.getInstance(
					CommonParameters.Common.ACCUMULO_CONNECT_FACTORY,
					BasicAccumuloOperationsFactory.class,
					DirectBasicAccumuloOperationsFactory.class).build(
					context);
		}
		catch (IllegalAccessException | AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Cannot connect to GeoWave for Adapter Inquiry (" + accumuloInstance + "@ " + zookeeper + ")",
					e);
			throw new InstantiationException(
					"Cannot connect to GeoWave for Adapter Inquiry (" + accumuloInstance + "@ " + zookeeper + ")");
		}
		return new AccumuloAdapterStore(
				basicAccumuloOperations);

	}

}
