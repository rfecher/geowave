package org.locationtech.geowave.datastore.redis.config;

import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.redis.RedisStoreFactoryFamily;

import com.beust.jcommander.ParametersDelegate;

public class RedisOptions extends
		StoreFactoryOptions
{

	@ParametersDelegate
	protected BaseDataStoreOptions baseOptions = new BaseDataStoreOptions() {
		@Override
		public boolean isServerSideLibraryEnabled() {
			return false;
		}
	};

	public RedisOptions() {
		super();
	}

	public RedisOptions(
			final String geowaveNamespace ) {
		super(
				geowaveNamespace);
	}

	@Override
	public StoreFactoryFamilySpi getStoreFactory() {
		return new RedisStoreFactoryFamily();
	}

	@Override
	public DataStoreOptions getStoreOptions() {
		return baseOptions;
	}

}
