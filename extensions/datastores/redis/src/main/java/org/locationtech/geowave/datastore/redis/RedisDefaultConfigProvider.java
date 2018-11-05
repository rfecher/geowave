package org.locationtech.geowave.datastore.redis;

import java.util.Properties;

import org.locationtech.geowave.core.cli.spi.DefaultConfigProviderSpi;

public class RedisDefaultConfigProvider implements
		DefaultConfigProviderSpi
{
	private final Properties configProperties = new Properties();

	/**
	 * Create the properties for the config-properties file
	 */
	private void setProperties() {
		configProperties.setProperty(
				"store.default-redis.opts.gwNamespace",
				"geowave.default");
		configProperties.setProperty(
				"store.default-redis.opts.persistDataStatistics",
				"true");
		configProperties.setProperty(
				"store.default-redis.opts.address",
				"redis://127.0.0.1:6379");
		configProperties.setProperty(
				"store.default-redis.type",
				"redis");
	}

	@Override
	public Properties getDefaultConfig() {
		setProperties();
		return configProperties;
	}
}
