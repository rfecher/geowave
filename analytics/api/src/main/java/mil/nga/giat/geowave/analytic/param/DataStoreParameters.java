package mil.nga.giat.geowave.analytic.param;

import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.commons.cli.Option;

public class DataStoreParameters
{
	public enum DataStoreParam
			implements
			GroupParameterEnum {
		ZOOKEEKER(
				String.class,
				"z",
				"A comma-separated list of zookeeper servers used by an Accumulo instance.",
				true),
		ACCUMULO_INSTANCE(
				String.class,
				"i",
				"The Accumulo instance ID",
				true),
		ACCUMULO_USER(
				String.class,
				"u",
				"A valid Accumulo user ID",
				true),
		ACCUMULO_PASSWORD(
				String.class,
				"p",
				"The password for the Accumulo user",
				true),
		ACCUMULO_NAMESPACE(
				String.class,
				"n",
				"The table namespace (optional; default is no namespace)",
				true);
		private final Class<?> baseClass;
		private final Option option;

		DataStoreParam(
				final Class<?> baseClass,
				final String name,
				final String description,
				boolean hasArg ) {
			this.baseClass = baseClass;
			this.option = PropertyManagement.newOption(
					this,
					name,
					description,
					hasArg);
		}

		@Override
		public Class<?> getBaseClass() {
			return baseClass;
		}

		@Override
		public Enum<?> self() {
			return this;
		}

		@Override
		public Option getOption() {
			return option;
		}

		@Override
		public void fillOptions(
				Set<Option> options ) {

		}
	}
}
