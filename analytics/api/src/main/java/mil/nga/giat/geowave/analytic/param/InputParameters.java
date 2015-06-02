package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.commons.cli.Option;
import org.apache.hadoop.fs.Path;

public class InputParameters
{
	public enum Input
			implements
			ParameterEnum {
		INPUT_FORMAT(
				FormatConfiguration.class,
				"ifc",
				"Input Format Class",
				true),
		HDFS_INPUT_PATH(
				Path.class,
				"iip",
				"Input HDFS File Path",
				true);

		private final Class<?> baseClass;
		private final Option option;

		Input(
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
	}
}
