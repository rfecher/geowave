package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.commons.cli.Option;
import org.apache.hadoop.fs.Path;

public class OutputParameters
{
	public enum Output
			implements
			ParameterEnum {
		REDUCER_COUNT(
				Integer.class,
				"orc",
				"Number of Reducers For Output",
				true),
		OUTPUT_FORMAT(
				FormatConfiguration.class,
				"ofc",
				"Output Format Class",
				true),
		HDFS_OUTPUT_PATH(
				Path.class,
				"oop",
				"Output HDFS File Path",
				true);
		private final Class<?> baseClass;
		private final Option option;

		Output(
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
