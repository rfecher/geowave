package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.commons.cli.Option;

public class GlobalParameters
{
	public enum Global
			implements
			ParameterEnum {
		PARENT_BATCH_ID(
				String.class,
				"pb",
				"Batch ID",
				true),
		CRS_ID(
				String.class,
				"crs",
				"CRS ID",
				true),
		BATCH_ID(
				String.class,
				"b",
				"Batch ID",
				true);
		private final Class<?> baseClass;
		private final Option option;

		Global(
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
