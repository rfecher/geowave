package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner;

import org.apache.commons.cli.Option;

public class PartitionParameters
{
	public enum Partition
			implements
			ParameterEnum {
		PARTITION_DISTANCE(
				Double.class,
				"pd",
				"Partition Distance",
				true),
		MAX_MEMBER_SELECTION(
				Integer.class,
				"pms",
				"Maximum number of members selected from a partition",
				true),
		PARTITIONER_CLASS(
				Partitioner.class,
				"pc",
				"Index Identifier for Centroids",
				true);

		private final Class<?> baseClass;

		private final Option option;

		Partition(
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
