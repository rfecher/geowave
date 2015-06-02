package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.db.AdapterStoreFactory;
import mil.nga.giat.geowave.analytic.db.BasicAccumuloOperationsFactory;
import mil.nga.giat.geowave.analytic.db.IndexStoreFactory;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytic.model.IndexModelBuilder;

import org.apache.commons.cli.Option;

public class CommonParameters
{
	public enum Common
			implements
			ParameterEnum {
		DIMENSION_EXTRACT_CLASS(
				DimensionExtractor.class,
				"dde",
				"Dimension Extractor Class implements mil.nga.giat.geowave.analytics.extract.DimensionExtractor",
				true),
		ACCUMULO_CONNECT_FACTORY(
				BasicAccumuloOperationsFactory.class,
				"ccf",
				"Data Store Connection Factory implements mil.nga.giat.geowave.analytics.tools.dbops.BasicAccumuloOperationsFactory",
				true),
		ADAPTER_STORE_FACTORY(
				AdapterStoreFactory.class,
				"caf",
				"Adapter Store factory implements mil.nga.giat.geowave.analytics.tools.dbops.AdapterStoreFactory",
				true),
		INDEX_STORE_FACTORY(
				IndexStoreFactory.class,
				"cif",
				"Index Store factory implements mil.nga.giat.geowave.analytics.tools.dbops.IndexStoreFactory",
				true),
		DISTANCE_FUNCTION_CLASS(
				DistanceFn.class,
				"cdf",
				"Distance Function Class implements mil.nga.giat.geowave.analytics.distance.DistanceFn",
				true),
		INDEX_MODEL_BUILDER_CLASS(
				IndexModelBuilder.class,
				"cim",
				"Class implements mil.nga.giat.geowave.analytics.tools.model.IndexModelBuilder",
				true);

		private final Class<?> baseClass;

		private final Option option;

		Common(
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
