package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.Projection;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.extract.CentroidExtractor;

import org.apache.commons.cli.Option;

public class HullParameters
{
	public enum Hull
			implements
			ParameterEnum {
		INDEX_ID(
				String.class,
				"hid",
				"Index Identifier for Centroids",
				true),
		DATA_TYPE_ID(
				String.class,
				"hdt",
				"Data Type ID for a centroid item",
				true),
		DATA_NAMESPACE_URI(
				String.class,
				"hns",
				"Data Type Namespace for a centroid item",
				true),
		REDUCER_COUNT(
				Integer.class,
				"hrc",
				"Centroid Reducer Count",
				true),
		PROJECTION_CLASS(
				Projection.class,
				"hpe",
				"Class to project on to 2D space. Implements mil.nga.giat.geowave.analytics.tools.Projection",
				true),
		EXTRACTOR_CLASS(
				CentroidExtractor.class,
				"hce",
				"Centroid Exractor Class implements mil.nga.giat.geowave.analytics.extract.CentroidExtractor",
				true),
		WRAPPER_FACTORY_CLASS(
				AnalyticItemWrapperFactory.class,
				"hfc",
				"Class to create analytic item to capture hulls. Implements mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory",
				true),
		ZOOM_LEVEL(
				Integer.class,
				"hzl",
				"Zoom Level Number",
				true);

		private final Class<?> baseClass;
		private final Option option;

		Hull(
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
