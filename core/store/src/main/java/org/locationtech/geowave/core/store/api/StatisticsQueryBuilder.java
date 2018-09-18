package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.store.adapter.statistics.BaseStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.PartitionStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsQueryBuilderImpl;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.NumericHistogram;

public interface StatisticsQueryBuilder<R, B extends StatisticsQueryBuilder<R, B>>
{
	B dataType(
			String dataTypeName );

	B addAuthorization(
			String authorization );

	B setAuthorizations(
			String[] authorizations );

	B noAuthorizations();

	StatisticsQuery<R> build();

	static <R> StatisticsQueryBuilder<R, ?> newBuilder() {
		return new StatisticsQueryBuilderImpl<>();
	}

	public static class QueryByStatisticsTypeFactory
	{
		public static BaseStatisticsQueryBuilder<Long> count() {
			return CountDataStatistics.STATS_TYPE.newBuilder();
		}

		public static PartitionStatisticsQueryBuilder<NumericHistogram> rowHistogram() {
			return RowRangeHistogramStatistics.STATS_TYPE.newBuilder();
		}
	}
}
