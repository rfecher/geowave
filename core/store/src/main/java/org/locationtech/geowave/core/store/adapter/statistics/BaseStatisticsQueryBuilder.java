package org.locationtech.geowave.core.store.adapter.statistics;

public class BaseStatisticsQueryBuilder<R> extends StatisticsQueryBuilderImpl<R,BaseStatisticsQueryBuilder<R>>
{
	private StatisticsType<R, BaseStatisticsQueryBuilder<R>> type;

	public BaseStatisticsQueryBuilder(
			StatisticsType<R, BaseStatisticsQueryBuilder<R>> type ) {
		this.type = type;
	}
}
