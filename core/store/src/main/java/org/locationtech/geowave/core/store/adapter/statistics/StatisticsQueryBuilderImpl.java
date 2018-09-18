package org.locationtech.geowave.core.store.adapter.statistics;

import org.apache.commons.lang.ArrayUtils;
import org.locationtech.geowave.core.store.api.StatisticsQuery;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;

public class StatisticsQueryBuilderImpl<R, B extends StatisticsQueryBuilder<R, B>> implements
		StatisticsQueryBuilder<R, B>
{
	private String dataTypeName;
	protected String[] authorizations = new String[0];
	protected StatisticsType<R, B> statsType = null;

	@Override
	public B dataType(
			final String dataTypeName ) {
		this.dataTypeName = dataTypeName;
		return (B) this;
	}
	
	@Override
	public B addAuthorization(
			final String authorization ) {
		ArrayUtils
				.add(
						authorizations,
						authorization);
		return (B) this;
	}

	@Override
	public B setAuthorizations(
			final String[] authorizations ) {
		this.authorizations = authorizations;
		return (B) this;
	}

	@Override
	public B noAuthorizations() {
		this.authorizations = new String[0];
		return (B) this;
	}

	@Override
	public StatisticsQuery<R> build() {
		return new StatisticsQuery<>(
				dataTypeName,
				statsType,
				extendedId(),
				authorizations);
	}
	
	protected String extendedId(){
		return null;
	}
}
