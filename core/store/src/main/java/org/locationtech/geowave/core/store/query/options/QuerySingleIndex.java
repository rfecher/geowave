package org.locationtech.geowave.core.store.query.options;

import org.locationtech.geowave.core.index.StringUtils;

public class QuerySingleIndex implements
		IndexQueryOptions
{
	private String indexName;

	public QuerySingleIndex() {
		this(
				null);
	}

	public QuerySingleIndex(
			final String indexName ) {
		this.indexName = indexName;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}

	@Override
	public byte[] toBinary() {
		if ((indexName == null) || indexName.isEmpty()) {
			return new byte[0];
		}
		return StringUtils.stringToBinary(indexName);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if (bytes.length == 0) {
			indexName = null;
		}
		else {
			indexName = StringUtils.stringFromBinary(bytes);
		}

	}

	@Override
	public boolean isAllIndicies() {
		return indexName != null;
	}
}
