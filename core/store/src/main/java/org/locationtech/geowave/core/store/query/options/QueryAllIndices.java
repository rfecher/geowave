package org.locationtech.geowave.core.store.query.options;

public class QueryAllIndices extends
		QuerySingleIndex
{

	public QueryAllIndices() {
		super(
				null);
	}

	@Override
	public byte[] toBinary() {
		return new byte[0];
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

}
