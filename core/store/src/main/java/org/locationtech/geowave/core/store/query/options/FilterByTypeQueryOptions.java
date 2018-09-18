package org.locationtech.geowave.core.store.query.options;

public class FilterByTypeQueryOptions<T> implements
		DataTypeQueryOptions<T>
{
	private String[] typeNames;
	private String[] fieldIds;

	protected FilterByTypeQueryOptions() {}

	public FilterByTypeQueryOptions(
			final String[] typeNames ) {
		this.typeNames = typeNames;
	}

	public FilterByTypeQueryOptions(
			final String typeName,
			final String... fieldIds ) {
		super();
		typeNames = new String[] {
			typeName
		};
		this.fieldIds = fieldIds;
	}

	@Override
	public String[] getTypeNames() {
		return typeNames;
	}

	public String[] getFieldIds() {
		return fieldIds;
	}

	@Override
	public byte[] toBinary() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		// TODO Auto-generated method stub

	}

}
