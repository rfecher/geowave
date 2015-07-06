package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.datastore.accumulo.Writer;

public interface SecondaryIndex
{
	public void write(
			final Object attributeValue,
			final Class<?> attributeType,
			final String attributeName,
			final ByteArrayId rowId );

	public Writer getWriter();
}
