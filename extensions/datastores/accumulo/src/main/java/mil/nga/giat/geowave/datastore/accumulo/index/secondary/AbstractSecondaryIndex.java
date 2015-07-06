package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.datastore.accumulo.Writer;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public abstract class AbstractSecondaryIndex<T> implements
		SecondaryIndex
{
	protected static final String TABLE_PREFIX = "GEOWAVE_2ND_IDX_";
	private final Writer writer;

	public AbstractSecondaryIndex(
			final Writer writer ) {
		this.writer = writer;
	}

	@Override
	public Writer getWriter() {
		return writer;
	}

	@Override
	public void write(
			final Object attributeValue,
			final Class<?> attributeType,
			final String attributeName,
			final ByteArrayId rowId ) {

		@SuppressWarnings("unchecked")
		final Mutation m = new Mutation(
				constructRowId((T) attributeValue));
		m.put(
				new Text(
						attributeType.toString()),
				new Text(
						attributeName),
				new Value(
						rowId.getBytes()));
		writer.write(m);
	}

	public abstract byte[] constructRowId(
			final T attributeValue );

}
