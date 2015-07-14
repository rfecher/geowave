package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.Writer;

import org.apache.accumulo.core.client.TableNotFoundException;

public class TextSecondaryIndex extends
		AbstractSecondaryIndex<String>
{
	public static final String TABLE_NAME = TABLE_PREFIX + "TEXT";
	private static TextSecondaryIndex instance;

	private TextSecondaryIndex(
			Writer writer ) {
		super(
				writer);
	}

	/**
	 * Returns a Singleton instance of {@link TextSecondaryIndex}
	 * 
	 * @param accumuloOperations
	 * @return
	 * @throws InstantiationException
	 *             if unable to construct the necessary {@link Writer}
	 */
	public static TextSecondaryIndex getInstance(
			final AccumuloOperations accumuloOperations )
			throws InstantiationException {
		if (instance == null) {
			try {
				instance = new TextSecondaryIndex(
						accumuloOperations.createWriter(
								TABLE_NAME,
								true,
								false));
			}
			catch (TableNotFoundException e) {
				throw new InstantiationException(
						"Could not construct writer for TextSecondaryIndex: " + e.getMessage());
			}
		}
		return instance;
	}

	@Override
	public void write(
			final Object attributeValue,
			final Class<?> attributeType,
			final String attributeName,
			final ByteArrayId rowId ) {

		// TODO most likely will need to override write() method for text

		super.write(
				attributeValue,
				attributeType,
				attributeName,
				rowId);
	}

	@Override
	public byte[] constructRowId(
			String attributeValue ) {
		// TODO Implement
		return null;
	}

}
