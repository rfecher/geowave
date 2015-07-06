package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.Writer;

import org.apache.accumulo.core.client.TableNotFoundException;

public class NumericSecondaryIndex extends
		AbstractSecondaryIndex<Number>
{
	public static final String TABLE_NAME = TABLE_PREFIX + "NUMERIC";
	private static NumericSecondaryIndex instance;

	private NumericSecondaryIndex(
			final Writer writer ) {
		super(
				writer);
	}

	/**
	 * Returns a Singleton instance of {@link NumericSecondaryIndex}
	 * 
	 * @param accumuloOperations
	 * @return
	 * @throws InstantiationException
	 *             if unable to construct the necessary {@link Writer}
	 */
	public static NumericSecondaryIndex getInstance(
			final AccumuloOperations accumuloOperations )
			throws InstantiationException {
		if (instance == null) {
			try {
				instance = new NumericSecondaryIndex(
						accumuloOperations.createWriter(TABLE_NAME));
			}
			catch (TableNotFoundException e) {
				throw new InstantiationException(
						"Could not construct writer for NumericSecondaryIndex: " + e.getMessage());
			}
		}
		return instance;
	}

	@Override
	public byte[] constructRowId(
			final Number attributeValue ) {
		return StringUtils.stringToBinary(attributeValue.toString());
	}

}
