package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import java.util.Date;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.Writer;

import org.apache.accumulo.core.client.TableNotFoundException;

public class TemporalSecondaryIndex extends
		AbstractSecondaryIndex<Date>
{
	public static final String TABLE_NAME = TABLE_PREFIX + "TEMPORAL";
	private static TemporalSecondaryIndex instance;

	private TemporalSecondaryIndex(
			final Writer writer ) {
		super(
				writer);
	}

	/**
	 * Returns a Singleton instance of {@link TemporalSecondaryIndex}
	 * 
	 * @param accumuloOperations
	 * @return
	 * @throws InstantiationException
	 *             if unable to construct the necessary {@link Writer}
	 */
	public static TemporalSecondaryIndex getInstance(
			final AccumuloOperations accumuloOperations )
			throws InstantiationException {
		if (instance == null) {
			try {
				instance = new TemporalSecondaryIndex(
						accumuloOperations.createWriter(
								TABLE_NAME,
								true,
								false));
			}
			catch (TableNotFoundException e) {
				throw new InstantiationException(
						"Could not construct writer for TemporalSecondaryIndex: " + e.getMessage());
			}
		}
		return instance;
	}

	@Override
	public byte[] constructRowId(
			final Date attributeValue ) {
		return StringUtils.stringToBinary(String.valueOf(attributeValue.getTime()));
	}

}
