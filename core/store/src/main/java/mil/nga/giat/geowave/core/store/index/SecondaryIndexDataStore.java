package mil.nga.giat.geowave.core.store.index;

import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;

/**
 * This is responsible for persisting secondary index entries
 */
public interface SecondaryIndexDataStore
{
	/**
	 * 
	 * @param indexID
	 * @param ranges
	 * @param visibility
	 * @param primaryIndexID
	 * @param indexRowIds
	 */
	public void store(
			ByteArrayId indexID,
			List<ByteArrayId> ranges,
			ByteArrayId visibility,
			ByteArrayId primaryIndexID,
			List<ByteArrayId> indexRowIds );

	/**
	 * 
	 * @param indexID
	 * @param ranges
	 * @param visibility
	 * @param primaryIndexID
	 * @param indexRowIds
	 */
	public void remove(
			ByteArrayId indexID,
			List<ByteArrayId> ranges,
			ByteArrayId visibility,
			ByteArrayId primaryIndexID,
			List<ByteArrayId> indexRowIds );

	/**
	 * 
	 * @param indexID
	 *            secondary index ID
	 * @param ranges
	 * @param visibility
	 * @return Primary Index ID associated Range Values
	 */
	public Map<ByteArrayId, List<ByteArrayRange>> query(
			ByteArrayId indexID,
			List<ByteArrayRange> ranges,
			String... visibility );
}
