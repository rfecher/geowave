package mil.nga.giat.geowave.analytics.mapreduce.nn;

import mil.nga.giat.geowave.index.ByteArrayId;

/**
 * Convert object consumed by NN to a 'smaller' object pertinent to any subclass
 * algorithms
 * 
 * @param <TYPE>
 */
public interface TypeConverter<TYPE>
{
	public TYPE convert(
			ByteArrayId id,
			Object o );
}
