package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.util.Map.Entry;

import mil.nga.giat.geowave.index.ByteArrayId;

public interface ConvertingList<OUTTYPE>
{
	Iterable<Entry<ByteArrayId, OUTTYPE>> getIterable();
}
