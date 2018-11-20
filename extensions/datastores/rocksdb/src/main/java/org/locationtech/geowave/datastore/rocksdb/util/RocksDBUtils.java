package org.locationtech.geowave.datastore.rocksdb.util;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.BaseReaderParams;
import org.locationtech.geowave.core.store.operations.MetadataType;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Streams;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;

public class RocksDBUtils
{
	protected static final int MAX_ROWS_FOR_PAGINATION = 1000000;
	public static int REDIS_DEFAULT_MAX_RANGE_DECOMPOSITION = 250;
	public static int REDIS_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION = 250;

	public static RocksDBMetadataTable getMetadataTable(
			final RocksDBClient client,
			final MetadataType metadataType ) {
		// stats also store a timestamp because stats can be the exact same but
		// need to still be unique (consider multiple count statistics that are
		// exactly the same count, but need to be merged)
		return client
				.getMetadataTable(
						metadataType);
	}

	public static String getTablePrefix(
			final String typeName,
			final String indexName ) {
		return typeName + "_" + indexName;
	}

	public static RocksDBIndexTable getIndexTable(
			final RocksDBClient client,
			final String setNamePrefix,
			final byte[] partitionKey,
			final boolean requiresTimestamp ) {
		return getIndexTable(
				client,
				getTableName(
						setNamePrefix,
						partitionKey),
				requiresTimestamp);

	}

	public static String getTableName(
			final String namespace,
			final String typeName,
			final String indexName,
			final byte[] partitionKey ) {
		return getTableName(
				getTablePrefix(
						typeName,
						indexName),
				partitionKey);
	}

	public static String getTableName(
			final String setNamePrefix,
			final byte[] partitionKey ) {
		String partitionStr;
		if ((partitionKey != null) && (partitionKey.length > 0)) {
			partitionStr = "_" + ByteArrayUtils
					.byteArrayToString(
							partitionKey);
		}
		else {
			partitionStr = "";
		}
		return setNamePrefix + partitionStr;
	}

	public static RocksDBIndexTable getIndexTable(
			final RocksDBClient client,
			final String tableName,
			final boolean requiresTimestamp ) {
		return client
				.getIndexTable(
						tableName,
						requiresTimestamp);

	}

	public static RocksDBIndexTable getIndexTable(
			final RocksDBClient client,
			final String typeName,
			final String indexName,
			final byte[] partitionKey,
			final boolean requiresTimestamp ) {
		return getIndexTable(
				client,
				getTablePrefix(
						typeName,
						indexName),
				partitionKey,
				requiresTimestamp);
	}

	public static Set<ByteArray> getPartitions(
			final String directory,
			final String tableNamePrefix ) {
		return Arrays
				.stream(
						new File(
								directory)
										.list(
												(
														dir,
														name ) -> name
																.startsWith(
																		tableNamePrefix)))
				.map(
						str -> str.length() > (tableNamePrefix.length() + 1) ? new ByteArray(
								ByteArrayUtils
										.byteArrayFromString(
												str
														.substring(
																tableNamePrefix.length() + 1)))
								: new ByteArray())
				.collect(
						Collectors.toSet());
	}

	public static Iterator<GeoWaveMetadata> groupByIds(
			final Iterable<GeoWaveMetadata> result ) {
		final ListMultimap<ByteArray, GeoWaveMetadata> multimap = MultimapBuilder.hashKeys().arrayListValues().build();
		result
				.forEach(
						r -> multimap
								.put(
										new ByteArray(
												Bytes
														.concat(
																r.getPrimaryId(),
																r.getSecondaryId())),
										r));
		return multimap.values().iterator();
	}

	public static boolean isSortByTime(
			final InternalDataAdapter<?> adapter ) {
		return adapter.getAdapter() instanceof RowMergingDataAdapter;
	}

	public static boolean isSortByKeyRequired(
			final BaseReaderParams<?> params ) {
		// subsampling needs to be sorted by sort key to work properly
		return (params.getMaxResolutionSubsamplingPerDimension() != null)
				&& (params.getMaxResolutionSubsamplingPerDimension().length > 0);
	}

	public static Iterator<GeoWaveRow> sortBySortKey(
			final Iterator<GeoWaveRow> it ) {
		return Streams
				.stream(
						it)
				.sorted(
						SortKeyOrder.SINGLETON)
				.iterator();
	}

	public static Pair<Boolean, Boolean> isGroupByRowAndIsSortByTime(
			final BaseReaderParams<?> readerParams,
			final short adapterId ) {
		final boolean sortByTime = isSortByTime(
				readerParams
						.getAdapterStore()
						.getAdapter(
								adapterId));
		return Pair
				.of(
						readerParams.isMixedVisibility() || sortByTime,
						sortByTime);
	}

	private static class SortKeyOrder implements
			Comparator<GeoWaveRow>,
			Serializable
	{
		private static SortKeyOrder SINGLETON = new SortKeyOrder();
		private static final long serialVersionUID = 23275155231L;

		@Override
		public int compare(
				final GeoWaveRow o1,
				final GeoWaveRow o2 ) {
			if (o1 == o2) {
				return 0;
			}
			if (o1 == null) {
				return 1;
			}
			if (o2 == null) {
				return -1;
			}
			byte[] otherComp = o2.getSortKey() == null ? new byte[0] : o2.getSortKey();
			byte[] thisComp = o1.getSortKey() == null ? new byte[0] : o1.getSortKey();

			int comp = UnsignedBytes
					.lexicographicalComparator()
					.compare(
							thisComp,
							otherComp);
			if (comp != 0) {
				return comp;
			}
			otherComp = o2.getPartitionKey() == null ? new byte[0] : o2.getPartitionKey();
			thisComp = o1.getPartitionKey() == null ? new byte[0] : o1.getPartitionKey();

			comp = UnsignedBytes
					.lexicographicalComparator()
					.compare(
							thisComp,
							otherComp);
			if (comp != 0) {
				return comp;
			}
			comp = Short
					.compare(
							o1.getAdapterId(),
							o2.getAdapterId());
			if (comp != 0) {
				return comp;
			}
			otherComp = o2.getDataId() == null ? new byte[0] : o2.getDataId();
			thisComp = o1.getDataId() == null ? new byte[0] : o1.getDataId();

			comp = UnsignedBytes
					.lexicographicalComparator()
					.compare(
							thisComp,
							otherComp);

			if (comp != 0) {
				return comp;
			}
			return Integer
					.compare(
							o1.getNumberOfDuplicates(),
							o2.getNumberOfDuplicates());
		}

	}
}
