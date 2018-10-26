package org.locationtech.geowave.datastore.redis.util;

import java.util.BitSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;

import com.google.common.collect.Streams;

public class RedisUtils
{
	public static RScoredSortedSet<GeoWaveMetadata> getMetadataSet(
			final RedissonClient client,
			final String namespace,
			final MetadataType metadataType ) {
		return client
				.getScoredSortedSet(
						namespace + "_" + metadataType.toString(),
						GeoWaveMetadataCodec.SINGLETON);
	}

	public static String getRowSetPrefix(
			final String namespace,
			final String typeName,
			final String indexName ) {
		return namespace + "_" + typeName + "_" + indexName;
	}

	public static RScoredSortedSet<GeoWaveRedisPersistedRow> getRowSet(
			final RedissonClient client,
			final String setNamePrefix,
			final byte[] partitionKey ) {
		return getRowSet(
				client,
				getRowSetName(
						setNamePrefix,
						partitionKey));

	}

	public static String getRowSetName(
			final String namespace,
			final String typeName,
			final String indexName,
			final byte[] partitionKey ) {
		return getRowSetName(
				getRowSetPrefix(
						namespace,
						typeName,
						indexName),
				partitionKey);
	}

	public static String getRowSetName(
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

	public static RScoredSortedSet<GeoWaveRedisPersistedRow> getRowSet(
			final RedissonClient client,
			final String setName ) {
		return client
				.getScoredSortedSet(
						setName,
						GeoWaveRedisRowCodec.SINGLETON);

	}

	public static RScoredSortedSet<GeoWaveRedisPersistedRow> getRowSet(
			final RedissonClient client,
			final String namespace,
			final String typeName,
			final String indexName,
			final byte[] partitionKey ) {
		return getRowSet(
				client,
				getRowSetPrefix(
						namespace,
						typeName,
						indexName),
				partitionKey);
	}

	public static double getScore(
			final byte[] byteArray ) {
		return bytesToLong(
				byteArray);
	}

	public static byte[] getSortKey(
			final double score ) {
		return shift(
				(long) score);
	}

	public static void main(
			final String[] args ) {
		for (int i = 0; i < 10; i++) {
			long val = new Random().nextLong();
			// * Math
			// .pow(
			// 2,
			// 52));

			final BitSet set = BitSet
					.valueOf(
							new long[] {
								val
							});
			for (int a = 0; a < 12; a++) {
				set
						.clear(
								a);
			}
			val = set.toLongArray()[0];
			System.err
					.println(
							val);

			System.err
					.println(
							(double) val);

			System.err
					.println(
							Long
									.toHexString(
											val));

			System.err
					.println(
							new ByteArrayId(
									shift(
											val)).getHexString());
			System.err
					.println(
							new ByteArrayId(
									getSortKey(
											val)).getHexString());

			System.err
					.println(
							getScore(
									getSortKey(
											val)));

			System.err
					.println(
							new ByteArrayId(
									getSortKey(
											getScore(
													getSortKey(
															val)))).getHexString());
		}
	}

	private static byte[] shift(
			long val ) {
		final byte[] array = new byte[8];

		final int radix = 1 << 8;
		final int mask = radix - 1;
		int pos = 8;
		do {
			array[--pos] = (byte) (((int) val) & mask);
			val >>>= 8;
		}
		while ((val != 0) && (pos > 0));

		return array;
	}

	// private static byte[] longToBytes(
	// final long value ) {
	// long l = value;
	// final byte[] bytes = new byte[8];
	//
	// bytes[7] = (byte) (l & 0x0f);
	// l >>= 4;
	// for (int i = 6; i > 1; i--) {
	// bytes[i] = (byte) (l & 0xff);
	// l >>= 8;
	// }
	// return bytes;
	// }

	private static long bytesToLong(
			final byte[] bytes ) {
		// grab the most significant 52 bits (matissa of a double) and make a
		// long
		// all of the left-most 6 bytes and the left 4 bits of the 7th byte
		long value = 0;
		// this accumulates the value for the first 6 bytes (48 bits)
		for (int i = 0; i < 8; i++) {
			value = (value << 8);
			if (i < bytes.length) {
				value += (bytes[i] & 0xff);
			}
		}
		// and this is the final accumulation for bits 49-52
		// value = (value << 4);
		// if (bytes.length < 7) {
		// value += (bytes[6] & 0xf0);
		// }
		return value;
	}

	public static Set<ByteArrayId> getPartitions(
			final RedissonClient client,
			final String setNamePrefix ) {
		return Streams
				.stream(
						client
								.getKeys()
								.getKeysByPattern(
										setNamePrefix + "*"))
				.map(
						str -> str.length() > (setNamePrefix.length() + 1) ? new ByteArrayId(
								ByteArrayUtils
										.byteArrayFromString(
												str
														.substring(
																setNamePrefix.length() + 1)))
								: new ByteArrayId())
				.collect(
						Collectors.toSet());
	}
}
