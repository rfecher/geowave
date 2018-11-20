package org.locationtech.geowave.datastore.rocksdb.util;

import java.time.Instant;

import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.common.primitives.Bytes;

public class RocksDBIndexTable
{
	private final RocksDB db;
	private final boolean requiresTimestamp;

	public RocksDBIndexTable(
			final RocksDB db,
			final boolean requiresTimestamp ) {
		super();
		this.db = db;
		this.requiresTimestamp = requiresTimestamp;
	}

	public void add(
			final byte[] sortKey,
			final byte[] dataId,
			final short numDuplicates,
			final GeoWaveValue value ) {
		byte[] key;
		if (requiresTimestamp) {
			final Instant instant = Instant.now();
			key = Bytes
					.concat(
							sortKey,
							dataId,
							Lexicoders.INT
									.toByteArray(
											(int) instant.getEpochSecond()),
							Lexicoders.INT
									.toByteArray(
											instant.getNano()),
							ByteArrayUtils
									.shortToByteArray(
											numDuplicates));
		}
		else {
			key = Bytes
					.concat(
							sortKey,
							dataId,
							ByteArrayUtils
									.shortToByteArray(
											numDuplicates));
		}
		put(
				key,
				valueToBytes(
						value));
	}

	private void put(
			final byte[] key,
			final byte[] value ) {
		// WriteBatch w = new WriteBatch();
		// w.put(key, value);
		// TODO batch writes
		try {
			db
					.put(
							key,
							value);
		}
		catch (final RocksDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void flush() {
		// TODO flush batch writes
		try {
			db.compactRange();
		}
		catch (final RocksDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static byte[] valueToBytes(
			final GeoWaveValue value ) {
		return Bytes
				.concat(
						new byte[] {
							(byte) value.getFieldMask().length,
							(byte) value.getVisibility().length
						},
						value.getFieldMask(),
						value.getVisibility(),
						value.getValue());
	}

	public void close() {
		db.close();
	}

}
