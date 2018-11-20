package org.locationtech.geowave.datastore.rocksdb.util;

import org.joda.time.Instant;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.common.primitives.Bytes;

public class RocksDBMetadataTable
{
	// TODO change this to private
	public RocksDB db;
	private final boolean requiresTimestamp;

	public RocksDBMetadataTable(
			final RocksDB db,
			final boolean requiresTimestamp ) {
		super();
		this.db = db;
		this.requiresTimestamp = requiresTimestamp;
	}

	public void add(
			final GeoWaveMetadata value ) {
		byte[] key;
		if (requiresTimestamp) {
			key = Bytes
					.concat(
							value.getPrimaryId(),
							value.getSecondaryId(),
							Lexicoders.LONG
									.toByteArray(
											Instant.now().getMillis()),
							new byte[] {
								(byte) value.getPrimaryId().length
							});
		}
		else {
			key = Bytes
					.concat(
							value.getPrimaryId(),
							value.getSecondaryId(),
							new byte[] {
								(byte) value.getPrimaryId().length
							});
		}
		put(
				key,
				valueToBytes(
						value));
	}

	private static byte[] valueToBytes(
			final GeoWaveMetadata value ) {
		return Bytes
				.concat(
						new byte[] {
							(byte) value.getVisibility().length
						},
						value.getVisibility(),
						value.getValue());
	}

	public void put(
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

	public void close() {
		db.close();
	}

}
