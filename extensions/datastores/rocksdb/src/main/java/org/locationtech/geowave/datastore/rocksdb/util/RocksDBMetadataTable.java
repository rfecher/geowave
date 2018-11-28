package org.locationtech.geowave.datastore.rocksdb.util;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

public class RocksDBMetadataTable
{
	private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBMetadataTable.class);
	private final RocksDB db;
	private final boolean requiresTimestamp;

	public RocksDBMetadataTable(
			final RocksDB db,
			final boolean requiresTimestamp ) {
		super();
		this.db = db;
		this.requiresTimestamp = requiresTimestamp;
	}

	public void remove(
			final byte[] key ) {
		try {
			db.delete(key);
		}
		catch (final RocksDBException e) {
			LOGGER.warn(
					"Unable to delete metadata",
					e);
		}
	}

	public void add(
			final GeoWaveMetadata value ) {
		byte[] key;
		final byte[] secondaryId = value.getSecondaryId() == null ? new byte[0] : value.getSecondaryId();
		final byte[] visibility = value.getVisibility() == null ? new byte[0] : value.getVisibility();
		if (requiresTimestamp) {
			key = Bytes.concat(
					value.getPrimaryId(),
					secondaryId,
					Longs.toByteArray(Long.MAX_VALUE - System.currentTimeMillis()),
					visibility,
					new byte[] {
						(byte) value.getPrimaryId().length,
						(byte) visibility.length
					});
		}
		else {
			key = Bytes.concat(
					value.getPrimaryId(),
					secondaryId,
					visibility,
					new byte[] {
						(byte) value.getPrimaryId().length,
						(byte) visibility.length
					});
		}
		put(
				key,
				value.getValue());
	}

	public CloseableIterator<GeoWaveMetadata> iterator(
			final byte[] primaryId ) {
		return prefixIterator(primaryId);
	}

	public CloseableIterator<GeoWaveMetadata> iterator(
			final byte[] primaryId,
			final byte[] secondaryId ) {
		return prefixIterator(Bytes.concat(
				primaryId,
				secondaryId));
	}

	private CloseableIterator<GeoWaveMetadata> prefixIterator(
			final byte[] prefix ) {
		final ReadOptions options = new ReadOptions().setPrefixSameAsStart(true);
		final RocksIterator it = db.newIterator(options);
		it.seek(prefix);
		return new RocksDBMetadataIterator(
				options,
				it,
				requiresTimestamp);
	}

	public CloseableIterator<GeoWaveMetadata> iterator() {
		final RocksIterator it = db.newIterator();
		it.seekToFirst();
		return new RocksDBMetadataIterator(
				it,
				requiresTimestamp);
	}

	public void put(
			final byte[] key,
			final byte[] value ) {
		// WriteBatch w = new WriteBatch();
		// w.put(key, value);
		// TODO batch writes
		try {
			db.put(
					key,
					value);
		}
		catch (final RocksDBException e) {
			LOGGER.warn(
					"Unable to add metadata",
					e);
		}
	}

	public void flush() {
		try {
			db.compactRange();
		}
		catch (final RocksDBException e) {
			LOGGER.warn(
					"Unable to compact metadata range",
					e);
		}
	}

	public void close() {
		db.close();
	}

}
