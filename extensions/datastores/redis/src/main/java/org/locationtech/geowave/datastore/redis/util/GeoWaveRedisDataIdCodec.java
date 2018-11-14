package org.locationtech.geowave.datastore.redis.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import com.google.common.primitives.Bytes;
import com.oath.halodb.HaloDB;
import com.oath.halodb.HaloDBException;
import com.oath.halodb.HaloDBOptions;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class GeoWaveRedisDataIdCodec extends
		BaseCodec
{
	protected static GeoWaveRedisDataIdCodec SINGLETON = new GeoWaveRedisDataIdCodec();
	private static AtomicInteger ctr = new AtomicInteger(
			0);
	private final Decoder<Object> decoder = new Decoder<Object>() {
		@Override
		public Object decode(
				final ByteBuf buf,
				final State state )
				throws IOException {
			// final byte[] result = new byte[buf.readableBytes()];
			// ;
			byte[] payload;
			try {
				payload = db.get(ByteBuffer.allocate(
						4).putInt(
						buf.readInt()).array());
				ByteBuffer in = ByteBuffer.wrap(payload);
				final byte[] dataId = new byte[in.get()];
				final byte[] fieldMask = new byte[in.get()];
				final byte[] visibility = new byte[in.get()];
				final int numDuplicates = in.get();
				final byte[] value = new byte[in.getInt()];
				in.get(
						dataId).get(
						fieldMask).get(
						visibility).get(
						value);
				return new GeoWaveRedisPersistedRow(
						(short) numDuplicates,
						dataId,
						new GeoWaveValueImpl(
								fieldMask,
								visibility,
								value));

			}
			catch (HaloDBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
			// GeoWaveRedisPersistedRow result = new GeoWaveRedisPersistedRow(0,
			// dataId, new GeoWaveValueImpl(null, null, )));
			// return result;
		}
	};
	private final Encoder encoder = new Encoder() {
		@Override
		public ByteBuf encode(
				final Object in )
				throws IOException {
			if (in instanceof GeoWaveRedisPersistedRow) {
				final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
				int id = ctr.getAndIncrement();
				buf.writeInt(id);
				try {
					final GeoWaveRedisPersistedRow row = (GeoWaveRedisPersistedRow) in;
					db.put(
							ByteBuffer.allocate(
									4).putInt(
									id).array(),
							Bytes.concat(
									new byte[] {
										(byte) row.getDataId().length,
										(byte) row.getFieldMask().length,
										(byte) row.getVisibility().length,
										(byte) row.getNumDuplicates()
									},
									ByteBuffer.allocate(
											4).putInt(
											row.getValue().length).array(),
									row.getDataId(),
									row.getFieldMask(),
									row.getVisibility(),
									row.getValue()));
				}
				catch (HaloDBException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return buf;
			}
			throw new IOException(
					"Encoder only supports byte arrays");
		}
	};

	// Represents a database instance and provides all methods for operating on
	// the database.
	HaloDB db = null;

	private GeoWaveRedisDataIdCodec() {
		// Open a db with default options.
		HaloDBOptions options = new HaloDBOptions();

		// size of each data file will be 1GB.
		options.setMaxFileSize(1024 * 1024 * 1024);

		// the threshold at which page cache is synced to disk.
		// data will be durable only if it is flushed to disk, therefore
		// more data will be lost if this value is set too high. Setting
		// this value too low might interfere with read and write performance.
		options.setFlushDataSizeBytes(10 * 1024 * 1024);

		// The percentage of stale data in a data file at which the file will be
		// compacted.
		// This value helps control write and space amplification. Increasing
		// this value will
		// reduce write amplification but will increase space amplification.
		// This along with the compactionJobRate below is the most important
		// setting
		// for tuning HaloDB performance. If this is set to x then write
		// amplification
		// will be approximately 1/x.
		options.setCompactionThresholdPerFile(0.7);

		// Controls how fast the compaction job should run.
		// This is the amount of data which will be copied by the compaction
		// thread per second.
		// Optimal value depends on the compactionThresholdPerFile option.
		options.setCompactionJobRate(50 * 1024 * 1024);

		// Setting this value is important as it helps to preallocate enough
		// memory for the off-heap cache. If the value is too low the db might
		// need to rehash the cache. For a db of size n set this value to 2*n.
		options.setNumberOfRecords(100_000_000);

		// Delete operation for a key will write a tombstone record to a
		// tombstone file.
		// the tombstone record can be removed only when all previous version of
		// that key
		// has been deleted by the compaction job.
		// enabling this option will delete during startup all tombstone records
		// whose previous
		// versions were removed from the data file.
		options.setCleanUpTombstonesDuringOpen(true);

		// HaloDB does native memory allocation for the in-memory index.
		// Enabling this option will release all allocated memory back to the
		// kernel when the db is closed.
		// This option is not necessary if the JVM is shutdown when the db is
		// closed, as in that case
		// allocated memory is released automatically by the kernel.
		// If using in-memory index without memory pool this option,
		// depending on the number of records in the database,
		// could be a slow as we need to call _free_ for each record.
		options.setCleanUpInMemoryIndexOnClose(false);

		// ** settings for memory pool **
		options.setUseMemoryPool(true);

		// Hash table implementation in HaloDB is similar to that of
		// ConcurrentHashMap in Java 7.
		// Hash table is divided into segments and each segment manages its own
		// native memory.
		// The number of segments is twice the number of cores in the machine.
		// A segment's memory is further divided into chunks whose size can be
		// configured here.
		options.setMemoryPoolChunkSize(32 * 1024 * 1024);

		// using a memory pool requires us to declare the size of keys in
		// advance.
		// Any write request with key length greater than the declared value
		// will fail, but it
		// is still possible to store keys smaller than this declared size.
		options.setFixedKeySize(4);

		// The directory will be created if it doesn't exist and all database
		// files will be stored in this directory
		String directory = "halodb-dir";

		// Open the database. Directory will be created if it doesn't exist.
		// If we are opening an existing database HaloDB needs to scan all the
		// index files to create the in-memory index, which, depending on the db
		// size, might take a few minutes.
		try {
			db = HaloDB.open(
					directory,
					options);
		}
		catch (HaloDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Decoder<Object> getValueDecoder() {
		return decoder;
	}

	@Override
	public Encoder getValueEncoder() {
		return encoder;
	}
}
