package org.locationtech.geowave.datastore.redis.util;

import java.io.IOException;

import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class GeoWaveRedisRowCodec extends
		BaseCodec
{
	protected static GeoWaveRedisRowCodec SINGLETON = new GeoWaveRedisRowCodec();
	private final Decoder<Object> decoder = new Decoder<Object>() {
		@Override
		public Object decode(
				final ByteBuf buf,
				final State state )
				throws IOException {
			final byte[] dataId = new byte[buf.readUnsignedByte()];
			final byte[] fieldMask = new byte[buf.readUnsignedByte()];
			final byte[] visibility = new byte[buf.readUnsignedByte()];
			final byte[] value = new byte[buf.readUnsignedShort()];
			final short numDuplicates = buf.readUnsignedByte();
			buf.readBytes(dataId);
			buf.readBytes(fieldMask);
			buf.readBytes(visibility);
			buf.readBytes(value);
			return new GeoWaveRedisPersistedRow(
					numDuplicates,
					dataId,
					new GeoWaveValueImpl(
							fieldMask,
							visibility,
							value));
		}
	};
	private final Encoder encoder = new Encoder() {
		@Override
		public ByteBuf encode(
				final Object in )
				throws IOException {
			if (in instanceof GeoWaveRedisPersistedRow) {
				final GeoWaveRedisPersistedRow row = (GeoWaveRedisPersistedRow) in;
				final ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
				out.writeByte(row.getDataId().length);
				out.writeByte(row.getFieldMask().length);
				out.writeByte(row.getVisibility().length);
				out.writeShort(row.getValue().length);
				out.writeByte(row.getNumDuplicates());
				out.writeBytes(row.getDataId());
				out.writeBytes(row.getFieldMask());
				out.writeBytes(row.getVisibility());
				out.writeBytes(row.getValue());
				return out;
			}
			throw new IOException(
					"Encoder only supports GeoWaveRedisRow");
		}
	};

	private GeoWaveRedisRowCodec() {}

	@Override
	public Decoder<Object> getValueDecoder() {
		return decoder;
	}

	@Override
	public Encoder getValueEncoder() {
		return encoder;
	}
}
