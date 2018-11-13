package org.locationtech.geowave.datastore.redis.util;

import java.io.IOException;

import org.redisson.client.codec.BaseCodec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class GeoWaveRedisDataIdCodec extends
		BaseCodec
{
	protected static GeoWaveRedisDataIdCodec SINGLETON = new GeoWaveRedisDataIdCodec();
	private final Decoder<Object> decoder = new Decoder<Object>() {
		@Override
		public Object decode(
				final ByteBuf buf,
				final State state )
				throws IOException {
			final byte[] result = new byte[buf.readableBytes()];
			buf.readBytes(result);
			return result;
		}
	};
	private final Encoder encoder = new Encoder() {
		@Override
		public ByteBuf encode(
				final Object in )
				throws IOException {
			if (in instanceof byte[]) {
				final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
				buf.writeBytes((byte[]) in);
				return buf;
			}
			throw new IOException(
					"Encoder only supports byte arrays");
		}
	};

	private GeoWaveRedisDataIdCodec() {}

	@Override
	public Decoder<Object> getValueDecoder() {
		return decoder;
	}

	@Override
	public Encoder getValueEncoder() {
		return encoder;
	}
}
