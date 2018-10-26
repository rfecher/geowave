package org.locationtech.geowave.datastore.redis.util;

import java.io.IOException;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class GeoWaveMetadataCodec extends
		BaseCodec
{
	protected static GeoWaveMetadataCodec SINGLETON = new GeoWaveMetadataCodec();
	private final Decoder<Object> decoder = new Decoder<Object>() {
		@Override
		public Object decode(
				final ByteBuf buf,
				final State state )
				throws IOException {
			final byte[] primaryId = new byte[buf.readUnsignedByte()];
			final byte[] secondaryId = new byte[buf.readUnsignedByte()];
			final byte[] visibility = new byte[buf.readUnsignedByte()];
			final byte[] value = new byte[buf.readUnsignedByte()];
			buf
					.readBytes(
							primaryId);
			buf
					.readBytes(
							secondaryId);
			buf
					.readBytes(
							visibility);
			buf
					.readBytes(
							value);
			return new GeoWaveMetadata(
					primaryId,
					secondaryId,
					visibility,
					value);
		}
	};
	private final Encoder encoder = new Encoder() {
		@Override
		public ByteBuf encode(
				final Object in )
				throws IOException {
			if (in instanceof GeoWaveMetadata) {
				final GeoWaveMetadata md = (GeoWaveMetadata) in;
				final ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
				out
						.writeByte(
								md.getPrimaryId().length);
				out
						.writeByte(
								md.getSecondaryId().length);
				out
						.writeByte(
								md.getVisibility().length);
				out
						.writeByte(
								md.getValue().length);
				out
						.writeBytes(
								md.getPrimaryId());
				out
						.writeBytes(
								md.getSecondaryId());
				out
						.writeBytes(
								md.getVisibility());
				out
						.writeBytes(
								md.getValue());
			}
			throw new IOException(
					"Encoder only supports GeoWave metadata");
		}
	};

	private GeoWaveMetadataCodec() {
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
