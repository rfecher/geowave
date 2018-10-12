package org.locationtech.geowave.core.store.query.options;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.data.field.FieldUtils;

public class CommonQueryOptions implements
		Persistable
{
	public static class HintKey<HintValueType>
	{
		private Class<HintValueType> cls;
		private Function<byte[], HintValueType> reader;
		private Function<HintValueType, byte[]> writer;

		public HintKey(
				final Class<HintValueType> cls ) {
			this(
					cls,
					FieldUtils
							.getDefaultReaderForClass(
									cls),
					FieldUtils
							.getDefaultWriterForClass(
									cls));
		}

		public HintKey(
				final Class<HintValueType> cls,
				final Function<byte[], HintValueType> reader,
				final Function<HintValueType, byte[]> writer ) {
			this.cls = cls;
			this.reader = reader;
			this.writer = writer;
		}
	}

	private final Map<HintKey<?>, Object> hints;
	private final Integer limit;
	private final String[] authorizations;

	public CommonQueryOptions(
			final String... authorizations ) {

		this(
				(Integer) null,
				authorizations);
	}

	public CommonQueryOptions(
			final Integer limit,
			final String... authorizations ) {
		this(
				limit,
				new HashMap<>(),
				authorizations);
	}

	public CommonQueryOptions(
			final Integer limit,
			final Map<HintKey<?>, Object> hints,
			final String... authorizations ) {
		super();
		this.hints = hints;
		this.limit = limit;
		this.authorizations = authorizations;
	}

	public Map<HintKey<?>, Object> getHints() {
		return hints;
	}

	public Integer getLimit() {
		return limit;
	}

	public String[] getAuthorizations() {
		return authorizations;
	}

	@Override
	public byte[] toBinary() {
		return null;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		// TODO Auto-generated method stub

	}

}