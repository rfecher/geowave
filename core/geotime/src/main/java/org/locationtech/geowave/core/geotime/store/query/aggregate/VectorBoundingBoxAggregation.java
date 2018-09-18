package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.geotime.store.query.aggregate.VectorBoundingBoxAggregation.FieldNameParam;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public class VectorBoundingBoxAggregation extends
		BoundingBoxAggregation<FieldNameParam, SimpleFeature>
{
	public static class FieldNameParam implements
			Persistable
	{
		// TODO we can also include a requested CRS in case we want to reproject
		// (although it seemingly can just as easily be done on the resulting
		// envelope rather than per feature)
		private String fieldName;

		public FieldNameParam() {
			this(
					null);
		}

		public FieldNameParam(
				final String fieldName ) {
			this.fieldName = fieldName;
		}

		@Override
		public byte[] toBinary() {
			if ((fieldName == null) || fieldName.isEmpty()) {
				return new byte[0];
			}
			return StringUtils
					.stringToBinary(
							fieldName);
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			if (bytes.length > 0) {
				fieldName = StringUtils
						.stringFromBinary(
								bytes);
			}
			else {
				fieldName = null;
			}
		}

		private boolean isEmpty() {
			return (fieldName == null) || fieldName.isEmpty();
		}

		public String getFieldName() {
			return fieldName;
		}
	}

	private FieldNameParam fieldNameParam;

	public VectorBoundingBoxAggregation() {
		this(
				null);
	}

	public VectorBoundingBoxAggregation(
			final FieldNameParam fieldNameParam ) {
		super();
		this.fieldNameParam = fieldNameParam;
	}

	@Override
	public FieldNameParam getParameters() {
		return fieldNameParam;
	}

	@Override
	public void setParameters(
			final FieldNameParam fieldNameParam ) {
		this.fieldNameParam = fieldNameParam;
	}

	@Override
	protected Envelope getEnvelope(
			final SimpleFeature entry ) {
		Object o;
		if ((fieldNameParam != null) && !fieldNameParam.isEmpty()) {
			o = entry
					.getAttribute(
							fieldNameParam.getFieldName());
		}
		else {
			o = entry.getDefaultGeometry();
		}
		if ((o != null) && (o instanceof Geometry)) {
			final Geometry geometry = (Geometry) o;
			if (!geometry.isEmpty()) {
				return geometry.getEnvelopeInternal();
			}
		}
		return null;
	}

}
