package org.locationtech.geowave.core.store.operations;

import java.util.Arrays;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import com.google.common.primitives.Bytes;

public class DefaultDataIndexRowWriterWrapper implements RowWriter {
  private final RowWriter delegateWriter;

  public DefaultDataIndexRowWriterWrapper(final RowWriter delegateWriter) {
    this.delegateWriter = delegateWriter;
  }

  @Override
  public void close() throws Exception {
    delegateWriter.close();
  }

  @Override
  public void write(final GeoWaveRow[] rows) {
    Arrays.stream(rows).forEach(r -> delegateWriter.write(new GeoWaveRowWrapper(r)));
  }

  @Override
  public void write(final GeoWaveRow row) {
    delegateWriter.write(row);
  }

  @Override
  public void flush() {
    delegateWriter.flush();
  }

  protected static class GeoWaveRowWrapper implements GeoWaveRow {
    private final GeoWaveRow row;

    protected GeoWaveRowWrapper(final GeoWaveRow row) {
      this.row = row;
    }

    @Override
    public GeoWaveValue[] getFieldValues() {
      return Arrays.stream(row.getFieldValues()).map(v -> new GeoWaveValueWrapper(v)).toArray(
          i -> new GeoWaveValue[i]);
    }

    @Override
    public byte[] getDataId() {
      return row.getDataId();
    }

    @Override
    public short getAdapterId() {
      return row.getAdapterId();
    }

    @Override
    public byte[] getSortKey() {
      final byte[] sortKey = row.getDataId();
      return Bytes.concat(new byte[] {(byte) sortKey.length}, sortKey);
    }

    @Override
    public byte[] getPartitionKey() {
      return row.getPartitionKey();
    }

    @Override
    public int getNumberOfDuplicates() {
      return row.getNumberOfDuplicates();
    }

  }
  private static class GeoWaveValueWrapper implements GeoWaveValue {
    private final GeoWaveValue value;

    public GeoWaveValueWrapper(final GeoWaveValue value) {
      this.value = value;
    }

    @Override
    public byte[] getFieldMask() {
      return new byte[0];
    }

    @Override
    public byte[] getVisibility() {
      return new byte[0];
    }

    @Override
    public byte[] getValue() {
      return value.getValue();
    }
  }
}
