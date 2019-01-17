package org.locationtech.geowave.datastore.hbase.operations;

import java.io.IOException;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.log4j.Logger;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;

public class HBaseDataIndexWriter implements RowWriter {
  private static final Logger LOGGER = Logger.getLogger(HBaseWriter.class);

  private final BufferedMutator mutator;

  public HBaseDataIndexWriter(final BufferedMutator mutator) {
    this.mutator = mutator;
  }

  @Override
  public void close() {
    try {
      mutator.close();
    } catch (final IOException e) {
      LOGGER.warn("Unable to close BufferedMutator", e);
    }
  }

  @Override
  public void flush() {
    try {
      mutator.flush();
    } catch (final IOException e) {
      LOGGER.warn("Unable to flush BufferedMutator", e);
    }
  }

  @Override
  public void write(final GeoWaveRow[] rows) {
    for (final GeoWaveRow row : rows) {
      write(row);
    }
  }

  @Override
  public void write(final GeoWaveRow row) {
    writeMutations(rowToMutation(row));
  }

  private void writeMutations(final RowMutations rowMutation) {
    try {
      mutator.mutate(rowMutation.getMutations());
    } catch (final IOException e) {
      LOGGER.error("Unable to write mutation.", e);
    }
  }

  private RowMutations rowToMutation(final GeoWaveRow row) {
    final RowMutations mutation = new RowMutations(row.getDataId());
    for (final GeoWaveValue value : row.getFieldValues()) {
      final Put put = new Put(row.getDataId());

      put.addColumn(
          StringUtils.stringToBinary(ByteArrayUtils.shortToString(row.getAdapterId())),
          new byte[0],
          value.getValue());
      try {
        mutation.add(put);
      } catch (final IOException e) {
        LOGGER.error("Error creating HBase row mutation: " + e.getMessage());
      }
    }

    return mutation;
  }
}
