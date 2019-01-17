package org.locationtech.geowave.datastore.accumulo.operations;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public class AccumuloDataIndexWriter extends AbstractAccumuloWriter {
  public AccumuloDataIndexWriter(
      final BatchWriter batchWriter,
      final AccumuloOperations operations,
      final String tableName) {
    super(batchWriter, operations, tableName);
  }

  public static Mutation rowToMutation(final GeoWaveRow row) {
    final Mutation mutation = new Mutation(row.getDataId());
    for (final GeoWaveValue value : row.getFieldValues()) {
      mutation.put(
          new Text(ByteArrayUtils.shortToString(row.getAdapterId())),
          new Text(),
          new Value(value.getValue()));
    }
    return mutation;
  }

  @Override
  protected Mutation internalRowToMutation(final GeoWaveRow row) {
    return rowToMutation(row);
  }

}
