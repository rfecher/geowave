package org.locationtech.geowave.datastore.accumulo.iterators;

import java.io.IOException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExceptionHandlingTransformingIterator extends TransformingIterator {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ExceptionHandlingTransformingIterator.class);

  @Override
  protected final void transformRange(SortedKeyValueIterator<Key, Value> input, KVBuffer output)
      throws IOException {
    try {
      transformRangeInternal(input, output);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("Uncaught exception while transforming range", e);
      throw new IOException(e);
    }
  }

  protected abstract void transformRangeInternal(
      SortedKeyValueIterator<Key, Value> input,
      KVBuffer output) throws IOException;

}
