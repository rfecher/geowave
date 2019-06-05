package org.locationtech.geowave.datastore.accumulo.iterators;

import java.io.IOException;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExceptionHandlingSkippingIterator extends SkippingIterator {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ExceptionHandlingSkippingIterator.class);

  @Override
  protected final void consume() throws IOException {
    try {
      consumeInternal();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("Uncaught exception while initializing transforming iterator", e);
      throw new IOException(e);
    }
  }

  protected abstract void consumeInternal() throws IOException;

}
