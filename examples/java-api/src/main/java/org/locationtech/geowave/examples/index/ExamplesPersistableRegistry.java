package org.locationtech.geowave.examples.index;

import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.examples.index.CustomIndexExample.UUIDConstraints;
import org.locationtech.geowave.examples.index.CustomIndexExample.UUIDIndexStrategy;

public class ExamplesPersistableRegistry implements PersistableRegistrySpi {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        new PersistableIdAndConstructor((short) 11000, UUIDConstraints::new),
        new PersistableIdAndConstructor((short) 11001, UUIDIndexStrategy::new),};
  }
}
