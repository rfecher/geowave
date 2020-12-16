package org.locationtech.geowave.core.index.text;

import java.util.function.Function;
import org.locationtech.geowave.core.index.persist.Persistable;

public interface TextIndexEntryConverter<E> extends Function<E, String>, Persistable {
}
