package org.locationtech.geowave.core.store.query.constraints;

import java.util.Collections;
import java.util.List;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.index.CustomIndexImpl;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.util.GenericTypeResolver;

public class CustomQueryConstraints<C extends Persistable> implements QueryConstraints {
  private C customConstraints;

  public CustomQueryConstraints() {
    super();
  }

  public CustomQueryConstraints(C customConstraints) {
    this.customConstraints = customConstraints;
  }

  public C getCustomConstraints() {
    return customConstraints;
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(customConstraints);
  }

  @Override
  public void fromBinary(byte[] bytes) {
    customConstraints = (C) PersistenceUtils.fromBinary(bytes);
  }

  @Override
  public List<QueryFilter> createFilters(Index index) {
    return Collections.emptyList();
  }

  @Override
  public List<MultiDimensionalNumericData> getIndexConstraints(Index index) {
    if (index instanceof CustomIndex) {
      final Class<?>[] genericClasses =
          GenericTypeResolver.resolveTypeArguments(index.getClass(), CustomIndex.class);
      if (genericClasses != null
          && genericClasses.length == 2
          && genericClasses[1].isInstance(customConstraints)) {
        return Collections.singletonList(new InternalCustomConstraints(customConstraints));
      }
    }
    return Collections.emptyList();
  }

  public static class InternalCustomConstraints<C extends Persistable> extends BasicNumericDataset {
    private C customConstraints;

    public InternalCustomConstraints() {}

    public InternalCustomConstraints(C customConstraints) {
      super();
      this.customConstraints = customConstraints;
    }

    public C getCustomConstraints() {
      return customConstraints;
    }

    @Override
    public byte[] toBinary() {
      return PersistenceUtils.toBinary(customConstraints);
    }

    @Override
    public void fromBinary(byte[] bytes) {
      customConstraints = (C) PersistenceUtils.fromBinary(bytes);
    }
  }
}
