package org.locationtech.geowave.core.index.text;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.index.CustomIndexStrategy;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;

class ForwardTextIndexStrategy<C extends Persistable> implements CustomIndexStrategy<String, C> {

  @Override
  public byte[] toBinary() {
    return new byte[0];
  }

  @Override
  public void fromBinary(final byte[] bytes) {}

  @Override
  public InsertionIds getInsertionIds(final String entry) {
    final List<byte[]> list = new ArrayList<>(1);
    list.add(StringUtils.stringToBinary(entry));
    return new InsertionIds(list);
  }

  @Override
  public QueryRanges getQueryRanges(final C constraints) {
    return null;
  }

}
