package org.locationtech.geowave.core.index.text;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import org.locationtech.geowave.core.index.CustomIndexStrategy;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;

abstract public class TextIndexStrategy<E, C extends Persistable> implements
    CustomIndexStrategy<E, C> {
  private EnumSet<TextSearchType> supportedSearchTypes;

  @Override
  public byte[] toBinary() {
    return VarintUtils.writeUnsignedInt(encode(supportedSearchTypes));
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    supportedSearchTypes = decode(VarintUtils.readUnsignedInt(ByteBuffer.wrap(bytes)));
  }

  @Override
  public InsertionIds getInsertionIds(final E entry) {
    return null;
  }

  @Override
  public QueryRanges getQueryRanges(final C constraints) {
    // TODO Auto-generated method stub
    return null;
  }

  abstract protected String entryToString(E entry);

  // From Adamski's answer
  public static int encode(final EnumSet<TextSearchType> set) {
    int ret = 0;

    for (final TextSearchType val : set) {
      ret |= 1 << val.ordinal();
    }

    return ret;
  }

  private static EnumSet<TextSearchType> decode(int code) {
    final TextSearchType[] values = TextSearchType.values();
    final EnumSet<TextSearchType> result = EnumSet.noneOf(TextSearchType.class);
    while (code != 0) {
      final int ordinal = Integer.numberOfTrailingZeros(code);
      code ^= Integer.lowestOneBit(code);
      result.add(values[ordinal]);
    }
    return result;
  }
}
