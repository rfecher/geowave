/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.text;

import org.locationtech.geowave.core.index.CustomIndexStrategy.PersistableBiPredicate;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;

public class TextSearchPredicate<E> implements PersistableBiPredicate<E, TextSearch> {
  private TextIndexEntryConverter<E> converter;

  public TextSearchPredicate() {}

  public TextSearchPredicate(final TextIndexEntryConverter<E> converter) {
    this.converter = converter;
  }

  @Override
  public boolean test(final E t, final TextSearch u) {
    final String value = converter.apply(t);
    final boolean caseSensitive = CaseSensitivity.CASE_SENSITIVE.equals(u.getCaseSensitivity());
    return u.getType().evaluate(
        ((value != null) && !caseSensitive) ? value.toLowerCase() : value,
        caseSensitive ? u.getSearchTerm() : u.getSearchTerm().toLowerCase());
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(converter);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    converter = (TextIndexEntryConverter<E>) PersistenceUtils.fromBinary(bytes);
  }

}
