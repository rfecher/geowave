/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.input;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.exceptions.AdapterException;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.mapreduce.HadoopWritableSerializationTool;

/**
 * This is used internally to translate Accumulo rows into native objects (using the appropriate
 * data adapter). It also performs any client-side filtering. It will peek at the next entry in the
 * accumulo iterator to always maintain a reference to the next value. It maintains the adapter ID,
 * data ID, and original accumulo key in the GeoWaveInputKey for use by the GeoWaveInputFormat.
 *
 * @param <T> The type for the entry
 */
public class InputFormatIteratorWrapper<T> implements Iterator<Entry<GeoWaveInputKey, T>> {
  private final RowReader<GeoWaveRow> reader;
  private final QueryFilter queryFilter;
  private final HadoopWritableSerializationTool serializationTool;
  private final boolean isOutputWritable;
  private Entry<GeoWaveInputKey, T> nextEntry;
  private final Index index;

  public InputFormatIteratorWrapper(
      final RowReader<GeoWaveRow> reader,
      final QueryFilter queryFilter,
      final TransientAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final Index index,
      final boolean isOutputWritable) {
    this.reader = reader;
    this.queryFilter = queryFilter;
    this.index = index;
    this.serializationTool =
        new HadoopWritableSerializationTool(adapterStore, internalAdapterStore);
    this.isOutputWritable = isOutputWritable;
  }

  private void findNext() {
    while ((this.nextEntry == null) && reader.hasNext()) {
      final GeoWaveRow nextRow = reader.next();
      if (nextRow != null) {
        final Entry<GeoWaveInputKey, T> decodedValue =
            decodeRow(
                nextRow, queryFilter, (InternalDataAdapter<T>) serializationTool
                    .getInternalAdapter(nextRow.getAdapterId()),
                index);
        if (decodedValue != null) {
          nextEntry = decodedValue;
          return;
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Entry<GeoWaveInputKey, T> decodeRow(
      final GeoWaveRow row,
      final QueryFilter clientFilter,
      final InternalDataAdapter<T> adapter,
      final Index index) {
    Object value = null;
    try {
      value =
          BaseDataStoreUtils.decodeRow(row, clientFilter, adapter, null, index, null, null, true);
    } catch (AdapterException e) {
      return null;
    }
    if (value == null) {
      return null;
    }
    final short adapterId = row.getAdapterId();
    final T result =
        (T) (isOutputWritable
            ? serializationTool.getHadoopWritableSerializerForAdapter(adapterId).toWritable(value)
            : value);
    final GeoWaveInputKey key = new GeoWaveInputKey(row, index.getName());
    return new GeoWaveInputFormatEntry(key, result);
  }

  @Override
  public boolean hasNext() {
    findNext();
    return nextEntry != null;
  }

  @Override
  public Entry<GeoWaveInputKey, T> next() throws NoSuchElementException {
    final Entry<GeoWaveInputKey, T> previousNext = nextEntry;
    if (nextEntry == null) {
      throw new NoSuchElementException();
    }
    nextEntry = null;
    return previousNext;
  }

  @Override
  public void remove() {
    reader.remove();
  }

  private final class GeoWaveInputFormatEntry implements Map.Entry<GeoWaveInputKey, T> {
    private final GeoWaveInputKey key;
    private T value;

    public GeoWaveInputFormatEntry(final GeoWaveInputKey key, final T value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public GeoWaveInputKey getKey() {
      return key;
    }

    @Override
    public T getValue() {
      return value;
    }

    @Override
    public T setValue(final T value) {
      final T old = this.value;
      this.value = value;
      return old;
    }
  }
}
