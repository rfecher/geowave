/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFileSystemIterator<T> implements CloseableIterator<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFileSystemIterator.class);
  final Iterator<Pair<byte[], Path>> iterator;
  boolean closed = false;

  public AbstractFileSystemIterator(
      final Path subDirectory,
      final byte[] startKey,
      final byte[] endKey) {
    super();
    iterator = FileSystemUtils.getSortedSet(subDirectory, startKey, endKey).iterator();
  }

  @Override
  public boolean hasNext() {
    return !closed && iterator.hasNext();
  }

  @Override
  public T next() {
    if (closed) {
      throw new NoSuchElementException();
    }
    Pair<byte[], Path> next = iterator.next();
    while (!Files.exists(next.getRight())) {
      if (!iterator.hasNext()) {
        LOGGER.warn("No more files exist in the directory");
        return null;
      }
      next = iterator.next();
    }
    try {
      return readRow(next.getLeft(), Files.readAllBytes(next.getRight()));
    } catch (final IOException e) {
      LOGGER.warn("Unable to read file " + next, e);
    }

    return null;
  }

  protected abstract T readRow(byte[] key, byte[] value);

  @Override
  public void close() {
    closed = true;
  }
}
