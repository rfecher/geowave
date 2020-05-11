/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractFileSystemTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFileSystemTable.class);

  protected final Path subDirectory;
  protected final short adapterId;
  protected boolean visibilityEnabled;

  public AbstractFileSystemTable(
      final String subDirectory,
      final short adapterId,
      final boolean visibilityEnabled) throws IOException {
    super();
    this.adapterId = adapterId;
    this.subDirectory = Files.createDirectories(Paths.get(subDirectory));
    this.visibilityEnabled = visibilityEnabled;
  }

  public void delete(final byte[] key) {
    try {
      Files.delete(subDirectory.resolve(FileSystemUtils.keyToFileName(key)));
    } catch (final IOException e) {
      LOGGER.warn("Unable to delete file", e);
    }
  }

  protected void put(final byte[] key, final byte[] value) {
    try {
      Files.write(
          subDirectory.resolve(FileSystemUtils.keyToFileName(key)),
          value,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.SYNC);
    } catch (final IOException e) {
      LOGGER.warn("Unable to write file", e);
    }
  }
}
