/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.config;

import org.locationtech.geowave.core.cli.spi.CLIOperationProviderSpi;

public class ConfigOperationProvider implements CLIOperationProviderSpi {

  private static final Class<?>[] OPERATIONS =
      new Class<?>[] {AddIndexCommand.class, AddIndexGroupCommand.class, AddStoreCommand.class,
          CopyIndexCommand.class, CopyStoreCommand.class, RemoveIndexCommand.class,
          RemoveIndexGroupCommand.class, RemoveStoreCommand.class};

  @Override
  public Class<?>[] getOperations() {
    return OPERATIONS;
  }
}
