package org.locationtech.geowave.datastore.filesystem.cli;

import org.locationtech.geowave.core.cli.spi.CLIOperationProviderSpi;

public class FileSystemOperationProvider implements CLIOperationProviderSpi {
  private static final Class<?>[] OPERATIONS =
      new Class<?>[] {FileSystemSection.class, ListFormatsCommand.class};

  @Override
  public Class<?>[] getOperations() {
    return OPERATIONS;
  }

}
