package org.locationtech.geowave.datastore.filesystem;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.locationtech.geowave.core.store.config.ConfigUtils;

public class FileSystemDataFormatterRegistry {

  private static Map<String, FileSystemDataFormatterSpi> dataFormatterRegistry = null;

  public FileSystemDataFormatterRegistry() {}

  @SuppressWarnings("rawtypes")
  private static void initDataFormatterRegistry() {
    dataFormatterRegistry = new HashMap<>();
    final Iterator<FileSystemDataFormatterSpi> pluginProviders =
        new SPIServiceRegistry(FileSystemDataFormatterRegistry.class).load(
            FileSystemDataFormatterSpi.class);
    while (pluginProviders.hasNext()) {
      final FileSystemDataFormatterSpi pluginProvider = pluginProviders.next();
      dataFormatterRegistry.put(
          ConfigUtils.cleanOptionName(pluginProvider.getFormatName()),
          pluginProvider);
    }
  }

  public static Map<String, FileSystemDataFormatterSpi> getDataFormatterRegistry() {
    if (dataFormatterRegistry == null) {
      initDataFormatterRegistry();
    }
    return dataFormatterRegistry;
  }
}
