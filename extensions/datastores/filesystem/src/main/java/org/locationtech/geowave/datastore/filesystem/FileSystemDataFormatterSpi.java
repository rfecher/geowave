package org.locationtech.geowave.datastore.filesystem;

public interface FileSystemDataFormatterSpi {

  FileSystemDataFormatter createFormatter(boolean visibilityEnabled);

  String getFormatName();

  String getFormatDescription();

}
