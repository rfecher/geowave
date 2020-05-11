package org.locationtech.geowave.datastore.filesystem;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public interface FileSystemDataFormatterSpi {

  public static class FormattedRow {
    private final String fileName;
    private final byte[] formattedValue;

    public FormattedRow(final String fileName, final byte[] formattedValue) {
      super();
      this.fileName = fileName;
      this.formattedValue = formattedValue;
    }

    public String getFileName() {
      return fileName;
    }

    public byte[] getFormattedValue() {
      return formattedValue;
    }
  }

  String getFormatName();

  FormattedRow writeFormat(GeoWaveRow row);

  GeoWaveRow readFormat(FormattedRow row);
}
