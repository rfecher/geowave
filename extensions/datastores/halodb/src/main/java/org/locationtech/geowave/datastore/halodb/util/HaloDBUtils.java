package org.locationtech.geowave.datastore.halodb.util;

import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;

public class HaloDBUtils {

  public static String getTableName(final String typeName) {
    return typeName + "_" + DataIndexUtils.DATA_ID_INDEX.getName();
  }
}
