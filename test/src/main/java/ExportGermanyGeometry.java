import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;
import org.opengis.feature.simple.SimpleFeature;

public class ExportGermanyGeometry {
  public static void main(final String[] args) throws MalformedURLException, IOException {
    final SimpleFeature f =
        resourceToFeature(
            new File(
                "D:\\development\\data\\ne_10m_admin_0_countries\\ne_10m_admin_0_countries.shp").toURL());
    System.err.println(f);
    try (FileWriter w =
        new FileWriter(new File("D:\\development\\data\\ne_10m_admin_0_countries\\Germany.wkt"))) {
      new WKTWriter().write((Geometry) f.getDefaultGeometry(), w);
    }

  }

  public static SimpleFeature resourceToFeature(final URL filterResource) throws IOException {
    final Map<String, Object> map = new HashMap<>();
    DataStore dataStore = null;
    map.put("url", filterResource);
    SimpleFeature savedFilter = null;
    SimpleFeatureIterator sfi = null;
    try {
      dataStore = DataStoreFinder.getDataStore(map);
      if (dataStore == null) {
        throw new IOException("Could not get dataStore instance, getDataStore returned null");
      }
      // just grab the first feature and use it as a filter
      sfi = dataStore.getFeatureSource(dataStore.getNames().get(0)).getFeatures().features();
      while (sfi.hasNext()) {
        savedFilter = sfi.next();
        if (savedFilter.getAttribute("NAME_EN").equals("Germany")) {
          return savedFilter;
        }
      }

    } finally {
      if (sfi != null) {
        sfi.close();
      }
      if (dataStore != null) {
        dataStore.dispose();
      }
    }
    return savedFilter;
  }
}
