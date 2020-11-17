import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class ReadParquet {
  public static void main(final String[] args) throws IOException, ParseException {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    final File dir = new File("F:\\xmode\\parquet\\");
    GenericRecord record = null;
    Geometry germany = null;
    try (FileReader r =
        new FileReader(new File("D:\\development\\data\\ne_10m_admin_0_countries\\Germany.wkt"))) {
      germany = new WKTReader().read(r);
    }
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    //
    final File f = new File("F:\\xmode\\csv", "Germany.csv");
    final FileWriter out = new FileWriter(f);
    try (final CSVPrinter printer = CSVFormat.DEFAULT.withHeader("lat", "lon", "time").print(out)) {
      long i = 0, j = 0;;
      for (final File in : dir.listFiles()) {
        try (final ParquetReader<GenericRecord> reader =
            (ParquetReader) AvroParquetReader.builder(new Path(in.toURI())).build()) {

          record = reader.read();
          while (record != null) {
            try {
              final long timeMillis = Long.parseLong(record.get("location_at").toString());

              final double lat = Double.parseDouble(record.get("latitude").toString());

              final double lon = Double.parseDouble(record.get("longitude").toString());
              if (germany.intersects(
                  GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(lon, lat)))) {
                printer.printRecord(lat, lon, sdf.format(new Date(timeMillis)));
                if ((j % 100000) == 0) {
                  System.err.println("Wrote: " + j);
                  printer.flush();
                }
                j++;
              }

              record = reader.read();
              if ((i % 1000000) == 0) {
                System.err.println("Scanned: " + i);
                printer.flush();
              }
              i++;
              // }
            } catch (final Exception e) {
              e.printStackTrace();
            }
            // }
          }
        }
      }
    }
  }
}
