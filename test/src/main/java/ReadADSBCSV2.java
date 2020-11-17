import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class ReadADSBCSV2 {
  public static void main(final String[] args) throws IOException, ParseException {
    final File in = new File("F:\\adsb\\round2\\Oct-2016-ADS-B.txt");
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    Geometry tempGermany = null;
    try (FileReader r =
        new FileReader(new File("D:\\development\\data\\ne_10m_admin_0_countries\\Germany.wkt"))) {
      tempGermany = new WKTReader().read(r);
    }
    final Geometry germany = tempGermany;
    final File f = new File("F:\\adsb\\csv", "Oct-2016-ADS-B-Germany.csv");
    final FileWriter out = new FileWriter(f);
    try (final CSVPrinter printer =
        CSVFormat.DEFAULT.withHeader(
            "trackId",
            "time",
            "lat",
            "lon",
            "alt",
            "hdg",
            "gspd",
            "modeS",
            "squawk",
            "fltNum",
            "eqType").print(out)) {
      final AtomicInteger i = new AtomicInteger(0);
      // for (final File in : dir.listFiles()) {
      System.err.println(in);
      // final Schema schema = dataFileReader.getSchema();
      // System.out.println(schema)
      // entity_id, lat, lon, alt, time, velocity, heading, climb
      // final File f = new File("F:\\adsb\\csv", in.getName() + ".csv");
      // if (!f.exists()) {
      // final FileWriter out = new FileWriter(f);
      final Iterable<CSVRecord> records = CSVFormat.TDF.parse(new FileReader(in));
      // try (final CSVPrinter printer =
      // CSVFormat.DEFAULT.withHeader(
      // "entity_id",
      // "lat",
      // "lon",
      // "alt",
      // "time",
      // "velocity",
      // "heading",
      // "climb").print(out)) {
      // wkt_geometry,spatial_altitude,temporal_start
      records.forEach(record -> {
        final double lat = Double.parseDouble(record.get(2));
        final double lon = Double.parseDouble(record.get(3));
        if (germany.intersects(
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(lon, lat)))) {
          try {
            printer.printRecord(
                record.get(0),
                sdf.format(new Date(Long.parseLong(record.get(1).toString()))),
                lat,
                lon,
                record.get(4),
                record.get(5),
                record.get(6),
                record.get(7),
                record.get(8),
                record.get(9),
                record.get(10));

            if ((i.getAndIncrement() % 100000) == 0) {
              System.err.println(i);
              printer.flush();
            }
          } catch (NumberFormatException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }

        }
      });
    }
  }
}
// }
// }
