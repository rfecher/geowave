import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class ReadADSB {
  public static void main(final String[] args) throws IOException, ParseException {
    final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    final File dir = new File("F:\\adsb\\adsb");;
    GenericRecord record = null;
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    final File f = new File("F:\\adsb\\csv", "Oct-13.csv");
    final FileWriter out = new FileWriter(f);
    try (final CSVPrinter printer =
        CSVFormat.DEFAULT.withHeader(
            "entity_id",
            "lat",
            "lon",
            "alt",
            "time",
            "velocity",
            "heading",
            "climb").print(out)) {
      int i = 0;
      final WKTReader r = new WKTReader();
      for (final File in : dir.listFiles(new FilenameFilter() {

        @Override
        public boolean accept(File dir, String name) {
          return name.contains("Oct 13");
        }
      })) {
        System.err.println(in);
        // final Schema schema = dataFileReader.getSchema();
        // System.out.println(schema)
        // entity_id, lat, lon, alt, time, velocity, heading, climb
        // final File f = new File("F:\\adsb\\csv", in.getName() + ".csv");
        // if (!f.exists()) {
        // final FileWriter out = new FileWriter(f);
        try (final DataFileReader<GenericRecord> dataFileReader =
            new DataFileReader<>(in, datumReader)) {
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
          while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);

            final Point geom = r.read(record.get("wkt_geometry").toString()).getCentroid();
            printer.printRecord(
                record.get("entity_id"),
                geom.getY(),
                geom.getX(),
                record.get("spatial_altitude"),
                sdf.format(new Date(Long.parseLong(record.get("temporal_start").toString()))),
                record.get("kinematic_velocity"),
                record.get("kinematic_heading"),
                record.get("kinematic_climb"));
            i++;
            if ((i % 100000) == 0) {
              System.err.println(i);
              printer.flush();
            }
          }
        }
      }
      // }
    }
  }
}
