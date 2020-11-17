import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

public class ReadGPSCSV {
  public static void main(final String[] args) throws IOException, ParseException {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    final File in = new File("F:\\gps\\csv\\Germany2.csv");
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    final File f = new File("F:\\gps\\csv\\Germany5.csv");
    final FileWriter out = new FileWriter(f);
    Date endDate = sdf.parse("2013-01-01T00:00:00.000Z");
    Date startDate = sdf.parse("2010-01-01T00:00:00.000Z");
    try (final CSVPrinter printer = CSVFormat.DEFAULT.withHeader("lat", "lon", "time").print(out)) {
      final AtomicInteger i = new AtomicInteger(0);
      final AtomicInteger j = new AtomicInteger(0);
      // for (final File in : dir.listFiles()) {
      System.err.println(in);
      // final Schema schema = dataFileReader.getSchema();
      // System.out.println(schema)
      // entity_id, lat, lon, alt, time, velocity, heading, climb
      // final File f = new File("F:\\adsb\\csv", in.getName() + ".csv");
      // if (!f.exists()) {
      // final FileWriter out = new FileWriter(f);
      final Iterable<CSVRecord> records =
          CSVFormat.DEFAULT.withHeader("lat", "lon", "time").withSkipHeaderRecord().parse(
              new FileReader(in));
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
        try {
          final Date date = sdf.parse(record.get(2));
          if (date.after(startDate) && date.before(endDate)) {
            printer.printRecord(record.get(0), record.get(1), record.get(2));

            if ((i.getAndIncrement() % 100000) == 0) {
              System.err.println("Output: " + i);
              printer.flush();
            }
          }
          if ((j.getAndIncrement() % 1000000) == 0) {
            System.err.println("Scanned: " + j);
            printer.flush();
          }
        } catch (NumberFormatException | IOException | ParseException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }


      });
    }
  }
}
