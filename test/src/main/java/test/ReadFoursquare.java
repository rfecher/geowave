package test;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

public class ReadFoursquare {
  public static void main(final String[] args) throws IOException, ParseException {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    final File placesDir = new File("F:\\foursquare\\places\\csv");
    final long time = System.currentTimeMillis();
    final Map<String, CSVRecord> places = new HashMap<>();
    for (final File in : placesDir.listFiles()) {
      final Iterable<CSVRecord> records =
          CSVFormat.TDF.withHeader(
              "venueid",
              "venuename",
              "address",
              "city",
              "dma",
              "state",
              "country",
              "geolat",
              "geolong",
              "catid",
              "catname",
              "level1catId",
              "level1cat",
              "level2catId",
              "level2cat",
              "level3catId",
              "level3cat",
              "chainid",
              "chainname",
              "parentvenueid",
              "zip").withSkipHeaderRecord(true).parse(new FileReader(in));
      for (final CSVRecord record : records) {
        if (record.get("geolat").isEmpty()
            || record.get("geolong").isEmpty()
            || record.get("venueid").isEmpty()) {
          System.err.println("error on empty string: " + record);
          continue;
        }
        if (Math.abs(Double.parseDouble(record.get("geolat"))) >= 90) {
          System.err.println("error on lat " + Double.parseDouble(record.get("geolat")));
          System.err.println(record);
          continue;
        }
        if (Math.abs(Double.parseDouble(record.get("geolong"))) > 180) {
          System.err.println("error on long " + Double.parseDouble(record.get("geolong")));
          System.err.println(record);
          continue;
        }
        places.put(record.get("venueid"), record);
      }
    }

    System.err.println("places completed in " + (System.currentTimeMillis() - time));
    final File f = new File("F:\\foursquare\\visits\\transformed", "March-2020-AllVisits.csv");
    final File visitsDir = new File("F:\\foursquare\\visits\\original\\csv");
    final SimpleDateFormat sdfIn = new SimpleDateFormat("yyyy-MM-dd");
    final SimpleDateFormat sdfOut = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    final AtomicInteger missingVisits = new AtomicInteger(0);
    final FileWriter out = new FileWriter(f);
    try (final CSVPrinter printer =
        CSVFormat.DEFAULT.withHeader(
            "lat",
            "lon",
            "utc_time",
            "local_hour",
            "venue",
            "category",
            "gender",
            "age").print(out)) {
      for (final File innerDir : visitsDir.listFiles()) {
        for (final File in : innerDir.listFiles()) {
          final Iterable<CSVRecord> records =
              CSVFormat.TDF.withHeader(
                  "venueid",
                  "utc_date",
                  "utc_hour",
                  "local_date",
                  "local_hour",
                  "gender",
                  "age",
                  "full_panel_reweighted_sag_score",
                  "dwell").withSkipHeaderRecord(true).parse(new FileReader(in));
          final long fileTime = System.currentTimeMillis();
          records.forEach(r -> {
            try {
              final Date dateIn = sdfIn.parse(r.get("utc_date"));
              final Calendar cal = Calendar.getInstance();
              cal.setTime(dateIn);
              cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(r.get("utc_hour")));
              cal.set(Calendar.MINUTE, 30);
              final CSVRecord place = places.get(r.get("venueid"));
              if (place == null) {
                System.err.println("no place found for " + r);
                missingVisits.incrementAndGet();
                return;
              }
              printer.printRecord(
                  place.get("geolat"),
                  place.get("geolong"),
                  sdfOut.format(cal.getTime()),
                  r.get("local_hour"),
                  place.get("venuename"),
                  place.get("level1cat"),
                  r.get("gender"),
                  r.get("age"));
            } catch (final java.text.ParseException e) {
              System.err.println("cannot parse date: " + r);
              return;
            } catch (final IOException e) {
              e.printStackTrace();
              return;
            }
          });
          System.err.println("finished " + in + " in: " + (fileTime - System.currentTimeMillis()));
        }
      }
    }
    System.err.println("missing visits: " + missingVisits.get());
    System.err.println("visits completed in " + (System.currentTimeMillis() - time));
  }
}
