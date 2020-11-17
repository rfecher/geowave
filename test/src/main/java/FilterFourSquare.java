import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.locationtech.jts.io.ParseException;

public class FilterFourSquare {
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
        // double lat = Double.parseDouble(record.get("geolat"));
        // if (lat > 40.813 || lat < 40.673) {
        // continue;
        // }
        // double lon = Double.parseDouble(record.get("geolong"));
        // if (lon < -75.058 || lon > -73.9) {
        // continue;
        // }
        places.put(record.get("venueid"), record);
      }
    }

    System.err.println("places completed in " + (System.currentTimeMillis() - time));
    final File f = new File("F:\\foursquare\\visits\\transformed2\\", "March-2020-65.csv");
    final File visitsDir = new File("F:\\foursquare\\visits\\original\\csv");
    final SimpleDateFormat sdfIn = new SimpleDateFormat("yyyy-MM-dd");
    final SimpleDateFormat sdfOut = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    final AtomicInteger missingVisits = new AtomicInteger(0);
    final FileWriter out = new FileWriter(f);
    List<String> age = new ArrayList<>();
    List<String> gender = new ArrayList<>();
    age.add("Age_20_to_24");
    age.add("Age_25_to_29");
    age.add("Age_30_to_34");
    age.add("Age_35_to_39");
    age.add("Age_40_to_44");
    age.add("Age_45_to_49");
    age.add("Age_50_to_54");
    age.add("Age_55_to_59");
    age.add("Age_60_to_64");
    age.add("Age_65_to_69");
    age.add("Age_70_to_74");
    age.add("Age_75_to_79");
    age.add("Age_80_to_84");
    age.add("Age_85_to_89");

//    List<CSVPrinter> ageFiles = new ArrayList<>();
//    age.forEach(a -> {
//      try {
//        ageFiles.add(
//            CSVFormat.DEFAULT.withHeader(
//                "lat",
//                "lon",
//                "utc_time",
//                "local_hour",
//                "venue",
//                "category",
//                "gender",
//                "age").print(
//                    new FileWriter(
//                        new File(
//                            "F:\\foursquare\\visits\\transformed2\\",
//                            "March-2020-" + a + ".csv"))));
//      } catch (IOException e2) {
//        // TODO Auto-generated catch block
//        e2.printStackTrace();
//      }
//    });
//    List<CSVPrinter> genderFiles = new ArrayList<>();
//    gender.add("Female");
//    gender.add("Male");
//    gender.forEach(g -> {
//      try {
//        genderFiles.add(
//            CSVFormat.DEFAULT.withHeader(
//                "lat",
//                "lon",
//                "utc_time",
//                "local_hour",
//                "venue",
//                "category",
//                "gender",
//                "age").print(
//                    new FileWriter(
//                        new File(
//                            "F:\\foursquare\\visits\\transformed2\\",
//                            "March-2020-" + g + ".csv"))));
//      } catch (IOException e1) {
//        // TODO Auto-generated catch block
//        e1.printStackTrace();
//      }
//    });
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
              String ageStr = r.get("age");
              int ageNum = -1;
              int ageIndex = -1;
              if (!ageStr.isEmpty()) {
                ageIndex = age.indexOf(ageStr.trim());
                ageNum = ageIndex * 5 + 20;
                if (ageNum < 65) {
                  return;
                }
              }
              else {
                return;
              }
              final CSVRecord place = places.get(r.get("venueid"));
              
              if (place == null) {
                System.err.println("no place found for " + r);
                missingVisits.incrementAndGet();
                return;
              }
              String genderStr = r.get("gender");
              int genderNum = -1;
              if (!genderStr.isEmpty()) {
                genderNum = gender.indexOf(genderStr.trim());
              }

              final Date dateIn = sdfIn.parse(r.get("utc_date"));
              final Calendar cal = Calendar.getInstance();
              cal.setTime(dateIn);
              cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(r.get("utc_hour")));
              cal.set(Calendar.MINUTE, 30);
//              if (genderNum >= 0) {
//                genderFiles.get(genderNum).printRecord(
//                    place.get("geolat"),
//                    place.get("geolong"),
//                    sdfOut.format(cal.getTime()),
//                    r.get("local_hour"),
//                    place.get("venuename"),
//                    place.get("level1cat"),
//                    genderNum,
//                    ageNum);
//              }
//              if (ageIndex >= 0) {
//                ageFiles.get(ageIndex).printRecord(
//                    place.get("geolat"),
//                    place.get("geolong"),
//                    sdfOut.format(cal.getTime()),
//                    r.get("local_hour"),
//                    place.get("venuename"),
//                    place.get("level1cat"),
//                    genderNum,
//                    ageNum);
//              }
              printer.printRecord(
                  place.get("geolat"),
                  place.get("geolong"),
                  sdfOut.format(cal.getTime()),
                  r.get("local_hour"),
                  place.get("venuename"),
                  place.get("level1cat"),
                  genderNum,
                  ageNum);
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
    } finally {
//      ageFiles.forEach(k -> {
//        try {
//          k.close();
//        } catch (IOException e) {
//          // TODO Auto-generated catch block
//          e.printStackTrace();
//        }
//      });
//      genderFiles.forEach(k -> {
//        try {
//          k.close();
//        } catch (IOException e) {
//          // TODO Auto-generated catch block
//          e.printStackTrace();
//        }
//      });
    }
    System.err.println("missing visits: " + missingVisits.get());
    System.err.println("visits completed in " + (System.currentTimeMillis() - time));
  }
}
