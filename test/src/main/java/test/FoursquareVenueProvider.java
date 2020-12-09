package test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import com.google.common.collect.Streams;
import test.StringProviderBenchmark.StringProvider;

public class FoursquareVenueProvider implements StringProvider {
  Iterable<CSVRecord> records;
  Iterable<CSVRecord> queryRecords;

  public FoursquareVenueProvider() {
    super();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

    try {
      queryRecords =
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
              "zip").withSkipHeaderRecord(true).parse(
                  new FileReader(
                      new File(
                          "C:\\development\\data\\foursquare\\venues\\part-00001-tid-6494183497854830470-f1dac821-b8de-4b24-8fd8-b084b5e38fb4-1855043-1-c000.csv")));
      records =
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
              "zip").withSkipHeaderRecord(true).parse(
                  new FileReader(
                      new File(
                          "C:\\development\\data\\foursquare\\venues\\part-00000-tid-6494183497854830470-f1dac821-b8de-4b24-8fd8-b084b5e38fb4-1855042-1-c000.csv")));
    } catch (final IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public Iterator<String> getIndexedStrings() {
    return Streams.stream(records).map(r -> r.get("venuename")).iterator();
  }

  @Override
  public Iterator<String> getQueryStrings(final int length, final int numSamples) {
    final Random rand = new Random();
    return Streams.stream(records).map(r -> r.get("venuename")).filter(
        s -> s.length() >= length).map(
            s -> s.substring((int) Math.round(rand.nextDouble() * (s.length() - length)))).map(
                str -> str.substring(0, length)).limit(numSamples).iterator();
  }

  @Override
  public void closeAll() {
    // TODO Auto-generated method stub

  }

}
