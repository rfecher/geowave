package test;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.RangeDecomposition;
import org.locationtech.geowave.core.index.sfc.SFCDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericValue;
import org.locationtech.geowave.core.index.sfc.xz.XZOrderSFC;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BinaryDataAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.memory.MemoryDataStoreOperations;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.locationtech.geowave.datastore.rocksdb.RocksDBStoreFactoryFamily;
import org.locationtech.geowave.datastore.rocksdb.config.RocksDBOptions;
import com.clearspring.analytics.hash.Lookup3Hash;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.Streams;
import com.google.common.collect.TreeMultiset;
import com.google.common.primitives.Bytes;
import test.StringProviderBenchmark.StringProvider;

public class SFCStringProviderBenchmark {
  private static enum Type {
    RANDOM(new RandomStringProvider()), FOURSQUARE(new FoursquareVenueProvider());

    StringProvider p;

    Type(final StringProvider p) {
      this.p = p;
    }

    public Iterator<String> getIndexedStrings() {
      return p.getIndexedStrings();
    }

    public Iterator<String> getQueryStrings(final int length, final int numSamples) {
      return p.getQueryStrings(length, numSamples);
    }

    public void closeAll() {
      p.closeAll();
    }
  }

  public static void main(final String[] args) throws IOException {
    final Type type = Type.valueOf(args[0]);
    final int numDimensions = Integer.parseInt(args[1]);
    final int numIndexed = Integer.parseInt(args[2]);
    final int numQueried = Integer.parseInt(args[3]);
    final int maxSearchString = Integer.parseInt(args[4]);
    final RocksDBOptions opts = new RocksDBOptions();
    opts.getStoreOptions().setSecondaryIndexing(true);
    final DataStore ds = new RocksDBStoreFactoryFamily().getDataStoreFactory().createStore(opts);
    ds.deleteAll();
    ds.addType(new BinaryDataAdapter("test"));
    final SFCDimensionDefinition[] defs = new SFCDimensionDefinition[numDimensions];
    for (int i = 0; i < numDimensions; i++) {
      defs[i] =
          new SFCDimensionDefinition(
              new BasicDimensionDefinition(Integer.MIN_VALUE, Integer.MAX_VALUE),
              64);
    }
    final XZOrderSFC sfc = new XZOrderSFC(defs);
    final int ngram = 2;
    int id = 0;
    try (final NGramTokenizer tokenizer = new NGramTokenizer(ngram, ngram)) {
      final CharTermAttribute charTermAttribute = tokenizer.addAttribute(CharTermAttribute.class);
      final List<String> vals = new ArrayList<>();
      try (final Writer<Pair<byte[], byte[]>> writer = ds.createWriter("test")) {
        final Iterator<String> iterator = type.getIndexedStrings();
        while (iterator.hasNext() && (vals.size() < numIndexed)) {
          final String str = iterator.next();
          vals.add(str);
          final double[] newRange = new double[numDimensions * 2];
          for (int d = 0; d < (numDimensions * 2); d += 2) {
            tokenizer.setReader(new StringReader(str));
            tokenizer.reset();
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            while (tokenizer.incrementToken()) {
              final String term = charTermAttribute.toString();
              final int val = hashTerm(term, d);
              min = Math.min(val, min);
              max = Math.max(val, max);
            }
            tokenizer.close();
            newRange[d] = min;
            newRange[d + 1] = max;
          }
          writer.write(
              Pair.of(
                  Bytes.concat(sfc.getId(newRange), ByteBuffer.allocate(4).putInt(id++).array()),
                  StringUtils.stringToBinary(str)));
        }
      }

      final SortedMultiset<Integer>[] hits = new SortedMultiset[maxSearchString - 2];
      final SortedMultiset<Integer>[] actualHits = new SortedMultiset[maxSearchString - 2];

      // final SortedMultiset<Integer>[] falseNegatives = new SortedMultiset[maxSearchString - 2];
      for (int i = 3; i <= maxSearchString; i++) {
        hits[i - 3] = TreeMultiset.create();
        actualHits[i - 3] = TreeMultiset.create();
        final Iterator<String> qIterator = type.getQueryStrings(i, numQueried);
        while (qIterator.hasNext()) {
          int hit = 0;
          // final int falseNegative = 0;
          String qStr = qIterator.next();
          if (qStr.length() != i) {
            System.err.println(
                "ERROR: query string is " + qStr.length() + " when it should be " + i);
          }
          final long actualHit = vals.stream().filter(str -> str.contains(qStr)).count();
          actualHits[i - 3].add((int) actualHit);
          // for (final int[][] r : ranges) {
          tokenizer.setReader(new StringReader(qStr));
          tokenizer.reset();
          Set<Integer> andedTokens = null;
          while (tokenizer.incrementToken()) {
            final String term = charTermAttribute.toString();
            final NumericData[] dataPerDimension = new NumericData[numDimensions];
            for (int d = 0; d < numDimensions; d++) {
              final int hashCode = hashTerm(term, d);
              dataPerDimension[d] = new NumericValue(hashCode);
            }
            final RangeDecomposition range =
                sfc.decomposeRangeFully(new BasicNumericDataset(dataPerDimension));
            Set<Integer> oredRanges = null;
            for (final ByteArrayRange r : range.getRanges()) {
              try (CloseableIterator<Pair<byte[], byte[]>> it =
                  ds.query(
                      (Query) QueryBuilder.newBuilder().addTypeName("test").constraints(
                          QueryBuilder.newBuilder().constraintsFactory().dataIdsByRange(
                              r.getStart(),
                              r.getEndAsNextPrefix())).build())) {
                Set<Integer> thisRange = Streams.stream(it).map(p -> {
                  ByteBuffer buf = ByteBuffer.wrap(p.getKey());
                  buf.position(buf.capacity() - 4);
                  return buf.getInt();
                }).collect(Collectors.toSet());
                if (oredRanges == null) {
                  oredRanges = thisRange;
                } else {
                  oredRanges.addAll(thisRange);
                }
              }
            }
            if (andedTokens == null) {
              andedTokens = oredRanges;
            } else {
              andedTokens.retainAll(oredRanges);
            }
          }
          hits[i - 3].add(andedTokens.size());
          tokenizer.close();

        }
      }
      for (int i = 3; i <= maxSearchString; i++) {
        int j = 0;
        final Iterator<Integer> actualHitsIt = actualHits[i - 3].iterator();
        System.err.println("Search String length " + i);
        System.err.println("Number of actual hits: ");
        while (actualHitsIt.hasNext()) {
          if ((j % (numQueried / 4)) == 0) {
            System.err.println(actualHitsIt.next());
          } else {
            actualHitsIt.next();
          }
          j++;
        }
        System.err.println(actualHits[i - 3].lastEntry().getElement());

        final Iterator<Integer> hitIt = hits[i - 3].iterator();
        j = 0;
        System.err.println("Number of query hits: ");
        while (hitIt.hasNext()) {
          if ((j % (numQueried / 4)) == 0) {
            System.err.println(hitIt.next());
          } else {
            hitIt.next();
          }
          j++;
        }
        System.err.println(hits[i - 3].lastEntry().getElement());

        // j = 0;
        // System.err.println("Number of false positives: ");
        // final Iterator<Integer> fpIt = falsePositives[i - 3].iterator();
        // while (fpIt.hasNext()) {
        // if ((j % (numQueried / 4)) == 0) {
        // System.err.println(fpIt.next());
        // } else {
        // fpIt.next();
        // }
        // j++;
        // }
        // System.err.println(falsePositives[i - 3].lastEntry().getElement());
      }
    }

    type.closeAll();
  }

  public static int hashTerm(final String term, final int d) {
    return Lookup3Hash.lookup3ycs(term, 0, term.length(), d);
    // return MurmurHash.hash(term.getBytes(), d);
    // return new HashCodeBuilder().appendSuper(new Random(d).nextInt()).append(term).hashCode();
  }
}
