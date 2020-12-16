package test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import com.clearspring.analytics.hash.Lookup3Hash;
import com.clearspring.analytics.hash.MurmurHash;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;

public class StringProviderBenchmark {
  public static interface StringProvider {
    Iterator<String> getIndexedStrings();

    Iterator<String> getQueryStrings(int length, int numSamples);

    void closeAll();
  }
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
    final List<int[][]> ranges = new ArrayList<>();
    final Type type = Type.valueOf(args[0]);
    final int numDimensions = Integer.parseInt(args[1]);
    final int numIndexed = Integer.parseInt(args[2]);
    final int numQueried = Integer.parseInt(args[3]);
    final int maxSearchString = Integer.parseInt(args[4]);
    int ngram = 2;
    try (final NGramTokenizer tokenizer = new NGramTokenizer(ngram, ngram)) {
      final CharTermAttribute charTermAttribute = tokenizer.addAttribute(CharTermAttribute.class);
      final Iterator<String> iterator = type.getIndexedStrings();
      final List<String> vals = new ArrayList<>();
      while (iterator.hasNext() && (vals.size() < numIndexed)) {
        final String str = iterator.next();
        vals.add(str);
        final int[][] newRange = new int[numDimensions][2];
        ranges.add(newRange);
        for (int d = 0; d < numDimensions; d++) {
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
          newRange[d][0] = min;
          newRange[d][1] = max;
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
          final String qStr = qIterator.next();
          if (qStr.length() != i) {
            System.err.println(
                "ERROR: query string is " + qStr.length() + " when it should be " + i);
          }
          final long actualHit = vals.stream().filter(str -> str.contains(qStr)).count();
          actualHits[i - 3].add((int) actualHit);
          for (final int[][] r : ranges) {
            boolean nohit = false;
            for (int d = 0; d < numDimensions; d++) {
              tokenizer.setReader(new StringReader(qStr));
              tokenizer.reset();
              while (tokenizer.incrementToken()) {
                final String term = charTermAttribute.toString();
                final int hashCode = hashTerm(term, d);
                if (!((hashCode >= r[d][0]) && (hashCode <= r[d][1]))) {
                  nohit = true;
                  break;
                }
              }

              tokenizer.close();
              if (nohit) {
                break;
              }
            }

            if (!nohit) {
              hit++;
            }
          }

          hits[i - 3].add(hit);
          // falseNegatives[i - 3].add(falseNegative);
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
//    return MurmurHash.hash(term.getBytes(), d);
//    return new HashCodeBuilder().appendSuper(new Random(d).nextInt()).append(term).hashCode();
  }
}
