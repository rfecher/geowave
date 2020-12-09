package test;

import java.util.Iterator;
import java.util.Random;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;

public class Main {
  public static void main(String[] args) {
    Random random = new Random();

    int[][][] ranges = new int[10000][10][2];
    for (int j = 0; j < ranges.length; j++) {
      for (int k = 0; k < ranges[j].length; k++) {
        int numGrams = (int) Math.max(Math.round(random.nextGaussian() * 10 + 10), 1);
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < numGrams; i++) {
          int val = random.nextInt();
          min = Math.min(val, min);
          max = Math.max(val, max);
        }
        ranges[j][k][0] = min;
        ranges[j][k][1] = max;
      }
    }
    SortedMultiset<Integer> hits = TreeMultiset.create();
    for (int i = 0; i < 10001; i++) {
      int hit = 0;
      int val = random.nextInt();
      for (int j = 0; j < ranges.length; j++) {
        boolean nohit = false;
        for (int k = 0; k < ranges[j].length; k++) {
          if (!(val >= ranges[j][k][0] && val <= ranges[j][k][1])) {
            nohit = true;
            break;
          }
        }
        if (!nohit) {
          hit++;
        }
      }
      hits.add(hit);
    }
    Iterator<Integer> hitIt = hits.iterator();
    int i = 0;
    while (hitIt.hasNext()) {
      if (i % 1000 == 0) {
        System.err.println(hitIt.next());
      } else {
        hitIt.next();
      }
      i++;
    }
  }
}
