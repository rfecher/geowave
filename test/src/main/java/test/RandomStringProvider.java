package test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import test.StringProviderBenchmark.StringProvider;

public class RandomStringProvider implements StringProvider {
  Random random = new Random();
  int totalIndexed = 10000;
  int totalQueried = 10001;

  @Override
  public Iterator<String> getIndexedStrings() {
    final int numCharacters = (int) Math.max(Math.round((random.nextGaussian() * 10) + 10), 3);

    final List<String> retVal = new ArrayList<>(totalIndexed);
    for (int j = 0; j < totalIndexed; j++) {
      final StringBuilder bldr = new StringBuilder();
      for (int i = 0; i < numCharacters; i++) {
        bldr.append((char) random.nextInt());
      }
      retVal.add(bldr.toString());
    }
    return retVal.iterator();
  }

  @Override
  public Iterator<String> getQueryStrings(final int numCharacters, final int numSamples) {
    final List<String> retVal = new ArrayList<>(numSamples);
    for (int j = 0; j < numSamples; j++) {
      final StringBuilder bldr = new StringBuilder();
      for (int i = 0; i < numCharacters; i++) {
        bldr.append((char) random.nextInt());
      }
      retVal.add(bldr.toString());
    }
    return retVal.iterator();
  }

  @Override
  public void closeAll() {
    // TODO Auto-generated method stub

  }

}
