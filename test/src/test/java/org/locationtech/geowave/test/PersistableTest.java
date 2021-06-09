package org.locationtech.geowave.test;

import java.lang.reflect.Modifier;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistableFactory;
import org.reflections.Reflections;
import edu.emory.mathcs.backport.java.util.Arrays;

public class PersistableTest {

  @Test
  public void testPersistables() {
    Reflections reflections = new Reflections("org.locationtech.geowave");
    Set<Class<? extends Persistable>> actual = reflections.getSubTypesOf(Persistable.class);
    Set<Class<Persistable>> registered =
        PersistableFactory.getInstance().getClassIdMapping().keySet();
    registered.forEach(c -> actual.remove(c));
    Assert.assertFalse(
        Arrays.toString(
            actual.stream().filter(
                c -> !c.isInterface() && !Modifier.isAbstract(c.getModifiers())).toArray(
                    Class[]::new))
            + " are concrete class implementing Persistable but are not registered",
        actual.stream().anyMatch(c -> !c.isInterface() && !Modifier.isAbstract(c.getModifiers())));
  }
}
