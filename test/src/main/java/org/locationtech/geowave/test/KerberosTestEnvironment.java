package org.locationtech.geowave.test;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.harness.MiniClusterHarness;
import org.apache.accumulo.harness.TestingKdc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class KerberosTestEnvironment implements TestEnvironment {

  private TestingKdc kdc;
  private static KerberosTestEnvironment singletonInstance = null;
  private ClusterUser rootUser;

  public static synchronized KerberosTestEnvironment getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new KerberosTestEnvironment();
    }
    return singletonInstance;
  }

  private KerberosTestEnvironment() {}

  @Override
  public void setup() throws Exception {
    kdc = new TestingKdc();
    kdc.start();
    System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, "true");
    rootUser = kdc.getRootUser();
  }

  @Override
  public void tearDown() throws Exception {
    if (null != kdc) {
      kdc.stop();
    }
    UserGroupInformation.setConfiguration(new Configuration(false));
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[0];
  }

  public ClusterUser getRootUser() {
    return rootUser;
  }
}
