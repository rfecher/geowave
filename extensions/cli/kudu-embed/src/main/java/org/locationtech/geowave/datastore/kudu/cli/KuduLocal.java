/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu.cli;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.io.FileUtils;
import org.apache.kudu.test.cluster.MiniKuduCluster;
import org.apache.kudu.test.cluster.MiniKuduCluster.MiniKuduClusterBuilder;

public class KuduLocal {

  // Tracking the cloudera precompiled package
  // https://www.cloudera.com/documentation/enterprise/5-16-x/topics/cdh_ig_yumrepo_local_create.html#topic_30__section_sl2_xdw_wm

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(KuduLocal.class);

  private static final long STARTUP_DELAY_MS = 1500L;

  public static final File DEFAULT_DIR = new File("./target/temp");
  private MiniKuduCluster kudu;
  private final MiniKuduClusterBuilder kuduBldr;
  private final File kuduLocalDir;
  private final File kuduDBDir; // storage for database files


  public KuduLocal(final RunKuduLocalOptions opt) {
    this(opt.getDirectory(), opt.getTablets());
  }

  public KuduLocal(final String localDir, final int numTablets) {
    if ((localDir != null) && !localDir.contentEquals("")) {
      kuduLocalDir = new File(localDir);
    } else {
      kuduLocalDir = new File(DEFAULT_DIR, "kudu");
    }
    if (!kuduLocalDir.exists() && !kuduLocalDir.mkdirs()) {
      LOGGER.error("unable to create directory {}", kuduLocalDir.getAbsolutePath());
    } else if (!kuduLocalDir.isDirectory()) {
      LOGGER.error("{} exists but is not a directory", kuduLocalDir.getAbsolutePath());
    }
    kuduDBDir = new File(kuduLocalDir, "db");
    kuduBldr =
        new MiniKuduClusterBuilder().numMasterServers(1).numTabletServers(numTablets).clusterRoot(
            localDir);
  }

  public boolean start() {
    try {
      startKuduLocal();
    } catch (IOException | InterruptedException e) {
      LOGGER.error("Kudu start error: {}", e.getMessage());
      return false;
    }

    return true;
  }

  public boolean isRunning() {
    return (kudu != null);
  }

  public void stop() throws IOException {
    kudu.kdestroy();
    kudu.killAllTabletServers();
    kudu.killAllMasterServers();

    try {
      Thread.sleep(STARTUP_DELAY_MS);
    } catch (final InterruptedException e) {
    }
  }

  public void destroyDB() throws IOException {
    try {
      FileUtils.deleteDirectory(kuduDBDir);
    } catch (final IOException e) {
      LOGGER.error("Could not destroy database files", e);
      throw e;
    }
  }

  private void startKuduLocal() throws ExecuteException, IOException, InterruptedException {
    if (!kuduDBDir.exists() && !kuduDBDir.mkdirs()) {
      LOGGER.error("unable to create directory {}", kuduDBDir.getAbsolutePath());
    } else if (!kuduDBDir.isDirectory()) {
      LOGGER.error("{} exists but is not a directory", kuduDBDir.getAbsolutePath());
    }
    if (kudu == null) {
      kudu = kuduBldr.build();
    }
    kudu.startAllMasterServers();
    kudu.startAllTabletServers();

    Thread.sleep(STARTUP_DELAY_MS);
  }

  public static void main(final String[] args) {
    final KuduLocal kudu = new KuduLocal(null, 1);
    kudu.start();
  }

}
