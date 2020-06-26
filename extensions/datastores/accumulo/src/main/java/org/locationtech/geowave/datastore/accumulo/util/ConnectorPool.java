/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.util;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ConnectorPool {
  private static ConnectorPool singletonInstance;

  public static synchronized ConnectorPool getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new ConnectorPool();
    }
    return singletonInstance;
  }

  private final Map<ConnectorConfig, Connector> connectorCache = new HashMap<>();

  public synchronized Connector getConnector(
      final String zookeeperUrl,
      final String instanceName,
      final String userName,
      final String password,
      boolean useSasl) throws AccumuloException, AccumuloSecurityException, IOException {

    final ConnectorConfig config =
        new ConnectorConfig(zookeeperUrl, instanceName, userName, password, useSasl);
    Connector connector = connectorCache.get(config);
    if (connector == null) {
      final Instance inst = new ZooKeeperInstance(instanceName, zookeeperUrl);
      if (useSasl) {
        connector = inst.getConnector(userName, new KerberosToken(userName));
      } else {
        connector = inst.getConnector(userName, new PasswordToken(password));
      }
      connectorCache.put(config, connector);
    }
    return connector;
  }

  private static class ConnectorConfig {
    private final String zookeeperUrl;
    private final String instanceName;
    private final String userName;
    private final String password;
    private final boolean useSasl;

    public ConnectorConfig(
        final String zookeeperUrl,
        final String instanceName,
        final String userName,
        final String password,
        boolean useSasl) {
      this.zookeeperUrl = zookeeperUrl;
      this.instanceName = instanceName;
      this.userName = userName;
      this.password = password;
      this.useSasl = useSasl;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      ConnectorConfig that = (ConnectorConfig) o;
      return useSasl == that.useSasl
          && zookeeperUrl.equals(that.zookeeperUrl)
          && instanceName.equals(that.instanceName)
          && userName.equals(that.userName)
          && Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
      return Objects.hash(zookeeperUrl, instanceName, userName, password, useSasl);
    }
  }
}
