/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.config;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import org.apache.hadoop.security.UserGroupInformation;
import org.locationtech.geowave.core.cli.converters.PasswordConverter;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.accumulo.AccumuloStoreFactoryFamily;
import java.io.IOException;

/**
 * Default, required options needed in order to execute any command for Accumulo.
 */
public class AccumuloRequiredOptions extends StoreFactoryOptions {

  public static final String ZOOKEEPER_CONFIG_KEY = "zookeeper";
  public static final String INSTANCE_CONFIG_KEY = "instance";
  public static final String USER_CONFIG_KEY = "user";
  // HP Fortify "Hardcoded Password - Password Management: Hardcoded Password"
  // false positive
  // This is a password label, not a password
  public static final String PASSWORD_CONFIG_KEY = "password";

  @Parameter(
      names = {"-z", "--" + ZOOKEEPER_CONFIG_KEY},
      description = "A comma-separated list of zookeeper servers that an Accumulo instance is using",
      required = true)
  private String zookeeper;

  @Parameter(
      names = {"-i", "--" + INSTANCE_CONFIG_KEY},
      description = "The Accumulo instance ID",
      required = true)
  private String instance;

  @Parameter(
      names = {"-u", "--" + USER_CONFIG_KEY},
      description = "A valid Accumulo user ID. If not provided and using SASL, the active Kerebros user will be used.",
      required = true)
  private String user;

  @Parameter(
      names = {"-p", "--" + PASSWORD_CONFIG_KEY},
      description = "The password for the user. " + PasswordConverter.DEFAULT_PASSWORD_DESCRIPTION,
      descriptionKey = "accumulo.pass.label",
      converter = PasswordConverter.class)
  private String password;

  @Parameter(names = "--sasl", description = "Use SASL to connect to Accumulo (Kerberos)")
  private boolean useSasl = false;

  @ParametersDelegate
  private AccumuloOptions additionalOptions = new AccumuloOptions();

  public AccumuloRequiredOptions() {}

  public String getZookeeper() {
    return zookeeper;
  }

  public void setZookeeper(final String zookeeper) {
    this.zookeeper = zookeeper;
  }

  public String getInstance() {
    return instance;
  }

  public void setInstance(final String instance) {
    this.instance = instance;
  }

  public String getUser() {
    if (user == null || user.isEmpty()) {
      if (isUseSasl()) {
        if (!UserGroupInformation.isSecurityEnabled()) {
          throw new IllegalArgumentException(
              "Kerberos security is not"
                  + " enabled. Run with --sasl or set 'sasl.enabled' in"
                  + " accumulo-client.properties");
        }
        try {
          UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
          user = ugi.getUserName();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return user;
  }

  public void setUser(final String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(final String password) {
    this.password = password;
  }

  public void setStoreOptions(final AccumuloOptions additionalOptions) {
    this.additionalOptions = additionalOptions;
  }

  public void setUseSasl(boolean useSasl) {
    this.useSasl = useSasl;
  }

  public boolean isUseSasl() {
    return useSasl;
  }

  @Override
  public StoreFactoryFamilySpi getStoreFactory() {
    return new AccumuloStoreFactoryFamily();
  }

  @Override
  public DataStoreOptions getStoreOptions() {
    return additionalOptions;
  }
}
