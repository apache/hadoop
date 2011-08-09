/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.util.StringUtils;

/**
 * Utility methods for reading, parsing, and building zookeeper configuration.
 */
public class ZKConfig {
  private static final Log LOG = LogFactory.getLog(ZKConfig.class);

  private static final String VARIABLE_START = "${";
  private static final int VARIABLE_START_LENGTH = VARIABLE_START.length();
  private static final String VARIABLE_END = "}";
  private static final int VARIABLE_END_LENGTH = VARIABLE_END.length();

  private static final String ZK_CFG_PROPERTY = "hbase.zookeeper.property.";
  private static final int ZK_CFG_PROPERTY_SIZE = ZK_CFG_PROPERTY.length();
  private static final String ZK_CLIENT_PORT_KEY = ZK_CFG_PROPERTY
      + "clientPort";

  /**
   * Make a Properties object holding ZooKeeper config equivalent to zoo.cfg.
   * If there is a zoo.cfg in the classpath, simply read it in. Otherwise parse
   * the corresponding config options from the HBase XML configs and generate
   * the appropriate ZooKeeper properties.
   * @param conf Configuration to read from.
   * @return Properties holding mappings representing ZooKeeper zoo.cfg file.
   */
  public static Properties makeZKProps(Configuration conf) {
    // First check if there is a zoo.cfg in the CLASSPATH. If so, simply read
    // it and grab its configuration properties.
    ClassLoader cl = HQuorumPeer.class.getClassLoader();
    final InputStream inputStream =
      cl.getResourceAsStream(HConstants.ZOOKEEPER_CONFIG_NAME);
    if (inputStream != null) {
      try {
        return parseZooCfg(conf, inputStream);
      } catch (IOException e) {
        LOG.warn("Cannot read " + HConstants.ZOOKEEPER_CONFIG_NAME +
                 ", loading from XML files", e);
      }
    }

    // Otherwise, use the configuration options from HBase's XML files.
    Properties zkProperties = new Properties();

    // Directly map all of the hbase.zookeeper.property.KEY properties.
    for (Entry<String, String> entry : conf) {
      String key = entry.getKey();
      if (key.startsWith(ZK_CFG_PROPERTY)) {
        String zkKey = key.substring(ZK_CFG_PROPERTY_SIZE);
        String value = entry.getValue();
        // If the value has variables substitutions, need to do a get.
        if (value.contains(VARIABLE_START)) {
          value = conf.get(key);
        }
        zkProperties.put(zkKey, value);
      }
    }

    // If clientPort is not set, assign the default
    if (zkProperties.getProperty(ZK_CLIENT_PORT_KEY) == null) {
      zkProperties.put(ZK_CLIENT_PORT_KEY,
                       HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    }

    // Create the server.X properties.
    int peerPort = conf.getInt("hbase.zookeeper.peerport", 2888);
    int leaderPort = conf.getInt("hbase.zookeeper.leaderport", 3888);

    final String[] serverHosts = conf.getStrings(HConstants.ZOOKEEPER_QUORUM,
                                                 "localhost");
    for (int i = 0; i < serverHosts.length; ++i) {
      String serverHost = serverHosts[i];
      String address = serverHost + ":" + peerPort + ":" + leaderPort;
      String key = "server." + i;
      zkProperties.put(key, address);
    }

    return zkProperties;
  }

  /**
   * Parse ZooKeeper's zoo.cfg, injecting HBase Configuration variables in.
   * This method is used for testing so we can pass our own InputStream.
   * @param conf HBaseConfiguration to use for injecting variables.
   * @param inputStream InputStream to read from.
   * @return Properties parsed from config stream with variables substituted.
   * @throws IOException if anything goes wrong parsing config
   */
  public static Properties parseZooCfg(Configuration conf,
      InputStream inputStream) throws IOException {
    Properties properties = new Properties();
    try {
      properties.load(inputStream);
    } catch (IOException e) {
      final String msg = "fail to read properties from "
        + HConstants.ZOOKEEPER_CONFIG_NAME;
      LOG.fatal(msg);
      throw new IOException(msg, e);
    }
    for (Entry<Object, Object> entry : properties.entrySet()) {
      String value = entry.getValue().toString().trim();
      String key = entry.getKey().toString().trim();
      StringBuilder newValue = new StringBuilder();
      int varStart = value.indexOf(VARIABLE_START);
      int varEnd = 0;
      while (varStart != -1) {
        varEnd = value.indexOf(VARIABLE_END, varStart);
        if (varEnd == -1) {
          String msg = "variable at " + varStart + " has no end marker";
          LOG.fatal(msg);
          throw new IOException(msg);
        }
        String variable = value.substring(varStart + VARIABLE_START_LENGTH, varEnd);

        String substituteValue = System.getProperty(variable);
        if (substituteValue == null) {
          substituteValue = conf.get(variable);
        }
        if (substituteValue == null) {
          String msg = "variable " + variable + " not set in system property "
                     + "or hbase configs";
          LOG.fatal(msg);
          throw new IOException(msg);
        }

        newValue.append(substituteValue);

        varEnd += VARIABLE_END_LENGTH;
        varStart = value.indexOf(VARIABLE_START, varEnd);
      }
      // Special case for 'hbase.cluster.distributed' property being 'true'
      if (key.startsWith("server.")) {
        if (conf.get(HConstants.CLUSTER_DISTRIBUTED).equals(HConstants.CLUSTER_IS_DISTRIBUTED)
            && value.startsWith("localhost")) {
          String msg = "The server in zoo.cfg cannot be set to localhost " +
              "in a fully-distributed setup because it won't be reachable. " +
              "See \"Getting Started\" for more information.";
          LOG.fatal(msg);
          throw new IOException(msg);
        }
      }
      newValue.append(value.substring(varEnd));
      properties.setProperty(key, newValue.toString());
    }
    return properties;
  }

  /**
   * Return the ZK Quorum servers string given zk properties returned by
   * makeZKProps
   * @param properties
   * @return Quorum servers String
   */
  public static String getZKQuorumServersString(Properties properties) {
    String clientPort = null;
    List<String> servers = new ArrayList<String>();

    // The clientPort option may come after the server.X hosts, so we need to
    // grab everything and then create the final host:port comma separated list.
    boolean anyValid = false;
    for (Entry<Object,Object> property : properties.entrySet()) {
      String key = property.getKey().toString().trim();
      String value = property.getValue().toString().trim();
      if (key.equals("clientPort")) {
        clientPort = value;
      }
      else if (key.startsWith("server.")) {
        String host = value.substring(0, value.indexOf(':'));
        servers.add(host);
        try {
          //noinspection ResultOfMethodCallIgnored
          InetAddress.getByName(host);
          anyValid = true;
        } catch (UnknownHostException e) {
          LOG.warn(StringUtils.stringifyException(e));
        }
      }
    }

    if (!anyValid) {
      LOG.error("no valid quorum servers found in " + HConstants.ZOOKEEPER_CONFIG_NAME);
      return null;
    }

    if (clientPort == null) {
      LOG.error("no clientPort found in " + HConstants.ZOOKEEPER_CONFIG_NAME);
      return null;
    }

    if (servers.isEmpty()) {
      LOG.fatal("No server.X lines found in conf/zoo.cfg. HBase must have a " +
                "ZooKeeper cluster configured for its operation.");
      return null;
    }

    StringBuilder hostPortBuilder = new StringBuilder();
    for (int i = 0; i < servers.size(); ++i) {
      String host = servers.get(i);
      if (i > 0) {
        hostPortBuilder.append(',');
      }
      hostPortBuilder.append(host);
      hostPortBuilder.append(':');
      hostPortBuilder.append(clientPort);
    }

    return hostPortBuilder.toString();
  }

  /**
   * Return the ZK Quorum servers string given the specified configuration.
   * @param conf
   * @return Quorum servers
   */
  public static String getZKQuorumServersString(Configuration conf) {
    return getZKQuorumServersString(makeZKProps(conf));
  }
}
