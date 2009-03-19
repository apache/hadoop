/**
 * Copyright 2009 The Apache Software Foundation
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
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

/**
 * HBase's version of ZooKeeper's QuorumPeer. When HBase is set to manage
 * ZooKeeper, this class is used to start up QuorumPeer instances. By doing
 * things in here rather than directly calling to ZooKeeper, we have more
 * control over the process. Currently, this class allows us to parse the
 * zoo.cfg and inject variables from HBase's site.xml configuration in.
 */
public class HQuorumPeer implements HConstants {
  private static final Log LOG = LogFactory.getLog(HQuorumPeer.class);
  private static final String VARIABLE_START = "${";
  private static final int VARIABLE_START_LENGTH = VARIABLE_START.length();
  private static final String VARIABLE_END = "}";
  private static final int VARIABLE_END_LENGTH = VARIABLE_END.length();

  /**
   * Parse ZooKeeper configuration and run a QuorumPeer.
   * While parsing the zoo.cfg, we substitute variables with values from
   * hbase-site.xml.
   * @param args String[] of command line arguments. Not used.
   */
  public static void main(String[] args) {
    try {
      Properties properties = parseZooKeeperConfig();
      QuorumPeerConfig.parseProperties(properties);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    if (ServerConfig.isStandalone()) {
      ZooKeeperServerMain.main(args);
    } else {
      QuorumPeerMain.runPeerFromConfig();
    }
  }

  /**
   * Parse ZooKeeper's zoo.cfg, injecting HBase Configuration variables in.
   * @return Properties parsed from config stream with variables substituted.
   * @throws IOException if anything goes wrong parsing config
   */
  public static Properties parseZooKeeperConfig() throws IOException {
    ClassLoader cl = HQuorumPeer.class.getClassLoader();
    InputStream inputStream = cl.getResourceAsStream(ZOOKEEPER_CONFIG_NAME);
    return parseConfig(inputStream);
  }

  /**
   * Parse ZooKeeper's zoo.cfg, injecting HBase Configuration variables in.
   * This method is used for testing so we can pass our own InputStream.
   * @param inputStream InputStream to read from.
   * @return Properties parsed from config stream with variables substituted.
   * @throws IOException if anything goes wrong parsing config
   */
  public static Properties parseConfig(InputStream inputStream) throws IOException {
    HBaseConfiguration conf = new HBaseConfiguration();
    Properties properties = new Properties();
    try {
      properties.load(inputStream);
    } catch (IOException e) {
      String msg = "fail to read properties from " + ZOOKEEPER_CONFIG_NAME;
      LOG.fatal(msg);
      throw new IOException(msg);
    }
    for (Entry<Object, Object> entry : properties.entrySet()) {
      String value = entry.getValue().toString().trim();
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

      newValue.append(value.substring(varEnd));

      String key = entry.getKey().toString().trim();
      properties.setProperty(key, newValue.toString());
    }
    return properties;
  }
}
