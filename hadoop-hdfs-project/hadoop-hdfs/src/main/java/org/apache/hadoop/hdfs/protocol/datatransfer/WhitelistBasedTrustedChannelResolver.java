/**
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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.util.CombinedIPWhiteList;


public class WhitelistBasedTrustedChannelResolver  extends TrustedChannelResolver {

  private CombinedIPWhiteList whiteListForServer;
  private CombinedIPWhiteList whitelistForClient;

  private static final String FIXEDWHITELIST_DEFAULT_LOCATION = "/etc/hadoop/fixedwhitelist";

  private static final String VARIABLEWHITELIST_DEFAULT_LOCATION = "/etc/hadoop/whitelist";

  /**
   * Path to the file to containing subnets and ip addresses to form fixed whitelist.
   */
  public static final String DFS_DATATRANSFER_SERVER_FIXEDWHITELIST_FILE =
    "dfs.datatransfer.server.fixedwhitelist.file";
  /**
   * Enables/Disables variable whitelist
   */
  public static final String DFS_DATATRANSFER_SERVER_VARIABLEWHITELIST_ENABLE =
    "dfs.datatransfer.server.variablewhitelist.enable";
  /**
   * Path to the file to containing subnets and ip addresses to form variable whitelist.
   */
  public static final String DFS_DATATRANSFER_SERVER_VARIABLEWHITELIST_FILE =
    "dfs.datatransfer.server.variablewhitelist.file";
  /**
   * time in seconds by which the variable whitelist file is checked for updates
   */
  public static final String DFS_DATATRANSFER_SERVER_VARIABLEWHITELIST_CACHE_SECS =
    "dfs.datatransfer.server.variablewhitelist.cache.secs";

  /**
   * Path to the file to containing subnets and ip addresses to form fixed whitelist.
   */
  public static final String DFS_DATATRANSFER_CLIENT_FIXEDWHITELIST_FILE =
    "dfs.datatransfer.client.fixedwhitelist.file";
  /**
   * Enables/Disables variable whitelist
   */
  public static final String DFS_DATATRANSFER_CLIENT_VARIABLEWHITELIST_ENABLE =
    "dfs.datatransfer.client.variablewhitelist.enable";
  /**
   * Path to the file to containing subnets and ip addresses to form variable whitelist.
   */
  public static final String DFS_DATATRANSFER_CLIENT_VARIABLEWHITELIST_FILE =
    "dfs.datatransfer.client.variablewhitelist.file";
  /**
   * time in seconds by which the variable whitelist file is checked for updates
   */
  public static final String DFS_DATATRANSFER_CLIENT_VARIABLEWHITELIST_CACHE_SECS =
    "dfs.datatransfer.client.variablewhitelist.cache.secs";

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    String fixedFile = conf.get(DFS_DATATRANSFER_SERVER_FIXEDWHITELIST_FILE,
        FIXEDWHITELIST_DEFAULT_LOCATION);
    String variableFile = null;
    long expiryTime = 0;

    if (conf.getBoolean(DFS_DATATRANSFER_SERVER_VARIABLEWHITELIST_ENABLE, false)) {
      variableFile = conf.get(DFS_DATATRANSFER_SERVER_VARIABLEWHITELIST_FILE,
          VARIABLEWHITELIST_DEFAULT_LOCATION);
      expiryTime =
        conf.getLong(DFS_DATATRANSFER_SERVER_VARIABLEWHITELIST_CACHE_SECS,3600) * 1000;
    }

    whiteListForServer = new CombinedIPWhiteList(fixedFile,variableFile,expiryTime);

    fixedFile = conf.get(DFS_DATATRANSFER_CLIENT_FIXEDWHITELIST_FILE, fixedFile);
    expiryTime = 0;

    if (conf.getBoolean(DFS_DATATRANSFER_CLIENT_VARIABLEWHITELIST_ENABLE, false)) {
      variableFile = conf.get(DFS_DATATRANSFER_CLIENT_VARIABLEWHITELIST_FILE,variableFile);
      expiryTime =
        conf.getLong(DFS_DATATRANSFER_CLIENT_VARIABLEWHITELIST_CACHE_SECS,3600) * 1000;
    }

    whitelistForClient = new CombinedIPWhiteList(fixedFile,variableFile,expiryTime);
  }

  public boolean isTrusted() {
    try {
      return whitelistForClient.isIn(InetAddress.getLocalHost().getHostAddress());
    } catch (UnknownHostException e) {
      return false;
    }
  }

  public boolean isTrusted(InetAddress clientAddress) {
    return whiteListForServer.isIn(clientAddress.getHostAddress());
  }
}
