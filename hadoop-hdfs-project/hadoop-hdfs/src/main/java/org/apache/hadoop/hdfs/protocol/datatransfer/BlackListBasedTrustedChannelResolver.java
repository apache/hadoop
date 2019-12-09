/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hadoop.util.CombinedIPList;

/**
 * Implements {@link TrustedChannelResolver}
 * to trust ips/host/subnets based on a blackList.
 */
public class BlackListBasedTrustedChannelResolver extends
    TrustedChannelResolver {

  private CombinedIPList blackListForServer;
  private CombinedIPList blackListForClient;

  private static final String FIXED_BLACK_LIST_DEFAULT_LOCATION = "/etc/hadoop"
      + "/fixedBlackList";

  private static final String VARIABLE_BLACK_LIST_DEFAULT_LOCATION = "/etc/"
      + "hadoop/blackList";

  /**
   * Path to the file containing subnets and ip addresses to form
   * fixed BlackList. Server side config.
   */
  public static final String DFS_DATATRANSFER_SERVER_FIXED_BLACK_LIST_FILE =
      "dfs.datatransfer.server.fixedBlackList.file";
  /**
   * Enables/Disables variable BlackList. Server side config.
   */
  public static final String DFS_DATATRANSFER_SERVER_VARIABLE_BLACK_LIST_ENABLE
      = "dfs.datatransfer.server.variableBlackList.enable";
  /**
   * Path to the file containing subnets and ip addresses to form
   * variable BlackList. Server side config.
   */
  public static final String DFS_DATATRANSFER_SERVER_VARIABLE_BLACK_LIST_FILE =
      "dfs.datatransfer.server.variableBlackList.file";
  /**
   * Time in seconds after which the variable BlackList file is checked for
   * updates. Server side config.
   */
  public static final String
      DFS_DATATRANSFER_SERVER_VARIABLE_BLACK_LIST_CACHE_SECS = "dfs."
      + "datatransfer.server.variableBlackList.cache.secs";

  /**
   * Path to the file containing subnets and ip addresses to
   * form fixed BlackList. This key is for client.
   */
  public static final String DFS_DATATRANSFER_CLIENT_FIXED_BLACK_LIST_FILE =
      "dfs.datatransfer.client.fixedBlackList.file";
  /**
   * Enables/Disables variable BlackList. This key is for client.
   */
  public static final String DFS_DATATRANSFER_CLIENT_VARIABLE_BLACK_LIST_ENABLE
      = "dfs.datatransfer.client.variableBlackList.enable";
  /**
   * Path to the file to containing subnets and ip addresses to form variable
   * BlackList. This key is for client.
   */
  public static final String DFS_DATATRANSFER_CLIENT_VARIABLE_BLACK_LIST_FILE =
      "dfs.datatransfer.client.variableBlackList.file";
  /**
   * Time in seconds after which the variable BlackList file is
   * checked for updates. This key is for client.
   */
  public static final String
      DFS_DATATRANSFER_CLIENT_VARIABLE_BLACK_LIST_CACHE_SECS =
      "dfs.datatransfer.client.variableBlackList.cache.secs";

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    String fixedFile = conf.get(DFS_DATATRANSFER_SERVER_FIXED_BLACK_LIST_FILE,
        FIXED_BLACK_LIST_DEFAULT_LOCATION);
    String variableFile = null;
    long expiryTime = 0;

    if (conf
        .getBoolean(DFS_DATATRANSFER_SERVER_VARIABLE_BLACK_LIST_ENABLE,
            false)) {
      variableFile = conf.get(DFS_DATATRANSFER_SERVER_VARIABLE_BLACK_LIST_FILE,
          VARIABLE_BLACK_LIST_DEFAULT_LOCATION);
      expiryTime =
          conf.getLong(DFS_DATATRANSFER_SERVER_VARIABLE_BLACK_LIST_CACHE_SECS,
              3600) * 1000;
    }

    blackListForServer = new CombinedIPList(fixedFile, variableFile,
        expiryTime);

    fixedFile = conf
        .get(DFS_DATATRANSFER_CLIENT_FIXED_BLACK_LIST_FILE, fixedFile);
    expiryTime = 0;

    if (conf
        .getBoolean(DFS_DATATRANSFER_CLIENT_VARIABLE_BLACK_LIST_ENABLE,
            false)) {
      variableFile = conf
          .get(DFS_DATATRANSFER_CLIENT_VARIABLE_BLACK_LIST_FILE, variableFile);
      expiryTime =
          conf.getLong(DFS_DATATRANSFER_CLIENT_VARIABLE_BLACK_LIST_CACHE_SECS,
              3600) * 1000;
    }

    blackListForClient = new CombinedIPList(fixedFile, variableFile,
        expiryTime);
  }

  public boolean isTrusted() {
    try {
      return !blackListForClient
          .isIn(InetAddress.getLocalHost().getHostAddress());
    } catch (UnknownHostException e) {
      return true;
    }
  }

  public boolean isTrusted(InetAddress clientAddress) {
    return !blackListForServer.isIn(clientAddress.getHostAddress());
  }
}
