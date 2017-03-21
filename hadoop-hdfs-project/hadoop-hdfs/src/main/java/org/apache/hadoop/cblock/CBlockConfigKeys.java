/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.cblock;

/**
 * This class contains constants for configuration keys used in CBlock.
 */
public final class CBlockConfigKeys {
  public static final String DFS_CBLOCK_ENABLED_KEY =
      "dfs.cblock.enabled";
  public static final String DFS_CBLOCK_SERVICERPC_ADDRESS_KEY =
      "dfs.cblock.servicerpc-address";
  public static final String DFS_CBLOCK_SERVICERPC_PORT_KEY =
      "dfs.cblock.servicerpc.port";
  public static final int DFS_CBLOCK_SERVICERPC_PORT_DEFAULT =
      9810;
  public static final String DFS_CBLOCK_SERVICERPC_HOSTNAME_KEY =
      "dfs.cblock.servicerpc.hostname";
  public static final String DFS_CBLOCK_SERVICERPC_HOSTNAME_DEFAULT =
      "0.0.0.0";
  public static final String DFS_CBLOCK_RPCSERVICE_IP_DEFAULT =
      "0.0.0.0";
  public static final String DFS_CBLOCK_SERVICERPC_ADDRESS_DEFAULT =
      DFS_CBLOCK_SERVICERPC_HOSTNAME_DEFAULT
          + ":" + DFS_CBLOCK_SERVICERPC_PORT_DEFAULT;

  public static final String DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY =
      "dfs.cblock.jscsi-address";

  //The port on CBlockManager node for jSCSI to ask
  public static final int DFS_CBLOCK_JSCSI_PORT_DEFAULT =
      9811;
  public static final String DFS_CBLOCK_JSCSIRPC_ADDRESS_DEFAULT =
      DFS_CBLOCK_RPCSERVICE_IP_DEFAULT
          + ":" + DFS_CBLOCK_JSCSI_PORT_DEFAULT;


  public static final String DFS_CBLOCK_SERVICERPC_BIND_HOST_KEY =
      "dfs.cblock.service.rpc-bind-host";
  public static final String DFS_CBLOCK_JSCSIRPC_BIND_HOST_KEY =
      "dfs.cblock.jscsi.rpc-bind-host";

  // default block size is 4KB
  public static final int DFS_CBLOCK_SERVICE_BLOCK_SIZE_DEFAULT =
      4096;

  public static final String DFS_CBLOCK_SERVICERPC_HANDLER_COUNT_KEY =
      "dfs.storage.service.handler.count";
  public static final int DFS_CBLOCK_SERVICERPC_HANDLER_COUNT_DEFAULT = 10;

  public static final String DFS_CBLOCK_SERVICE_LEVELDB_PATH_KEY =
      "dfs.cblock.service.leveldb.path";
  //TODO : find a better place
  public static final String DFS_CBLOCK_SERVICE_LEVELDB_PATH_DEFAULT =
      "/tmp/cblock_levelDB.dat";

  private CBlockConfigKeys() {

  }
}
