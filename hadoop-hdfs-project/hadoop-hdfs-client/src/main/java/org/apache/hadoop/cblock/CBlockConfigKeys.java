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

import static java.lang.Thread.NORM_PRIORITY;

/**
 * This class contains constants for configuration keys used in CBlock.
 */
public final class CBlockConfigKeys {
  public static final String DFS_CBLOCK_SERVICERPC_ADDRESS_KEY =
      "dfs.cblock.servicerpc-address";
  public static final int DFS_CBLOCK_SERVICERPC_PORT_DEFAULT =
      9810;
  public static final String DFS_CBLOCK_SERVICERPC_HOSTNAME_DEFAULT =
      "0.0.0.0";

  public static final String DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY =
      "dfs.cblock.jscsi-address";

  //The port on CBlockManager node for jSCSI to ask
  public static final String DFS_CBLOCK_JSCSI_PORT_KEY =
      "dfs.cblock.jscsi.port";
  public static final int DFS_CBLOCK_JSCSI_PORT_DEFAULT =
      9811;

  public static final String DFS_CBLOCK_SERVICERPC_BIND_HOST_KEY =
      "dfs.cblock.service.rpc-bind-host";
  public static final String DFS_CBLOCK_JSCSIRPC_BIND_HOST_KEY =
      "dfs.cblock.jscsi.rpc-bind-host";

  // default block size is 4KB
  public static final int DFS_CBLOCK_SERVICE_BLOCK_SIZE_DEFAULT =
      4096;

  public static final String DFS_CBLOCK_SERVICERPC_HANDLER_COUNT_KEY =
      "dfs.cblock.service.handler.count";
  public static final int DFS_CBLOCK_SERVICERPC_HANDLER_COUNT_DEFAULT = 10;

  public static final String DFS_CBLOCK_SERVICE_LEVELDB_PATH_KEY =
      "dfs.cblock.service.leveldb.path";
  //TODO : find a better place
  public static final String DFS_CBLOCK_SERVICE_LEVELDB_PATH_DEFAULT =
      "/tmp/cblock_levelDB.dat";


  public static final String DFS_CBLOCK_DISK_CACHE_PATH_KEY =
      "dfs.cblock.disk.cache.path";
  public static final String DFS_CBLOCK_DISK_CACHE_PATH_DEFAULT =
      "/tmp/cblockCacheDB";
  /**
   * Setting this flag to true makes the block layer compute a sha256 hash of
   * the data and log that information along with block ID. This is very
   * useful for doing trace based simulation of various workloads. Since it is
   * computing a hash for each block this could be expensive, hence default
   * is false.
   */
  public static final String DFS_CBLOCK_TRACE_IO = "dfs.cblock.trace.io";
  public static final boolean DFS_CBLOCK_TRACE_IO_DEFAULT = false;

  public static final String DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO =
      "dfs.cblock.short.circuit.io";
  public static final boolean DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO_DEFAULT =
      false;

  /**
   * Cache size in 1000s of entries. 256 indicates 256 * 1024.
   */
  public static final String DFS_CBLOCK_CACHE_QUEUE_SIZE_KB =
      "dfs.cblock.cache.queue.size.in.kb";
  public static final int DFS_CBLOCK_CACHE_QUEUE_SIZE_KB_DEFAULT = 256;

  /**
   *  Minimum Number of threads that cache pool will use for background I/O.
   */
  public static final String DFS_CBLOCK_CACHE_CORE_MIN_POOL_SIZE =
      "dfs.cblock.cache.core.min.pool.size";
  public static final int DFS_CBLOCK_CACHE_CORE_MIN_POOL_SIZE_DEFAULT = 16;

  /**
   *  Maximum Number of threads that cache pool will use for background I/O.
   */

  public static final String DFS_CBLOCK_CACHE_MAX_POOL_SIZE =
      "dfs.cblock.cache.max.pool.size";
  public static final int DFS_CBLOCK_CACHE_MAX_POOL_SIZE_DEFAULT = 256;

  /**
   * Number of seconds to keep the Thread alive when it is idle.
   */
  public static final String DFS_CBLOCK_CACHE_KEEP_ALIVE =
      "dfs.cblock.cache.keep.alive";
  public static final String DFS_CBLOCK_CACHE_KEEP_ALIVE_DEFAULT = "60s";

  /**
   * Priority of cache flusher thread, affecting the relative performance of
   * write and read.
   */
  public static final String DFS_CBLOCK_CACHE_THREAD_PRIORITY =
      "dfs.cblock.cache.thread.priority";
  public static final int DFS_CBLOCK_CACHE_THREAD_PRIORITY_DEFAULT =
      NORM_PRIORITY;

  /**
   * Block Buffer size in terms of blockID entries, 512 means 512 blockIDs.
   */
  public static final String DFS_CBLOCK_CACHE_BLOCK_BUFFER_SIZE =
      "dfs.cblock.cache.block.buffer.size";
  public static final int DFS_CBLOCK_CACHE_BLOCK_BUFFER_SIZE_DEFAULT = 512;

  public static final String DFS_CBLOCK_BLOCK_BUFFER_FLUSH_INTERVAL =
      "dfs.cblock.block.buffer.flush.interval";
  public static final String DFS_CBLOCK_BLOCK_BUFFER_FLUSH_INTERVAL_DEFAULT =
      "60s";

  // jscsi server settings
  public static final String DFS_CBLOCK_JSCSI_SERVER_ADDRESS_KEY =
      "dfs.cblock.jscsi.server.address";
  public static final String DFS_CBLOCK_JSCSI_SERVER_ADDRESS_DEFAULT =
      "0.0.0.0";
  public static final String DFS_CBLOCK_JSCSI_CBLOCK_SERVER_ADDRESS_KEY =
      "dfs.cblock.jscsi.cblock.server.address";
  public static final String DFS_CBLOCK_JSCSI_CBLOCK_SERVER_ADDRESS_DEFAULT =
      "127.0.0.1";

  // to what address cblock server should talk to scm?
  public static final String DFS_CBLOCK_SCM_IPADDRESS_KEY =
      "dfs.cblock.scm.ipaddress";
  public static final String DFS_CBLOCK_SCM_IPADDRESS_DEFAULT =
      "127.0.0.1";
  public static final String DFS_CBLOCK_SCM_PORT_KEY =
      "dfs.cblock.scm.port";
  public static final int DFS_CBLOCK_SCM_PORT_DEFAULT = 9860;

  public static final String DFS_CBLOCK_CONTAINER_SIZE_GB_KEY =
      "dfs.cblock.container.size.gb";
  public static final int DFS_CBLOCK_CONTAINER_SIZE_GB_DEFAULT =
      5;

  // LevelDB cache file uses an off-heap cache in LevelDB of 256 MB.
  public static final String DFS_CBLOCK_CACHE_LEVELDB_CACHE_SIZE_MB_KEY =
      "dfs.cblock.cache.leveldb.cache.size.mb";
  public static final int DFS_CBLOCK_CACHE_LEVELDB_CACHE_SIZE_MB_DEFAULT = 256;

  /**
   * Cache does an best case attempt to write a block to a container.
   * At some point of time, we will need to handle the case where we did try
   * 64K times and is till not able to write to the container.
   *
   * TODO: We will need cBlock Server to allow us to do a remapping of the
   * block location in case of failures, at that point we should reduce the
   * retry count to a more normal number. This is approximately 18 hours of
   * retry.
   */
  public static final String DFS_CBLOCK_CACHE_MAX_RETRY_KEY =
      "dfs.cblock.cache.max.retry";
  public static final int DFS_CBLOCK_CACHE_MAX_RETRY_DEFAULT =
      64 * 1024;

  /**
   * Cblock CLI configs.
   */
  public static final String DFS_CBLOCK_MANAGER_POOL_SIZE =
      "dfs.cblock.manager.pool.size";
  public static final int DFS_CBLOCK_MANAGER_POOL_SIZE_DEFAULT = 16;

  /**
   * currently the largest supported volume is about 8TB, which might take
   * > 20 seconds to finish creating containers. thus set timeout to 30 sec.
   */
  public static final String DFS_CBLOCK_RPC_TIMEOUT =
      "dfs.cblock.rpc.timeout";
  public static final String DFS_CBLOCK_RPC_TIMEOUT_DEFAULT = "300s";

  public static final String DFS_CBLOCK_ISCSI_ADVERTISED_IP =
      "dfs.cblock.iscsi.advertised.ip";

  public static final String DFS_CBLOCK_ISCSI_ADVERTISED_PORT =
      "dfs.cblock.iscsi.advertised.port";

  public static final int DFS_CBLOCK_ISCSI_ADVERTISED_PORT_DEFAULT = 3260;

  private CBlockConfigKeys() {

  }
}
