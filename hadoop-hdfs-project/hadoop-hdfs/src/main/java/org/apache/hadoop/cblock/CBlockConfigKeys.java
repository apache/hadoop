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
  public static final String DFS_CBLOCK_SERVICERPC_PORT_KEY =
      "dfs.cblock.servicerpc.port";
  public static final int DFS_CBLOCK_SERVICERPC_PORT_DEFAULT =
      9810;
  public static final String DFS_CBLOCK_SERVICERPC_HOSTNAME_KEY =
      "dfs.cblock.servicerpc.hostname";
  public static final String DFS_CBLOCK_SERVICERPC_HOSTNAME_DEFAULT =
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
      DFS_CBLOCK_SERVICERPC_HOSTNAME_DEFAULT
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
      "dfs.cblock.cache.cache.size.in.kb";
  public static final int DFS_CBLOCK_CACHE_QUEUE_SIZE_KB_DEFAULT = 256;

  /**
   *  Minimum Number of threads that cache pool will use for background I/O.
   */
  public static final String DFS_CBLOCK_CACHE_CORE_POOL_SIZE =
      "dfs.cblock.cache.core.pool.size";
  public static final int DFS_CBLOCK_CACHE_CORE_POOL_SIZE_DEFAULT = 16;

  /**
   *  Maximum Number of threads that cache pool will use for background I/O.
   */

  public static final String DFS_CBLOCK_CACHE_MAX_POOL_SIZE =
      "dfs.cblock.cache.max.pool.size";
  public static final int DFS_CBLOCK_CACHE_MAX_POOL_SIZE_DEFAULT = 256;

  /**
   * Number of seconds to keep the Thread alive when it is idle.
   */
  public static final String DFS_CBLOCK_CACHE_KEEP_ALIVE_SECONDS =
      "dfs.cblock.cache.keep.alive.seconds";
  public static final long DFS_CBLOCK_CACHE_KEEP_ALIVE_SECONDS_DEFAULT = 60;

  /**
   * Priority of cache flusher thread, affecting the relative performance of
   * write and read.
   */
  public static final String DFS_CBLOCK_CACHE_THREAD_PRIORITY =
      "dfs.cblock.cache.thread.priority";
  public static final int DFS_CBLOCK_CACHE_THREAD_PRIORITY_DEFAULT =
      NORM_PRIORITY;

  /**
   * Block Buffer size in 1024 entries, 128 means 128 * 1024 blockIDs.
   */
  public static final String DFS_CBLOCK_CACHE_BLOCK_BUFFER_SIZE =
      "dfs.cblock.cache.block.buffer.size";
  public static final int DFS_CBLOCK_CACHE_BLOCK_BUFFER_SIZE_DEFAULT = 128;

  private CBlockConfigKeys() {

  }
}
