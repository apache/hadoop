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

package org.apache.hadoop.ozone;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.scm.ScmConfigKeys;

/**
 * This class contains constants for configuration keys used in Ozone.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class OzoneConfigKeys {
  public static final String DFS_CONTAINER_IPC_PORT =
      "dfs.container.ipc";
  public static final int DFS_CONTAINER_IPC_PORT_DEFAULT = 50011;

  /**
   *
   * When set to true, allocate a random free port for ozone container,
   * so that a mini cluster is able to launch multiple containers on a node.
   *
   * When set to false (default), container port is fixed as specified by
   * DFS_CONTAINER_IPC_PORT_DEFAULT.
   */
  public static final String DFS_CONTAINER_IPC_RANDOM_PORT =
      "dfs.container.ipc.random.port";
  public static final boolean DFS_CONTAINER_IPC_RANDOM_PORT_DEFAULT =
      false;

  public static final String OZONE_LOCALSTORAGE_ROOT =
      "ozone.localstorage.root";
  public static final String OZONE_LOCALSTORAGE_ROOT_DEFAULT = "/tmp/ozone";
  public static final String OZONE_ENABLED =
      "ozone.enabled";
  public static final boolean OZONE_ENABLED_DEFAULT = false;
  public static final String OZONE_HANDLER_TYPE_KEY =
      "ozone.handler.type";
  public static final String OZONE_HANDLER_TYPE_DEFAULT = "distributed";
  public static final String OZONE_TRACE_ENABLED_KEY =
      "ozone.trace.enabled";
  public static final boolean OZONE_TRACE_ENABLED_DEFAULT = false;

  public static final String OZONE_CONTAINER_METADATA_DIRS =
      "ozone.container.metadata.dirs";

  public static final String OZONE_METADATA_STORE_IMPL =
      "ozone.metastore.impl";
  public static final String OZONE_METADATA_STORE_IMPL_LEVELDB =
      "LevelDB";
  public static final String OZONE_METADATA_STORE_IMPL_ROCKSDB =
      "RocksDB";
  public static final String OZONE_METADATA_STORE_IMPL_DEFAULT =
      OZONE_METADATA_STORE_IMPL_LEVELDB;

  public static final String OZONE_KEY_CACHE = "ozone.key.cache.size";
  public static final int OZONE_KEY_CACHE_DEFAULT = 1024;

  public static final String OZONE_SCM_BLOCK_SIZE_KEY =
      "ozone.scm.block.size";
  public static final long OZONE_SCM_BLOCK_SIZE_DEFAULT = 256 * OzoneConsts.MB;

  /**
   * Ozone administrator users delimited by comma.
   * If not set, only the user who launches an ozone service will be the
   * admin user. This property must be set if ozone services are started by
   * different users. Otherwise the RPC layer will reject calls from
   * other servers which are started by users not in the list.
   * */
  public static final String OZONE_ADMINISTRATORS =
      "ozone.administrators";

  public static final String OZONE_CLIENT_SOCKET_TIMEOUT_MS =
      "ozone.client.socket.timeout.ms";
  public static final int OZONE_CLIENT_SOCKET_TIMEOUT_MS_DEFAULT = 5000;
  public static final String OZONE_CLIENT_CONNECTION_TIMEOUT_MS =
      "ozone.client.connection.timeout.ms";
  public static final int OZONE_CLIENT_CONNECTION_TIMEOUT_MS_DEFAULT = 5000;

  /**
   * Configuration properties for Ozone Block Deleting Service.
   */
  public static final String OZONE_BLOCK_DELETING_SERVICE_INTERVAL_MS =
      "ozone.block.deleting.service.interval.ms";
  public static final int OZONE_BLOCK_DELETING_SERVICE_INTERVAL_MS_DEFAULT
      = 60000;

  public static final String OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER =
      "ozone.block.deleting.limit.per.task";
  public static final int OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER_DEFAULT
      = 1000;

  public static final String OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL
      = "ozone.block.deleting.container.limit.per.interval";
  public static final int
      OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL_DEFAULT = 10;

  public static final String DFS_CONTAINER_RATIS_ENABLED_KEY
      = ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY;
  public static final boolean DFS_CONTAINER_RATIS_ENABLED_DEFAULT
      = ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT;
  public static final String DFS_CONTAINER_RATIS_RPC_TYPE_KEY
      = ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY;
  public static final String DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT
      = ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT;
  /** A unique ID to identify a Ratis server. */
  public static final String DFS_CONTAINER_RATIS_SERVER_ID =
      "dfs.container.ratis.server.id";
  public static final String DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR =
      "dfs.container.ratis.datanode.storage.dir";

  public static final String OZONE_SCM_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL =
      "ozone.web.authentication.kerberos.principal";

  /**
   * There is no need to instantiate this class.
   */
  private OzoneConfigKeys() {
  }
}
