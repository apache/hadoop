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
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;

import org.apache.ratis.util.TimeDuration;

/**
 * This class contains constants for configuration keys used in Ozone.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class OzoneConfigKeys {
  public static final String OZONE_TAGS_SYSTEM_KEY =
      "ozone.tags.system";
  public static final String DFS_CONTAINER_IPC_PORT =
      "dfs.container.ipc";
  public static final int DFS_CONTAINER_IPC_PORT_DEFAULT = 9859;

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

  /**
   * Ratis Port where containers listen to.
   */
  public static final String DFS_CONTAINER_RATIS_IPC_PORT =
      "dfs.container.ratis.ipc";
  public static final int DFS_CONTAINER_RATIS_IPC_PORT_DEFAULT = 9858;

  /**
   * When set to true, allocate a random free port for ozone container, so that
   * a mini cluster is able to launch multiple containers on a node.
   */
  public static final String DFS_CONTAINER_RATIS_IPC_RANDOM_PORT =
      "dfs.container.ratis.ipc.random.port";
  public static final boolean DFS_CONTAINER_RATIS_IPC_RANDOM_PORT_DEFAULT =
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

  public static final String OZONE_METADATA_DIRS =
      "ozone.metadata.dirs";

  public static final String OZONE_METADATA_STORE_IMPL =
      "ozone.metastore.impl";
  public static final String OZONE_METADATA_STORE_IMPL_LEVELDB =
      "LevelDB";
  public static final String OZONE_METADATA_STORE_IMPL_ROCKSDB =
      "RocksDB";
  public static final String OZONE_METADATA_STORE_IMPL_DEFAULT =
      OZONE_METADATA_STORE_IMPL_ROCKSDB;

  public static final String OZONE_METADATA_STORE_ROCKSDB_STATISTICS =
      "ozone.metastore.rocksdb.statistics";

  public static final String  OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT =
      "ALL";
  public static final String OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF =
      "OFF";

  public static final String OZONE_CONTAINER_CACHE_SIZE =
      "ozone.container.cache.size";
  public static final int OZONE_CONTAINER_CACHE_DEFAULT = 1024;

  public static final String OZONE_SCM_BLOCK_SIZE_IN_MB =
      "ozone.scm.block.size.in.mb";
  public static final long OZONE_SCM_BLOCK_SIZE_DEFAULT = 256;

  /**
   * Ozone administrator users delimited by comma.
   * If not set, only the user who launches an ozone service will be the
   * admin user. This property must be set if ozone services are started by
   * different users. Otherwise the RPC layer will reject calls from
   * other servers which are started by users not in the list.
   * */
  public static final String OZONE_ADMINISTRATORS =
      "ozone.administrators";

  public static final String OZONE_CLIENT_PROTOCOL =
      "ozone.client.protocol";

  // This defines the overall connection limit for the connection pool used in
  // RestClient.
  public static final String OZONE_REST_CLIENT_HTTP_CONNECTION_MAX =
      "ozone.rest.client.http.connection.max";
  public static final int OZONE_REST_CLIENT_HTTP_CONNECTION_DEFAULT = 100;

  // This defines the connection limit per one HTTP route/host.
  public static final String OZONE_REST_CLIENT_HTTP_CONNECTION_PER_ROUTE_MAX =
      "ozone.rest.client.http.connection.per-route.max";

  public static final int
      OZONE_REST_CLIENT_HTTP_CONNECTION_PER_ROUTE_MAX_DEFAULT = 20;

  public static final String OZONE_CLIENT_SOCKET_TIMEOUT =
      "ozone.client.socket.timeout";
  public static final int OZONE_CLIENT_SOCKET_TIMEOUT_DEFAULT = 5000;
  public static final String OZONE_CLIENT_CONNECTION_TIMEOUT =
      "ozone.client.connection.timeout";
  public static final int OZONE_CLIENT_CONNECTION_TIMEOUT_DEFAULT = 5000;

  public static final String OZONE_REPLICATION = "ozone.replication";
  public static final int OZONE_REPLICATION_DEFAULT =
      ReplicationFactor.THREE.getValue();

  public static final String OZONE_REPLICATION_TYPE = "ozone.replication.type";
  public static final String OZONE_REPLICATION_TYPE_DEFAULT =
      ReplicationType.RATIS.toString();

  /**
   * Configuration property to configure the cache size of client list calls.
   */
  public static final String OZONE_CLIENT_LIST_CACHE_SIZE =
      "ozone.client.list.cache";
  public static final int OZONE_CLIENT_LIST_CACHE_SIZE_DEFAULT = 1000;

  /**
   * Configuration properties for Ozone Block Deleting Service.
   */
  public static final String OZONE_BLOCK_DELETING_SERVICE_INTERVAL =
      "ozone.block.deleting.service.interval";
  public static final String OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT
      = "60s";

  /**
   * The interval of open key clean service.
   */
  public static final String OZONE_OPEN_KEY_CLEANUP_SERVICE_INTERVAL_SECONDS =
      "ozone.open.key.cleanup.service.interval.seconds";
  public static final int
      OZONE_OPEN_KEY_CLEANUP_SERVICE_INTERVAL_SECONDS_DEFAULT
      = 24 * 3600; // a total of 24 hour

  /**
   * An open key gets cleaned up when it is being in open state for too long.
   */
  public static final String OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS =
      "ozone.open.key.expire.threshold";
  public static final int OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS_DEFAULT =
      24 * 3600;

  public static final String OZONE_BLOCK_DELETING_SERVICE_TIMEOUT =
      "ozone.block.deleting.service.timeout";
  public static final String OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT
      = "300s"; // 300s for default

  public static final String OZONE_KEY_PREALLOCATION_MAXSIZE =
      "ozone.key.preallocation.maxsize";
  public static final long OZONE_KEY_PREALLOCATION_MAXSIZE_DEFAULT
      = 128 * OzoneConsts.MB;

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
  public static final String DFS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_KEY
      = ScmConfigKeys.DFS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_KEY;
  public static final int DFS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_DEFAULT
      = ScmConfigKeys.DFS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_DEFAULT;
  public static final String DFS_CONTAINER_RATIS_SEGMENT_SIZE_KEY
      = ScmConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_SIZE_KEY;
  public static final int DFS_CONTAINER_RATIS_SEGMENT_SIZE_DEFAULT
      = ScmConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_SIZE_DEFAULT;
  public static final String DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY
      = ScmConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY;
  public static final int DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT
      = ScmConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT;
  public static final int DFS_CONTAINER_CHUNK_MAX_SIZE
      = ScmConfigKeys.OZONE_SCM_CHUNK_MAX_SIZE;
  public static final String DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR =
      "dfs.container.ratis.datanode.storage.dir";
  public static final String DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_KEY =
      ScmConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_KEY;
  public static final TimeDuration
      DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT =
      ScmConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT;
  public static final String DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_KEY =
      ScmConfigKeys.DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_KEY;
  public static final TimeDuration
      DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_DEFAULT =
      ScmConfigKeys.DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_DEFAULT;

  public static final String OZONE_SCM_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL =
      "ozone.web.authentication.kerberos.principal";

  public static final String HDDS_DATANODE_PLUGINS_KEY =
      "hdds.datanode.plugins";

  public static final String
      HDDS_DATANODE_STORAGE_UTILIZATION_WARNING_THRESHOLD =
      "hdds.datanode.storage.utilization.warning.threshold";
  public static final double
      HDDS_DATANODE_STORAGE_UTILIZATION_WARNING_THRESHOLD_DEFAULT = 0.95;
  public static final String
      HDDS_DATANODE_STORAGE_UTILIZATION_CRITICAL_THRESHOLD =
      "hdds.datanode.storage.utilization.critical.threshold";
  public static final double
      HDDS_DATANODE_STORAGE_UTILIZATION_CRITICAL_THRESHOLD_DEFAULT = 0.75;

  public static final String
      HDDS_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY =
      "hdds.write.lock.reporting.threshold.ms";
  public static final long
      HDDS_WRITE_LOCK_REPORTING_THRESHOLD_MS_DEFAULT = 5000L;
  public static final String
      HDDS_LOCK_SUPPRESS_WARNING_INTERVAL_MS_KEY =
      "hdds.lock.suppress.warning.interval.ms";
  public static final long
      HDDS_LOCK_SUPPRESS_WARNING_INTERVAL_MS_DEAFULT = 10000L;

  /**
   * There is no need to instantiate this class.
   */
  private OzoneConfigKeys() {
  }
}
