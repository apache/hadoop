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

import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
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

  public static final String DFS_CONTAINER_CHUNK_WRITE_SYNC_KEY =
      "dfs.container.chunk.write.sync";
  public static final boolean DFS_CONTAINER_CHUNK_WRITE_SYNC_DEFAULT = true;
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
  public static final String OZONE_ENABLED =
      "ozone.enabled";
  public static final boolean OZONE_ENABLED_DEFAULT = false;
  public static final String OZONE_TRACE_ENABLED_KEY =
      "ozone.trace.enabled";
  public static final boolean OZONE_TRACE_ENABLED_DEFAULT = false;

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
      "OFF";
  public static final String OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF =
      "OFF";

  public static final String OZONE_UNSAFEBYTEOPERATIONS_ENABLED =
      "ozone.UnsafeByteOperations.enabled";
  public static final boolean OZONE_UNSAFEBYTEOPERATIONS_ENABLED_DEFAULT
      = true;

  public static final String OZONE_CONTAINER_CACHE_SIZE =
      "ozone.container.cache.size";
  public static final int OZONE_CONTAINER_CACHE_DEFAULT = 1024;

  public static final String OZONE_SCM_BLOCK_SIZE =
      "ozone.scm.block.size";
  public static final String OZONE_SCM_BLOCK_SIZE_DEFAULT = "256MB";

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

  public static final String OZONE_CLIENT_STREAM_BUFFER_FLUSH_SIZE =
      "ozone.client.stream.buffer.flush.size";

  public static final String OZONE_CLIENT_STREAM_BUFFER_FLUSH_SIZE_DEFAULT =
      "64MB";

  public static final String OZONE_CLIENT_STREAM_BUFFER_MAX_SIZE =
      "ozone.client.stream.buffer.max.size";

  public static final String OZONE_CLIENT_STREAM_BUFFER_MAX_SIZE_DEFAULT =
      "128MB";

  public static final String OZONE_CLIENT_WATCH_REQUEST_TIMEOUT =
      "ozone.client.watch.request.timeout";

  public static final String OZONE_CLIENT_WATCH_REQUEST_TIMEOUT_DEFAULT =
      "30s";

  public static final String OZONE_CLIENT_MAX_RETRIES =
      "ozone.client.max.retries";
  public static final int OZONE_CLIENT_MAX_RETRIES_DEFAULT = 5;


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

  public static final String OZONE_KEY_PREALLOCATION_BLOCKS_MAX =
      "ozone.key.preallocation.max.blocks";
  public static final int OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT
      = 64;

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
  public static final String DFS_CONTAINER_RATIS_REPLICATION_LEVEL_KEY
      = ScmConfigKeys.DFS_CONTAINER_RATIS_REPLICATION_LEVEL_KEY;
  public static final ReplicationLevel
      DFS_CONTAINER_RATIS_REPLICATION_LEVEL_DEFAULT
      = ScmConfigKeys.DFS_CONTAINER_RATIS_REPLICATION_LEVEL_DEFAULT;
  public static final String DFS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_KEY
      = ScmConfigKeys.DFS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_KEY;
  public static final int DFS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_DEFAULT
      = ScmConfigKeys.DFS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_DEFAULT;
  public static final String DFS_CONTAINER_RATIS_SEGMENT_SIZE_KEY
      = ScmConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_SIZE_KEY;
  public static final String DFS_CONTAINER_RATIS_SEGMENT_SIZE_DEFAULT
      = ScmConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_SIZE_DEFAULT;
  public static final String DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY
      = ScmConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY;
  public static final String
      DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT =
      ScmConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT;

  // config settings to enable stateMachineData write timeout
  public static final String
      DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT =
      ScmConfigKeys.DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT;
  public static final TimeDuration
      DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT_DEFAULT =
      ScmConfigKeys.DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT_DEFAULT;

  public static final String
      DFS_CONTAINER_RATIS_STATEMACHINEDATA_CACHE_EXPIRY_INTERVAL =
      ScmConfigKeys.
          DFS_CONTAINER_RATIS_STATEMACHINEDATA_CACHE_EXPIRY_INTERVAL;
  public static final String
      DFS_CONTAINER_RATIS_STATEMACHINEDATA_CACHE_EXPIRY_INTERVAL_DEFAULT =
      ScmConfigKeys.
          DFS_CONTAINER_RATIS_STATEMACHINEDATA_CACHE_EXPIRY_INTERVAL_DEFAULT;

  public static final String DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR =
      "dfs.container.ratis.datanode.storage.dir";
  public static final String DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_KEY =
      ScmConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_KEY;
  public static final TimeDuration
      DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT =
      ScmConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT;
  public static final String DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY =
      ScmConfigKeys.DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY;
  public static final int DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_DEFAULT =
      ScmConfigKeys.DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_DEFAULT;
  public static final String DFS_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_KEY =
      ScmConfigKeys.DFS_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_KEY;
  public static final TimeDuration
      DFS_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_DEFAULT =
      ScmConfigKeys.DFS_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_DEFAULT;
  public static final String DFS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_KEY =
      ScmConfigKeys.DFS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_KEY;
  public static final TimeDuration
      DFS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_DEFAULT =
      ScmConfigKeys.DFS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_DEFAULT;
  public static final String
      DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_RETRIES =
      ScmConfigKeys.DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_RETRIES;
  public static final int
      DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_RETRIES_DEFAULT =
      ScmConfigKeys.DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_RETRIES_DEFAULT;
  public static final String DFS_CONTAINER_RATIS_LOG_QUEUE_NUM_ELEMENTS =
      ScmConfigKeys.DFS_CONTAINER_RATIS_LOG_QUEUE_NUM_ELEMENTS;
  public static final int DFS_CONTAINER_RATIS_LOG_QUEUE_NUM_ELEMENTS_DEFAULT =
      ScmConfigKeys.DFS_CONTAINER_RATIS_LOG_QUEUE_NUM_ELEMENTS_DEFAULT;
  public static final String DFS_CONTAINER_RATIS_LOG_QUEUE_BYTE_LIMIT =
      ScmConfigKeys.DFS_CONTAINER_RATIS_LOG_QUEUE_BYTE_LIMIT;
  public static final String DFS_CONTAINER_RATIS_LOG_QUEUE_BYTE_LIMIT_DEFAULT =
      ScmConfigKeys.DFS_CONTAINER_RATIS_LOG_QUEUE_BYTE_LIMIT_DEFAULT;
  public static final String
      DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS =
      ScmConfigKeys.DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS;
  public static final int
      DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS_DEFAULT =
      ScmConfigKeys.DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS_DEFAULT;
  public static final String DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT =
      ScmConfigKeys.DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT;
  public static final String
      DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT =
      ScmConfigKeys.DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT;
  public static final String DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_KEY =
      ScmConfigKeys.DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_KEY;
  public static final TimeDuration
      DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_DEFAULT =
      ScmConfigKeys.DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_DEFAULT;
  public static final String
      DFS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY =
      ScmConfigKeys.DFS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY;
  public static final TimeDuration
      DFS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_DEFAULT =
      ScmConfigKeys.DFS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_DEFAULT;
  public static final String DFS_RATIS_SNAPSHOT_THRESHOLD_KEY =
      ScmConfigKeys.DFS_RATIS_SNAPSHOT_THRESHOLD_KEY;
  public static final long DFS_RATIS_SNAPSHOT_THRESHOLD_DEFAULT =
      ScmConfigKeys.DFS_RATIS_SNAPSHOT_THRESHOLD_DEFAULT;

  public static final String DFS_RATIS_SERVER_FAILURE_DURATION_KEY =
      ScmConfigKeys.DFS_RATIS_SERVER_FAILURE_DURATION_KEY;
  public static final TimeDuration
      DFS_RATIS_SERVER_FAILURE_DURATION_DEFAULT =
      ScmConfigKeys.DFS_RATIS_SERVER_FAILURE_DURATION_DEFAULT;

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

  public static final String OZONE_SECURITY_ENABLED_KEY =
      "ozone.security.enabled";
  public static final boolean OZONE_SECURITY_ENABLED_DEFAULT = false;

  public static final String OZONE_CONTAINER_COPY_WORKDIR =
      "hdds.datanode.replication.work.dir";

  /**
   * Config properties to set client side checksum properties.
   */
  public static final String OZONE_CLIENT_CHECKSUM_TYPE =
      "ozone.client.checksum.type";
  public static final String OZONE_CLIENT_CHECKSUM_TYPE_DEFAULT = "SHA256";
  public static final String OZONE_CLIENT_BYTES_PER_CHECKSUM =
      "ozone.client.bytes.per.checksum";
  public static final String OZONE_CLIENT_BYTES_PER_CHECKSUM_DEFAULT = "1MB";
  public static final int OZONE_CLIENT_BYTES_PER_CHECKSUM_DEFAULT_BYTES =
      1024 * 1024;
  public static final int OZONE_CLIENT_BYTES_PER_CHECKSUM_MIN_SIZE = 256 * 1024;
  public static final String OZONE_CLIENT_VERIFY_CHECKSUM =
      "ozone.client.verify.checksum";
  public static final boolean OZONE_CLIENT_VERIFY_CHECKSUM_DEFAULT = true;
  public static final String OZONE_ACL_AUTHORIZER_CLASS =
      "ozone.acl.authorizer.class";
  public static final String OZONE_ACL_AUTHORIZER_CLASS_DEFAULT =
      "org.apache.hadoop.ozone.security.acl.OzoneAccessAuthorizer";
  public static final String OZONE_ACL_ENABLED =
      "ozone.acl.enabled";
  public static final boolean OZONE_ACL_ENABLED_DEFAULT =
      false;
  public static final String OZONE_S3_TOKEN_MAX_LIFETIME_KEY =
      "ozone.s3.token.max.lifetime";
  public static final String OZONE_S3_TOKEN_MAX_LIFETIME_KEY_DEFAULT = "3m";
  //For technical reasons this is unused and hardcoded to the
  // OzoneFileSystem.initialize.
  public static final String OZONE_FS_ISOLATED_CLASSLOADER =
      "ozone.fs.isolated-classloader";

  // Ozone Client Retry and Failover configurations
  public static final String OZONE_CLIENT_RETRY_MAX_ATTEMPTS_KEY =
      "ozone.client.retry.max.attempts";
  public static final int OZONE_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT =
      10;
  public static final String OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY =
      "ozone.client.failover.max.attempts";
  public static final int OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT =
      15;
  public static final String OZONE_CLIENT_FAILOVER_SLEEP_BASE_MILLIS_KEY =
      "ozone.client.failover.sleep.base.millis";
  public static final int OZONE_CLIENT_FAILOVER_SLEEP_BASE_MILLIS_DEFAULT =
      500;
  public static final String OZONE_CLIENT_FAILOVER_SLEEP_MAX_MILLIS_KEY =
      "ozone.client.failover.sleep.max.millis";
  public static final int OZONE_CLIENT_FAILOVER_SLEEP_MAX_MILLIS_DEFAULT =
      15000;

  public static final String OZONE_FREON_HTTP_ENABLED_KEY =
      "ozone.freon.http.enabled";
  public static final String OZONE_FREON_HTTP_BIND_HOST_KEY =
      "ozone.freon.http-bind-host";
  public static final String OZONE_FREON_HTTPS_BIND_HOST_KEY =
      "ozone.freon.https-bind-host";
  public static final String OZONE_FREON_HTTP_ADDRESS_KEY =
      "ozone.freon.http-address";
  public static final String OZONE_FREON_HTTPS_ADDRESS_KEY =
      "ozone.freon.https-address";

  public static final String OZONE_FREON_HTTP_BIND_HOST_DEFAULT = "0.0.0.0";
  public static final int OZONE_FREON_HTTP_BIND_PORT_DEFAULT = 9884;
  public static final int OZONE_FREON_HTTPS_BIND_PORT_DEFAULT = 9885;
  public static final String
      OZONE_FREON_HTTP_KERBEROS_PRINCIPAL_KEY =
      "ozone.freon.http.kerberos.principal";
  public static final String
      OZONE_FREON_HTTP_KERBEROS_KEYTAB_FILE_KEY =
      "ozone.freon.http.kerberos.keytab";

  /**
   * There is no need to instantiate this class.
   */
  private OzoneConfigKeys() {
  }
}
