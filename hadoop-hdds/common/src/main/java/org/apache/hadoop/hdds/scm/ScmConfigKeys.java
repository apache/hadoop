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
package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.TimeUnit;

/**
 * This class contains constants for configuration keys used in SCM.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class ScmConfigKeys {

  // Location of SCM DB files. For now we just support a single
  // metadata dir but in future we may support multiple for redundancy or
  // performance.
  public static final String OZONE_SCM_DB_DIRS = "ozone.scm.db.dirs";

  public static final String SCM_CONTAINER_CLIENT_STALE_THRESHOLD_KEY =
      "scm.container.client.idle.threshold";
  public static final String SCM_CONTAINER_CLIENT_STALE_THRESHOLD_DEFAULT =
      "10s";

  public static final String SCM_CONTAINER_CLIENT_MAX_SIZE_KEY =
      "scm.container.client.max.size";
  public static final int SCM_CONTAINER_CLIENT_MAX_SIZE_DEFAULT =
      256;

  public static final String SCM_CONTAINER_CLIENT_MAX_OUTSTANDING_REQUESTS =
      "scm.container.client.max.outstanding.requests";
  public static final int SCM_CONTAINER_CLIENT_MAX_OUTSTANDING_REQUESTS_DEFAULT
      = 100;

  public static final String DFS_CONTAINER_RATIS_ENABLED_KEY
      = "dfs.container.ratis.enabled";
  public static final boolean DFS_CONTAINER_RATIS_ENABLED_DEFAULT
      = false;
  public static final String DFS_CONTAINER_RATIS_RPC_TYPE_KEY
      = "dfs.container.ratis.rpc.type";
  public static final String DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT
      = "GRPC";
  public static final String DFS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_KEY
      = "dfs.container.ratis.num.write.chunk.threads";
  public static final int DFS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_DEFAULT
      = 60;
  public static final String DFS_CONTAINER_RATIS_REPLICATION_LEVEL_KEY
      = "dfs.container.ratis.replication.level";
  public static final ReplicationLevel
      DFS_CONTAINER_RATIS_REPLICATION_LEVEL_DEFAULT = ReplicationLevel.MAJORITY;
  public static final String DFS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_KEY
      = "dfs.container.ratis.num.container.op.executors";
  public static final int DFS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_DEFAULT
      = 10;
  public static final String DFS_CONTAINER_RATIS_SEGMENT_SIZE_KEY =
      "dfs.container.ratis.segment.size";
  public static final String DFS_CONTAINER_RATIS_SEGMENT_SIZE_DEFAULT =
      "16KB";
  public static final String DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY =
      "dfs.container.ratis.segment.preallocated.size";
  public static final String
      DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT = "16KB";
  public static final String
      DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT =
      "dfs.container.ratis.statemachinedata.sync.timeout";
  public static final TimeDuration
      DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT_DEFAULT =
      TimeDuration.valueOf(10, TimeUnit.SECONDS);
  public static final String
      DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_RETRIES =
      "dfs.container.ratis.statemachinedata.sync.retries";
  public static final int
      DFS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_RETRIES_DEFAULT = -1;
  public static final String DFS_CONTAINER_RATIS_LOG_QUEUE_NUM_ELEMENTS =
      "dfs.container.ratis.log.queue.num-elements";
  public static final int DFS_CONTAINER_RATIS_LOG_QUEUE_NUM_ELEMENTS_DEFAULT =
      1024;
  public static final String DFS_CONTAINER_RATIS_LOG_QUEUE_BYTE_LIMIT =
      "dfs.container.ratis.log.queue.byte-limit";
  public static final String DFS_CONTAINER_RATIS_LOG_QUEUE_BYTE_LIMIT_DEFAULT =
      "4GB";
  public static final String
      DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS =
      "dfs.container.ratis.log.appender.queue.num-elements";
  public static final int
      DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS_DEFAULT = 1;
  public static final String DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT =
      "dfs.container.ratis.log.appender.queue.byte-limit";
  public static final String
      DFS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT = "32MB";
  // expiry interval stateMachineData cache entry inside containerStateMachine
  public static final String
      DFS_CONTAINER_RATIS_STATEMACHINEDATA_CACHE_EXPIRY_INTERVAL =
      "dfs.container.ratis.statemachine.cache.expiry.interval";
  public static final String
      DFS_CONTAINER_RATIS_STATEMACHINEDATA_CACHE_EXPIRY_INTERVAL_DEFAULT =
      "10s";
  public static final String DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_KEY =
      "dfs.ratis.client.request.timeout.duration";
  public static final TimeDuration
      DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT =
      TimeDuration.valueOf(3000, TimeUnit.MILLISECONDS);
  public static final String DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY =
      "dfs.ratis.client.request.max.retries";
  public static final int DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_DEFAULT = 20;
  public static final String DFS_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_KEY =
      "dfs.ratis.client.request.retry.interval";
  public static final TimeDuration
      DFS_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_DEFAULT =
      TimeDuration.valueOf(500, TimeUnit.MILLISECONDS);
  public static final String DFS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_KEY =
      "dfs.ratis.server.retry-cache.timeout.duration";
  public static final TimeDuration
      DFS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_DEFAULT =
      TimeDuration.valueOf(600000, TimeUnit.MILLISECONDS);
  public static final String DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_KEY =
      "dfs.ratis.server.request.timeout.duration";
  public static final TimeDuration
      DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_DEFAULT =
      TimeDuration.valueOf(3000, TimeUnit.MILLISECONDS);
  public static final String
      DFS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY =
      "dfs.ratis.leader.election.minimum.timeout.duration";
  public static final TimeDuration
      DFS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_DEFAULT =
      TimeDuration.valueOf(1, TimeUnit.SECONDS);

  public static final String DFS_RATIS_SNAPSHOT_THRESHOLD_KEY =
      "dfs.ratis.snapshot.threshold";
  public static final long DFS_RATIS_SNAPSHOT_THRESHOLD_DEFAULT = 10000;

  public static final String DFS_RATIS_SERVER_FAILURE_DURATION_KEY =
      "dfs.ratis.server.failure.duration";
  public static final TimeDuration
      DFS_RATIS_SERVER_FAILURE_DURATION_DEFAULT =
      TimeDuration.valueOf(120, TimeUnit.SECONDS);

  // TODO : this is copied from OzoneConsts, may need to move to a better place
  public static final String OZONE_SCM_CHUNK_SIZE_KEY = "ozone.scm.chunk.size";
  // 16 MB by default
  public static final String OZONE_SCM_CHUNK_SIZE_DEFAULT = "16MB";

  public static final String OZONE_SCM_CLIENT_PORT_KEY =
      "ozone.scm.client.port";
  public static final int OZONE_SCM_CLIENT_PORT_DEFAULT = 9860;

  public static final String OZONE_SCM_DATANODE_PORT_KEY =
      "ozone.scm.datanode.port";
  public static final int OZONE_SCM_DATANODE_PORT_DEFAULT = 9861;

  // OZONE_OM_PORT_DEFAULT = 9862
  public static final String OZONE_SCM_BLOCK_CLIENT_PORT_KEY =
      "ozone.scm.block.client.port";
  public static final int OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT = 9863;

  public static final String OZONE_SCM_SECURITY_SERVICE_PORT_KEY =
      "ozone.scm.security.service.port";
  public static final int OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT = 9961;

  // Container service client
  public static final String OZONE_SCM_CLIENT_ADDRESS_KEY =
      "ozone.scm.client.address";
  public static final String OZONE_SCM_CLIENT_BIND_HOST_KEY =
      "ozone.scm.client.bind.host";
  public static final String OZONE_SCM_CLIENT_BIND_HOST_DEFAULT =
      "0.0.0.0";

  // Block service client
  public static final String OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY =
      "ozone.scm.block.client.address";
  public static final String OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY =
      "ozone.scm.block.client.bind.host";
  public static final String OZONE_SCM_BLOCK_CLIENT_BIND_HOST_DEFAULT =
      "0.0.0.0";

  // SCM Security service address.
  public static final String OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY =
      "ozone.scm.security.service.address";
  public static final String OZONE_SCM_SECURITY_SERVICE_BIND_HOST_KEY =
      "ozone.scm.security.service.bind.host";
  public static final String OZONE_SCM_SECURITY_SERVICE_BIND_HOST_DEFAULT =
      "0.0.0.0";

  public static final String OZONE_SCM_DATANODE_ADDRESS_KEY =
      "ozone.scm.datanode.address";
  public static final String OZONE_SCM_DATANODE_BIND_HOST_KEY =
      "ozone.scm.datanode.bind.host";
  public static final String OZONE_SCM_DATANODE_BIND_HOST_DEFAULT =
      "0.0.0.0";

  public static final String OZONE_SCM_HTTP_ENABLED_KEY =
      "ozone.scm.http.enabled";
  public static final String OZONE_SCM_HTTP_BIND_HOST_KEY =
      "ozone.scm.http-bind-host";
  public static final String OZONE_SCM_HTTPS_BIND_HOST_KEY =
      "ozone.scm.https-bind-host";
  public static final String OZONE_SCM_HTTP_ADDRESS_KEY =
      "ozone.scm.http-address";
  public static final String OZONE_SCM_HTTPS_ADDRESS_KEY =
      "ozone.scm.https-address";
  public static final String HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY =
      "hdds.scm.kerberos.keytab.file";
  public static final String HDDS_SCM_KERBEROS_PRINCIPAL_KEY =
      "hdds.scm.kerberos.principal";
  public static final String OZONE_SCM_HTTP_BIND_HOST_DEFAULT = "0.0.0.0";
  public static final int OZONE_SCM_HTTP_BIND_PORT_DEFAULT = 9876;
  public static final int OZONE_SCM_HTTPS_BIND_PORT_DEFAULT = 9877;

  public static final String HDDS_REST_HTTP_ADDRESS_KEY =
      "hdds.rest.http-address";
  public static final String HDDS_REST_HTTP_ADDRESS_DEFAULT = "0.0.0.0:9880";
  public static final String HDDS_DATANODE_DIR_KEY = "hdds.datanode.dir";
  public static final String HDDS_REST_CSRF_ENABLED_KEY =
      "hdds.rest.rest-csrf.enabled";
  public static final boolean HDDS_REST_CSRF_ENABLED_DEFAULT = false;
  public static final String HDDS_REST_NETTY_HIGH_WATERMARK =
      "hdds.rest.netty.high.watermark";
  public static final int HDDS_REST_NETTY_HIGH_WATERMARK_DEFAULT = 65536;
  public static final int HDDS_REST_NETTY_LOW_WATERMARK_DEFAULT = 32768;
  public static final String HDDS_REST_NETTY_LOW_WATERMARK =
      "hdds.rest.netty.low.watermark";

  public static final String OZONE_SCM_HANDLER_COUNT_KEY =
      "ozone.scm.handler.count.key";
  public static final int OZONE_SCM_HANDLER_COUNT_DEFAULT = 10;

  public static final String OZONE_SCM_SECURITY_HANDLER_COUNT_KEY =
      "ozone.scm.security.handler.count.key";
  public static final int OZONE_SCM_SECURITY_HANDLER_COUNT_DEFAULT = 2;

  public static final String OZONE_SCM_DEADNODE_INTERVAL =
      "ozone.scm.dead.node.interval";
  public static final String OZONE_SCM_DEADNODE_INTERVAL_DEFAULT =
      "10m";

  public static final String OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL =
      "ozone.scm.heartbeat.thread.interval";
  public static final String OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_DEFAULT =
      "3s";

  public static final String OZONE_SCM_STALENODE_INTERVAL =
      "ozone.scm.stale.node.interval";
  public static final String OZONE_SCM_STALENODE_INTERVAL_DEFAULT =
      "90s";

  public static final String OZONE_SCM_HEARTBEAT_RPC_TIMEOUT =
      "ozone.scm.heartbeat.rpc-timeout";
  public static final String OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT =
      "1s";

  /**
   * Defines how frequently we will log the missing of heartbeat to a specific
   * SCM. In the default case we will write a warning message for each 10
   * sequential heart beats that we miss to a specific SCM. This is to avoid
   * overrunning the log with lots of HB missed Log statements.
   */
  public static final String OZONE_SCM_HEARTBEAT_LOG_WARN_INTERVAL_COUNT =
      "ozone.scm.heartbeat.log.warn.interval.count";
  public static final int OZONE_SCM_HEARTBEAT_LOG_WARN_DEFAULT =
      10;

  // ozone.scm.names key is a set of DNS | DNS:PORT | IP Address | IP:PORT.
  // Written as a comma separated string. e.g. scm1, scm2:8020, 7.7.7.7:7777
  //
  // If this key is not specified datanodes will not be able to find
  // SCM. The SCM membership can be dynamic, so this key should contain
  // all possible SCM names. Once the SCM leader is discovered datanodes will
  // get the right list of SCMs to heartbeat to from the leader.
  // While it is good for the datanodes to know the names of all SCM nodes,
  // it is sufficient to actually know the name of on working SCM. That SCM
  // will be able to return the information about other SCMs that are part of
  // the SCM replicated Log.
  //
  //In case of a membership change, any one of the SCM machines will be
  // able to send back a new list to the datanodes.
  public static final String OZONE_SCM_NAMES = "ozone.scm.names";

  public static final int OZONE_SCM_DEFAULT_PORT =
      OZONE_SCM_DATANODE_PORT_DEFAULT;
  // File Name and path where datanode ID is to written to.
  // if this value is not set then container startup will fail.
  public static final String OZONE_SCM_DATANODE_ID = "ozone.scm.datanode.id";

  public static final String OZONE_SCM_DATANODE_ID_PATH_DEFAULT = "datanode.id";

  public static final String OZONE_SCM_DB_CACHE_SIZE_MB =
      "ozone.scm.db.cache.size.mb";
  public static final int OZONE_SCM_DB_CACHE_SIZE_DEFAULT = 128;

  public static final String OZONE_SCM_CONTAINER_SIZE =
      "ozone.scm.container.size";
  public static final String OZONE_SCM_CONTAINER_SIZE_DEFAULT = "5GB";

  public static final String OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY =
      "ozone.scm.container.placement.impl";

  public static final String OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT =
      "ozone.scm.pipeline.owner.container.count";
  public static final int OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT_DEFAULT = 3;

  public static final String
      OZONE_SCM_KEY_VALUE_CONTAINER_DELETION_CHOOSING_POLICY =
      "ozone.scm.keyvalue.container.deletion-choosing.policy";

  public static final String OZONE_SCM_CONTAINER_CREATION_LEASE_TIMEOUT =
      "ozone.scm.container.creation.lease.timeout";

  public static final String
      OZONE_SCM_CONTAINER_CREATION_LEASE_TIMEOUT_DEFAULT = "60s";

  public static final String OZONE_SCM_PIPELINE_DESTROY_TIMEOUT =
      "ozone.scm.pipeline.destroy.timeout";

  public static final String OZONE_SCM_PIPELINE_DESTROY_TIMEOUT_DEFAULT =
      "300s";

  public static final String OZONE_SCM_PIPELINE_CREATION_INTERVAL =
      "ozone.scm.pipeline.creation.interval";
  public static final String OZONE_SCM_PIPELINE_CREATION_INTERVAL_DEFAULT =
      "120s";

  public static final String OZONE_SCM_BLOCK_DELETION_MAX_RETRY =
      "ozone.scm.block.deletion.max.retry";
  public static final int OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT = 4096;

  public static final String HDDS_SCM_WATCHER_TIMEOUT =
      "hdds.scm.watcher.timeout";

  public static final String HDDS_SCM_WATCHER_TIMEOUT_DEFAULT =
      "10m";

  public static final String HDDS_SCM_REPLICATION_THREAD_INTERVAL =
      "hdds.scm.replication.thread.interval";

  public static final String HDDS_SCM_REPLICATION_THREAD_INTERVAL_DEFAULT =
      "5m";

  public static final String HDDS_SCM_REPLICATION_EVENT_TIMEOUT =
      "hdds.scm.replication.event.timeout";

  public static final String HDDS_SCM_REPLICATION_EVENT_TIMEOUT_DEFAULT =
      "10m";

  public static final String
      HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY =
      "hdds.scm.http.kerberos.principal";
  public static final String
      HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY =
      "hdds.scm.http.kerberos.keytab";

  // Network topology
  public static final String OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_TYPE =
          "ozone.scm.network.topology.schema.file.type";
  public static final String OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE =
      "ozone.scm.network.topology.schema.file";
  public static final String OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_DEFAULT =
      "network-topology-default.xml";

  public static final String HDDS_TRACING_ENABLED = "hdds.tracing.enabled";
  public static final boolean HDDS_TRACING_ENABLED_DEFAULT = true;

  /**
   * Never constructed.
   */
  private ScmConfigKeys() {

  }
}
