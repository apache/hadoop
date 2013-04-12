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

package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.apache.hadoop.security.authorize.Service;

/** 
 * This class contains constants for configuration keys used
 * in the common code.
 *
 * It inherits all the publicly documented configuration keys
 * and adds unsupported keys.
 *
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CommonConfigurationKeys extends CommonConfigurationKeysPublic {

  /** Default location for user home directories */
  public static final String  FS_HOME_DIR_KEY = "fs.homeDir";
  /** Default value for FS_HOME_DIR_KEY */
  public static final String  FS_HOME_DIR_DEFAULT = "/user";
  /** Default umask for files created in HDFS */
  public static final String  FS_PERMISSIONS_UMASK_KEY =
    "fs.permissions.umask-mode";
  /** Default value for FS_PERMISSIONS_UMASK_KEY */
  public static final int     FS_PERMISSIONS_UMASK_DEFAULT = 0022;
  /** How often does RPC client send pings to RPC server */
  public static final String  IPC_PING_INTERVAL_KEY = "ipc.ping.interval";
  /** Default value for IPC_PING_INTERVAL_KEY */
  public static final int     IPC_PING_INTERVAL_DEFAULT = 60000; // 1 min
  /** Enables pings from RPC client to the server */
  public static final String  IPC_CLIENT_PING_KEY = "ipc.client.ping";
  /** Default value of IPC_CLIENT_PING_KEY */
  public static final boolean IPC_CLIENT_PING_DEFAULT = true;
  /** Responses larger than this will be logged */
  public static final String  IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY =
    "ipc.server.max.response.size";
  /** Default value for IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY */
  public static final int     IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT =
    1024*1024;
  /** Number of threads in RPC server reading from the socket */
  public static final String  IPC_SERVER_RPC_READ_THREADS_KEY =
    "ipc.server.read.threadpool.size";
  /** Default value for IPC_SERVER_RPC_READ_THREADS_KEY */
  public static final int     IPC_SERVER_RPC_READ_THREADS_DEFAULT = 1;

  /** How many calls per handler are allowed in the queue. */
  public static final String  IPC_SERVER_HANDLER_QUEUE_SIZE_KEY =
    "ipc.server.handler.queue.size";
  /** Default value for IPC_SERVER_HANDLER_QUEUE_SIZE_KEY */
  public static final int     IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT = 100;

  /** Internal buffer size for Lzo compressor/decompressors */
  public static final String  IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY =
    "io.compression.codec.lzo.buffersize";
  /** Default value for IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY */
  public static final int     IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_DEFAULT =
    64*1024;
  /** This is for specifying the implementation for the mappings from
   * hostnames to the racks they belong to
   */
  public static final String  NET_TOPOLOGY_CONFIGURED_NODE_MAPPING_KEY =
    "net.topology.configured.node.mapping";

  /** Internal buffer size for Snappy compressor/decompressors */
  public static final String IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY =
      "io.compression.codec.snappy.buffersize";

  /** Default value for IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY */
  public static final int IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT =
      256 * 1024;

  /** Internal buffer size for Snappy compressor/decompressors */
  public static final String IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY =
      "io.compression.codec.lz4.buffersize";

  /** Default value for IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY */
  public static final int IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT =
      256 * 1024;

  /**
   * Service Authorization
   */
  public static final String 
  HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_POLICY = 
      "security.refresh.policy.protocol.acl";
  public static final String 
  HADOOP_SECURITY_SERVICE_AUTHORIZATION_GET_USER_MAPPINGS =
      "security.get.user.mappings.protocol.acl";
  public static final String 
  HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_USER_MAPPINGS =
      "security.refresh.user.mappings.protocol.acl";
  public static final String 
  SECURITY_HA_SERVICE_PROTOCOL_ACL = "security.ha.service.protocol.acl";
  public static final String 
  SECURITY_ZKFC_PROTOCOL_ACL = "security.zkfc.protocol.acl";
  public static final String
  SECURITY_CLIENT_PROTOCOL_ACL = "security.client.protocol.acl";
  public static final String SECURITY_CLIENT_DATANODE_PROTOCOL_ACL =
      "security.client.datanode.protocol.acl";
  public static final String
  SECURITY_DATANODE_PROTOCOL_ACL = "security.datanode.protocol.acl";
  public static final String
  SECURITY_INTER_DATANODE_PROTOCOL_ACL = "security.inter.datanode.protocol.acl";
  public static final String
  SECURITY_NAMENODE_PROTOCOL_ACL = "security.namenode.protocol.acl";
  public static final String SECURITY_QJOURNAL_SERVICE_PROTOCOL_ACL =
      "security.qjournal.service.protocol.acl";
  public static final String HADOOP_SECURITY_TOKEN_SERVICE_USE_IP =
      "hadoop.security.token.service.use_ip";
  public static final boolean HADOOP_SECURITY_TOKEN_SERVICE_USE_IP_DEFAULT =
      true;
  
  /**
   * HA health monitor and failover controller.
   */
 
  /** How often to retry connecting to the service. */
  public static final String HA_HM_CONNECT_RETRY_INTERVAL_KEY =
    "ha.health-monitor.connect-retry-interval.ms";
  public static final long HA_HM_CONNECT_RETRY_INTERVAL_DEFAULT = 1000;
 
  /* How often to check the service. */
  public static final String HA_HM_CHECK_INTERVAL_KEY =
    "ha.health-monitor.check-interval.ms";  
  public static final long HA_HM_CHECK_INTERVAL_DEFAULT = 1000;
 
  /* How long to sleep after an unexpected RPC error. */
  public static final String HA_HM_SLEEP_AFTER_DISCONNECT_KEY =
    "ha.health-monitor.sleep-after-disconnect.ms";
  public static final long HA_HM_SLEEP_AFTER_DISCONNECT_DEFAULT = 1000;
 
  /* Timeout for the actual monitorHealth() calls. */
  public static final String HA_HM_RPC_TIMEOUT_KEY =
    "ha.health-monitor.rpc-timeout.ms";
  public static final int HA_HM_RPC_TIMEOUT_DEFAULT = 45000;
  
  /* Timeout that the FC waits for the new active to become active */
  public static final String HA_FC_NEW_ACTIVE_TIMEOUT_KEY =
    "ha.failover-controller.new-active.rpc-timeout.ms";
  public static final int HA_FC_NEW_ACTIVE_TIMEOUT_DEFAULT = 60000;
  
  /* Timeout that the FC waits for the old active to go to standby */
  public static final String HA_FC_GRACEFUL_FENCE_TIMEOUT_KEY =
    "ha.failover-controller.graceful-fence.rpc-timeout.ms";
  public static final int HA_FC_GRACEFUL_FENCE_TIMEOUT_DEFAULT = 5000;
  
  /* FC connection retries for graceful fencing */
  public static final String HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES =
      "ha.failover-controller.graceful-fence.connection.retries";
  public static final int HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES_DEFAULT = 1;

  /* Timeout that the CLI (manual) FC waits for monitorHealth, getServiceState */
  public static final String HA_FC_CLI_CHECK_TIMEOUT_KEY =
    "ha.failover-controller.cli-check.rpc-timeout.ms";
  public static final int HA_FC_CLI_CHECK_TIMEOUT_DEFAULT = 20000;

  /** Static user web-filter properties.
   * See {@link StaticUserWebFilter}.
   */
  public static final String HADOOP_HTTP_STATIC_USER =
    "hadoop.http.staticuser.user";
  public static final String DEFAULT_HADOOP_HTTP_STATIC_USER =
    "dr.who";

  /** Enable/Disable aliases serving from jetty */
  public static final String HADOOP_JETTY_LOGS_SERVE_ALIASES =
    "hadoop.jetty.logs.serve.aliases";
  public static final boolean DEFAULT_HADOOP_JETTY_LOGS_SERVE_ALIASES =
    true;

  /* Path to the Kerberos ticket cache.  Setting this will force
   * UserGroupInformation to use only this ticket cache file when creating a
   * FileSystem instance.
   */
  public static final String KERBEROS_TICKET_CACHE_PATH =
      "hadoop.security.kerberos.ticket.cache.path";

  public static final String HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY =
    "hadoop.security.uid.cache.secs";

  public static final long HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT =
    4*60*60; // 4 hours

}
