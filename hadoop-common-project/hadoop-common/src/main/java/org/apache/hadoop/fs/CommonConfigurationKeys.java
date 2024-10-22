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
import org.apache.hadoop.net.DomainNameResolver;
import org.apache.hadoop.net.DNSDomainNameResolver;

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
  /** Timeout value for RPC client on waiting for response. */
  public static final String IPC_CLIENT_RPC_TIMEOUT_KEY =
      "ipc.client.rpc-timeout.ms";
  /** Default value for IPC_CLIENT_RPC_TIMEOUT_KEY. */
  public static final int IPC_CLIENT_RPC_TIMEOUT_DEFAULT = 120000;
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
  
  /** Number of pending connections that may be queued per socket reader */
  public static final String IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY =
      "ipc.server.read.connection-queue.size";
  /** Default value for IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE */
  public static final int IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_DEFAULT =
      100;

  /** Max request size a server will accept. */
  public static final String IPC_MAXIMUM_DATA_LENGTH =
      "ipc.maximum.data.length";
  /** Default value for IPC_MAXIMUM_DATA_LENGTH. */
  public static final int IPC_MAXIMUM_DATA_LENGTH_DEFAULT = 128 * 1024 * 1024;
  /** Maximum number of stacked calls for one connection. **/
  public static final String IPC_CONNECTION_MAXIMUM_STACKED_CALL =
      "ipc.connection.maximum.stacked.call";
  public static final long IPC_CONNECTION_MAXIMUM_STACKED_CALL_DEFAULT = 0;

  /** Max response size a client will accept. */
  public static final String IPC_MAXIMUM_RESPONSE_LENGTH =
      "ipc.maximum.response.length";
  /** Default value for IPC_MAXIMUM_RESPONSE_LENGTH. */
  public static final int IPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT =
      128 * 1024 * 1024;

  /** How many calls per handler are allowed in the queue. */
  public static final String  IPC_SERVER_HANDLER_QUEUE_SIZE_KEY =
    "ipc.server.handler.queue.size";
  /** Default value for IPC_SERVER_HANDLER_QUEUE_SIZE_KEY */
  public static final int     IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT = 100;

  /**
   * CallQueue related settings. These are not used directly, but rather
   * combined with a namespace and port. For instance:
   * IPC_NAMESPACE + ".8020." + IPC_CALLQUEUE_IMPL_KEY
   */
  public static final String IPC_NAMESPACE = "ipc";
  public static final String IPC_CALLQUEUE_IMPL_KEY = "callqueue.impl";
  public static final String IPC_SCHEDULER_IMPL_KEY = "scheduler.impl";
  public static final String IPC_IDENTITY_PROVIDER_KEY = "identity-provider.impl";
  public static final String IPC_COST_PROVIDER_KEY = "cost-provider.impl";
  public static final String IPC_BACKOFF_ENABLE = "backoff.enable";
  public static final boolean IPC_BACKOFF_ENABLE_DEFAULT = false;
  // Callqueue overflow trigger failover for stateless servers.
  public static final String IPC_CALLQUEUE_SERVER_FAILOVER_ENABLE =
      "callqueue.overflow.trigger.failover";
  public static final boolean IPC_CALLQUEUE_SERVER_FAILOVER_ENABLE_DEFAULT =
      false;
  /** Callqueue subqueue capacity weights. */
  public static final String IPC_CALLQUEUE_CAPACITY_WEIGHTS_KEY =
      "callqueue.capacity.weights";

  /**
   * IPC scheduler priority levels.
   */
  public static final String IPC_SCHEDULER_PRIORITY_LEVELS_KEY =
      "scheduler.priority.levels";
  public static final int IPC_SCHEDULER_PRIORITY_LEVELS_DEFAULT_KEY = 4;

  /** This is for specifying the implementation for the mappings from
   * hostnames to the racks they belong to
   */
  public static final String  NET_TOPOLOGY_CONFIGURED_NODE_MAPPING_KEY =
      "net.topology.configured.node.mapping";

  /**
   * Supported compression codec classes
   */
  public static final String IO_COMPRESSION_CODECS_KEY = "io.compression.codecs";

  /** Internal buffer size for Lzo compressor/decompressors */
  public static final String  IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY =
    "io.compression.codec.lzo.buffersize";

  /** Default value for IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY */
  public static final int     IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_DEFAULT =
    64*1024;

  /** Internal buffer size for Snappy compressor/decompressors */
  public static final String IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY =
      "io.compression.codec.snappy.buffersize";

  /** Default value for IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY */
  public static final int IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT =
      256 * 1024;

  /** ZStandard compression level. */
  public static final String IO_COMPRESSION_CODEC_ZSTD_LEVEL_KEY =
      "io.compression.codec.zstd.level";

  /** Default value for IO_COMPRESSION_CODEC_ZSTD_LEVEL_KEY. */
  public static final int IO_COMPRESSION_CODEC_ZSTD_LEVEL_DEFAULT = 3;

  /** ZStandard buffer size. */
  public static final String IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_KEY =
      "io.compression.codec.zstd.buffersize";

  /** ZStandard buffer size a value of 0 means use the recommended zstd
   * buffer size that the library recommends. */
  public static final int
      IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_DEFAULT = 0;

  /** Internal buffer size for Lz4 compressor/decompressors */
  public static final String IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY =
      "io.compression.codec.lz4.buffersize";

  /** Default value for IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY */
  public static final int IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT =
      256 * 1024;

  /** Use lz4hc(slow but with high compression ratio) for lz4 compression */
  public static final String IO_COMPRESSION_CODEC_LZ4_USELZ4HC_KEY =
      "io.compression.codec.lz4.use.lz4hc";

  /** Default value for IO_COMPRESSION_CODEC_USELZ4HC_KEY */
  public static final boolean IO_COMPRESSION_CODEC_LZ4_USELZ4HC_DEFAULT =
      false;



  /**
   * Service Authorization
   */
  public static final String 
  HADOOP_SECURITY_SERVICE_AUTHORIZATION_DEFAULT_ACL = 
      "security.service.authorization.default.acl";
  public static final String 
  HADOOP_SECURITY_SERVICE_AUTHORIZATION_DEFAULT_BLOCKED_ACL =
      "security.service.authorization.default.acl.blocked";
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
  HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_CALLQUEUE =
      "security.refresh.callqueue.protocol.acl";
  public static final String
  HADOOP_SECURITY_SERVICE_AUTHORIZATION_GENERIC_REFRESH =
      "security.refresh.generic.protocol.acl";
  public static final String
  HADOOP_SECURITY_SERVICE_AUTHORIZATION_TRACING =
      "security.trace.protocol.acl";
  public static final String
      HADOOP_SECURITY_SERVICE_AUTHORIZATION_DATANODE_LIFELINE =
          "security.datanode.lifeline.protocol.acl";
  public static final String
      HADOOP_SECURITY_SERVICE_AUTHORIZATION_RECONFIGURATION =
      "security.reconfiguration.protocol.acl";
  public static final String 
  SECURITY_HA_SERVICE_PROTOCOL_ACL = "security.ha.service.protocol.acl";
  public static final String 
  SECURITY_ZKFC_PROTOCOL_ACL = "security.zkfc.protocol.acl";
  public static final String
  SECURITY_CLIENT_PROTOCOL_ACL = "security.client.protocol.acl";
  public static final String SECURITY_CLIENT_DATANODE_PROTOCOL_ACL =
      "security.client.datanode.protocol.acl";
  public static final String SECURITY_ROUTER_ADMIN_PROTOCOL_ACL =
      "security.router.admin.protocol.acl";
  public static final String
  SECURITY_DATANODE_PROTOCOL_ACL = "security.datanode.protocol.acl";
  public static final String
  SECURITY_INTER_DATANODE_PROTOCOL_ACL = "security.inter.datanode.protocol.acl";
  public static final String
  SECURITY_NAMENODE_PROTOCOL_ACL = "security.namenode.protocol.acl";
  public static final String SECURITY_QJOURNAL_SERVICE_PROTOCOL_ACL =
      "security.qjournal.service.protocol.acl";
  public static final String SECURITY_INTERQJOURNAL_SERVICE_PROTOCOL_ACL =
      "security.interqjournal.service.protocol.acl";
  public static final String HADOOP_SECURITY_TOKEN_SERVICE_USE_IP =
      "hadoop.security.token.service.use_ip";
  public static final boolean HADOOP_SECURITY_TOKEN_SERVICE_USE_IP_DEFAULT =
      true;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_ENABLED_KEY =
      "hadoop.security.dns.log-slow-lookups.enabled";
  public static final boolean
      HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_ENABLED_DEFAULT = false;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String
      HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY =
      "hadoop.security.dns.log-slow-lookups.threshold.ms";
  public static final int
      HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT = 1000;

  /**
   * HA health monitor and failover controller.
   */
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

  /** How many time to retry connecting to the service. */
  public static final String HA_HM_RPC_CONNECT_MAX_RETRIES_KEY =
    "ha.health-monitor.rpc.connect.max.retries";
  public static final int HA_HM_RPC_CONNECT_MAX_RETRIES_DEFAULT = 1;

  /** How often to retry connecting to the service. */
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

  /** number of zookeeper operation retry times in ActiveStandbyElector */
  public static final String HA_FC_ELECTOR_ZK_OP_RETRIES_KEY =
      "ha.failover-controller.active-standby-elector.zk.op.retries";
  public static final int HA_FC_ELECTOR_ZK_OP_RETRIES_DEFAULT = 3;

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

  /**
   * User{@literal ->}groups static mapping to override the groups lookup
   */
  public static final String HADOOP_USER_GROUP_STATIC_OVERRIDES = 
      "hadoop.user.group.static.mapping.overrides";
  public static final String HADOOP_USER_GROUP_STATIC_OVERRIDES_DEFAULT =
      "dr.who=;";

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
  
  public static final String  IPC_CLIENT_ASYNC_CALLS_MAX_KEY =
      "ipc.client.async.calls.max";
  public static final int     IPC_CLIENT_ASYNC_CALLS_MAX_DEFAULT = 100;
  public static final String  IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY = "ipc.client.fallback-to-simple-auth-allowed";
  public static final boolean IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT = false;

  public static final String  IPC_CLIENT_BIND_WILDCARD_ADDR_KEY = "ipc.client"
      + ".bind.wildcard.addr";
  public static final boolean IPC_CLIENT_BIND_WILDCARD_ADDR_DEFAULT = false;

  public static final String IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY =
    "ipc.client.connect.max.retries.on.sasl";
  public static final int    IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_DEFAULT = 5;

  /** How often the server scans for idle connections */
  public static final String IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_KEY =
      "ipc.client.connection.idle-scan-interval.ms";
  /** Default value for IPC_SERVER_CONNECTION_IDLE_SCAN_INTERVAL_KEY */
  public static final int IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_DEFAULT =
      10000;

  public static final String HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS =
    "hadoop.user.group.metrics.percentiles.intervals";

  public static final String RPC_METRICS_QUANTILE_ENABLE =
      "rpc.metrics.quantile.enable";
  public static final boolean RPC_METRICS_QUANTILE_ENABLE_DEFAULT = false;
  public static final String  RPC_METRICS_PERCENTILES_INTERVALS_KEY =
      "rpc.metrics.percentiles.intervals";

  public static final String RPC_METRICS_TIME_UNIT = "rpc.metrics.timeunit";

  /** Allowed hosts for nfs exports */
  public static final String NFS_EXPORTS_ALLOWED_HOSTS_SEPARATOR = ";";
  public static final String NFS_EXPORTS_ALLOWED_HOSTS_KEY = "nfs.exports.allowed.hosts";
  public static final String NFS_EXPORTS_ALLOWED_HOSTS_KEY_DEFAULT = "* rw";

  // HDFS client HTrace configuration.
  public static final String  FS_CLIENT_HTRACE_PREFIX = "fs.client.htrace.";

  // Global ZooKeeper configuration keys
  public static final String ZK_PREFIX = "hadoop.zk.";
  /** ACL for the ZooKeeper ensemble. */
  public static final String ZK_ACL = ZK_PREFIX + "acl";
  public static final String ZK_ACL_DEFAULT = "world:anyone:rwcda";
  /** Authentication for the ZooKeeper ensemble. */
  public static final String ZK_AUTH = ZK_PREFIX + "auth";
  /** Principal name for zookeeper servers. */
  public static final String ZK_SERVER_PRINCIPAL = ZK_PREFIX + "server.principal";
  /** Kerberos principal name for zookeeper connection. */
  public static final String ZK_KERBEROS_PRINCIPAL = ZK_PREFIX + "kerberos.principal";
  /** Kerberos keytab for zookeeper connection. */
  public static final String ZK_KERBEROS_KEYTAB = ZK_PREFIX + "kerberos.keytab";

  /** Address of the ZooKeeper ensemble. */
  public static final String ZK_ADDRESS = ZK_PREFIX + "address";
  /** Maximum number of retries for a ZooKeeper operation. */
  public static final String ZK_NUM_RETRIES = ZK_PREFIX + "num-retries";
  public static final int    ZK_NUM_RETRIES_DEFAULT = 1000;
  /** Timeout for a ZooKeeper operation in ZooKeeper in milliseconds. */
  public static final String ZK_TIMEOUT_MS = ZK_PREFIX + "timeout-ms";
  public static final int    ZK_TIMEOUT_MS_DEFAULT = 10000;
  /** How often to retry a ZooKeeper operation  in milliseconds. */
  public static final String ZK_RETRY_INTERVAL_MS =
      ZK_PREFIX + "retry-interval-ms";

  /** SSL enablement for all Hadoop-&gt;ZK communication. */
  //Note: except ZKSignerSecretProvider in hadoop-auth to avoid circular dependency.
  public static final String ZK_CLIENT_SSL_ENABLED = ZK_PREFIX + "ssl.enabled";
  /** Keystore location for ZooKeeper client connection over SSL. */
  public static final String ZK_SSL_KEYSTORE_LOCATION = ZK_PREFIX + "ssl.keystore.location";
  /** Keystore password for ZooKeeper client connection over SSL. */
  public static final String ZK_SSL_KEYSTORE_PASSWORD = ZK_PREFIX + "ssl.keystore.password";
  /** Truststore location for ZooKeeper client connection over SSL. */
  public static final String ZK_SSL_TRUSTSTORE_LOCATION = ZK_PREFIX + "ssl.truststore.location";
  /** Truststore password for ZooKeeper client connection over SSL.  */
  public static final String ZK_SSL_TRUSTSTORE_PASSWORD = ZK_PREFIX + "ssl.truststore.password";

  public static final int    ZK_RETRY_INTERVAL_MS_DEFAULT = 1000;
  /** Default domain name resolver for hadoop to use. */
  public static final String HADOOP_DOMAINNAME_RESOLVER_IMPL =
      "hadoop.domainname.resolver.impl";
  public static final Class<? extends DomainNameResolver>
      HADOOP_DOMAINNAME_RESOLVER_IMPL_DEFAULT =
      DNSDomainNameResolver.class;
  /*
   *  Ignore KMS default URI returned from NameNode.
   *  When set to true, kms uri is searched in the following order:
   *  1. If there is a mapping in Credential's secrets map for namenode uri.
   *  2. Fallback to local conf.
   *  If client choose to ignore KMS uri provided by NameNode then client
   *  should set KMS URI using 'hadoop.security.key.provider.path' to access
   *  the right KMS for encrypted files.
   * */
  public static final String DFS_CLIENT_IGNORE_NAMENODE_DEFAULT_KMS_URI =
      "dfs.client.ignore.namenode.default.kms.uri";
  public static final boolean
      DFS_CLIENT_IGNORE_NAMENODE_DEFAULT_KMS_URI_DEFAULT = false;

  /**
   * Whether or not ThreadMXBean is used for getting thread info in JvmMetrics,
   * ThreadGroup approach is preferred for better performance.
   */
  public static final String HADOOP_METRICS_JVM_USE_THREAD_MXBEAN =
      "hadoop.metrics.jvm.use-thread-mxbean";
  public static final boolean HADOOP_METRICS_JVM_USE_THREAD_MXBEAN_DEFAULT =
      false;

  /** logging level for IOStatistics (debug or info). */
  public static final String IOSTATISTICS_LOGGING_LEVEL
      = "fs.iostatistics.logging.level";

  /** DEBUG logging level for IOStatistics logging. */
  public static final String IOSTATISTICS_LOGGING_LEVEL_DEBUG
      = "debug";

  /** WARN logging level for IOStatistics logging. */
  public static final String IOSTATISTICS_LOGGING_LEVEL_WARN
      = "warn";

  /** ERROR logging level for IOStatistics logging. */
  public static final String IOSTATISTICS_LOGGING_LEVEL_ERROR
      = "error";

  /** INFO logging level for IOStatistics logging. */
  public static final String IOSTATISTICS_LOGGING_LEVEL_INFO
      = "info";

  /** Default value for IOStatistics logging level. */
  public static final String IOSTATISTICS_LOGGING_LEVEL_DEFAULT
      = IOSTATISTICS_LOGGING_LEVEL_DEBUG;

  /**
   * default hadoop temp dir on local system: {@value}.
   */
  public static final String HADOOP_TMP_DIR = "hadoop.tmp.dir";

  /**
   * Thread-level IOStats Support.
   * {@value}
   */
  public static final String IOSTATISTICS_THREAD_LEVEL_ENABLED =
      "fs.iostatistics.thread.level.enabled";

  /**
   * Default value for Thread-level IOStats Support is true.
   */
  public static final boolean IOSTATISTICS_THREAD_LEVEL_ENABLED_DEFAULT =
      true;

  public static final String HADOOP_SECURITY_RESOLVER_IMPL =
      "hadoop.security.resolver.impl";

}
