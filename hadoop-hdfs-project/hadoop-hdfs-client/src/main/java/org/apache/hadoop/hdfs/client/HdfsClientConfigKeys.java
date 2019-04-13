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
package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import java.util.concurrent.TimeUnit;

/** Client configuration properties */
@InterfaceAudience.Private
public interface HdfsClientConfigKeys {
  long SECOND = 1000L;
  long MINUTE = 60 * SECOND;

  String  DFS_BLOCK_SIZE_KEY = "dfs.blocksize";
  long    DFS_BLOCK_SIZE_DEFAULT = 128*1024*1024;
  String  DFS_REPLICATION_KEY = "dfs.replication";
  short   DFS_REPLICATION_DEFAULT = 3;
  String  DFS_WEBHDFS_USER_PATTERN_KEY =
      "dfs.webhdfs.user.provider.user.pattern";
  String  DFS_WEBHDFS_USER_PATTERN_DEFAULT = "^[A-Za-z_][A-Za-z0-9._-]*[$]?$";
  String  DFS_WEBHDFS_ACL_PERMISSION_PATTERN_KEY =
      "dfs.webhdfs.acl.provider.permission.pattern";
  String DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT =
      "^(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?(,(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?)*$";

  String  DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY =
      "dfs.webhdfs.socket.connect-timeout";
  String  DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY =
      "dfs.webhdfs.socket.read-timeout";

  String DFS_WEBHDFS_OAUTH_ENABLED_KEY = "dfs.webhdfs.oauth2.enabled";
  boolean DFS_WEBHDFS_OAUTH_ENABLED_DEFAULT = false;

  String DFS_WEBHDFS_REST_CSRF_ENABLED_KEY = "dfs.webhdfs.rest-csrf.enabled";
  boolean DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT = false;
  String DFS_WEBHDFS_REST_CSRF_CUSTOM_HEADER_KEY =
      "dfs.webhdfs.rest-csrf.custom-header";
  String DFS_WEBHDFS_REST_CSRF_CUSTOM_HEADER_DEFAULT = "X-XSRF-HEADER";
  String DFS_WEBHDFS_REST_CSRF_METHODS_TO_IGNORE_KEY =
      "dfs.webhdfs.rest-csrf.methods-to-ignore";
  String DFS_WEBHDFS_REST_CSRF_METHODS_TO_IGNORE_DEFAULT =
      "GET,OPTIONS,HEAD,TRACE";
  String DFS_WEBHDFS_REST_CSRF_BROWSER_USERAGENTS_REGEX_KEY =
      "dfs.webhdfs.rest-csrf.browser-useragents-regex";

  String OAUTH_CLIENT_ID_KEY = "dfs.webhdfs.oauth2.client.id";
  String OAUTH_REFRESH_URL_KEY = "dfs.webhdfs.oauth2.refresh.url";

  String ACCESS_TOKEN_PROVIDER_KEY = "dfs.webhdfs.oauth2.access.token.provider";

  String PREFIX = "dfs.client.";
  String  DFS_NAMESERVICES = "dfs.nameservices";
  String DFS_NAMENODE_RPC_ADDRESS_KEY = "dfs.namenode.rpc-address";

  String DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_SUFFIX = "auxiliary-ports";
  String DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_KEY = DFS_NAMENODE_RPC_ADDRESS_KEY
      + "." + DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_SUFFIX;

  int     DFS_NAMENODE_HTTP_PORT_DEFAULT = 9870;
  String  DFS_NAMENODE_HTTP_ADDRESS_KEY = "dfs.namenode.http-address";
  int     DFS_NAMENODE_HTTPS_PORT_DEFAULT = 9871;
  String  DFS_NAMENODE_HTTPS_ADDRESS_KEY = "dfs.namenode.https-address";
  String DFS_HA_NAMENODES_KEY_PREFIX = "dfs.ha.namenodes";
  int DFS_NAMENODE_RPC_PORT_DEFAULT = 8020;
  String DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY =
      "dfs.namenode.kerberos.principal";
  String  DFS_CLIENT_WRITE_PACKET_SIZE_KEY = "dfs.client-write-packet-size";
  int     DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64*1024;
  String  DFS_CLIENT_SOCKET_TIMEOUT_KEY = "dfs.client.socket-timeout";
  String  DFS_CLIENT_SOCKET_SEND_BUFFER_SIZE_KEY =
      "dfs.client.socket.send.buffer.size";
  int     DFS_CLIENT_SOCKET_SEND_BUFFER_SIZE_DEFAULT =
      HdfsConstants.DEFAULT_DATA_SOCKET_SIZE;
  String  DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY =
      "dfs.client.socketcache.capacity";
  int     DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT = 16;
  String  DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY =
      "dfs.client.socketcache.expiryMsec";
  long    DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT = 3000;
  String  DFS_CLIENT_USE_DN_HOSTNAME = "dfs.client.use.datanode.hostname";
  boolean DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT = false;
  String  DFS_CLIENT_CACHE_DROP_BEHIND_WRITES =
      "dfs.client.cache.drop.behind.writes";
  String  DFS_CLIENT_CACHE_DROP_BEHIND_READS =
      "dfs.client.cache.drop.behind.reads";
  String  DFS_CLIENT_CACHE_READAHEAD = "dfs.client.cache.readahead";
  String  DFS_CLIENT_CACHED_CONN_RETRY_KEY = "dfs.client.cached.conn.retry";
  int     DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT = 3;
  String  DFS_CLIENT_CONTEXT = "dfs.client.context";
  String  DFS_CLIENT_CONTEXT_DEFAULT = "default";
  String  DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL =
      "dfs.client.use.legacy.blockreader.local";
  boolean DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT = false;
  String  DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY =
      "dfs.client.datanode-restart.timeout";
  long    DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT = 30;
  // Much code in hdfs is not yet updated to use these keys.
  // the initial delay (unit is ms) for locateFollowingBlock, the delay time
  // will increase exponentially(double) for each retry.
  String  DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY =
      "dfs.client.max.block.acquire.failures";
  int     DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT = 3;
  String  DFS_CLIENT_SERVER_DEFAULTS_VALIDITY_PERIOD_MS_KEY =
      "dfs.client.server-defaults.validity.period.ms";
  long    DFS_CLIENT_SERVER_DEFAULTS_VALIDITY_PERIOD_MS_DEFAULT =
      TimeUnit.HOURS.toMillis(1);
  String  DFS_CHECKSUM_TYPE_KEY = "dfs.checksum.type";
  String  DFS_CHECKSUM_TYPE_DEFAULT = "CRC32C";
  String  DFS_BYTES_PER_CHECKSUM_KEY = "dfs.bytes-per-checksum";
  int     DFS_BYTES_PER_CHECKSUM_DEFAULT = 512;
  String  DFS_CHECKSUM_COMBINE_MODE_KEY = "dfs.checksum.combine.mode";
  String  DFS_CHECKSUM_COMBINE_MODE_DEFAULT = "MD5MD5CRC";
  String  DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY =
      "dfs.datanode.socket.write.timeout";
  String  DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC =
      "dfs.client.domain.socket.data.traffic";
  boolean DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT = false;
  String  DFS_DOMAIN_SOCKET_PATH_KEY = "dfs.domain.socket.path";
  String  DFS_DOMAIN_SOCKET_PATH_DEFAULT = "";
  String DFS_DOMAIN_SOCKET_DISABLE_INTERVAL_SECOND_KEY =
      "dfs.domain.socket.disable.interval.seconds";
  long DFS_DOMAIN_SOCKET_DISABLE_INTERVAL_SECOND_DEFAULT = 600;
  String  DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS =
      "dfs.short.circuit.shared.memory.watcher.interrupt.check.ms";
  int     DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT =
      60000;
  String  DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY =
      "dfs.client.slow.io.warning.threshold.ms";
  long    DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT = 30000;
  String  DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_MS =
          "dfs.client.key.provider.cache.expiry";
  long    DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT =
              TimeUnit.DAYS.toMillis(10); // 10 days

  String  DFS_DATANODE_KERBEROS_PRINCIPAL_KEY =
      "dfs.datanode.kerberos.principal";
  String  DFS_DATANODE_READAHEAD_BYTES_KEY = "dfs.datanode.readahead.bytes";
  long    DFS_DATANODE_READAHEAD_BYTES_DEFAULT = 4 * 1024 * 1024; // 4MB

  String DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY =
      "dfs.encrypt.data.transfer.cipher.suites";

  String DFS_ENCRYPT_DATA_OVERWRITE_DOWNSTREAM_NEW_QOP_KEY =
      "dfs.encrypt.data.overwrite.downstream.new.qop";

  String DFS_DATA_TRANSFER_PROTECTION_KEY = "dfs.data.transfer.protection";
  String DFS_DATA_TRANSFER_PROTECTION_DEFAULT = "";
  String DFS_DATA_TRANSFER_SASL_PROPS_RESOLVER_CLASS_KEY =
      "dfs.data.transfer.saslproperties.resolver.class";

  String DFS_ENCRYPT_DATA_TRANSFER_CIPHER_KEY_BITLENGTH_KEY =
      "dfs.encrypt.data.transfer.cipher.key.bitlength";
  int    DFS_ENCRYPT_DATA_TRANSFER_CIPHER_KEY_BITLENGTH_DEFAULT = 128;

  String DFS_TRUSTEDCHANNEL_RESOLVER_CLASS =
      "dfs.trustedchannel.resolver.class";

  String REPLICA_ACCESSOR_BUILDER_CLASSES_KEY =
      PREFIX + "replica.accessor.builder.classes";

  // The number of NN response dropped by client proactively in each RPC call.
  // For testing NN retry cache, we can set this property with positive value.
  String  DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY =
      "dfs.client.test.drop.namenode.response.number";
  int     DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_DEFAULT = 0;
  String  DFS_CLIENT_LOCAL_INTERFACES = "dfs.client.local.interfaces";
  String  DFS_USER_HOME_DIR_PREFIX_KEY = "dfs.user.home.dir.prefix";
  String  DFS_USER_HOME_DIR_PREFIX_DEFAULT = "/user";

  String DFS_DATA_TRANSFER_CLIENT_TCPNODELAY_KEY =
      "dfs.data.transfer.client.tcpnodelay";
  boolean DFS_DATA_TRANSFER_CLIENT_TCPNODELAY_DEFAULT = true;

  String DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES =
      "dfs.namenode.snapshot.capture.openfiles";
  boolean DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES_DEFAULT = false;

  String DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS =
      "dfs.provided.aliasmap.inmemory.dnrpc-address";

  /**
   * These are deprecated config keys to client code.
   */
  interface DeprecatedKeys {
    String DFS_NAMENODE_BACKUP_ADDRESS_KEY =
        "dfs.namenode.backup.address";
    String DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY =
        "dfs.namenode.backup.http-address";
    String DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY =
        "dfs.datanode.balance.bandwidthPerSec";
    //Following keys have no defaults
    String DFS_DATANODE_DATA_DIR_KEY = "dfs.datanode.data.dir";
    String DFS_NAMENODE_MAX_OBJECTS_KEY = "dfs.namenode.max.objects";
    String DFS_NAMENODE_NAME_DIR_KEY = "dfs.namenode.name.dir";
    String DFS_NAMENODE_NAME_DIR_RESTORE_KEY = "dfs.namenode.name.dir.restore";
    String DFS_NAMENODE_EDITS_DIR_KEY = "dfs.namenode.edits.dir";
    String DFS_NAMENODE_SAFEMODE_EXTENSION_KEY =
        "dfs.namenode.safemode.extension";
    String DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY =
        "dfs.namenode.safemode.threshold-pct";
    String DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY =
        "dfs.namenode.secondary.http-address";
    String DFS_NAMENODE_CHECKPOINT_DIR_KEY = "dfs.namenode.checkpoint.dir";
    String DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY =
        "dfs.namenode.checkpoint.edits.dir";
    String DFS_NAMENODE_CHECKPOINT_PERIOD_KEY =
        "dfs.namenode.checkpoint.period";
    String DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY =
        "dfs.namenode.heartbeat.recheck-interval";
    String DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY =
        "dfs.client.https.keystore.resource";
    String DFS_CLIENT_HTTPS_NEED_AUTH_KEY = "dfs.client.https.need-auth";
    String DFS_DATANODE_HOST_NAME_KEY = "dfs.datanode.hostname";
    String DFS_METRICS_SESSION_ID_KEY = "dfs.metrics.session-id";
    String DFS_NAMENODE_ACCESSTIME_PRECISION_KEY =
        "dfs.namenode.accesstime.precision";
    String DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY =
        "dfs.namenode.redundancy.considerLoad";
    String DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_FACTOR =
        "dfs.namenode.redundancy.considerLoad.factor";
    String DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY =
        "dfs.namenode.redundancy.interval.seconds";
    String DFS_NAMENODE_REPLICATION_MIN_KEY = "dfs.namenode.replication.min";
    String DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY =
        "dfs.namenode.reconstruction.pending.timeout-sec";
    String DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY =
        "dfs.namenode.replication.max-streams";
    String DFS_PERMISSIONS_ENABLED_KEY = "dfs.permissions.enabled";
    String DFS_PERMISSIONS_SUPERUSERGROUP_KEY =
        "dfs.permissions.superusergroup";
    String DFS_DATANODE_MAX_RECEIVER_THREADS_KEY =
        "dfs.datanode.max.transfer.threads";
    String DFS_NAMESERVICE_ID = "dfs.nameservice.id";
  }

  /** dfs.client.retry configuration properties */
  interface Retry {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "retry.";

    String  POLICY_ENABLED_KEY = PREFIX + "policy.enabled";
    boolean POLICY_ENABLED_DEFAULT = false;
    String  POLICY_SPEC_KEY = PREFIX + "policy.spec";
    String  POLICY_SPEC_DEFAULT = "10000,6,60000,10"; //t1,n1,t2,n2,...

    String  TIMES_GET_LAST_BLOCK_LENGTH_KEY =
        PREFIX + "times.get-last-block-length";
    int     TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT = 3;
    String  INTERVAL_GET_LAST_BLOCK_LENGTH_KEY =
        PREFIX + "interval-ms.get-last-block-length";
    int     INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT = 4000;

    String  MAX_ATTEMPTS_KEY = PREFIX + "max.attempts";
    int     MAX_ATTEMPTS_DEFAULT = 10;

    String  WINDOW_BASE_KEY = PREFIX + "window.base";
    int     WINDOW_BASE_DEFAULT = 3000;
  }

  /** dfs.client.failover configuration properties */
  interface Failover {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "failover.";

    String  PROXY_PROVIDER_KEY_PREFIX = PREFIX + "proxy.provider";
    String  MAX_ATTEMPTS_KEY = PREFIX + "max.attempts";
    int     MAX_ATTEMPTS_DEFAULT = 15;
    String  SLEEPTIME_BASE_KEY = PREFIX + "sleep.base.millis";
    int     SLEEPTIME_BASE_DEFAULT = 500;
    String  SLEEPTIME_MAX_KEY = PREFIX + "sleep.max.millis";
    int     SLEEPTIME_MAX_DEFAULT = 15000;
    String  CONNECTION_RETRIES_KEY = PREFIX + "connection.retries";
    int     CONNECTION_RETRIES_DEFAULT = 0;
    String  CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY =
        PREFIX + "connection.retries.on.timeouts";
    int     CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 0;
    String  RANDOM_ORDER = PREFIX + "random.order";
    boolean RANDOM_ORDER_DEFAULT = false;
    String  RESOLVE_ADDRESS_NEEDED_KEY = PREFIX + "resolve-needed";
    boolean RESOLVE_ADDRESS_NEEDED_DEFAULT = false;
    String RESOLVE_SERVICE_KEY = PREFIX + "resolver.impl";
    String  RESOLVE_ADDRESS_TO_FQDN = PREFIX + "resolver.useFQDN";
    boolean RESOLVE_ADDRESS_TO_FQDN_DEFAULT = true;
  }

  /** dfs.client.write configuration properties */
  interface Write {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "write.";

    String  MAX_PACKETS_IN_FLIGHT_KEY = PREFIX + "max-packets-in-flight";
    int     MAX_PACKETS_IN_FLIGHT_DEFAULT = 80;
    String  EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_KEY =
        PREFIX + "exclude.nodes.cache.expiry.interval.millis";
    long    EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT = 10*MINUTE;

    interface ByteArrayManager {
      String PREFIX = Write.PREFIX + "byte-array-manager.";

      String  ENABLED_KEY = PREFIX + "enabled";
      boolean ENABLED_DEFAULT = false;
      String  COUNT_THRESHOLD_KEY = PREFIX + "count-threshold";
      int     COUNT_THRESHOLD_DEFAULT = 128;
      String  COUNT_LIMIT_KEY = PREFIX + "count-limit";
      int     COUNT_LIMIT_DEFAULT = 2048;
      String  COUNT_RESET_TIME_PERIOD_MS_KEY =
          PREFIX + "count-reset-time-period-ms";
      long    COUNT_RESET_TIME_PERIOD_MS_DEFAULT = 10*SECOND;
    }
  }

  /** dfs.client.block.write configuration properties */
  interface BlockWrite {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "block.write.";

    String  RETRIES_KEY = PREFIX + "retries";
    int     RETRIES_DEFAULT = 3;
    String  LOCATEFOLLOWINGBLOCK_RETRIES_KEY =
        PREFIX + "locateFollowingBlock.retries";
    int     LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT = 5;
    String  LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_KEY =
        PREFIX + "locateFollowingBlock.initial.delay.ms";
    int     LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_DEFAULT = 400;
    String  LOCATEFOLLOWINGBLOCK_MAX_DELAY_MS_KEY =
        PREFIX + "locateFollowingBlock.max.delay.ms";
    int     LOCATEFOLLOWINGBLOCK_MAX_DELAY_MS_DEFAULT = 60000;

    interface ReplaceDatanodeOnFailure {
      String PREFIX = BlockWrite.PREFIX + "replace-datanode-on-failure.";

      String  ENABLE_KEY = PREFIX + "enable";
      boolean ENABLE_DEFAULT = true;
      String  POLICY_KEY = PREFIX + "policy";
      String  POLICY_DEFAULT = "DEFAULT";
      String  BEST_EFFORT_KEY = PREFIX + "best-effort";
      boolean BEST_EFFORT_DEFAULT = false;
      String MIN_REPLICATION = PREFIX + "min-replication";
      short MIN_REPLICATION_DEFAULT = 0;
    }
  }

  /** dfs.client.read configuration properties */
  interface Read {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "read.";

    String  PREFETCH_SIZE_KEY = PREFIX + "prefetch.size";

    interface ShortCircuit {
      String PREFIX = Read.PREFIX + "shortcircuit.";

      String  KEY = PREFIX.substring(0, PREFIX.length()-1);
      boolean DEFAULT = false;
      String  SKIP_CHECKSUM_KEY = PREFIX + "skip.checksum";
      boolean SKIP_CHECKSUM_DEFAULT = false;
      String  BUFFER_SIZE_KEY = PREFIX + "buffer.size";
      int     BUFFER_SIZE_DEFAULT = 1024 * 1024;

      String  STREAMS_CACHE_SIZE_KEY = PREFIX + "streams.cache.size";
      int     STREAMS_CACHE_SIZE_DEFAULT = 256;
      String  STREAMS_CACHE_EXPIRY_MS_KEY = PREFIX + "streams.cache.expiry.ms";
      long    STREAMS_CACHE_EXPIRY_MS_DEFAULT = 5*MINUTE;

      String  METRICS_SAMPLING_PERCENTAGE_KEY =
          PREFIX + "metrics.sampling.percentage";
      int     METRICS_SAMPLING_PERCENTAGE_DEFAULT = 0;
    }
  }

  /** dfs.client.short.circuit configuration properties */
  interface ShortCircuit {
    String PREFIX = Read.PREFIX + "short.circuit.";

    String  REPLICA_STALE_THRESHOLD_MS_KEY =
        PREFIX + "replica.stale.threshold.ms";
    long    REPLICA_STALE_THRESHOLD_MS_DEFAULT = 30*MINUTE;
  }

  /** dfs.client.mmap configuration properties */
  interface Mmap {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "mmap.";

    String  ENABLED_KEY = PREFIX + "enabled";
    boolean ENABLED_DEFAULT = true;
    String  CACHE_SIZE_KEY = PREFIX + "cache.size";
    int     CACHE_SIZE_DEFAULT = 256;
    String  CACHE_TIMEOUT_MS_KEY = PREFIX + "cache.timeout.ms";
    long    CACHE_TIMEOUT_MS_DEFAULT  = 60*MINUTE;
    String  RETRY_TIMEOUT_MS_KEY = PREFIX + "retry.timeout.ms";
    long    RETRY_TIMEOUT_MS_DEFAULT = 5*MINUTE;
  }

  /** dfs.client.hedged.read configuration properties */
  interface HedgedRead {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "hedged.read.";

    String  THRESHOLD_MILLIS_KEY = PREFIX + "threshold.millis";
    long    THRESHOLD_MILLIS_DEFAULT = 500;
    String  THREADPOOL_SIZE_KEY = PREFIX + "threadpool.size";
    int     THREADPOOL_SIZE_DEFAULT = 0;
  }

  /** dfs.client.read.striped configuration properties */
  interface StripedRead {
    String PREFIX = Read.PREFIX + "striped.";

    String  THREADPOOL_SIZE_KEY = PREFIX + "threadpool.size";
    /**
     * With default RS-6-3-1024k erasure coding policy, each normal read could
     * span 6 DNs, so this default value accommodates 3 read streams
     */
    int     THREADPOOL_SIZE_DEFAULT = 18;
  }

  /** dfs.http.client configuration properties */
  interface HttpClient {
    String  PREFIX = "dfs.http.client.";

    // retry
    String  RETRY_POLICY_ENABLED_KEY = PREFIX + "retry.policy.enabled";
    boolean RETRY_POLICY_ENABLED_DEFAULT = false;
    String  RETRY_POLICY_SPEC_KEY = PREFIX + "retry.policy.spec";
    String  RETRY_POLICY_SPEC_DEFAULT = "10000,6,60000,10"; //t1,n1,t2,n2,...
    String  RETRY_MAX_ATTEMPTS_KEY = PREFIX + "retry.max.attempts";
    int     RETRY_MAX_ATTEMPTS_DEFAULT = 10;

    // failover
    String  FAILOVER_MAX_ATTEMPTS_KEY = PREFIX + "failover.max.attempts";
    int     FAILOVER_MAX_ATTEMPTS_DEFAULT =  15;
    String  FAILOVER_SLEEPTIME_BASE_KEY = PREFIX + "failover.sleep.base.millis";
    int     FAILOVER_SLEEPTIME_BASE_DEFAULT = 500;
    String  FAILOVER_SLEEPTIME_MAX_KEY = PREFIX + "failover.sleep.max.millis";
    int     FAILOVER_SLEEPTIME_MAX_DEFAULT =  15000;
  }
}
