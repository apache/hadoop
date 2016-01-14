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

package org.apache.hadoop.hdfs;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.RamDiskReplicaLruTracker;
import org.apache.hadoop.hdfs.web.AuthFilter;
import org.apache.hadoop.http.HttpConfig;

/** 
 * This class contains constants for configuration keys used
 * in hdfs.
 *
 */

@InterfaceAudience.Private
public class DFSConfigKeys extends CommonConfigurationKeys {

  public static final String  DFS_BLOCK_SIZE_KEY = "dfs.blocksize";
  public static final long    DFS_BLOCK_SIZE_DEFAULT = 128*1024*1024;
  public static final String  DFS_REPLICATION_KEY = "dfs.replication";
  public static final short   DFS_REPLICATION_DEFAULT = 3;
  public static final String  DFS_STREAM_BUFFER_SIZE_KEY = "dfs.stream-buffer-size";
  public static final int     DFS_STREAM_BUFFER_SIZE_DEFAULT = 4096;
  public static final String  DFS_BYTES_PER_CHECKSUM_KEY = "dfs.bytes-per-checksum";
  public static final int     DFS_BYTES_PER_CHECKSUM_DEFAULT = 512;
  public static final String  DFS_USER_HOME_DIR_PREFIX_KEY = "dfs.user.home.dir.prefix";
  public static final String  DFS_USER_HOME_DIR_PREFIX_DEFAULT = "/user";
  public static final String  DFS_CLIENT_RETRY_POLICY_ENABLED_KEY = "dfs.client.retry.policy.enabled";
  public static final boolean DFS_CLIENT_RETRY_POLICY_ENABLED_DEFAULT = false; 
  public static final String  DFS_CLIENT_RETRY_POLICY_SPEC_KEY = "dfs.client.retry.policy.spec";
  public static final String  DFS_CLIENT_RETRY_POLICY_SPEC_DEFAULT = "10000,6,60000,10"; //t1,n1,t2,n2,... 
  public static final String  DFS_CHECKSUM_TYPE_KEY = "dfs.checksum.type";
  public static final String  DFS_CHECKSUM_TYPE_DEFAULT = "CRC32C";
  public static final String  DFS_CLIENT_WRITE_MAX_PACKETS_IN_FLIGHT_KEY = "dfs.client.write.max-packets-in-flight";
  public static final int     DFS_CLIENT_WRITE_MAX_PACKETS_IN_FLIGHT_DEFAULT = 80;
  public static final String  DFS_CLIENT_WRITE_PACKET_SIZE_KEY = "dfs.client-write-packet-size";
  public static final int     DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64*1024;
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_ENABLED_KEY
      = "dfs.client.write.byte-array-manager.enabled";
  public static final boolean DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_ENABLED_DEFAULT
      = false;
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_THRESHOLD_KEY
      = "dfs.client.write.byte-array-manager.count-threshold";
  public static final int     DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_THRESHOLD_DEFAULT
      = 128;
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_LIMIT_KEY
      = "dfs.client.write.byte-array-manager.count-limit";
  public static final int     DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_LIMIT_DEFAULT
      = 2048;
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_RESET_TIME_PERIOD_MS_KEY
      = "dfs.client.write.byte-array-manager.count-reset-time-period-ms";
  public static final long    DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_RESET_TIME_PERIOD_MS_DEFAULT
      = 10L * 1000;

  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY = "dfs.client.block.write.replace-datanode-on-failure.enable";
  public static final boolean DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_DEFAULT = true;
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY = "dfs.client.block.write.replace-datanode-on-failure.policy";
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_DEFAULT = "DEFAULT";
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY = "dfs.client.block.write.replace-datanode-on-failure.best-effort";
  public static final boolean DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_DEFAULT = false;
  public static final String  DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY = "dfs.client.socketcache.capacity";
  public static final int     DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT = 16;
  public static final String  DFS_CLIENT_USE_DN_HOSTNAME = "dfs.client.use.datanode.hostname";
  public static final boolean DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT = false;
  public static final String  DFS_CLIENT_CACHE_DROP_BEHIND_WRITES = "dfs.client.cache.drop.behind.writes";
  public static final String  DFS_CLIENT_CACHE_DROP_BEHIND_READS = "dfs.client.cache.drop.behind.reads";
  public static final String  DFS_CLIENT_CACHE_READAHEAD = "dfs.client.cache.readahead";
  public static final String  DFS_CLIENT_CONTEXT = "dfs.client.context";
  public static final String  DFS_CLIENT_CONTEXT_DEFAULT = "default";
  public static final String  DFS_HDFS_BLOCKS_METADATA_ENABLED = "dfs.datanode.hdfs-blocks-metadata.enabled";
  public static final boolean DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT = false;
  public static final String  DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_NUM_THREADS = "dfs.client.file-block-storage-locations.num-threads";
  public static final int     DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_NUM_THREADS_DEFAULT = 10;
  public static final String  DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS = "dfs.client.file-block-storage-locations.timeout.millis";
  public static final int     DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS_DEFAULT = 1000;
  public static final String  DFS_CLIENT_RETRY_TIMES_GET_LAST_BLOCK_LENGTH = "dfs.client.retry.times.get-last-block-length";
  public static final int     DFS_CLIENT_RETRY_TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT = 3;
  public static final String  DFS_CLIENT_RETRY_INTERVAL_GET_LAST_BLOCK_LENGTH = "dfs.client.retry.interval-ms.get-last-block-length";
  public static final int     DFS_CLIENT_RETRY_INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT = 4000;
  public static final String DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT =
      "^(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?(,(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?)*$";

  // HDFS HTrace configuration is controlled by dfs.htrace.spanreceiver.classes,
  // etc.
  public static final String  DFS_SERVER_HTRACE_PREFIX = "dfs.htrace.";

  // HDFS client HTrace configuration.
  public static final String  DFS_CLIENT_HTRACE_PREFIX = "dfs.client.htrace.";

  // HA related configuration
  public static final String  DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX = "dfs.client.failover.proxy.provider";
  public static final String  DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY = "dfs.client.failover.max.attempts";
  public static final int     DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT = 15;
  public static final String  DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY = "dfs.client.failover.sleep.base.millis";
  public static final int     DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT = 500;
  public static final String  DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY = "dfs.client.failover.sleep.max.millis";
  public static final int     DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT = 15000;
  public static final String  DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_KEY = "dfs.client.failover.connection.retries";
  public static final int     DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_DEFAULT = 0;
  public static final String  DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY = "dfs.client.failover.connection.retries.on.timeouts";
  public static final int     DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 0;
  public static final String  DFS_CLIENT_RETRY_MAX_ATTEMPTS_KEY = "dfs.client.retry.max.attempts";
  public static final int     DFS_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT = 10;
  
  public static final String  DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY = "dfs.client.socketcache.expiryMsec";
  public static final long    DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT = 3000;
  public static final String  DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL = "dfs.client.write.exclude.nodes.cache.expiry.interval.millis";
  public static final long    DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT = 10 * 60 * 1000; // 10 minutes, in ms
  public static final String  DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY = "dfs.client.datanode-restart.timeout";
  public static final long    DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT = 30;
  public static final String  DFS_DATANODE_RESTART_REPLICA_EXPIRY_KEY = "dfs.datanode.restart.replica.expiration";
  public static final long    DFS_DATANODE_RESTART_REPLICA_EXPIRY_DEFAULT = 50;
  public static final String  DFS_NAMENODE_BACKUP_ADDRESS_KEY = "dfs.namenode.backup.address";
  public static final String  DFS_NAMENODE_BACKUP_ADDRESS_DEFAULT = "localhost:50100";
  public static final String  DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY = "dfs.namenode.backup.http-address";
  public static final String  DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT = "0.0.0.0:50105";
  public static final String  DFS_NAMENODE_BACKUP_SERVICE_RPC_ADDRESS_KEY = "dfs.namenode.backup.dnrpc-address";
  public static final String  DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY = "dfs.datanode.balance.bandwidthPerSec";
  public static final long    DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT = 1024*1024;
  public static final String  DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY = "dfs.datanode.balance.max.concurrent.moves";
  public static final int     DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT = 5;
  public static final String  DFS_DATANODE_READAHEAD_BYTES_KEY = "dfs.datanode.readahead.bytes";
  public static final long    DFS_DATANODE_READAHEAD_BYTES_DEFAULT = 4 * 1024 * 1024; // 4MB
  public static final String  DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_KEY = "dfs.datanode.drop.cache.behind.writes";
  public static final boolean DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_DEFAULT = false;
  public static final String  DFS_DATANODE_SYNC_BEHIND_WRITES_KEY = "dfs.datanode.sync.behind.writes";
  public static final boolean DFS_DATANODE_SYNC_BEHIND_WRITES_DEFAULT = false;
  public static final String  DFS_DATANODE_SYNC_BEHIND_WRITES_IN_BACKGROUND_KEY = "dfs.datanode.sync.behind.writes.in.background";
  public static final boolean DFS_DATANODE_SYNC_BEHIND_WRITES_IN_BACKGROUND_DEFAULT = false;
  public static final String  DFS_DATANODE_DROP_CACHE_BEHIND_READS_KEY = "dfs.datanode.drop.cache.behind.reads";
  public static final boolean DFS_DATANODE_DROP_CACHE_BEHIND_READS_DEFAULT = false;
  public static final String  DFS_DATANODE_USE_DN_HOSTNAME = "dfs.datanode.use.datanode.hostname";
  public static final boolean DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT = false;
  public static final String  DFS_DATANODE_MAX_LOCKED_MEMORY_KEY = "dfs.datanode.max.locked.memory";
  public static final long    DFS_DATANODE_MAX_LOCKED_MEMORY_DEFAULT = 0;
  public static final String  DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_KEY = "dfs.datanode.fsdatasetcache.max.threads.per.volume";
  public static final int     DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_DEFAULT = 4;
  public static final String  DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC = "dfs.datanode.lazywriter.interval.sec";
  public static final int     DFS_DATANODE_LAZY_WRITER_INTERVAL_DEFAULT_SEC = 60;
  public static final String  DFS_DATANODE_RAM_DISK_REPLICA_TRACKER_KEY = "dfs.datanode.ram.disk.replica.tracker";
  public static final Class<RamDiskReplicaLruTracker>  DFS_DATANODE_RAM_DISK_REPLICA_TRACKER_DEFAULT = RamDiskReplicaLruTracker.class;
  public static final String  DFS_DATANODE_RAM_DISK_LOW_WATERMARK_PERCENT = "dfs.datanode.ram.disk.low.watermark.percent";
  public static final float   DFS_DATANODE_RAM_DISK_LOW_WATERMARK_PERCENT_DEFAULT = 10.0f;
  public static final String  DFS_DATANODE_RAM_DISK_LOW_WATERMARK_BYTES = "dfs.datanode.ram.disk.low.watermark.bytes";
  public static final long    DFS_DATANODE_RAM_DISK_LOW_WATERMARK_BYTES_DEFAULT = DFS_BLOCK_SIZE_DEFAULT;
  public static final String  DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_KEY = "dfs.datanode.network.counts.cache.max.size";
  public static final int     DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_DEFAULT = Integer.MAX_VALUE;

  // This setting is for testing/internal use only.
  public static final String  DFS_DATANODE_DUPLICATE_REPLICA_DELETION = "dfs.datanode.duplicate.replica.deletion";
  public static final boolean DFS_DATANODE_DUPLICATE_REPLICA_DELETION_DEFAULT = true;

  public static final String  DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT =
    "dfs.namenode.path.based.cache.block.map.allocation.percent";
  public static final float    DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT_DEFAULT = 0.25f;

  public static final String  DFS_NAMENODE_HTTP_PORT_KEY = "dfs.http.port";
  public static final int     DFS_NAMENODE_HTTP_PORT_DEFAULT = 50070;
  public static final String  DFS_NAMENODE_HTTP_ADDRESS_KEY = "dfs.namenode.http-address";
  public static final String  DFS_NAMENODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_NAMENODE_HTTP_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_HTTP_BIND_HOST_KEY = "dfs.namenode.http-bind-host";
  public static final String  DFS_NAMENODE_RPC_ADDRESS_KEY = "dfs.namenode.rpc-address";
  public static final String  DFS_NAMENODE_RPC_BIND_HOST_KEY = "dfs.namenode.rpc-bind-host";
  public static final String  DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY = "dfs.namenode.servicerpc-address";
  public static final String  DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY = "dfs.namenode.servicerpc-bind-host";
  public static final String  DFS_NAMENODE_MAX_OBJECTS_KEY = "dfs.namenode.max.objects";
  public static final long    DFS_NAMENODE_MAX_OBJECTS_DEFAULT = 0;
  public static final String  DFS_NAMENODE_SAFEMODE_EXTENSION_KEY = "dfs.namenode.safemode.extension";
  public static final int     DFS_NAMENODE_SAFEMODE_EXTENSION_DEFAULT = 30000;
  public static final String  DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY = "dfs.namenode.safemode.threshold-pct";
  public static final float   DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT = 0.999f;
  // set this to a slightly smaller value than
  // DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT to populate
  // needed replication queues before exiting safe mode
  public static final String  DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY =
    "dfs.namenode.replqueue.threshold-pct";
  public static final String  DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY = "dfs.namenode.safemode.min.datanodes";
  public static final int     DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT = 0;
  public static final String  DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY = "dfs.namenode.secondary.http-address";
  public static final String  DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_DEFAULT = "0.0.0.0:50090";
  public static final String  DFS_NAMENODE_SECONDARY_HTTPS_ADDRESS_KEY = "dfs.namenode.secondary.https-address";
  public static final String  DFS_NAMENODE_SECONDARY_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:50091";
  public static final String  DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY = "dfs.namenode.checkpoint.check.period";
  public static final long    DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT = 60;
  public static final String  DFS_NAMENODE_CHECKPOINT_PERIOD_KEY = "dfs.namenode.checkpoint.period";
  public static final long    DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT = 3600;
  public static final String  DFS_NAMENODE_CHECKPOINT_TXNS_KEY = "dfs.namenode.checkpoint.txns";
  public static final long    DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT = 1000000;
  public static final String  DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_KEY = "dfs.namenode.checkpoint.max-retries";
  public static final int     DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_DEFAULT = 3;
  public static final String  DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY = "dfs.namenode.heartbeat.recheck-interval";
  public static final int     DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT = 5*60*1000;
  public static final String  DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY = "dfs.namenode.tolerate.heartbeat.multiplier";
  public static final int     DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT = 4;
  public static final String  DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY = "dfs.client.https.keystore.resource";
  public static final String  DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_DEFAULT = "ssl-client.xml";
  public static final String  DFS_CLIENT_HTTPS_NEED_AUTH_KEY = "dfs.client.https.need-auth";
  public static final boolean DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT = false;
  public static final String  DFS_CLIENT_CACHED_CONN_RETRY_KEY = "dfs.client.cached.conn.retry";
  public static final int     DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT = 3;
  public static final String  DFS_NAMENODE_ACCESSTIME_PRECISION_KEY = "dfs.namenode.accesstime.precision";
  public static final long    DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT = 3600000;
  public static final String  DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY = "dfs.namenode.replication.considerLoad";
  public static final boolean DFS_NAMENODE_REPLICATION_CONSIDERLOAD_DEFAULT = true;
  public static final String  DFS_NAMENODE_REPLICATION_INTERVAL_KEY = "dfs.namenode.replication.interval";
  public static final int     DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT = 3;
  public static final String  DFS_NAMENODE_REPLICATION_MIN_KEY = "dfs.namenode.replication.min";
  public static final int     DFS_NAMENODE_REPLICATION_MIN_DEFAULT = 1;
  public static final String  DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY = "dfs.namenode.replication.pending.timeout-sec";
  public static final int     DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_DEFAULT = -1;
  public static final String  DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY = "dfs.namenode.replication.max-streams";
  public static final int     DFS_NAMENODE_REPLICATION_MAX_STREAMS_DEFAULT = 2;
  public static final String  DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY = "dfs.namenode.replication.max-streams-hard-limit";
  public static final int     DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT = 4;
  public static final String  DFS_WEBHDFS_AUTHENTICATION_FILTER_KEY = "dfs.web.authentication.filter";
  public static final String  DFS_WEBHDFS_AUTHENTICATION_FILTER_DEFAULT = AuthFilter.class.getName();
  public static final String  DFS_WEBHDFS_ENABLED_KEY = "dfs.webhdfs.enabled";
  public static final boolean DFS_WEBHDFS_ENABLED_DEFAULT = true;
  public static final String  DFS_WEBHDFS_USER_PATTERN_KEY = "dfs.webhdfs.user.provider.user.pattern";
  public static final String  DFS_WEBHDFS_USER_PATTERN_DEFAULT = "^[A-Za-z_][A-Za-z0-9._-]*[$]?$";
  public static final String  DFS_PERMISSIONS_ENABLED_KEY = "dfs.permissions.enabled";
  public static final boolean DFS_PERMISSIONS_ENABLED_DEFAULT = true;
  public static final String  DFS_PERMISSIONS_SUPERUSERGROUP_KEY = "dfs.permissions.superusergroup";
  public static final String  DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT = "supergroup";
  public static final String  DFS_NAMENODE_ACLS_ENABLED_KEY = "dfs.namenode.acls.enabled";
  public static final boolean DFS_NAMENODE_ACLS_ENABLED_DEFAULT = false;
  public static final String  DFS_NAMENODE_XATTRS_ENABLED_KEY = "dfs.namenode.xattrs.enabled";
  public static final boolean DFS_NAMENODE_XATTRS_ENABLED_DEFAULT = true;
  public static final String  DFS_ADMIN = "dfs.cluster.administrators";
  public static final String  DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY = "dfs.https.server.keystore.resource";
  public static final String  DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_DEFAULT = "ssl-server.xml";
  public static final String  DFS_SERVER_HTTPS_KEYPASSWORD_KEY = "ssl.server.keystore.keypassword";
  public static final String  DFS_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY = "ssl.server.keystore.password";
  public static final String  DFS_SERVER_HTTPS_TRUSTSTORE_PASSWORD_KEY = "ssl.server.truststore.password";
  public static final String  DFS_NAMENODE_NAME_DIR_RESTORE_KEY = "dfs.namenode.name.dir.restore";
  public static final boolean DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT = false;
  public static final String  DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY = "dfs.namenode.support.allow.format";
  public static final boolean DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_DEFAULT = true;
  public static final String  DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_KEY = "dfs.namenode.num.checkpoints.retained";
  public static final int     DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_DEFAULT = 2;
  public static final String  DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY = "dfs.namenode.num.extra.edits.retained";
  public static final int     DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_DEFAULT = 1000000; //1M
  public static final String  DFS_NAMENODE_MAX_EXTRA_EDITS_SEGMENTS_RETAINED_KEY = "dfs.namenode.max.extra.edits.segments.retained";
  public static final int     DFS_NAMENODE_MAX_EXTRA_EDITS_SEGMENTS_RETAINED_DEFAULT = 10000; // 10k
  public static final String  DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_KEY = "dfs.namenode.min.supported.datanode.version";
  public static final String  DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_DEFAULT = "2.1.0-beta";

  public static final String  DFS_NAMENODE_EDITS_DIR_MINIMUM_KEY = "dfs.namenode.edits.dir.minimum";
  public static final int     DFS_NAMENODE_EDITS_DIR_MINIMUM_DEFAULT = 1;

  public static final String  DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD = "dfs.namenode.edit.log.autoroll.multiplier.threshold";
  public static final float   DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT = 2.0f;
  public static final String  DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS = "dfs.namenode.edit.log.autoroll.check.interval.ms";
  public static final int     DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS_DEFAULT = 5*60*1000;

  public static final String  DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC = "dfs.namenode.lazypersist.file.scrub.interval.sec";
  public static final int     DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC_DEFAULT = 5 * 60;
  
  public static final String  DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH = "dfs.namenode.edits.noeditlogchannelflush";
  public static final boolean DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH_DEFAULT = false;
  
  public static final String  DFS_LIST_LIMIT = "dfs.ls.limit";
  public static final int     DFS_LIST_LIMIT_DEFAULT = 1000;
  public static final String  DFS_CONTENT_SUMMARY_LIMIT_KEY = "dfs.content-summary.limit";
  public static final int     DFS_CONTENT_SUMMARY_LIMIT_DEFAULT = 5000;
  public static final String  DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_KEY = "dfs.content-summary.sleep-microsec";
  public static final long    DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_DEFAULT = 500;
  public static final String  DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY = "dfs.datanode.failed.volumes.tolerated";
  public static final int     DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT = 0;
  public static final String  DFS_DATANODE_SYNCONCLOSE_KEY = "dfs.datanode.synconclose";
  public static final boolean DFS_DATANODE_SYNCONCLOSE_DEFAULT = false;
  public static final String  DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY = "dfs.datanode.socket.reuse.keepalive";
  public static final int     DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_DEFAULT = 4000;
  public static final String  DFS_DATANODE_OOB_TIMEOUT_KEY = "dfs.datanode.oob.timeout-ms";
  public static final String  DFS_DATANODE_OOB_TIMEOUT_DEFAULT = "1500,0,0,0"; // OOB_TYPE1, OOB_TYPE2, OOB_TYPE3, OOB_TYPE4

  public static final String DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS = "dfs.datanode.cache.revocation.timeout.ms";
  public static final long DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS_DEFAULT = 900000L;

  public static final String DFS_DATANODE_CACHE_REVOCATION_POLLING_MS = "dfs.datanode.cache.revocation.polling.ms";
  public static final long DFS_DATANODE_CACHE_REVOCATION_POLLING_MS_DEFAULT = 500L;

  public static final String DFS_NAMENODE_DATANODE_REGISTRATION_IP_HOSTNAME_CHECK_KEY = "dfs.namenode.datanode.registration.ip-hostname-check";
  public static final boolean DFS_NAMENODE_DATANODE_REGISTRATION_IP_HOSTNAME_CHECK_DEFAULT = true;
  
  public static final String  DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES =
      "dfs.namenode.list.cache.pools.num.responses";
  public static final int     DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES_DEFAULT = 100;
  public static final String  DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES =
      "dfs.namenode.list.cache.directives.num.responses";
  public static final int     DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES_DEFAULT = 100;
  public static final String  DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS =
      "dfs.namenode.path.based.cache.refresh.interval.ms";
  public static final long    DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS_DEFAULT = 30000L;

  /** Pending period of block deletion since NameNode startup */
  public static final String  DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY = "dfs.namenode.startup.delay.block.deletion.sec";
  public static final long    DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_DEFAULT = 0L;

  // Whether to enable datanode's stale state detection and usage for reads
  public static final String DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY = "dfs.namenode.avoid.read.stale.datanode";
  public static final boolean DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_DEFAULT = false;
  // Whether to enable datanode's stale state detection and usage for writes
  public static final String DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY = "dfs.namenode.avoid.write.stale.datanode";
  public static final boolean DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_DEFAULT = false;
  // The default value of the time interval for marking datanodes as stale
  public static final String DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY = "dfs.namenode.stale.datanode.interval";
  public static final long DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT = 30 * 1000; // 30s
  // The stale interval cannot be too small since otherwise this may cause too frequent churn on stale states. 
  // This value uses the times of heartbeat interval to define the minimum value for stale interval.  
  public static final String DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_KEY = "dfs.namenode.stale.datanode.minimum.interval";
  public static final int DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_DEFAULT = 3; // i.e. min_interval is 3 * heartbeat_interval = 9s
  
  // When the percentage of stale datanodes reaches this ratio,
  // allow writing to stale nodes to prevent hotspots.
  public static final String DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY = "dfs.namenode.write.stale.datanode.ratio";
  public static final float DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_DEFAULT = 0.5f;

  // Number of blocks to rescan for each iteration of postponedMisreplicatedBlocks.
  public static final String DFS_NAMENODE_BLOCKS_PER_POSTPONEDBLOCKS_RESCAN_KEY = "dfs.namenode.blocks.per.postponedblocks.rescan";
  public static final long DFS_NAMENODE_BLOCKS_PER_POSTPONEDBLOCKS_RESCAN_KEY_DEFAULT = 10000;

  // Replication monitoring related keys
  public static final String DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION =
      "dfs.namenode.invalidate.work.pct.per.iteration";
  public static final float DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION_DEFAULT = 0.32f;
  public static final String DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION =
      "dfs.namenode.replication.work.multiplier.per.iteration";
  public static final int DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION_DEFAULT = 2;

  //Delegation token related keys
  public static final String  DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY = "dfs.namenode.delegation.key.update-interval";
  public static final long    DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT = 24*60*60*1000; // 1 day
  public static final String  DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY = "dfs.namenode.delegation.token.renew-interval";
  public static final long    DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT = 24*60*60*1000;  // 1 day
  public static final String  DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY = "dfs.namenode.delegation.token.max-lifetime";
  public static final long    DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT = 7*24*60*60*1000; // 7 days
  public static final String  DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY = "dfs.namenode.delegation.token.always-use"; // for tests
  public static final boolean DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_DEFAULT = false;

  //Filesystem limit keys
  public static final String  DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY = "dfs.namenode.fs-limits.max-component-length";
  public static final int     DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT = 255;
  public static final String  DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY = "dfs.namenode.fs-limits.max-directory-items";
  public static final int     DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT = 1024*1024;
  public static final String  DFS_NAMENODE_MIN_BLOCK_SIZE_KEY = "dfs.namenode.fs-limits.min-block-size";
  public static final long    DFS_NAMENODE_MIN_BLOCK_SIZE_DEFAULT = 1024*1024;
  public static final String  DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY = "dfs.namenode.fs-limits.max-blocks-per-file";
  public static final long    DFS_NAMENODE_MAX_BLOCKS_PER_FILE_DEFAULT = 1024*1024;
  public static final String  DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY = "dfs.namenode.fs-limits.max-xattrs-per-inode";
  public static final int     DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT = 32;
  public static final String  DFS_NAMENODE_MAX_XATTR_SIZE_KEY = "dfs.namenode.fs-limits.max-xattr-size";
  public static final int     DFS_NAMENODE_MAX_XATTR_SIZE_DEFAULT = 16384;


  //Following keys have no defaults
  public static final String  DFS_DATANODE_DATA_DIR_KEY = "dfs.datanode.data.dir";
  public static final String  DFS_NAMENODE_HTTPS_PORT_KEY = "dfs.https.port";
  public static final int     DFS_NAMENODE_HTTPS_PORT_DEFAULT = 50470;
  public static final String  DFS_NAMENODE_HTTPS_ADDRESS_KEY = "dfs.namenode.https-address";
  public static final String  DFS_NAMENODE_HTTPS_BIND_HOST_KEY = "dfs.namenode.https-bind-host";
  public static final String  DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_NAMENODE_HTTPS_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_NAME_DIR_KEY = "dfs.namenode.name.dir";
  public static final String  DFS_NAMENODE_EDITS_DIR_KEY = "dfs.namenode.edits.dir";
  public static final String  DFS_NAMENODE_SHARED_EDITS_DIR_KEY = "dfs.namenode.shared.edits.dir";
  public static final String  DFS_NAMENODE_EDITS_PLUGIN_PREFIX = "dfs.namenode.edits.journal-plugin";
  public static final String  DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY = "dfs.namenode.edits.dir.required";
  public static final String  DFS_NAMENODE_EDITS_DIR_DEFAULT = "file:///tmp/hadoop/dfs/name";
  public static final String  DFS_CLIENT_READ_PREFETCH_SIZE_KEY = "dfs.client.read.prefetch.size"; 
  public static final String  DFS_CLIENT_RETRY_WINDOW_BASE= "dfs.client.retry.window.base";
  public static final String  DFS_METRICS_SESSION_ID_KEY = "dfs.metrics.session-id";
  public static final String  DFS_METRICS_PERCENTILES_INTERVALS_KEY = "dfs.metrics.percentiles.intervals";
  public static final String  DFS_DATANODE_HOST_NAME_KEY = "dfs.datanode.hostname";
  public static final String  DFS_NAMENODE_HOSTS_KEY = "dfs.namenode.hosts";
  public static final String  DFS_NAMENODE_HOSTS_EXCLUDE_KEY = "dfs.namenode.hosts.exclude";
  public static final String  DFS_CLIENT_SOCKET_TIMEOUT_KEY = "dfs.client.socket-timeout";
  public static final String  DFS_NAMENODE_CHECKPOINT_DIR_KEY = "dfs.namenode.checkpoint.dir";
  public static final String  DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY = "dfs.namenode.checkpoint.edits.dir";
  public static final String  DFS_HOSTS = "dfs.hosts";
  public static final String  DFS_HOSTS_EXCLUDE = "dfs.hosts.exclude";
  public static final String  DFS_CLIENT_LOCAL_INTERFACES = "dfs.client.local.interfaces";
  public static final String  DFS_NAMENODE_AUDIT_LOGGERS_KEY = "dfs.namenode.audit.loggers";
  public static final String  DFS_NAMENODE_DEFAULT_AUDIT_LOGGER_NAME = "default";
  public static final String  DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY = "dfs.namenode.audit.log.token.tracking.id";
  public static final boolean DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT = false;
  public static final String  DFS_NAMENODE_AUDIT_LOG_ASYNC_KEY = "dfs.namenode.audit.log.async";
  public static final boolean DFS_NAMENODE_AUDIT_LOG_ASYNC_DEFAULT = false;

  // Much code in hdfs is not yet updated to use these keys.
  public static final String  DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY = "dfs.client.block.write.locateFollowingBlock.retries";
  public static final int     DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT = 5;
  public static final String  DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY = "dfs.client.block.write.retries";
  public static final int     DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT = 3;
  public static final String  DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY = "dfs.client.max.block.acquire.failures";
  public static final int     DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT = 3;
  public static final String  DFS_CLIENT_USE_LEGACY_BLOCKREADER = "dfs.client.use.legacy.blockreader";
  public static final boolean DFS_CLIENT_USE_LEGACY_BLOCKREADER_DEFAULT = false;
  public static final String  DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL = "dfs.client.use.legacy.blockreader.local";
  public static final boolean DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT = false;
  public static final String  DFS_BALANCER_MOVEDWINWIDTH_KEY = "dfs.balancer.movedWinWidth";
  public static final long    DFS_BALANCER_MOVEDWINWIDTH_DEFAULT = 5400*1000L;
  public static final String  DFS_BALANCER_MOVERTHREADS_KEY = "dfs.balancer.moverThreads";
  public static final int     DFS_BALANCER_MOVERTHREADS_DEFAULT = 1000;
  public static final String  DFS_BALANCER_DISPATCHERTHREADS_KEY = "dfs.balancer.dispatcherThreads";
  public static final int     DFS_BALANCER_DISPATCHERTHREADS_DEFAULT = 200;

  public static final String  DFS_MOVER_MOVEDWINWIDTH_KEY = "dfs.mover.movedWinWidth";
  public static final long    DFS_MOVER_MOVEDWINWIDTH_DEFAULT = 5400*1000L;
  public static final String  DFS_MOVER_MOVERTHREADS_KEY = "dfs.mover.moverThreads";
  public static final int     DFS_MOVER_MOVERTHREADS_DEFAULT = 1000;
  public static final String  DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY = "dfs.mover.retry.max.attempts";
  public static final int     DFS_MOVER_RETRY_MAX_ATTEMPTS_DEFAULT = 10;

  public static final String  DFS_DATANODE_ADDRESS_KEY = "dfs.datanode.address";
  public static final int     DFS_DATANODE_DEFAULT_PORT = 50010;
  public static final String  DFS_DATANODE_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_DATANODE_DEFAULT_PORT;
  public static final String  DFS_DATANODE_DATA_DIR_PERMISSION_KEY = "dfs.datanode.data.dir.perm";
  public static final String  DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT = "700";
  public static final String  DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY = "dfs.datanode.directoryscan.interval";
  public static final int     DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT = 21600;
  public static final String  DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY = "dfs.datanode.directoryscan.threads";
  public static final int     DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT = 1;
  public static final String  DFS_DATANODE_DNS_INTERFACE_KEY = "dfs.datanode.dns.interface";
  public static final String  DFS_DATANODE_DNS_INTERFACE_DEFAULT = "default";
  public static final String  DFS_DATANODE_DNS_NAMESERVER_KEY = "dfs.datanode.dns.nameserver";
  public static final String  DFS_DATANODE_DNS_NAMESERVER_DEFAULT = "default";
  public static final String  DFS_DATANODE_DU_RESERVED_KEY = "dfs.datanode.du.reserved";
  public static final long    DFS_DATANODE_DU_RESERVED_DEFAULT = 0;
  public static final String  DFS_DATANODE_HANDLER_COUNT_KEY = "dfs.datanode.handler.count";
  public static final int     DFS_DATANODE_HANDLER_COUNT_DEFAULT = 10;
  public static final String  DFS_DATANODE_HTTP_ADDRESS_KEY = "dfs.datanode.http.address";
  public static final int     DFS_DATANODE_HTTP_DEFAULT_PORT = 50075;
  public static final String  DFS_DATANODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_DATANODE_HTTP_DEFAULT_PORT;
  public static final String  DFS_DATANODE_MAX_RECEIVER_THREADS_KEY = "dfs.datanode.max.transfer.threads";
  public static final int     DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT = 4096;
  public static final String  DFS_DATANODE_SCAN_PERIOD_HOURS_KEY = "dfs.datanode.scan.period.hours";
  public static final int     DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT = 21 * 24;  // 3 weeks.
  public static final String  DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND = "dfs.block.scanner.volume.bytes.per.second";
  public static final long    DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND_DEFAULT = 1048576L;
  public static final String  DFS_DATANODE_TRANSFERTO_ALLOWED_KEY = "dfs.datanode.transferTo.allowed";
  public static final boolean DFS_DATANODE_TRANSFERTO_ALLOWED_DEFAULT = true;
  public static final String  DFS_HEARTBEAT_INTERVAL_KEY = "dfs.heartbeat.interval";
  public static final long    DFS_HEARTBEAT_INTERVAL_DEFAULT = 3;
  public static final String  DFS_NAMENODE_PATH_BASED_CACHE_RETRY_INTERVAL_MS = "dfs.namenode.path.based.cache.retry.interval.ms";
  public static final long    DFS_NAMENODE_PATH_BASED_CACHE_RETRY_INTERVAL_MS_DEFAULT = 30000L;
  public static final String  DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY = "dfs.namenode.decommission.interval";
  public static final int     DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT = 30;
  public static final String  DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY = "dfs.namenode.decommission.blocks.per.interval";
  public static final int     DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT = 500000;
  public static final String  DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES = "dfs.namenode.decommission.max.concurrent.tracked.nodes";
  public static final int     DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES_DEFAULT = 100;
  public static final String  DFS_NAMENODE_HANDLER_COUNT_KEY = "dfs.namenode.handler.count";
  public static final int     DFS_NAMENODE_HANDLER_COUNT_DEFAULT = 10;
  public static final String  DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY = "dfs.namenode.service.handler.count";
  public static final int     DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT = 10;
  public static final String  DFS_SUPPORT_APPEND_KEY = "dfs.support.append";
  public static final boolean DFS_SUPPORT_APPEND_DEFAULT = true;
  public static final String  DFS_HTTPS_ENABLE_KEY = "dfs.https.enable";
  public static final boolean DFS_HTTPS_ENABLE_DEFAULT = false;
  public static final String  DFS_HTTP_POLICY_KEY = "dfs.http.policy";
  public static final String  DFS_HTTP_POLICY_DEFAULT =  HttpConfig.Policy.HTTP_ONLY.name();
  public static final String  DFS_DEFAULT_CHUNK_VIEW_SIZE_KEY = "dfs.default.chunk.view.size";
  public static final int     DFS_DEFAULT_CHUNK_VIEW_SIZE_DEFAULT = 32*1024;
  public static final String  DFS_DATANODE_HTTPS_ADDRESS_KEY = "dfs.datanode.https.address";
  public static final String  DFS_DATANODE_HTTPS_PORT_KEY = "datanode.https.port";
  public static final int     DFS_DATANODE_HTTPS_DEFAULT_PORT = 50475;
  public static final String  DFS_DATANODE_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_DATANODE_HTTPS_DEFAULT_PORT;
  public static final String  DFS_DATANODE_IPC_ADDRESS_KEY = "dfs.datanode.ipc.address";
  public static final int     DFS_DATANODE_IPC_DEFAULT_PORT = 50020;
  public static final String  DFS_DATANODE_IPC_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_DATANODE_IPC_DEFAULT_PORT;
  public static final String  DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_KEY = "dfs.datanode.min.supported.namenode.version";
  public static final String  DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_DEFAULT = "2.1.0-beta";
  public static final String  DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY = "dfs.namenode.inode.attributes.provider.class";
  public static final String  DFS_DATANODE_BP_READY_TIMEOUT_KEY = "dfs.datanode.bp-ready.timeout";
  public static final long    DFS_DATANODE_BP_READY_TIMEOUT_DEFAULT = 20;

  public static final String  DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY = "dfs.block.access.token.enable";
  public static final boolean DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT = false;
  public static final String  DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY = "dfs.block.access.key.update.interval";
  public static final long    DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_DEFAULT = 600L;
  public static final String  DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY = "dfs.block.access.token.lifetime";
  public static final long    DFS_BLOCK_ACCESS_TOKEN_LIFETIME_DEFAULT = 600L;

  public static final String DFS_BLOCK_REPLICATOR_CLASSNAME_KEY = "dfs.block.replicator.classname";
  public static final Class<BlockPlacementPolicyDefault> DFS_BLOCK_REPLICATOR_CLASSNAME_DEFAULT = BlockPlacementPolicyDefault.class;
  public static final String  DFS_REPLICATION_MAX_KEY = "dfs.replication.max";
  public static final int     DFS_REPLICATION_MAX_DEFAULT = 512;

  public static final String  DFS_DF_INTERVAL_KEY = "dfs.df.interval";
  public static final int     DFS_DF_INTERVAL_DEFAULT = 60000;
  public static final String  DFS_BLOCKREPORT_INTERVAL_MSEC_KEY = "dfs.blockreport.intervalMsec";
  public static final long    DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT = 6 * 60 * 60 * 1000;
  public static final String  DFS_BLOCKREPORT_INITIAL_DELAY_KEY = "dfs.blockreport.initialDelay";
  public static final int     DFS_BLOCKREPORT_INITIAL_DELAY_DEFAULT = 0;
  public static final String  DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY = "dfs.blockreport.split.threshold";
  public static final long    DFS_BLOCKREPORT_SPLIT_THRESHOLD_DEFAULT = 1000 * 1000;
  public static final String  DFS_CACHEREPORT_INTERVAL_MSEC_KEY = "dfs.cachereport.intervalMsec";
  public static final long    DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT = 10 * 1000;
  public static final String  DFS_BLOCK_INVALIDATE_LIMIT_KEY = "dfs.block.invalidate.limit";
  public static final int     DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT = 1000;
  public static final String  DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED_KEY = "dfs.corruptfilesreturned.max";
  public static final int     DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED = 500;
  /* Maximum number of blocks to process for initializing replication queues */
  public static final String  DFS_BLOCK_MISREPLICATION_PROCESSING_LIMIT = "dfs.block.misreplication.processing.limit";
  public static final int     DFS_BLOCK_MISREPLICATION_PROCESSING_LIMIT_DEFAULT = 10000;
  public static final String DFS_CLIENT_READ_SHORTCIRCUIT_KEY = "dfs.client.read.shortcircuit";
  public static final boolean DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT = false;
  public static final String DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY = "dfs.client.read.shortcircuit.skip.checksum";
  public static final boolean DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_DEFAULT = false;
  public static final String DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_KEY = "dfs.client.read.shortcircuit.buffer.size";
  public static final int DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_DEFAULT = 1024 * 1024;
  public static final String DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_KEY = "dfs.client.read.shortcircuit.streams.cache.size";
  public static final int DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_DEFAULT = 256;
  public static final String DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_KEY = "dfs.client.read.shortcircuit.streams.cache.expiry.ms";
  public static final long DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_DEFAULT = 5 * 60 * 1000;
  public static final String DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC = "dfs.client.domain.socket.data.traffic";
  public static final boolean DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT = false;
  public static final String DFS_CLIENT_MMAP_ENABLED= "dfs.client.mmap.enabled";
  public static final boolean DFS_CLIENT_MMAP_ENABLED_DEFAULT = true;
  public static final String DFS_CLIENT_MMAP_CACHE_SIZE = "dfs.client.mmap.cache.size";
  public static final int DFS_CLIENT_MMAP_CACHE_SIZE_DEFAULT = 256;
  public static final String DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS = "dfs.client.mmap.cache.timeout.ms";
  public static final long DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS_DEFAULT  = 60 * 60 * 1000;
  public static final String DFS_CLIENT_MMAP_RETRY_TIMEOUT_MS = "dfs.client.mmap.retry.timeout.ms";
  public static final long DFS_CLIENT_MMAP_RETRY_TIMEOUT_MS_DEFAULT = 5 * 60 * 1000;
  public static final String DFS_CLIENT_SHORT_CIRCUIT_REPLICA_STALE_THRESHOLD_MS = "dfs.client.short.circuit.replica.stale.threshold.ms";
  public static final long DFS_CLIENT_SHORT_CIRCUIT_REPLICA_STALE_THRESHOLD_MS_DEFAULT = 30 * 60 * 1000;

  // property for fsimage compression
  public static final String DFS_IMAGE_COMPRESS_KEY = "dfs.image.compress";
  public static final boolean DFS_IMAGE_COMPRESS_DEFAULT = false;
  public static final String DFS_IMAGE_COMPRESSION_CODEC_KEY =
                                   "dfs.image.compression.codec";
  public static final String DFS_IMAGE_COMPRESSION_CODEC_DEFAULT =
                                   "org.apache.hadoop.io.compress.DefaultCodec";

  public static final String DFS_IMAGE_TRANSFER_RATE_KEY =
                                           "dfs.image.transfer.bandwidthPerSec";
  public static final long DFS_IMAGE_TRANSFER_RATE_DEFAULT = 0;  //no throttling

  // Image transfer timeout
  public static final String DFS_IMAGE_TRANSFER_TIMEOUT_KEY = "dfs.image.transfer.timeout";
  public static final int DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT = 60 * 1000;

  // Image transfer chunksize
  public static final String DFS_IMAGE_TRANSFER_CHUNKSIZE_KEY = "dfs.image.transfer.chunksize";
  public static final int DFS_IMAGE_TRANSFER_CHUNKSIZE_DEFAULT = 64 * 1024;

  //Keys with no defaults
  public static final String  DFS_DATANODE_PLUGINS_KEY = "dfs.datanode.plugins";
  public static final String  DFS_DATANODE_FSDATASET_FACTORY_KEY = "dfs.datanode.fsdataset.factory";
  public static final String  DFS_DATANODE_FSDATASET_VOLUME_CHOOSING_POLICY_KEY = "dfs.datanode.fsdataset.volume.choosing.policy";
  public static final String  DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_KEY = "dfs.datanode.available-space-volume-choosing-policy.balanced-space-threshold";
  public static final long    DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_DEFAULT = 1024L * 1024L * 1024L * 10L; // 10 GB
  public static final String  DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY = "dfs.datanode.available-space-volume-choosing-policy.balanced-space-preference-fraction";
  public static final float   DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT = 0.75f;
  public static final String  DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY = "dfs.datanode.socket.write.timeout";
  public static final String  DFS_DATANODE_STARTUP_KEY = "dfs.datanode.startup";
  public static final String  DFS_NAMENODE_PLUGINS_KEY = "dfs.namenode.plugins";
  public static final String  DFS_WEB_UGI_KEY = "dfs.web.ugi";
  public static final String  DFS_NAMENODE_STARTUP_KEY = "dfs.namenode.startup";
  public static final String  DFS_DATANODE_KEYTAB_FILE_KEY = "dfs.datanode.keytab.file";
  public static final String  DFS_DATANODE_KERBEROS_PRINCIPAL_KEY = "dfs.datanode.kerberos.principal";
  @Deprecated
  public static final String  DFS_DATANODE_USER_NAME_KEY = DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
  public static final String  DFS_DATANODE_SHARED_FILE_DESCRIPTOR_PATHS = "dfs.datanode.shared.file.descriptor.paths";
  public static final String  DFS_DATANODE_SHARED_FILE_DESCRIPTOR_PATHS_DEFAULT = "/dev/shm,/tmp";
  public static final String  DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS = "dfs.short.circuit.shared.memory.watcher.interrupt.check.ms";
  public static final int     DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT = 60000;
  public static final String  DFS_NAMENODE_KEYTAB_FILE_KEY = "dfs.namenode.keytab.file";
  public static final String  DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY = "dfs.namenode.kerberos.principal";
  @Deprecated
  public static final String  DFS_NAMENODE_USER_NAME_KEY = DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
  public static final String  DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY = "dfs.namenode.kerberos.internal.spnego.principal";
  @Deprecated
  public static final String  DFS_NAMENODE_INTERNAL_SPNEGO_USER_NAME_KEY = DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY;
  public static final String  DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY = "dfs.secondary.namenode.keytab.file";
  public static final String  DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL_KEY = "dfs.secondary.namenode.kerberos.principal";
  @Deprecated
  public static final String  DFS_SECONDARY_NAMENODE_USER_NAME_KEY = DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL_KEY;
  public static final String  DFS_SECONDARY_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY = "dfs.secondary.namenode.kerberos.internal.spnego.principal";
  @Deprecated
  public static final String  DFS_SECONDARY_NAMENODE_INTERNAL_SPNEGO_USER_NAME_KEY = DFS_SECONDARY_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY;
  public static final String  DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY = "dfs.namenode.name.cache.threshold";
  public static final int     DFS_NAMENODE_NAME_CACHE_THRESHOLD_DEFAULT = 10;
  public static final String  DFS_NAMENODE_LEGACY_OIV_IMAGE_DIR_KEY = "dfs.namenode.legacy-oiv-image.dir";
  
  public static final String  DFS_NAMESERVICES = "dfs.nameservices";
  public static final String  DFS_NAMESERVICE_ID = "dfs.nameservice.id";
  public static final String  DFS_INTERNAL_NAMESERVICES_KEY = "dfs.internal.nameservices";
  public static final String  DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY = "dfs.namenode.resource.check.interval";
  public static final int     DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT = 5000;
  public static final String  DFS_NAMENODE_DU_RESERVED_KEY = "dfs.namenode.resource.du.reserved";
  public static final long    DFS_NAMENODE_DU_RESERVED_DEFAULT = 1024 * 1024 * 100; // 100 MB
  public static final String  DFS_NAMENODE_CHECKED_VOLUMES_KEY = "dfs.namenode.resource.checked.volumes";
  public static final String  DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY = "dfs.namenode.resource.checked.volumes.minimum";
  public static final int     DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT = 1;
  public static final String  DFS_WEB_AUTHENTICATION_SIMPLE_ANONYMOUS_ALLOWED = "dfs.web.authentication.simple.anonymous.allowed";
  public static final String  DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY = "dfs.web.authentication.kerberos.principal";
  public static final String  DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY = "dfs.web.authentication.kerberos.keytab";
  public static final String  DFS_NAMENODE_MAX_OP_SIZE_KEY = "dfs.namenode.max.op.size";
  public static final int     DFS_NAMENODE_MAX_OP_SIZE_DEFAULT = 50 * 1024 * 1024;
  
  public static final String DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY = "dfs.block.local-path-access.user";
  public static final String DFS_DOMAIN_SOCKET_PATH_KEY = "dfs.domain.socket.path";
  public static final String DFS_DOMAIN_SOCKET_PATH_DEFAULT = "";

  public static final String  DFS_STORAGE_POLICY_ENABLED_KEY = "dfs.storage.policy.enabled";
  public static final boolean DFS_STORAGE_POLICY_ENABLED_DEFAULT = true;

  public static final String  DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY = "dfs.quota.by.storage.type.enabled";
  public static final boolean DFS_QUOTA_BY_STORAGETYPE_ENABLED_DEFAULT = true;

  // HA related configuration
  public static final String DFS_HA_NAMENODES_KEY_PREFIX = "dfs.ha.namenodes";
  public static final String DFS_HA_NAMENODE_ID_KEY = "dfs.ha.namenode.id";
  public static final String  DFS_HA_STANDBY_CHECKPOINTS_KEY = "dfs.ha.standby.checkpoints";
  public static final boolean DFS_HA_STANDBY_CHECKPOINTS_DEFAULT = true;
  public static final String DFS_HA_LOGROLL_PERIOD_KEY = "dfs.ha.log-roll.period";
  public static final int DFS_HA_LOGROLL_PERIOD_DEFAULT = 2 * 60; // 2m
  public static final String DFS_HA_TAILEDITS_PERIOD_KEY = "dfs.ha.tail-edits.period";
  public static final int DFS_HA_TAILEDITS_PERIOD_DEFAULT = 60; // 1m
  public static final String DFS_HA_LOGROLL_RPC_TIMEOUT_KEY = "dfs.ha.log-roll.rpc.timeout";
  public static final int DFS_HA_LOGROLL_RPC_TIMEOUT_DEFAULT = 20000; // 20s
  public static final String DFS_HA_FENCE_METHODS_KEY = "dfs.ha.fencing.methods";
  public static final String DFS_HA_AUTO_FAILOVER_ENABLED_KEY = "dfs.ha.automatic-failover.enabled";
  public static final boolean DFS_HA_AUTO_FAILOVER_ENABLED_DEFAULT = false;
  public static final String DFS_HA_ZKFC_PORT_KEY = "dfs.ha.zkfc.port";
  public static final int DFS_HA_ZKFC_PORT_DEFAULT = 8019;
  
  // Security-related configs
  public static final String DFS_ENCRYPT_DATA_TRANSFER_KEY = "dfs.encrypt.data.transfer";
  public static final boolean DFS_ENCRYPT_DATA_TRANSFER_DEFAULT = false;
  public static final String DFS_ENCRYPT_DATA_TRANSFER_CIPHER_KEY_BITLENGTH_KEY = "dfs.encrypt.data.transfer.cipher.key.bitlength";
  public static final int    DFS_ENCRYPT_DATA_TRANSFER_CIPHER_KEY_BITLENGTH_DEFAULT = 128;
  public static final String DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY = "dfs.encrypt.data.transfer.cipher.suites";
  public static final String DFS_DATA_ENCRYPTION_ALGORITHM_KEY = "dfs.encrypt.data.transfer.algorithm";
  public static final String DFS_TRUSTEDCHANNEL_RESOLVER_CLASS = "dfs.trustedchannel.resolver.class";
  public static final String DFS_DATA_TRANSFER_PROTECTION_KEY = "dfs.data.transfer.protection";
  public static final String DFS_DATA_TRANSFER_PROTECTION_DEFAULT = "";
  public static final String DFS_DATA_TRANSFER_SASL_PROPS_RESOLVER_CLASS_KEY = "dfs.data.transfer.saslproperties.resolver.class";
  public static final int    DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES_DEFAULT = 100;
  public static final String DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES = "dfs.namenode.list.encryption.zones.num.responses";
  public static final String DFS_ENCRYPTION_KEY_PROVIDER_URI = "dfs.encryption.key.provider.uri";

  // Journal-node related configs. These are read on the JN side.
  public static final String  DFS_JOURNALNODE_EDITS_DIR_KEY = "dfs.journalnode.edits.dir";
  public static final String  DFS_JOURNALNODE_EDITS_DIR_DEFAULT = "/tmp/hadoop/dfs/journalnode/";
  public static final String  DFS_JOURNALNODE_RPC_ADDRESS_KEY = "dfs.journalnode.rpc-address";
  public static final int     DFS_JOURNALNODE_RPC_PORT_DEFAULT = 8485;
  public static final String  DFS_JOURNALNODE_RPC_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_JOURNALNODE_RPC_PORT_DEFAULT;
    
  public static final String  DFS_JOURNALNODE_HTTP_ADDRESS_KEY = "dfs.journalnode.http-address";
  public static final int     DFS_JOURNALNODE_HTTP_PORT_DEFAULT = 8480;
  public static final String  DFS_JOURNALNODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_JOURNALNODE_HTTP_PORT_DEFAULT;
  public static final String  DFS_JOURNALNODE_HTTPS_ADDRESS_KEY = "dfs.journalnode.https-address";
  public static final int     DFS_JOURNALNODE_HTTPS_PORT_DEFAULT = 8481;
  public static final String  DFS_JOURNALNODE_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_JOURNALNODE_HTTPS_PORT_DEFAULT;

  public static final String  DFS_JOURNALNODE_KEYTAB_FILE_KEY = "dfs.journalnode.keytab.file";
  public static final String  DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY = "dfs.journalnode.kerberos.principal";
  public static final String  DFS_JOURNALNODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY = "dfs.journalnode.kerberos.internal.spnego.principal";

  // Journal-node related configs for the client side.
  public static final String  DFS_QJOURNAL_QUEUE_SIZE_LIMIT_KEY = "dfs.qjournal.queued-edits.limit.mb";
  public static final int     DFS_QJOURNAL_QUEUE_SIZE_LIMIT_DEFAULT = 10;
  
  // Quorum-journal timeouts for various operations. Unlikely to need
  // to be tweaked, but configurable just in case.
  public static final String  DFS_QJOURNAL_START_SEGMENT_TIMEOUT_KEY = "dfs.qjournal.start-segment.timeout.ms";
  public static final String  DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_KEY = "dfs.qjournal.prepare-recovery.timeout.ms";
  public static final String  DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_KEY = "dfs.qjournal.accept-recovery.timeout.ms";
  public static final String  DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_KEY = "dfs.qjournal.finalize-segment.timeout.ms";
  public static final String  DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_KEY = "dfs.qjournal.select-input-streams.timeout.ms";
  public static final String  DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_KEY = "dfs.qjournal.get-journal-state.timeout.ms";
  public static final String  DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_KEY = "dfs.qjournal.new-epoch.timeout.ms";
  public static final String  DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_KEY = "dfs.qjournal.write-txns.timeout.ms";
  public static final int     DFS_QJOURNAL_START_SEGMENT_TIMEOUT_DEFAULT = 20000;
  public static final int     DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_DEFAULT = 20000;
  public static final int     DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_DEFAULT = 20000;
  
  public static final String DFS_MAX_NUM_BLOCKS_TO_LOG_KEY = "dfs.namenode.max-num-blocks-to-log";
  public static final long   DFS_MAX_NUM_BLOCKS_TO_LOG_DEFAULT = 1000l;
  
  public static final String DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY = "dfs.namenode.enable.retrycache";
  public static final boolean DFS_NAMENODE_ENABLE_RETRY_CACHE_DEFAULT = true;
  public static final String DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY = "dfs.namenode.retrycache.expirytime.millis";
  public static final long DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT = 600000; // 10 minutes
  public static final String DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_KEY = "dfs.namenode.retrycache.heap.percent";
  public static final float DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_DEFAULT = 0.03f;
  
  // The number of NN response dropped by client proactively in each RPC call.
  // For testing NN retry cache, we can set this property with positive value.
  public static final String DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY = "dfs.client.test.drop.namenode.response.number";
  public static final int DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_DEFAULT = 0;


  // Hidden configuration undocumented in hdfs-site. xml
  // Timeout to wait for block receiver and responder thread to stop
  public static final String DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_KEY = "dfs.datanode.xceiver.stop.timeout.millis";
  public static final long   DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT = 60000;

  // WebHDFS retry policy
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY = "dfs.http.client.retry.policy.enabled";
  public static final boolean DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_DEFAULT = false;
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_SPEC_KEY = "dfs.http.client.retry.policy.spec";
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_SPEC_DEFAULT = "10000,6,60000,10"; //t1,n1,t2,n2,...
  public static final String  DFS_HTTP_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY = "dfs.http.client.failover.max.attempts";
  public static final int     DFS_HTTP_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT = 15;
  public static final String  DFS_HTTP_CLIENT_RETRY_MAX_ATTEMPTS_KEY = "dfs.http.client.retry.max.attempts";
  public static final int     DFS_HTTP_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT = 10;
  public static final String  DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY = "dfs.http.client.failover.sleep.base.millis";
  public static final int     DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT = 500;
  public static final String  DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY = "dfs.http.client.failover.sleep.max.millis";
  public static final int     DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT = 15000;
  
  // Handling unresolved DN topology mapping
  public static final String  DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_KEY = 
      "dfs.namenode.reject-unresolved-dn-topology-mapping";
  public static final boolean DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_DEFAULT =
      false;
  
  // hedged read properties
  public static final String DFS_DFSCLIENT_HEDGED_READ_THRESHOLD_MILLIS =
      "dfs.client.hedged.read.threshold.millis";
  public static final long DEFAULT_DFSCLIENT_HEDGED_READ_THRESHOLD_MILLIS =
      500;

  public static final String DFS_DFSCLIENT_HEDGED_READ_THREADPOOL_SIZE =
      "dfs.client.hedged.read.threadpool.size";
  public static final int     DEFAULT_DFSCLIENT_HEDGED_READ_THREADPOOL_SIZE = 0;

  // Slow io warning log threshold settings for dfsclient and datanode.
  public static final String DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY =
    "dfs.client.slow.io.warning.threshold.ms";
  public static final long DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT = 30000;
  public static final String DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY =
    "dfs.datanode.slow.io.warning.threshold.ms";
  public static final long DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT = 300;

  public static final String DFS_NAMENODE_INOTIFY_MAX_EVENTS_PER_RPC_KEY =
      "dfs.namenode.inotify.max.events.per.rpc";
  public static final int DFS_NAMENODE_INOTIFY_MAX_EVENTS_PER_RPC_DEFAULT =
      1000;

  public static final String DFS_DATANODE_BLOCK_ID_LAYOUT_UPGRADE_THREADS_KEY =
      "dfs.datanode.block.id.layout.upgrade.threads";
  public static final int DFS_DATANODE_BLOCK_ID_LAYOUT_UPGRADE_THREADS = 12;
  public static final String IGNORE_SECURE_PORTS_FOR_TESTING_KEY =
      "ignore.secure.ports.for.testing";
  public static final boolean IGNORE_SECURE_PORTS_FOR_TESTING_DEFAULT = false;

  // nntop Configurations
  public static final String NNTOP_ENABLED_KEY =
      "dfs.namenode.top.enabled";
  public static final boolean NNTOP_ENABLED_DEFAULT = true;
  public static final String NNTOP_BUCKETS_PER_WINDOW_KEY =
      "dfs.namenode.top.window.num.buckets";
  public static final int NNTOP_BUCKETS_PER_WINDOW_DEFAULT = 10;
  public static final String NNTOP_NUM_USERS_KEY =
      "dfs.namenode.top.num.users";
  public static final int NNTOP_NUM_USERS_DEFAULT = 10;
  // comma separated list of nntop reporting periods in minutes
  public static final String NNTOP_WINDOWS_MINUTES_KEY =
      "dfs.namenode.top.windows.minutes";
  public static final String[] NNTOP_WINDOWS_MINUTES_DEFAULT = {"1","5","25"};
  public static final String DFS_PIPELINE_ECN_ENABLED = "dfs.pipeline.ecn";
  public static final boolean DFS_PIPELINE_ECN_ENABLED_DEFAULT = false;

  // Key Provider Cache Expiry
  public static final String DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_MS =
      "dfs.client.key.provider.cache.expiry";
  // 10 days
  public static final long DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT =
      TimeUnit.DAYS.toMillis(10);
  public static final String DFS_DATANODE_BLOCK_PINNING_ENABLED = 
    "dfs.datanode.block-pinning.enabled";
  public static final boolean DFS_DATANODE_BLOCK_PINNING_ENABLED_DEFAULT =
    false;
}
