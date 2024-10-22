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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyRackFaultTolerant;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.RamDiskReplicaLruTracker;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.ReservedSpaceCalculator;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.http.HttpConfig;

import java.util.concurrent.TimeUnit;

/** 
 * This class contains constants for configuration keys and default values
 * used in hdfs.
 */
@InterfaceAudience.Private
public class DFSConfigKeys extends CommonConfigurationKeys {
  public static final String  DFS_BLOCK_SIZE_KEY =
      HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY;
  public static final long    DFS_BLOCK_SIZE_DEFAULT =
      HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
  public static final String  DFS_REPLICATION_KEY =
      HdfsClientConfigKeys.DFS_REPLICATION_KEY;
  public static final short   DFS_REPLICATION_DEFAULT =
      HdfsClientConfigKeys.DFS_REPLICATION_DEFAULT;

  public static final String  DFS_STREAM_BUFFER_SIZE_KEY = "dfs.stream-buffer-size";
  public static final int     DFS_STREAM_BUFFER_SIZE_DEFAULT = 4096;
  public static final String  DFS_BYTES_PER_CHECKSUM_KEY =
      HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
  public static final int     DFS_BYTES_PER_CHECKSUM_DEFAULT =
      HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
  @Deprecated
  public static final String  DFS_USER_HOME_DIR_PREFIX_KEY =
      HdfsClientConfigKeys.DFS_USER_HOME_DIR_PREFIX_KEY;
  @Deprecated
  public static final String  DFS_USER_HOME_DIR_PREFIX_DEFAULT =
      HdfsClientConfigKeys.DFS_USER_HOME_DIR_PREFIX_DEFAULT;
  public static final String  DFS_CHECKSUM_TYPE_KEY = HdfsClientConfigKeys
      .DFS_CHECKSUM_TYPE_KEY;
  public static final String  DFS_CHECKSUM_TYPE_DEFAULT =
      HdfsClientConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT;
  @Deprecated
  public static final String DFS_WEBHDFS_ACL_PERMISSION_PATTERN_KEY =
      HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_KEY;
  @Deprecated
  public static final String DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT =
      HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT;
  public static final String  DFS_WEBHDFS_NETTY_LOW_WATERMARK =
      "dfs.webhdfs.netty.low.watermark";
  public static final int  DFS_WEBHDFS_NETTY_LOW_WATERMARK_DEFAULT = 32768;
  public static final String  DFS_WEBHDFS_NETTY_HIGH_WATERMARK =
      "dfs.webhdfs.netty.high.watermark";
  public static final int  DFS_WEBHDFS_NETTY_HIGH_WATERMARK_DEFAULT = 65535;
  public static final String  DFS_WEBHDFS_UGI_EXPIRE_AFTER_ACCESS_KEY =
      "dfs.webhdfs.ugi.expire.after.access";
  public static final int     DFS_WEBHDFS_UGI_EXPIRE_AFTER_ACCESS_DEFAULT =
      10*60*1000; //10 minutes
  public static final String DFS_WEBHDFS_USE_IPC_CALLQ =
      "dfs.webhdfs.use.ipc.callq";
  public static final boolean DFS_WEBHDFS_USE_IPC_CALLQ_DEFAULT = true;

  // HA related configuration
  public static final String  DFS_DATANODE_RESTART_REPLICA_EXPIRY_KEY = "dfs.datanode.restart.replica.expiration";
  public static final long    DFS_DATANODE_RESTART_REPLICA_EXPIRY_DEFAULT = 50;
  public static final String  DFS_NAMENODE_BACKUP_ADDRESS_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_BACKUP_ADDRESS_DEFAULT = "localhost:50100";
  public static final String  DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT = "0.0.0.0:50105";
  public static final String  DFS_NAMENODE_BACKUP_SERVICE_RPC_ADDRESS_KEY = "dfs.namenode.backup.dnrpc-address";
  public static final String DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS =
      HdfsClientConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS;
  public static final String DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS_DEFAULT = "0.0.0.0:50200";
  public static final String DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_BIND_HOST = "dfs.provided.aliasmap.inmemory.rpc.bind-host";

  public static final String DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR = "dfs.provided.aliasmap.inmemory.leveldb.dir";
  public static final String DFS_PROVIDED_ALIASMAP_INMEMORY_BATCH_SIZE = "dfs.provided.aliasmap.inmemory.batch-size";
  public static final int DFS_PROVIDED_ALIASMAP_INMEMORY_BATCH_SIZE_DEFAULT = 500;
  public static final String DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED = "dfs.provided.aliasmap.inmemory.enabled";
  public static final boolean DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED_DEFAULT = false;
  public static final String DFS_PROVIDED_ALIASMAP_INMEMORY_SERVER_LOG = "dfs.provided.aliasmap.inmemory.server.log";
  public static final boolean DFS_PROVIDED_ALIASMAP_INMEMORY_SERVER_LOG_DEFAULT = false;

  public static final String  DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY;
  public static final long    DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT =
      100 * 1024*1024;
  public static final String  DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY
      = "dfs.datanode.balance.max.concurrent.moves";
  public static final int
      DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT = 100;
  public static final String DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY =
      "dfs.datanode.data.transfer.bandwidthPerSec";
  public static final long DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_DEFAULT =
      0; // A value of zero indicates no limit
  public static final String DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY =
      "dfs.datanode.data.write.bandwidthPerSec";
  // A value of zero indicates no limit
  public static final long DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_DEFAULT = 0;
  public static final String DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY =
      "dfs.datanode.data.read.bandwidthPerSec";
  // A value of zero indicates no limit
  public static final long DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_DEFAULT = 0;
  public static final String DFS_DATANODE_EC_RECONSTRUCT_READ_BANDWIDTHPERSEC_KEY =
      "dfs.datanode.ec.reconstruct.read.bandwidthPerSec";
  public static final long DFS_DATANODE_EC_RECONSTRUCT_READ_BANDWIDTHPERSEC_DEFAULT =
      0; // A value of zero indicates no limit
  public static final String DFS_DATANODE_EC_RECONSTRUCT_WRITE_BANDWIDTHPERSEC_KEY =
      "dfs.datanode.ec.reconstruct.write.bandwidthPerSec";
  public static final long DFS_DATANODE_EC_RECONSTRUCT_WRITE_BANDWIDTHPERSEC_DEFAULT =
      0; // A value of zero indicates no limit
  @Deprecated
  public static final String  DFS_DATANODE_READAHEAD_BYTES_KEY =
      HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_KEY;
  @Deprecated
  public static final long    DFS_DATANODE_READAHEAD_BYTES_DEFAULT =
      HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT;
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
  public static final String  DFS_DATANODE_FSDATASETASYNCDISK_MAX_THREADS_PER_VOLUME_KEY =
      "dfs.datanode.fsdatasetasyncdisk.max.threads.per.volume";
  public static final int     DFS_DATANODE_FSDATASETASYNCDISK_MAX_THREADS_PER_VOLUME_DEFAULT = 4;
  public static final String  DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC = "dfs.datanode.lazywriter.interval.sec";
  public static final int     DFS_DATANODE_LAZY_WRITER_INTERVAL_DEFAULT_SEC = 60;
  public static final String  DFS_DATANODE_RAM_DISK_REPLICA_TRACKER_KEY = "dfs.datanode.ram.disk.replica.tracker";
  public static final Class<RamDiskReplicaLruTracker>  DFS_DATANODE_RAM_DISK_REPLICA_TRACKER_DEFAULT = RamDiskReplicaLruTracker.class;
  public static final String  DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_KEY = "dfs.datanode.network.counts.cache.max.size";
  public static final int     DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_DEFAULT = Integer.MAX_VALUE;
  public static final String DFS_DATANODE_NON_LOCAL_LAZY_PERSIST =
      "dfs.datanode.non.local.lazy.persist";
  public static final boolean DFS_DATANODE_NON_LOCAL_LAZY_PERSIST_DEFAULT =
      false;
  public static final String DFS_DATANODE_FIXED_VOLUME_SIZE_KEY =
      "dfs.datanode.fixed.volume.size";
  public static final boolean DFS_DATANODE_FIXED_VOLUME_SIZE_DEFAULT = false;
  public static final String  DFS_DATANODE_REPLICA_CACHE_ROOT_DIR_KEY =
      "dfs.datanode.replica.cache.root.dir";
  public static final String DFS_DATANODE_REPLICA_CACHE_EXPIRY_TIME_KEY =
      "dfs.datanode.replica.cache.expiry.time";
  public static final long DFS_DATANODE_REPLICA_CACHE_EXPIRY_TIME_DEFAULT =
      300000;

  // This setting is for testing/internal use only.
  public static final String  DFS_DATANODE_DUPLICATE_REPLICA_DELETION = "dfs.datanode.duplicate.replica.deletion";
  public static final boolean DFS_DATANODE_DUPLICATE_REPLICA_DELETION_DEFAULT = true;

  public static final String DFS_DN_CACHED_DFSUSED_CHECK_INTERVAL_MS =
      "dfs.datanode.cached-dfsused.check.interval.ms";
  public static final long DFS_DN_CACHED_DFSUSED_CHECK_INTERVAL_DEFAULT_MS =
      600000;

  public static final String  DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT =
      "dfs.namenode.path.based.cache.block.map.allocation.percent";
  public static final float    DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT_DEFAULT = 0.25f;

  public static final String DFS_NAMENODE_CRM_CHECKLOCKTIME_ENABLE =
      "dfs.namenode.crm.checklocktime.enable";
  public static final boolean DFS_NAMENODE_CRM_CHECKLOCKTIME_DEFAULT = false;

  public static final String DFS_NAMENODE_CRM_MAXLOCKTIME_MS =
      "dfs.namenode.crm.maxlocktime.ms";
  public static final long DFS_NAMENODE_CRM_MAXLOCKTIME_MS_DEFAULT = 1000;

  public static final String DFS_NAMENODE_CRM_SLEEP_TIME_MS =
      "dfs.namenode.crm.sleeptime.ms";
  public static final long DFS_NAMENODE_CRM_SLEEP_TIME_MS_DEFAULT = 300;

  public static final int     DFS_NAMENODE_HTTP_PORT_DEFAULT =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_HTTP_ADDRESS_KEY =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_NAMENODE_HTTP_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_HTTP_BIND_HOST_KEY = "dfs.namenode.http-bind-host";
  public static final String  DFS_NAMENODE_RPC_ADDRESS_KEY =
      HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_RPC_BIND_HOST_KEY = "dfs.namenode.rpc-bind-host";
  public static final String  DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY = "dfs.namenode.servicerpc-address";
  public static final String  DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY = "dfs.namenode.servicerpc-bind-host";
  public static final String  DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY =
      "dfs.namenode.lifeline.rpc-address";
  public static final String  DFS_NAMENODE_LIFELINE_RPC_BIND_HOST_KEY =
      "dfs.namenode.lifeline.rpc-bind-host";
  public static final String  DFS_NAMENODE_MAX_OBJECTS_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_MAX_OBJECTS_KEY;
  public static final long    DFS_NAMENODE_MAX_OBJECTS_DEFAULT = 0;
  public static final String  DFS_NAMENODE_SAFEMODE_EXTENSION_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY;
  public static final int     DFS_NAMENODE_SAFEMODE_EXTENSION_DEFAULT = 30000;
  public static final String  DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY;
  public static final float   DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT = 0.999f;
  // set this to a slightly smaller value than
  // DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT to populate
  // needed replication queues before exiting safe mode
  public static final String  DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY =
      "dfs.namenode.replqueue.threshold-pct";
  public static final String  DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY = "dfs.namenode.safemode.min.datanodes";
  public static final int     DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT = 0;
  public static final String  DFS_NAMENODE_SAFEMODE_RECHECK_INTERVAL_KEY =
      "dfs.namenode.safemode.recheck.interval";
  public static final long    DFS_NAMENODE_SAFEMODE_RECHECK_INTERVAL_DEFAULT =
      1000;
  public static final String  DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_DEFAULT =
      "0.0.0.0:9868";
  public static final String  DFS_NAMENODE_SECONDARY_HTTPS_ADDRESS_KEY = "dfs.namenode.secondary.https-address";
  public static final String  DFS_NAMENODE_SECONDARY_HTTPS_ADDRESS_DEFAULT =
      "0.0.0.0:9869";
  public static final String  DFS_NAMENODE_CHECKPOINT_QUIET_MULTIPLIER_KEY = "dfs.namenode.checkpoint.check.quiet-multiplier";
  public static final double  DFS_NAMENODE_CHECKPOINT_QUIET_MULTIPLIER_DEFAULT = 1.5;
  public static final String  DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY = "dfs.namenode.checkpoint.check.period";
  public static final long    DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT = 60;
  public static final String  DFS_NAMENODE_CHECKPOINT_PERIOD_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY;
  public static final long    DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT = 3600;
  public static final String  DFS_NAMENODE_CHECKPOINT_TXNS_KEY = "dfs.namenode.checkpoint.txns";
  public static final long    DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT = 1000000;
  public static final String  DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_KEY = "dfs.namenode.checkpoint.max-retries";
  public static final int     DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_DEFAULT = 3;
  public static final String  DFS_NAMENODE_MISSING_CHECKPOINT_PERIODS_BEFORE_SHUTDOWN_KEY = "dfs.namenode.missing.checkpoint.periods.before.shutdown";
  public static final int     DFS_NAMENODE_MISSING_CHECKPOINT_PERIODS_BEFORE_SHUTDOWN_DEFAULT = 3;
  public static final String  DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;
  public static final int     DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT = 5*60*1000;
  public static final String  DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY = "dfs.namenode.tolerate.heartbeat.multiplier";
  public static final int     DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT = 4;
  public static final String  DFS_NAMENODE_ACCESSTIME_PRECISION_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
  public static final long    DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT = 3600000;
  public static final String DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY;
  public static final boolean DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_DEFAULT =
      true;
  public static final String
      DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYSTORAGETYPE_KEY =
      "dfs.namenode.redundancy.considerLoadByStorageType";
  public static final boolean
      DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYSTORAGETYPE_DEFAULT = false;
  public static final String  DFS_NAMENODE_READ_CONSIDERLOAD_KEY =
      "dfs.namenode.read.considerLoad";
  public static final boolean DFS_NAMENODE_READ_CONSIDERLOAD_DEFAULT =
      false;
  public static final String DFS_NAMENODE_READ_CONSIDERSTORAGETYPE_KEY =
      "dfs.namenode.read.considerStorageType";
  public static final boolean DFS_NAMENODE_READ_CONSIDERSTORAGETYPE_DEFAULT =
      false;
  public static final String  DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_FACTOR =
      "dfs.namenode.redundancy.considerLoad.factor";
  public static final double
      DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_FACTOR_DEFAULT = 2.0;
  public static final String DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYVOLUME_KEY =
      "dfs.namenode.redundancy.considerLoadByVolume";
  public static final boolean
      DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYVOLUME_DEFAULT
      = false;
  public static final String DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY;
  public static final int DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_DEFAULT = 3;
  public static final String DFS_NAMENODE_REDUNDANCY_QUEUE_RESTART_ITERATIONS =
      "dfs.namenode.redundancy.queue.restart.iterations";
  public static final int
      DFS_NAMENODE_REDUNDANCY_QUEUE_RESTART_ITERATIONS_DEFAULT = 2400;
  public static final String  DFS_NAMENODE_REPLICATION_MIN_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_REPLICATION_MIN_KEY;
  public static final int     DFS_NAMENODE_REPLICATION_MIN_DEFAULT = 1;
  public static final String  DFS_NAMENODE_FILE_CLOSE_NUM_COMMITTED_ALLOWED_KEY
      = "dfs.namenode.file.close.num-committed-allowed";
  public static final int     DFS_NAMENODE_FILE_CLOSE_NUM_COMMITTED_ALLOWED_DEFAULT
      = 0;
  public static final String  DFS_NAMENODE_SAFEMODE_REPLICATION_MIN_KEY =
      "dfs.namenode.safemode.replication.min";

  public static final String  DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY =
      "dfs.namenode.reconstruction.pending.timeout-sec";
  public static final int
      DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_DEFAULT = 300;

  public static final String  DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_SEC_KEY =
      "dfs.namenode.excess.redundancy.timeout-sec";
  public static final long DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_SEC_DEAFULT = 3600;
  public static final String DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_CHECK_LIMIT
      = "dfs.namenode.excess.redundancy.timeout.check.limit";
  public static final long DFS_NAMENODE_EXCESS_REDUNDANCY_TIMEOUT_CHECK_LIMIT_DEFAULT = 1000;

  public static final String  DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY =
      "dfs.namenode.maintenance.replication.min";
  public static final int     DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_DEFAULT
      = 1;

  public static final String  DFS_NAMENODE_MAX_CORRUPT_FILE_BLOCKS_RETURNED_KEY = "dfs.namenode.max-corrupt-file-blocks-returned";
  public static final int     DFS_NAMENODE_MAX_CORRUPT_FILE_BLOCKS_RETURNED_DEFAULT = 100;

  public static final String  DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY;
  public static final int     DFS_NAMENODE_REPLICATION_MAX_STREAMS_DEFAULT = 2;
  public static final String  DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY = "dfs.namenode.replication.max-streams-hard-limit";
  public static final int     DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT = 4;
  public static final String DFS_NAMENODE_BLOCKREPORT_QUEUE_SIZE_KEY
      = "dfs.namenode.blockreport.queue.size";
  public static final int    DFS_NAMENODE_BLOCKREPORT_QUEUE_SIZE_DEFAULT
      = 1024;
  public static final String DFS_NAMENODE_BLOCKREPORT_MAX_LOCK_HOLD_TIME
      = "dfs.namenode.blockreport.max.lock.hold.time";
  public static final long
      DFS_NAMENODE_BLOCKREPORT_MAX_LOCK_HOLD_TIME_DEFAULT = 4;

  public static final String
      DFS_NAMENODE_CORRUPT_BLOCK_DELETE_IMMEDIATELY_ENABLED =
      "dfs.namenode.corrupt.block.delete.immediately.enabled";
  public static final boolean
      DFS_NAMENODE_CORRUPT_BLOCK_DELETE_IMMEDIATELY_ENABLED_DEFAULT = true;

  @Deprecated
  public static final String  DFS_WEBHDFS_USER_PATTERN_KEY =
      HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY;
  @Deprecated
  public static final String  DFS_WEBHDFS_USER_PATTERN_DEFAULT =
      HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT;
  public static final String  DFS_PERMISSIONS_ENABLED_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_PERMISSIONS_ENABLED_KEY;
  public static final String  DFS_PERMISSIONS_CONTENT_SUMMARY_SUBACCESS_KEY
      = "dfs.permissions.ContentSummary.subAccess";
  public static final boolean DFS_PERMISSIONS_CONTENT_SUMMARY_SUBACCESS_DEFAULT
      = false;
  public static final boolean DFS_PERMISSIONS_ENABLED_DEFAULT = true;
  public static final String  DFS_PERMISSIONS_SUPERUSERGROUP_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY;
  public static final String  DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT = "supergroup";
  public static final String DFS_PERMISSIONS_ALLOW_OWNER_SET_QUOTA_KEY
      = "dfs.permissions.allow.owner.set.quota";
  public static final boolean DFS_PERMISSIONS_ALLOW_OWNER_SET_QUOTA_DEFAULT
      = false;
  public static final String  DFS_NAMENODE_ACLS_ENABLED_KEY = "dfs.namenode.acls.enabled";
  public static final boolean DFS_NAMENODE_ACLS_ENABLED_DEFAULT = true;
  public static final String DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_KEY =
      "dfs.namenode.posix.acl.inheritance.enabled";
  public static final boolean
      DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_DEFAULT = true;
  public static final String DFS_REFORMAT_DISABLED = "dfs.reformat.disabled";
  public static final boolean DFS_REFORMAT_DISABLED_DEFAULT = false;

  public static final String  DFS_NAMENODE_XATTRS_ENABLED_KEY = "dfs.namenode.xattrs.enabled";
  public static final boolean DFS_NAMENODE_XATTRS_ENABLED_DEFAULT = true;
  public static final String  DFS_ADMIN = "dfs.cluster.administrators";
  public static final String  DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY = "dfs.https.server.keystore.resource";
  public static final String  DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_DEFAULT = "ssl-server.xml";
  public static final String  DFS_SERVER_HTTPS_KEYPASSWORD_KEY = "ssl.server.keystore.keypassword";
  public static final String  DFS_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY = "ssl.server.keystore.password";
  public static final String  DFS_SERVER_HTTPS_KEYSTORE_LOCATION_KEY = "ssl.server.keystore.location";
  public static final String  DFS_SERVER_HTTPS_TRUSTSTORE_LOCATION_KEY = "ssl.server.truststore.location";
  public static final String  DFS_SERVER_HTTPS_TRUSTSTORE_PASSWORD_KEY = "ssl.server.truststore.password";
  public static final String  DFS_NAMENODE_NAME_DIR_RESTORE_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY;
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
  public static final String  DFS_NAMENODE_QUOTA_INIT_THREADS_KEY = "dfs.namenode.quota.init-threads";
  public static final int     DFS_NAMENODE_QUOTA_INIT_THREADS_DEFAULT = 12;

  public static final String  DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD = "dfs.namenode.edit.log.autoroll.multiplier.threshold";
  public static final float
      DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT = 0.5f;
  public static final String  DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS = "dfs.namenode.edit.log.autoroll.check.interval.ms";
  public static final int     DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS_DEFAULT = 5*60*1000;

  public static final String  DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC = "dfs.namenode.lazypersist.file.scrub.interval.sec";
  public static final int     DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC_DEFAULT = 5 * 60;
  
  public static final String  DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH = "dfs.namenode.edits.noeditlogchannelflush";
  public static final boolean DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH_DEFAULT = false;

  public static final String  DFS_NAMENODE_EDITS_ASYNC_LOGGING =
      "dfs.namenode.edits.asynclogging";
  public static final boolean DFS_NAMENODE_EDITS_ASYNC_LOGGING_DEFAULT = true;
  public static final String
      DFS_NAMENODE_EDITS_ASYNC_LOGGING_PENDING_QUEUE_SIZE =
      "dfs.namenode.edits.asynclogging.pending.queue.size";
  public static final int
      DFS_NAMENODE_EDITS_ASYNC_LOGGING_PENDING_QUEUE_SIZE_DEFAULT = 4096;

  public static final String DFS_NAMENODE_PROVIDED_ENABLED = "dfs.namenode.provided.enabled";
  public static final boolean DFS_NAMENODE_PROVIDED_ENABLED_DEFAULT = false;

  public static final String DFS_PROVIDER_STORAGEUUID = "dfs.provided.storage.id";
  public static final String DFS_PROVIDER_STORAGEUUID_DEFAULT =  "DS-PROVIDED";
  public static final String DFS_PROVIDED_ALIASMAP_CLASS = "dfs.provided.aliasmap.class";
  public static final String DFS_PROVIDED_ALIASMAP_LOAD_RETRIES = "dfs.provided.aliasmap.load.retries";

  public static final String DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER = "dfs.provided.aliasmap.text.delimiter";
  public static final String DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER_DEFAULT = ",";

  public static final String DFS_PROVIDED_ALIASMAP_TEXT_READ_FILE = "dfs.provided.aliasmap.text.read.file";
  public static final String DFS_PROVIDED_ALIASMAP_TEXT_READ_FILE_DEFAULT = "file:///tmp/blocks.csv";

  public static final String DFS_PROVIDED_ALIASMAP_TEXT_CODEC = "dfs.provided.aliasmap.text.codec";
  public static final String DFS_PROVIDED_ALIASMAP_TEXT_WRITE_DIR = "dfs.provided.aliasmap.text.write.dir";
  public static final String DFS_PROVIDED_ALIASMAP_TEXT_WRITE_DIR_DEFAULT = "file:///tmp/";

  public static final String DFS_PROVIDED_ALIASMAP_LEVELDB_PATH = "dfs.provided.aliasmap.leveldb.path";
  public static final String DFS_PROVIDED_ACLS_IMPORT_ENABLED =
      "dfs.provided.acls.import.enabled";
  public static final boolean DFS_PROVIDED_ACLS_IMPORT_ENABLED_DEFAULT = false;

  public static final String  DFS_LIST_LIMIT = "dfs.ls.limit";
  public static final int     DFS_LIST_LIMIT_DEFAULT = 1000;
  public static final String  DFS_NAMENODE_BATCHED_LISTING_LIMIT = "dfs.batched.ls.limit";
  public static final int     DFS_NAMENODE_BATCHED_LISTING_LIMIT_DEFAULT = 100;
  public static final String  DFS_CONTENT_SUMMARY_LIMIT_KEY = "dfs.content-summary.limit";
  public static final int     DFS_CONTENT_SUMMARY_LIMIT_DEFAULT = 5000;
  public static final String  DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_KEY = "dfs.content-summary.sleep-microsec";
  public static final long    DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_DEFAULT = 500;
  public static final String  DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY = "dfs.datanode.failed.volumes.tolerated";
  public static final int     DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT = 0;
  public static final String
      DFS_DATANODE_VOLUMES_REPLICA_ADD_THREADPOOL_SIZE_KEY =
      "dfs.datanode.volumes.replica-add.threadpool.size";
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

  // Multiple dirs separated by "," are acceptable.
  public static final String DFS_DATANODE_PMEM_CACHE_DIRS_KEY =
      "dfs.datanode.pmem.cache.dirs";
  public static final String  DFS_DATANODE_PMEM_CACHE_RECOVERY_KEY =
      "dfs.datanode.pmem.cache.recovery";
  public static final boolean DFS_DATANODE_PMEM_CACHE_RECOVERY_DEFAULT =
      true;

  public static final String DFS_DATANODE_PROCESS_COMMANDS_THRESHOLD_KEY =
      "dfs.datanode.processcommands.threshold";
  public static final long DFS_DATANODE_PROCESS_COMMANDS_THRESHOLD_DEFAULT =
      TimeUnit.SECONDS.toMillis(2);

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
  public static final String  DFS_NAMENODE_CACHING_ENABLED_KEY =
      "dfs.namenode.caching.enabled";
  // TODO: Default value to be set false in 4.0.0 release onwards (HDFS-16209)
  public static final boolean DFS_NAMENODE_CACHING_ENABLED_DEFAULT = true;

  /** Pending period of block deletion since NameNode startup */
  public static final String  DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY = "dfs.namenode.startup.delay.block.deletion.sec";
  public static final long    DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_DEFAULT = 0L;

  /** The limit of single lock holding duration.*/
  public static final String DFS_NAMENODE_BLOCK_DELETION_LOCK_THRESHOLD_MS =
      "dfs.namenode.block.deletion.lock.threshold.ms";
  public static final int DFS_NAMENODE_BLOCK_DELETION_LOCK_THRESHOLD_MS_DEFAULT =
      50;
  /** The sleep interval for releasing lock.*/
  public static final String DFS_NAMENODE_BLOCK_DELETION_UNLOCK_INTERVAL_MS =
      "dfs.namenode.block.deletion.unlock.interval.ms";
  public static final int DFS_NAMENODE_BLOCK_DELETION_UNLOCK_INTERVAL_MS_DEFAULT =
      10;

  public static final String DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES =
      HdfsClientConfigKeys.DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES;
  public static final boolean DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES_DEFAULT =
      HdfsClientConfigKeys.DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES_DEFAULT;

  public static final String DFS_NAMENODE_SNAPSHOT_SKIP_CAPTURE_ACCESSTIME_ONLY_CHANGE = "dfs.namenode.snapshot.skip.capture.accesstime-only-change";
  public static final boolean DFS_NAMENODE_SNAPSHOT_SKIP_CAPTURE_ACCESSTIME_ONLY_CHANGE_DEFAULT = false;

  public static final String
      DFS_NAMENODE_SNAPSHOT_DIFF_ALLOW_SNAP_ROOT_DESCENDANT =
      "dfs.namenode.snapshotdiff.allow.snap-root-descendant";
  public static final boolean
      DFS_NAMENODE_SNAPSHOT_DIFF_ALLOW_SNAP_ROOT_DESCENDANT_DEFAULT =
      true;

  public static final String
      DFS_NAMENODE_SNAPSHOT_DIFF_LISTING_LIMIT  =
      "dfs.namenode.snapshotdiff.listing.limit";
  public static final int
      DFS_NAMENODE_SNAPSHOT_DIFF_LISTING_LIMIT_DEFAULT = 1000;

  public static final String DFS_NAMENODE_SNAPSHOT_MAX_LIMIT =
      "dfs.namenode.snapshot.max.limit";
  public static final int DFS_NAMENODE_SNAPSHOT_MAX_LIMIT_DEFAULT = 65536;
  public static final String
      DFS_NAMENODE_SNAPSHOT_FILESYSTEM_LIMIT =
      "dfs.namenode.snapshot.filesystem.limit";
  // default value is same as snapshot quota set for a snapshottable directory
  public static final int
      DFS_NAMENODE_SNAPSHOT_FILESYSTEM_LIMIT_DEFAULT = 65536;

  public static final String DFS_NAMENODE_SNAPSHOT_SKIPLIST_SKIP_INTERVAL =
      "dfs.namenode.snapshot.skiplist.interval";
  public static final int DFS_NAMENODE_SNAPSHOT_SKIPLIST_SKIP_INTERVAL_DEFAULT =
      10;
  public static final String DFS_NAMENODE_SNAPSHOT_SKIPLIST_MAX_LEVELS =
      "dfs.namenode.snapshot.skiplist.max.levels";
  public static final int
      DFS_NAMENODE_SNAPSHOT_SKIPLIST_MAX_SKIP_LEVELS_DEFAULT = 0;

  // Whether to enable datanode's stale state detection and usage for reads
  public static final String DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY = "dfs.namenode.avoid.read.stale.datanode";
  public static final boolean DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_DEFAULT = false;
  public static final String DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_KEY =
      "dfs.namenode.avoid.read.slow.datanode";
  public static final boolean
      DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_DEFAULT = false;
  // Whether to enable datanode's stale state detection and usage for writes
  public static final String DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY = "dfs.namenode.avoid.write.stale.datanode";
  public static final boolean DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_DEFAULT = false;
  // enable and disable logging datanode staleness. Disabled by default.
  public static final String DFS_NAMENODE_ENABLE_LOG_STALE_DATANODE_KEY =
      "dfs.namenode.enable.log.stale.datanode";
  public static final boolean DFS_NAMENODE_ENABLE_LOG_STALE_DATANODE_DEFAULT =
      false;
  // The default value of the time interval for marking datanodes as stale
  public static final String DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY = "dfs.namenode.stale.datanode.interval";
  public static final long DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT = 30 * 1000; // 30s
  // The stale interval cannot be too small since otherwise this may cause too frequent churn on stale states. 
  // This value uses the times of heartbeat interval to define the minimum value for stale interval.  
  public static final String DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_KEY = "dfs.namenode.stale.datanode.minimum.interval";
  public static final int DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_DEFAULT = 3; // i.e. min_interval is 3 * heartbeat_interval = 9s
  public static final String DFS_NAMENODE_REMOVE_DEAD_DATANODE_BATCHNUM_KEY
      = "dfs.namenode.remove.dead.datanode.batchnum";
  public static final int DFS_NAMENODE_REMOVE_BAD_BATCH_NUM_DEFAULT = 10;
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
  public static final long    DFS_NAMENODE_MAX_BLOCKS_PER_FILE_DEFAULT = 10*1000;
  public static final String  DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY = "dfs.namenode.fs-limits.max-xattrs-per-inode";
  public static final int     DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT = 32;
  public static final String  DFS_NAMENODE_MAX_XATTR_SIZE_KEY = "dfs.namenode.fs-limits.max-xattr-size";
  public static final int     DFS_NAMENODE_MAX_XATTR_SIZE_DEFAULT = 16384;
  public static final int     DFS_NAMENODE_MAX_XATTR_SIZE_HARD_LIMIT = 32768;

  public static final String  DFS_NAMENODE_LEASE_RECHECK_INTERVAL_MS_KEY =
      "dfs.namenode.lease-recheck-interval-ms";
  public static final long    DFS_NAMENODE_LEASE_RECHECK_INTERVAL_MS_DEFAULT =
      2000;
  public static final String
      DFS_NAMENODE_MAX_LOCK_HOLD_TO_RELEASE_LEASE_MS_KEY =
      "dfs.namenode.max-lock-hold-to-release-lease-ms";
  public static final long
      DFS_NAMENODE_MAX_LOCK_HOLD_TO_RELEASE_LEASE_MS_DEFAULT = 25;

  public static final String DFS_NAMENODE_FSLOCK_FAIR_KEY =
      "dfs.namenode.fslock.fair";
  public static final boolean DFS_NAMENODE_FSLOCK_FAIR_DEFAULT = true;

  public static final String  DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY =
      "dfs.namenode.lock.detailed-metrics.enabled";
  public static final boolean DFS_NAMENODE_LOCK_DETAILED_METRICS_DEFAULT =
      false;
  // Threshold for how long namenode locks must be held for the
  // event to be logged
  public static final String  DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY =
      "dfs.namenode.write-lock-reporting-threshold-ms";
  public static final long    DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_DEFAULT = 5000L;
  public static final String  DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY =
      "dfs.namenode.read-lock-reporting-threshold-ms";
  public static final long    DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_DEFAULT = 5000L;

  public static final String DFS_NAMENODE_ACCESS_CONTROL_ENFORCER_REPORTING_THRESHOLD_MS_KEY
      = "dfs.namenode.access-control-enforcer-reporting-threshold-ms";
  public static final long   DFS_NAMENODE_ACCESS_CONTROL_ENFORCER_REPORTING_THRESHOLD_MS_DEFAULT
      = 1000L;

  // Threshold for how long the lock warnings must be suppressed
  public static final String DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY =
      "dfs.lock.suppress.warning.interval";
  public static final long DFS_LOCK_SUPPRESS_WARNING_INTERVAL_DEFAULT =
      10000; //ms
  public static final String DFS_DATANODE_LOCK_FAIR_KEY =
      "dfs.datanode.lock.fair";
  public static final boolean DFS_DATANODE_LOCK_FAIR_DEFAULT = true;

  public static final String  DFS_UPGRADE_DOMAIN_FACTOR = "dfs.namenode.upgrade.domain.factor";
  public static final int DFS_UPGRADE_DOMAIN_FACTOR_DEFAULT = DFS_REPLICATION_DEFAULT;

  //Following keys have no defaults
  public static final String  DFS_DATANODE_DATA_DIR_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_DATANODE_DATA_DIR_KEY;
  public static final int     DFS_NAMENODE_HTTPS_PORT_DEFAULT =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_HTTPS_ADDRESS_KEY =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_HTTPS_BIND_HOST_KEY = "dfs.namenode.https-bind-host";
  public static final String  DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_NAMENODE_HTTPS_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_NAME_DIR_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_NAME_DIR_KEY;
  public static final String DFS_NAMENODE_NAME_DIR_PERMISSION_KEY =
      "dfs.namenode.storage.dir.perm";
  public static final String DFS_NAMENODE_NAME_DIR_PERMISSION_DEFAULT =
      "700";
  public static final String  DFS_NAMENODE_EDITS_DIR_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_EDITS_DIR_KEY;
  public static final String  DFS_NAMENODE_SHARED_EDITS_DIR_KEY = "dfs.namenode.shared.edits.dir";
  public static final String  DFS_NAMENODE_EDITS_PLUGIN_PREFIX = "dfs.namenode.edits.journal-plugin";
  public static final String  DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY = "dfs.namenode.edits.dir.required";
  public static final String  DFS_NAMENODE_EDITS_DIR_DEFAULT = "file:///tmp/hadoop/dfs/name";

  public static final String
      DFS_NAMENODE_EDITS_QJOURNALS_RESOLUTION_ENABLED =
      "dfs.namenode.edits.qjournals.resolution-enabled";
  public static final boolean
      DFS_NAMENODE_EDITS_QJOURNALS_RESOLUTION_ENABLED_DEFAULT = false;

  public static final String
      DFS_NAMENODE_EDITS_QJOURNALS_RESOLUTION_RESOLVER_IMPL =
      "dfs.namenode.edits.qjournals.resolver.impl";

  public static final String  DFS_METRICS_SESSION_ID_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_METRICS_SESSION_ID_KEY;
  public static final String  DFS_METRICS_PERCENTILES_INTERVALS_KEY = "dfs.metrics.percentiles.intervals";

  public static final String  DFS_DATANODE_PEER_STATS_ENABLED_KEY =
      "dfs.datanode.peer.stats.enabled";
  public static final boolean DFS_DATANODE_PEER_STATS_ENABLED_DEFAULT = false;
  public static final String
          DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY =
      "dfs.datanode.peer.metrics.min.outlier.detection.samples";
  public static final long
          DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_DEFAULT =
      1000;
  public static final String DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY =
      "dfs.datanode.min.outlier.detection.nodes";
  public static final long DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_DEFAULT =
      10L;
  public static final String DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY =
      "dfs.datanode.slowpeer.low.threshold.ms";
  public static final long DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_DEFAULT =
      5L;
  public static final String DFS_DATANODE_MAX_NODES_TO_REPORT_KEY =
      "dfs.datanode.max.nodes.to.report";
  public static final int DFS_DATANODE_MAX_NODES_TO_REPORT_DEFAULT =
      5;
  public static final String DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY =
      "dfs.datanode.min.outlier.detection.disks";
  public static final long DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_DEFAULT =
      5L;
  public static final String DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY =
      "dfs.datanode.slowdisk.low.threshold.ms";
  public static final long DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_DEFAULT =
      20L;
  public static final String DFS_DATANODE_MAX_DISKS_TO_REPORT_KEY =
      "dfs.datanode.max.disks.to.report";
  public static final int DFS_DATANODE_MAX_DISKS_TO_REPORT_DEFAULT =
      5;
  public static final String DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY =
      "dfs.datanode.max.slowdisks.to.exclude";
  public static final int DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_DEFAULT =
      0;
  public static final String  DFS_DATANODE_HOST_NAME_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_DATANODE_HOST_NAME_KEY;
  public static final String  DFS_NAMENODE_CHECKPOINT_DIR_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY;
  public static final String  DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY;
  public static final String  DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY =
      "dfs.namenode.hosts.provider.classname";
  public static final String  DFS_HOSTS = "dfs.hosts";
  public static final String  DFS_HOSTS_EXCLUDE = "dfs.hosts.exclude";
  public static final String  DFS_HOSTS_TIMEOUT = "dfs.hosts.timeout";
  public static final int     DFS_HOSTS_TIMEOUT_DEFAULT = 0;
  public static final String  DFS_NAMENODE_AUDIT_LOGGERS_KEY = "dfs.namenode.audit.loggers";
  public static final String  DFS_NAMENODE_DEFAULT_AUDIT_LOGGER_NAME = "default";
  public static final String  DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY = "dfs.namenode.audit.log.token.tracking.id";
  public static final boolean DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT = false;
  public static final String  DFS_NAMENODE_AUDIT_LOG_DEBUG_CMDLIST = "dfs.namenode.audit.log.debug.cmdlist";
  public static final String  DFS_NAMENODE_METRICS_LOGGER_PERIOD_SECONDS_KEY =
      "dfs.namenode.metrics.logger.period.seconds";
  public static final int     DFS_NAMENODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT =
      600;
  public static final String DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY =
      "dfs.datanode.metrics.logger.period.seconds";
  public static final int DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT =
      600;
  public static final String  DFS_NAMENODE_AUDIT_LOG_WITH_REMOTE_PORT_KEY =
      "dfs.namenode.audit.log.with.remote.port";
  public static final boolean DFS_NAMENODE_AUDIT_LOG_WITH_REMOTE_PORT_DEFAULT =
      false;
  /**
   * The maximum number of getBlocks RPCs data movement utilities can make to
   * a NameNode per second. Values &lt;= 0 disable throttling. This affects
   * anything that uses a NameNodeConnector, i.e., the Balancer, Mover,
   * and StoragePolicySatisfier.
   */
  public static final String  DFS_NAMENODE_GETBLOCKS_MAX_QPS_KEY = "dfs.namenode.get-blocks.max-qps";
  public static final int     DFS_NAMENODE_GETBLOCKS_MAX_QPS_DEFAULT = 20;
  public static final String  DFS_NAMENODE_GETBLOCKS_CHECK_OPERATION_KEY
      = "dfs.namenode.get-blocks.check.operation";
  public static final boolean  DFS_NAMENODE_GETBLOCKS_CHECK_OPERATION_DEFAULT
      = true;

  public static final String  DFS_BALANCER_MOVEDWINWIDTH_KEY = "dfs.balancer.movedWinWidth";
  public static final long    DFS_BALANCER_MOVEDWINWIDTH_DEFAULT = 5400*1000L;
  public static final String  DFS_BALANCER_MOVERTHREADS_KEY = "dfs.balancer.moverThreads";
  public static final int     DFS_BALANCER_MOVERTHREADS_DEFAULT = 1000;
  public static final String  DFS_BALANCER_DISPATCHERTHREADS_KEY = "dfs.balancer.dispatcherThreads";
  public static final int     DFS_BALANCER_DISPATCHERTHREADS_DEFAULT = 200;
  public static final String  DFS_BALANCER_MAX_SIZE_TO_MOVE_KEY = "dfs.balancer.max-size-to-move";
  public static final long    DFS_BALANCER_MAX_SIZE_TO_MOVE_DEFAULT = 10L*1024*1024*1024;
  public static final String  DFS_BALANCER_GETBLOCKS_SIZE_KEY = "dfs.balancer.getBlocks.size";
  public static final long    DFS_BALANCER_GETBLOCKS_SIZE_DEFAULT = 2L*1024*1024*1024; // 2GB
  public static final String  DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY = "dfs.balancer.getBlocks.min-block-size";
  public static final long    DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_DEFAULT = 10L*1024*1024; // 10MB
  public static final String  DFS_BALANCER_GETBLOCKS_HOT_TIME_INTERVAL_KEY =
      "dfs.balancer.getBlocks.hot-time-interval";
  public static final long    DFS_BALANCER_GETBLOCKS_HOT_TIME_INTERVAL_DEFAULT =
      0L;
  public static final String  DFS_BALANCER_KEYTAB_ENABLED_KEY = "dfs.balancer.keytab.enabled";
  public static final boolean DFS_BALANCER_KEYTAB_ENABLED_DEFAULT = false;
  public static final String  DFS_BALANCER_ADDRESS_KEY = "dfs.balancer.address";
  public static final String  DFS_BALANCER_ADDRESS_DEFAULT= "0.0.0.0:0";
  public static final String  DFS_BALANCER_KEYTAB_FILE_KEY = "dfs.balancer.keytab.file";
  public static final String  DFS_BALANCER_KERBEROS_PRINCIPAL_KEY = "dfs.balancer.kerberos.principal";
  public static final String  DFS_BALANCER_BLOCK_MOVE_TIMEOUT = "dfs.balancer.block-move.timeout";
  public static final int     DFS_BALANCER_BLOCK_MOVE_TIMEOUT_DEFAULT = 0;
  public static final String  DFS_BALANCER_MAX_NO_MOVE_INTERVAL_KEY = "dfs.balancer.max-no-move-interval";
  public static final int     DFS_BALANCER_MAX_NO_MOVE_INTERVAL_DEFAULT = 60*1000; // One minute
  public static final String  DFS_BALANCER_MAX_ITERATION_TIME_KEY = "dfs.balancer.max-iteration-time";
  public static final long    DFS_BALANCER_MAX_ITERATION_TIME_DEFAULT = 20 * 60 * 1000L; // 20 mins
  public static final String  DFS_BALANCER_SERVICE_INTERVAL_KEY = "dfs.balancer.service.interval";
  public static final long    DFS_BALANCER_SERVICE_INTERVAL_DEFAULT = TimeUnit.MINUTES.toMillis(5); //5 mins
  public static final String  DFS_BALANCER_SERVICE_RETRIES_ON_EXCEPTION = "dfs.balancer.service.retries.on.exception";
  public static final int     DFS_BALANCER_SERVICE_RETRIES_ON_EXCEPTION_DEFAULT = 5;

  public static final String  DFS_MOVER_MOVEDWINWIDTH_KEY = "dfs.mover.movedWinWidth";
  public static final long    DFS_MOVER_MOVEDWINWIDTH_DEFAULT = 5400*1000L;
  public static final String  DFS_MOVER_MOVERTHREADS_KEY = "dfs.mover.moverThreads";
  public static final int     DFS_MOVER_MOVERTHREADS_DEFAULT = 1000;
  public static final String  DFS_DATANODE_MOVER_MAX_NUM_CONCURRENT_MOVES_KEY =
      "dfs.datanode.mover.max.concurrent.moves";
  public static final int     DFS_DATANODE_MOVER_MAX_NUM_CONCURRENT_MOVES_DEFAULT = 10;
  public static final String  DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY = "dfs.mover.retry.max.attempts";
  public static final int     DFS_MOVER_RETRY_MAX_ATTEMPTS_DEFAULT = 10;
  public static final String  DFS_MOVER_KEYTAB_ENABLED_KEY =
      "dfs.mover.keytab.enabled";
  public static final boolean DFS_MOVER_KEYTAB_ENABLED_DEFAULT = false;
  public static final String  DFS_MOVER_ADDRESS_KEY = "dfs.mover.address";
  public static final String  DFS_MOVER_ADDRESS_DEFAULT= "0.0.0.0:0";
  public static final String  DFS_MOVER_KEYTAB_FILE_KEY =
      "dfs.mover.keytab.file";
  public static final String  DFS_MOVER_KERBEROS_PRINCIPAL_KEY =
      "dfs.mover.kerberos.principal";
  public static final String  DFS_MOVER_MAX_NO_MOVE_INTERVAL_KEY = "dfs.mover.max-no-move-interval";
  public static final int    DFS_MOVER_MAX_NO_MOVE_INTERVAL_DEFAULT = 60*1000; // One minute

  // StoragePolicySatisfier (SPS) related configurations
  public static final String  DFS_STORAGE_POLICY_SATISFIER_MODE_KEY =
      "dfs.storage.policy.satisfier.mode";
  public static final String DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT =
      StoragePolicySatisfierMode.NONE.toString();
  public static final String  DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_KEY =
      "dfs.storage.policy.satisfier.queue.limit";
  public static final int  DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_DEFAULT =
      1000;
  public static final String DFS_SPS_WORK_MULTIPLIER_PER_ITERATION =
      "dfs.storage.policy.satisfier.work.multiplier.per.iteration";
  public static final int DFS_SPS_WORK_MULTIPLIER_PER_ITERATION_DEFAULT =
      1;
  public static final String DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY =
      "dfs.storage.policy.satisfier.recheck.timeout.millis";
  public static final int DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_DEFAULT =
      1 * 60 * 1000;
  public static final String DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_KEY =
      "dfs.storage.policy.satisfier.self.retry.timeout.millis";
  public static final int DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_DEFAULT =
      5 * 60 * 1000;
  public static final String DFS_STORAGE_POLICY_SATISFIER_MAX_RETRY_ATTEMPTS_KEY =
      "dfs.storage.policy.satisfier.retry.max.attempts";
  public static final int DFS_STORAGE_POLICY_SATISFIER_MAX_RETRY_ATTEMPTS_DEFAULT =
      3;
  public static final String DFS_STORAGE_POLICY_SATISFIER_MOVE_TASK_MAX_RETRY_ATTEMPTS_KEY =
      "dfs.storage.policy.satisfier.move.task.retry.max.attempts";
  public static final int DFS_STORAGE_POLICY_SATISFIER_MOVE_TASK_MAX_RETRY_ATTEMPTS_DEFAULT =
      3;
  public static final String DFS_STORAGE_DEFAULT_POLICY =
      "dfs.storage.default.policy";
  public static final HdfsConstants.StoragePolicy
      DFS_STORAGE_DEFAULT_POLICY_DEFAULT = HdfsConstants.StoragePolicy.HOT;

  public static final String DFS_SPS_MAX_OUTSTANDING_PATHS_KEY =
      "dfs.storage.policy.satisfier.max.outstanding.paths";
  public static final int DFS_SPS_MAX_OUTSTANDING_PATHS_DEFAULT = 10000;
  // SPS datanode cache config, defaulting to 5mins.
  public static final String DFS_SPS_DATANODE_CACHE_REFRESH_INTERVAL_MS =
      "dfs.storage.policy.satisfier.datanode.cache.refresh.interval.ms";
  public static final long DFS_SPS_DATANODE_CACHE_REFRESH_INTERVAL_MS_DEFAULT =
      300000L;

  // SPS keytab configurations, by default it is disabled.
  public static final String  DFS_SPS_ADDRESS_KEY =
      "dfs.storage.policy.satisfier.address";
  public static final String  DFS_SPS_ADDRESS_DEFAULT= "0.0.0.0:0";
  public static final String  DFS_SPS_KEYTAB_FILE_KEY =
      "dfs.storage.policy.satisfier.keytab.file";
  public static final String  DFS_SPS_KERBEROS_PRINCIPAL_KEY =
      "dfs.storage.policy.satisfier.kerberos.principal";

  public static final String  DFS_DATANODE_ADDRESS_KEY = "dfs.datanode.address";
  public static final int     DFS_DATANODE_DEFAULT_PORT = 9866;
  public static final String  DFS_DATANODE_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_DATANODE_DEFAULT_PORT;
  public static final String  DFS_DATANODE_DATA_DIR_PERMISSION_KEY = "dfs.datanode.data.dir.perm";
  public static final String  DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT = "700";
  public static final String  DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY = "dfs.datanode.directoryscan.interval";
  public static final int     DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT = 21600;
  public static final String  DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY = "dfs.datanode.directoryscan.threads";
  public static final int     DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT = 1;
  public static final String  DFS_DATANODE_RECONCILE_BLOCKS_BATCH_SIZE =
      "dfs.datanode.reconcile.blocks.batch.size";
  public static final int
      DFS_DATANODE_RECONCILE_BLOCKS_BATCH_SIZE_DEFAULT = 1000;
  public static final String DFS_DATANODE_RECONCILE_BLOCKS_BATCH_INTERVAL
      = "dfs.datanode.reconcile.blocks.batch.interval";
  public static final long
      DFS_DATANODE_RECONCILE_BLOCKS_BATCH_INTERVAL_DEFAULT = 2000;

  public static final String DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY =
      "dfs.datanode.disk.check.min.gap";
  public static final String DFS_DATANODE_DISK_CHECK_MIN_GAP_DEFAULT =
      "15m";

  public static final String DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY =
      "dfs.datanode.disk.check.timeout";
  public static final String DFS_DATANODE_DISK_CHECK_TIMEOUT_DEFAULT =
      "10m";

  public static final String  DFS_NAMENODE_EC_POLICIES_MAX_CELLSIZE_KEY = "dfs.namenode.ec.policies.max.cellsize";
  public static final int     DFS_NAMENODE_EC_POLICIES_MAX_CELLSIZE_DEFAULT = 4 * 1024 * 1024;
  public static final String DFS_NAMENODE_EC_POLICIES_USERPOLICIES_ALLOWED_KEY =
      "dfs.namenode.ec.userdefined.policy.allowed";
  public static final boolean
      DFS_NAMENODE_EC_POLICIES_USERPOLICIES_ALLOWED_KEY_DEFAULT = true;
  public static final String  DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY =
      "dfs.namenode.ec.system.default.policy";
  public static final String  DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY_DEFAULT =
      "RS-6-3-1024k";
  public static final String  DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY = "dfs.datanode.ec.reconstruction.stripedread.buffer.size";
  public static final int     DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_DEFAULT = 64 * 1024;
  public static final String  DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_KEY = "dfs.datanode.ec.reconstruction.stripedread.timeout.millis";
  public static final int     DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_DEFAULT = 5000; //5s
  public static final String  DFS_DN_EC_RECONSTRUCTION_THREADS_KEY = "dfs.datanode.ec.reconstruction.threads";
  public static final int     DFS_DN_EC_RECONSTRUCTION_THREADS_DEFAULT = 8;
  public static final String  DFS_DN_EC_RECONSTRUCTION_XMITS_WEIGHT_KEY =
      "dfs.datanode.ec.reconstruction.xmits.weight";
  public static final float   DFS_DN_EC_RECONSTRUCTION_XMITS_WEIGHT_DEFAULT =
      0.5f;
  public static final String DFS_DN_EC_RECONSTRUCTION_VALIDATION_KEY =
      "dfs.datanode.ec.reconstruction.validation";
  public static final boolean DFS_DN_EC_RECONSTRUCTION_VALIDATION_VALUE = false;

  public static final String
      DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY =
      "dfs.datanode.directoryscan.throttle.limit.ms.per.sec";
  public static final int
      DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_DEFAULT = -1;
  public static final String
      DFS_DATANODE_DIRECTORYSCAN_MAX_NOTIFY_COUNT_KEY =
      "dfs.datanode.directoryscan.max.notify.count";
  public static final long
      DFS_DATANODE_DIRECTORYSCAN_MAX_NOTIFY_COUNT_DEFAULT = 5;
  public static final String  DFS_DATANODE_DNS_INTERFACE_KEY = "dfs.datanode.dns.interface";
  public static final String  DFS_DATANODE_DNS_INTERFACE_DEFAULT = "default";
  public static final String  DFS_DATANODE_DNS_NAMESERVER_KEY = "dfs.datanode.dns.nameserver";
  public static final String  DFS_DATANODE_DNS_NAMESERVER_DEFAULT = "default";
  public static final String DFS_DATANODE_DU_RESERVED_CALCULATOR_KEY =
      "dfs.datanode.du.reserved.calculator";
  public static final Class<? extends ReservedSpaceCalculator>
      DFS_DATANODE_DU_RESERVED_CALCULATOR_DEFAULT =
          ReservedSpaceCalculator.ReservedSpaceCalculatorAbsolute.class;
  public static final String  DFS_DATANODE_DU_RESERVED_KEY = "dfs.datanode.du.reserved";
  public static final long    DFS_DATANODE_DU_RESERVED_DEFAULT = 0;
  public static final String  DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY =
      "dfs.datanode.du.reserved.pct";
  public static final int     DFS_DATANODE_DU_RESERVED_PERCENTAGE_DEFAULT = 0;
  public static final String  DFS_DATANODE_HANDLER_COUNT_KEY = "dfs.datanode.handler.count";
  public static final int     DFS_DATANODE_HANDLER_COUNT_DEFAULT = 10;
  public static final String  DFS_DATANODE_HTTP_ADDRESS_KEY = "dfs.datanode.http.address";
  public static final int     DFS_DATANODE_HTTP_DEFAULT_PORT = 9864;
  public static final String  DFS_DATANODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_DATANODE_HTTP_DEFAULT_PORT;
  public static final String  DFS_DATANODE_HTTP_INTERNAL_PROXY_PORT =
      "dfs.datanode.http.internal-proxy.port";
  public static final String  DFS_DATANODE_MAX_RECEIVER_THREADS_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY;
  public static final int     DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT = 4096;
  public static final String  DFS_DATANODE_SCAN_PERIOD_HOURS_KEY = "dfs.datanode.scan.period.hours";
  public static final int     DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT = 21 * 24;  // 3 weeks.
  public static final String  DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND = "dfs.block.scanner.volume.bytes.per.second";
  public static final long    DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND_DEFAULT = 1048576L;
  /**
   * The amount of time in milliseconds that the BlockScanner times out waiting
   * for the VolumeScanner thread to join during a shutdown call.
   */
  public static final String  DFS_BLOCK_SCANNER_VOLUME_JOIN_TIMEOUT_MSEC_KEY =
      "dfs.block.scanner.volume.join.timeout.ms";
  public static final long DFS_BLOCK_SCANNER_VOLUME_JOIN_TIMEOUT_MSEC_DEFAULT =
      TimeUnit.SECONDS.toMillis(5);
  public static final String  DFS_BLOCK_SCANNER_SKIP_RECENT_ACCESSED =
      "dfs.block.scanner.skip.recent.accessed";
  public static final boolean DFS_BLOCK_SCANNER_SKIP_RECENT_ACCESSED_DEFAULT =
      false;
  public static final String  DFS_DATANODE_TRANSFERTO_ALLOWED_KEY = "dfs.datanode.transferTo.allowed";
  public static final boolean DFS_DATANODE_TRANSFERTO_ALLOWED_DEFAULT = true;
  public static final String  DFS_HEARTBEAT_INTERVAL_KEY = "dfs.heartbeat.interval";
  public static final long    DFS_HEARTBEAT_INTERVAL_DEFAULT = 3;
  public static final String  DFS_DATANODE_LIFELINE_INTERVAL_SECONDS_KEY =
      "dfs.datanode.lifeline.interval.seconds";
  public static final String  DFS_NAMENODE_PATH_BASED_CACHE_RETRY_INTERVAL_MS = "dfs.namenode.path.based.cache.retry.interval.ms";
  public static final long    DFS_NAMENODE_PATH_BASED_CACHE_RETRY_INTERVAL_MS_DEFAULT = 30000L;
  public static final String  DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY = "dfs.namenode.decommission.interval";
  public static final int     DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT = 30;
  public static final String  DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY = "dfs.namenode.decommission.blocks.per.interval";
  public static final int     DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT = 500000;
  public static final String  DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES = "dfs.namenode.decommission.max.concurrent.tracked.nodes";
  public static final int     DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES_DEFAULT = 100;
  public static final String  DFS_NAMENODE_DECOMMISSION_MONITOR_CLASS
      = "dfs.namenode.decommission.monitor.class";
  public static final String
      DFS_NAMENODE_DECOMMISSION_MONITOR_CLASS_DEFAULT =
      "org.apache.hadoop.hdfs.server.blockmanagement."+
          "DatanodeAdminDefaultMonitor";
  public static final String
      DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT
      = "dfs.namenode.decommission.backoff.monitor.pending.limit";
  public static final int
      DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT_DEFAULT = 10000;
  public static final String DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK =
      "dfs.namenode.decommission.backoff.monitor.pending.blocks.per.lock";
  public static final int DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK_DEFAULT
      = 1000;
  public static final String  DFS_NAMENODE_HANDLER_COUNT_KEY = "dfs.namenode.handler.count";
  public static final int     DFS_NAMENODE_HANDLER_COUNT_DEFAULT = 10;
  public static final String  DFS_NAMENODE_LIFELINE_HANDLER_RATIO_KEY =
      "dfs.namenode.lifeline.handler.ratio";
  public static final float   DFS_NAMENODE_LIFELINE_HANDLER_RATIO_DEFAULT =
      0.1f;
  public static final String  DFS_NAMENODE_LIFELINE_HANDLER_COUNT_KEY =
      "dfs.namenode.lifeline.handler.count";
  public static final String  DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY = "dfs.namenode.service.handler.count";
  public static final int     DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT = 10;
  // List of users that can override their client ip
  public static final String  DFS_NAMENODE_IP_PROXY_USERS = "dfs.namenode.ip-proxy-users";
  public static final String  DFS_HTTP_POLICY_KEY = "dfs.http.policy";
  public static final String  DFS_HTTP_POLICY_DEFAULT =  HttpConfig.Policy.HTTP_ONLY.name();
  public static final String  DFS_DATANODE_HTTPSERVER_FILTER_HANDLERS = "dfs.datanode.httpserver.filter.handlers";
  public static final String  DFS_DATANODE_HTTPSERVER_FILTER_HANDLERS_DEFAULT = "org.apache.hadoop.hdfs.server.datanode.web.RestCsrfPreventionFilterHandler";
  public static final String  DFS_DEFAULT_CHUNK_VIEW_SIZE_KEY = "dfs.default.chunk.view.size";
  public static final int     DFS_DEFAULT_CHUNK_VIEW_SIZE_DEFAULT = 32*1024;
  public static final String  DFS_DATANODE_HTTPS_ADDRESS_KEY = "dfs.datanode.https.address";
  public static final String  DFS_DATANODE_HTTPS_PORT_KEY = "datanode.https.port";
  public static final int     DFS_DATANODE_HTTPS_DEFAULT_PORT = 9865;
  public static final String  DFS_DATANODE_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_DATANODE_HTTPS_DEFAULT_PORT;
  public static final String  DFS_DATANODE_IPC_ADDRESS_KEY = "dfs.datanode.ipc.address";
  public static final int     DFS_DATANODE_IPC_DEFAULT_PORT = 9867;
  public static final String  DFS_DATANODE_IPC_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_DATANODE_IPC_DEFAULT_PORT;
  public static final String  DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_KEY = "dfs.datanode.min.supported.namenode.version";
  public static final String  DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_DEFAULT = "2.1.0-beta";
  public static final String  DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY = "dfs.namenode.inode.attributes.provider.class";
  public static final String  DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_BYPASS_USERS_KEY = "dfs.namenode.inode.attributes.provider.bypass.users";
  public static final String  DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_BYPASS_USERS_DEFAULT = "";

  public static final String  DFS_DATANODE_BP_READY_TIMEOUT_KEY = "dfs.datanode.bp-ready.timeout";
  public static final long    DFS_DATANODE_BP_READY_TIMEOUT_DEFAULT = 20;

  public static final String  DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY = "dfs.block.access.token.enable";
  public static final boolean DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT = false;
  public static final String  DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY = "dfs.block.access.key.update.interval";
  public static final long    DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_DEFAULT = 600L;
  public static final String  DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY = "dfs.block.access.token.lifetime";
  public static final long    DFS_BLOCK_ACCESS_TOKEN_LIFETIME_DEFAULT = 600L;
  public static final String  DFS_BLOCK_ACCESS_TOKEN_PROTOBUF_ENABLE = "dfs.block.access.token.protobuf.enable";
  public static final boolean DFS_BLOCK_ACCESS_TOKEN_PROTOBUF_ENABLE_DEFAULT = false;

  public static final String DFS_BLOCK_REPLICATOR_CLASSNAME_KEY = "dfs.block.replicator.classname";
  public static final Class<BlockPlacementPolicyDefault> DFS_BLOCK_REPLICATOR_CLASSNAME_DEFAULT = BlockPlacementPolicyDefault.class;
  public static final String  DFS_REPLICATION_MAX_KEY = "dfs.replication.max";
  public static final int     DFS_REPLICATION_MAX_DEFAULT = 512;
  public static final String DFS_BLOCK_PLACEMENT_EC_CLASSNAME_KEY = "dfs.block.placement.ec.classname";
  public static final Class<BlockPlacementPolicyRackFaultTolerant> DFS_BLOCK_PLACEMENT_EC_CLASSNAME_DEFAULT = BlockPlacementPolicyRackFaultTolerant.class;

  public static final String  DFS_DF_INTERVAL_KEY = "dfs.df.interval";
  public static final int     DFS_DF_INTERVAL_DEFAULT = 60000;
  public static final String  DFS_BLOCKREPORT_INCREMENTAL_INTERVAL_MSEC_KEY
      = "dfs.blockreport.incremental.intervalMsec";
  public static final long    DFS_BLOCKREPORT_INCREMENTAL_INTERVAL_MSEC_DEFAULT
      = 0;
  public static final String  DFS_BLOCKREPORT_INTERVAL_MSEC_KEY = "dfs.blockreport.intervalMsec";
  public static final long    DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT = 6 * 60 * 60 * 1000;
  public static final String  DFS_BLOCKREPORT_INITIAL_DELAY_KEY = "dfs.blockreport.initialDelay";
  public static final int     DFS_BLOCKREPORT_INITIAL_DELAY_DEFAULT = 0;
  public static final String  DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY = "dfs.blockreport.split.threshold";
  public static final long    DFS_BLOCKREPORT_SPLIT_THRESHOLD_DEFAULT = 1000 * 1000;
  public static final String  DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES = "dfs.namenode.max.full.block.report.leases";
  public static final int     DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES_DEFAULT = 6;
  public static final String  DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS = "dfs.namenode.full.block.report.lease.length.ms";
  public static final long    DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT = 5L * 60L * 1000L;
  public static final String  DFS_CACHEREPORT_INTERVAL_MSEC_KEY = "dfs.cachereport.intervalMsec";
  public static final long    DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT = 10 * 1000;
  public static final String  DFS_BLOCK_INVALIDATE_LIMIT_KEY = "dfs.block.invalidate.limit";
  public static final int     DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT = 1000;
  public static final String  DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED_KEY = "dfs.corruptfilesreturned.max";
  public static final int     DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED = 500;
  /* Maximum number of blocks to process for initializing replication queues */
  public static final String  DFS_BLOCK_MISREPLICATION_PROCESSING_LIMIT = "dfs.block.misreplication.processing.limit";
  public static final int     DFS_BLOCK_MISREPLICATION_PROCESSING_LIMIT_DEFAULT = 10000;

  public static final String DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY =
      "dfs.datanode.outliers.report.interval";
  public static final String DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_DEFAULT =
      "30m";
  public static final String DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY =
      "dfs.namenode.max.slowpeer.collect.nodes";
  public static final int DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_DEFAULT =
      5;
  public static final String DFS_NAMENODE_SLOWPEER_COLLECT_INTERVAL_KEY =
      "dfs.namenode.slowpeer.collect.interval";
  public static final String DFS_NAMENODE_SLOWPEER_COLLECT_INTERVAL_DEFAULT =
      "30m";

  // property for fsimage compression
  public static final String DFS_IMAGE_COMPRESS_KEY = "dfs.image.compress";
  public static final boolean DFS_IMAGE_COMPRESS_DEFAULT = false;
  public static final String DFS_IMAGE_COMPRESSION_CODEC_KEY =
                                   "dfs.image.compression.codec";
  public static final String DFS_IMAGE_COMPRESSION_CODEC_DEFAULT =
                                   "org.apache.hadoop.io.compress.DefaultCodec";

  public static final String DFS_IMAGE_TRANSFER_RATE_KEY =
                                           "dfs.image.transfer.bandwidthPerSec";
  public static final long DFS_IMAGE_TRANSFER_RATE_DEFAULT = 52428800;

  public static final String DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY =
      "dfs.image.transfer-bootstrap-standby.bandwidthPerSec";
  public static final long DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT =
      0;  //no throttling

  // Image transfer timeout
  public static final String DFS_IMAGE_TRANSFER_TIMEOUT_KEY = "dfs.image.transfer.timeout";
  public static final int DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT = 60 * 1000;

  // Image transfer chunksize
  public static final String DFS_IMAGE_TRANSFER_CHUNKSIZE_KEY = "dfs.image.transfer.chunksize";
  public static final int DFS_IMAGE_TRANSFER_CHUNKSIZE_DEFAULT = 64 * 1024;

  public static final String DFS_IMAGE_PARALLEL_LOAD_KEY =
      "dfs.image.parallel.load";
  public static final boolean DFS_IMAGE_PARALLEL_LOAD_DEFAULT = false;

  public static final String DFS_IMAGE_PARALLEL_TARGET_SECTIONS_KEY =
      "dfs.image.parallel.target.sections";
  public static final int DFS_IMAGE_PARALLEL_TARGET_SECTIONS_DEFAULT = 12;

  public static final String DFS_IMAGE_PARALLEL_INODE_THRESHOLD_KEY =
      "dfs.image.parallel.inode.threshold";
  public static final int DFS_IMAGE_PARALLEL_INODE_THRESHOLD_DEFAULT = 1000000;

  public static final String DFS_IMAGE_PARALLEL_THREADS_KEY =
      "dfs.image.parallel.threads";
  public static final int DFS_IMAGE_PARALLEL_THREADS_DEFAULT = 4;

  // Edit Log segment transfer timeout
  public static final String DFS_EDIT_LOG_TRANSFER_TIMEOUT_KEY =
      "dfs.edit.log.transfer.timeout";
  public static final int DFS_EDIT_LOG_TRANSFER_TIMEOUT_DEFAULT = 30 * 1000;

  // Throttling Edit Log Segment transfer for Journal Sync
  public static final String DFS_EDIT_LOG_TRANSFER_RATE_KEY =
      "dfs.edit.log.transfer.bandwidthPerSec";
  public static final long DFS_EDIT_LOG_TRANSFER_RATE_DEFAULT = 0; //no throttling

  public static final String DFS_QJM_OPERATIONS_TIMEOUT =
      "dfs.qjm.operations.timeout";
  public static final long DFS_QJM_OPERATIONS_TIMEOUT_DEFAULT = 60000;

  // Datanode File IO Stats
  public static final String DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_KEY =
      "dfs.datanode.enable.fileio.fault.injection";
  public static final boolean
      DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_DEFAULT = false;
  public static final String
      DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY =
      "dfs.datanode.fileio.profiling.sampling.percentage";
  public static final int
      DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_DEFAULT = 0;

  //Keys with no defaults
  public static final String  DFS_DATANODE_PLUGINS_KEY = "dfs.datanode.plugins";
  public static final String  DFS_DATANODE_FSDATASET_FACTORY_KEY = "dfs.datanode.fsdataset.factory";
  public static final String  DFS_DATANODE_FSDATASET_VOLUME_CHOOSING_POLICY_KEY = "dfs.datanode.fsdataset.volume.choosing.policy";

  public static final String  DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_KEY = "dfs.datanode.available-space-volume-choosing-policy.balanced-space-threshold";
  public static final long    DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_DEFAULT = 1024L * 1024L * 1024L * 10L; // 10 GB
  public static final String  DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY = "dfs.datanode.available-space-volume-choosing-policy.balanced-space-preference-fraction";
  public static final float   DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT = 0.75f;
  public static final String
      DFS_DATANODE_ROUND_ROBIN_VOLUME_CHOOSING_POLICY_ADDITIONAL_AVAILABLE_SPACE_KEY =
      "dfs.datanode.round-robin-volume-choosing-policy.additional-available-space";
  public static final long
      DFS_DATANODE_ROUND_ROBIN_VOLUME_CHOOSING_POLICY_ADDITIONAL_AVAILABLE_SPACE_DEFAULT =
      1024L * 1024L * 1024L; // 1 GB
  public static final String  DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY =
      HdfsClientConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY;
  public static final String  DFS_DATANODE_STARTUP_KEY = "dfs.datanode.startup";
  public static final String  DFS_NAMENODE_PLUGINS_KEY = "dfs.namenode.plugins";
  public static final String  DFS_WEB_UGI_KEY = "dfs.web.ugi";
  public static final String  DFS_NAMENODE_STARTUP_KEY = "dfs.namenode.startup";
  public static final String  DFS_NAMENODE_OBSERVER_ENABLED_KEY = "dfs.namenode.observer.enabled";
  public static final boolean DFS_NAMENODE_OBSERVER_ENABLED_DEFAULT = false;
  public static final String  DFS_DATANODE_KEYTAB_FILE_KEY = "dfs.datanode.keytab.file";
  public static final String  DFS_DATANODE_KERBEROS_PRINCIPAL_KEY =
      HdfsClientConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
  @Deprecated
  public static final String  DFS_DATANODE_USER_NAME_KEY = DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
  public static final String  DFS_DATANODE_SHARED_FILE_DESCRIPTOR_PATHS = "dfs.datanode.shared.file.descriptor.paths";
  public static final String  DFS_DATANODE_SHARED_FILE_DESCRIPTOR_PATHS_DEFAULT = "/dev/shm,/tmp";
  public static final String
      DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS =
      HdfsClientConfigKeys
          .DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS;
  public static final int
      DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT =
      HdfsClientConfigKeys
          .DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT;
  public static final String  DFS_NAMENODE_KEYTAB_FILE_KEY = "dfs.namenode.keytab.file";
  public static final String  DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY =
      HdfsClientConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
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

  public static final String  DFS_NAMESERVICES =
      HdfsClientConfigKeys.DFS_NAMESERVICES;
  public static final String  DFS_NAMESERVICE_ID =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMESERVICE_ID;
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
  public static final String  DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY =
      "dfs.namenode.available-space-block-placement-policy.balanced-space-preference-fraction";
  public static final float   DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT =
      0.6f;
  public static final String
          DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_KEY =
          "dfs.namenode.available-space-block-placement-policy.balanced-space-tolerance";
  public static final int
      DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT =
      5;
  public static final String
      DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_LIMIT_KEY =
      "dfs.namenode.available-space-block-placement-policy.balanced-space-tolerance-limit";
  public static final int
      DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_LIMIT_DEFAULT =
      100;
  public static final String
      DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY =
      "dfs.namenode.available-space-rack-fault-tolerant-block-placement-policy"
          + ".balanced-space-preference-fraction";
  public static final float
      DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT =
      0.6f;
  public static final String
      DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_KEY =
      "dfs.namenode.available-space-rack-fault-tolerant-block-placement-policy"
          + ".balanced-space-tolerance";
  public static final int
      DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT =
      5;
  public static final String
      DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCE_LOCAL_NODE_KEY =
      "dfs.namenode.available-space-block-placement-policy.balance-local-node";
  public static final boolean
      DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCE_LOCAL_NODE_DEFAULT =
      false;
  public static final String  DFS_NAMENODE_BLOCKPLACEMENTPOLICY_DEFAULT_PREFER_LOCAL_NODE_KEY =
      "dfs.namenode.block-placement-policy.default.prefer-local-node";
  public static final boolean  DFS_NAMENODE_BLOCKPLACEMENTPOLICY_DEFAULT_PREFER_LOCAL_NODE_DEFAULT = true;
  public static final String
      DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY =
      "dfs.namenode.block-placement-policy.exclude-slow-nodes.enabled";
  public static final boolean
      DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_DEFAULT =
      false;

  public static final String DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_KEY =
      "dfs.namenode.block-placement.min-blocks-for.write";
  public static final int DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_DEFAULT =
      HdfsServerConstants.MIN_BLOCKS_FOR_WRITE;

  public static final String DFS_NAMENODE_GC_TIME_MONITOR_ENABLE =
      "dfs.namenode.gc.time.monitor.enable";
  public static final boolean DFS_NAMENODE_GC_TIME_MONITOR_ENABLE_DEFAULT =
      true;
  public static final String
      DFS_NAMENODE_GC_TIME_MONITOR_OBSERVATION_WINDOW_MS =
      "dfs.namenode.gc.time.monitor.observation.window.ms";
  public static final long
      DFS_NAMENODE_GC_TIME_MONITOR_OBSERVATION_WINDOW_MS_DEFAULT =
      TimeUnit.MINUTES.toMillis(1);
  public static final String DFS_NAMENODE_GC_TIME_MONITOR_SLEEP_INTERVAL_MS =
      "dfs.namenode.gc.time.monitor.sleep.interval.ms";
  public static final long
      DFS_NAMENODE_GC_TIME_MONITOR_SLEEP_INTERVAL_MS_DEFAULT =
      TimeUnit.SECONDS.toMillis(5);

  public static final String DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY = "dfs.block.local-path-access.user";
  public static final String DFS_DOMAIN_SOCKET_PATH_KEY =
      HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY;
  public static final String DFS_DOMAIN_SOCKET_PATH_DEFAULT =
      HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT;

  public static final String  DFS_STORAGE_POLICY_ENABLED_KEY = "dfs.storage.policy.enabled";
  public static final boolean DFS_STORAGE_POLICY_ENABLED_DEFAULT = true;
  public static final String DFS_STORAGE_POLICY_PERMISSIONS_SUPERUSER_ONLY_KEY =
      "dfs.storage.policy.permissions.superuser-only";
  public static final boolean
      DFS_STORAGE_POLICY_PERMISSIONS_SUPERUSER_ONLY_DEFAULT = false;

  public static final String  DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY = "dfs.quota.by.storage.type.enabled";
  public static final boolean DFS_QUOTA_BY_STORAGETYPE_ENABLED_DEFAULT = true;

  // HA related configuration
  public static final String DFS_HA_NAMENODES_KEY_PREFIX =
      HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
  public static final String DFS_HA_NAMENODE_ID_KEY = "dfs.ha.namenode.id";
  public static final String  DFS_HA_STANDBY_CHECKPOINTS_KEY = "dfs.ha.standby.checkpoints";
  public static final boolean DFS_HA_STANDBY_CHECKPOINTS_DEFAULT = true;
  public static final String DFS_HA_LOGROLL_PERIOD_KEY = "dfs.ha.log-roll.period";
  public static final int DFS_HA_LOGROLL_PERIOD_DEFAULT = 2 * 60; // 2m
  public static final String DFS_HA_TAILEDITS_PERIOD_KEY = "dfs.ha.tail-edits.period";
  public static final int DFS_HA_TAILEDITS_PERIOD_DEFAULT = 60; // 1m
  public static final String DFS_HA_TAILEDITS_PERIOD_BACKOFF_MAX_KEY = "dfs.ha.tail-edits.period.backoff-max";
  public static final int DFS_HA_TAILEDITS_PERIOD_BACKOFF_MAX_DEFAULT = 0; // disabled
  public static final String DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_KEY = "dfs.ha.tail-edits.namenode-retries";
  public static final int DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_DEFAULT = 3;
  public static final String  DFS_HA_TAILEDITS_INPROGRESS_KEY =
          "dfs.ha.tail-edits.in-progress";
  public static final boolean DFS_HA_TAILEDITS_INPROGRESS_DEFAULT = false;
  public static final String DFS_HA_TAILEDITS_ROLLEDITS_TIMEOUT_KEY =
      "dfs.ha.tail-edits.rolledits.timeout";
  public static final int DFS_HA_TAILEDITS_ROLLEDITS_TIMEOUT_DEFAULT = 60; // 1m
  public static final String DFS_HA_LOGROLL_RPC_TIMEOUT_KEY = "dfs.ha.log-roll.rpc.timeout";
  public static final int DFS_HA_LOGROLL_RPC_TIMEOUT_DEFAULT = 20000; // 20s
  public static final String DFS_HA_FENCE_METHODS_KEY = "dfs.ha.fencing.methods";
  public static final String DFS_HA_AUTO_FAILOVER_ENABLED_KEY = "dfs.ha.automatic-failover.enabled";
  public static final boolean DFS_HA_AUTO_FAILOVER_ENABLED_DEFAULT = false;
  public static final String DFS_HA_ZKFC_PORT_KEY = "dfs.ha.zkfc.port";
  public static final int DFS_HA_ZKFC_PORT_DEFAULT = 8019;
  public static final String DFS_HA_ZKFC_NN_HTTP_TIMEOUT_KEY = "dfs.ha.zkfc.nn.http.timeout.ms";
  public static final int DFS_HA_ZKFC_NN_HTTP_TIMEOUT_KEY_DEFAULT = 20000;
  /** Enable Zookeeper SSL/TLS communication. */
  public static final String ZK_CLIENT_SSL_ENABLED = "dfs.ha.zkfc.client.ssl.enabled";
  public static final boolean DEFAULT_ZK_CLIENT_SSL_ENABLED = false;
  public static final String DFS_HA_NN_NOT_BECOME_ACTIVE_IN_SAFEMODE =
      "dfs.ha.nn.not-become-active-in-safemode";
  public static final boolean DFS_HA_NN_NOT_BECOME_ACTIVE_IN_SAFEMODE_DEFAULT =
      false;
  public static final String DFS_HA_ALLOW_STALE_READ_KEY =
      "dfs.ha.allow.stale.reads";
  public static final boolean DFS_HA_ALLOW_STALE_READ_DEFAULT = false;

  // Security-related configs
  public static final String DFS_ENCRYPT_DATA_TRANSFER_KEY = "dfs.encrypt.data.transfer";
  public static final String DFS_ENCRYPT_DATA_OVERWRITE_DOWNSTREAM_DERIVED_QOP_KEY =
      "dfs.encrypt.data.overwrite.downstream.derived.qop";
  public static final boolean DFS_ENCRYPT_DATA_OVERWRITE_DOWNSTREAM_DERIVED_QOP_DEFAULT =
      false;
  public static final boolean DFS_ENCRYPT_DATA_TRANSFER_DEFAULT = false;
  public static final String DFS_XFRAME_OPTION_ENABLED = "dfs.xframe.enabled";
  public static final boolean DFS_XFRAME_OPTION_ENABLED_DEFAULT = true;

  public static final String DFS_XFRAME_OPTION_VALUE = "dfs.xframe.value";
  public static final String DFS_XFRAME_OPTION_VALUE_DEFAULT = "SAMEORIGIN";

  @Deprecated
  public static final String DFS_ENCRYPT_DATA_TRANSFER_CIPHER_KEY_BITLENGTH_KEY =
      HdfsClientConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_CIPHER_KEY_BITLENGTH_KEY;
  @Deprecated
  public static final int    DFS_ENCRYPT_DATA_TRANSFER_CIPHER_KEY_BITLENGTH_DEFAULT =
      HdfsClientConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_CIPHER_KEY_BITLENGTH_DEFAULT;
  @Deprecated
  public static final String DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY =
      HdfsClientConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY;
  public static final String DFS_DATA_ENCRYPTION_ALGORITHM_KEY = "dfs.encrypt.data.transfer.algorithm";
  @Deprecated
  public static final String DFS_TRUSTEDCHANNEL_RESOLVER_CLASS =
      HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS;
  @Deprecated
  public static final String DFS_DATA_TRANSFER_PROTECTION_KEY =
      HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
  @Deprecated
  public static final String DFS_DATA_TRANSFER_PROTECTION_DEFAULT =
      HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_DEFAULT;
  @Deprecated
  public static final String DFS_DATA_TRANSFER_SASL_PROPS_RESOLVER_CLASS_KEY =
      HdfsClientConfigKeys.DFS_DATA_TRANSFER_SASL_PROPS_RESOLVER_CLASS_KEY;
  public static final int    DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES_DEFAULT = 100;
  public static final String DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES = "dfs.namenode.list.encryption.zones.num.responses";
  public static final int    DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_DEFAULT = 100;
  public static final String DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_KEY = "dfs.namenode.list.reencryption.status.num.responses";
  public static final String DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES =
      "dfs.namenode.list.openfiles.num.responses";
  public static final int    DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES_DEFAULT =
      1000;
  public static final String DFS_NAMENODE_EDEKCACHELOADER_INTERVAL_MS_KEY = "dfs.namenode.edekcacheloader.interval.ms";
  public static final int DFS_NAMENODE_EDEKCACHELOADER_INTERVAL_MS_DEFAULT = 1000;
  public static final String DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY = "dfs.namenode.edekcacheloader.initial.delay.ms";
  public static final int DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_DEFAULT = 3000;
  public static final String DFS_NAMENODE_EDEKCACHELOADER_MAX_RETRIES_KEY =
      "dfs.namenode.edekcacheloader.max-retries";
  public static final int DFS_NAMENODE_EDEKCACHELOADER_MAX_RETRIES_DEFAULT = 10;
  public static final String DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_KEY = "dfs.namenode.reencrypt.sleep.interval";
  public static final String DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_DEFAULT = "1m";
  public static final String DFS_NAMENODE_REENCRYPT_BATCH_SIZE_KEY = "dfs.namenode.reencrypt.batch.size";
  public static final int DFS_NAMENODE_REENCRYPT_BATCH_SIZE_DEFAULT = 1000;
  public static final String DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_KEY = "dfs.namenode.reencrypt.throttle.limit.handler.ratio";
  public static final double DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_DEFAULT = 1.0;
  public static final String DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_UPDATER_RATIO_KEY = "dfs.namenode.reencrypt.throttle.limit.updater.ratio";
  public static final double DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_UPDATER_RATIO_DEFAULT = 1.0;
  public static final String DFS_NAMENODE_REENCRYPT_EDEK_THREADS_KEY = "dfs.namenode.reencrypt.edek.threads";
  public static final int DFS_NAMENODE_REENCRYPT_EDEK_THREADS_DEFAULT = 10;

  // Journal-node related configs. These are read on the JN side.
  public static final String  DFS_JOURNALNODE_EDITS_DIR_KEY = "dfs.journalnode.edits.dir";
  public static final String  DFS_JOURNALNODE_EDITS_DIR_DEFAULT = "/tmp/hadoop/dfs/journalnode/";
  public static final String  DFS_JOURNALNODE_RPC_ADDRESS_KEY = "dfs.journalnode.rpc-address";
  public static final int     DFS_JOURNALNODE_RPC_PORT_DEFAULT = 8485;
  public static final String  DFS_JOURNALNODE_RPC_BIND_HOST_KEY = "dfs.journalnode.rpc-bind-host";
  public static final String  DFS_JOURNALNODE_RPC_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_JOURNALNODE_RPC_PORT_DEFAULT;
  public static final String DFS_JOURNAL_EDITS_DIR_PERMISSION_KEY =
      "dfs.journalnode.edits.dir.perm";
  public static final String DFS_JOURNAL_EDITS_DIR_PERMISSION_DEFAULT =
      "700";
  public static final String  DFS_JOURNALNODE_HANDLER_COUNT_KEY =
      "dfs.journalnode.handler.count";
  public static final int     DFS_JOURNALNODE_HANDLER_COUNT_DEFAULT = 5;


  public static final String  DFS_JOURNALNODE_HTTP_ADDRESS_KEY = "dfs.journalnode.http-address";
  public static final int     DFS_JOURNALNODE_HTTP_PORT_DEFAULT = 8480;
  public static final String  DFS_JOURNALNODE_HTTP_BIND_HOST_KEY = "dfs.journalnode.http-bind-host";
  public static final String  DFS_JOURNALNODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_JOURNALNODE_HTTP_PORT_DEFAULT;
  public static final String  DFS_JOURNALNODE_HTTPS_ADDRESS_KEY = "dfs.journalnode.https-address";
  public static final int     DFS_JOURNALNODE_HTTPS_PORT_DEFAULT = 8481;
  public static final String  DFS_JOURNALNODE_HTTPS_BIND_HOST_KEY = "dfs.journalnode.https-bind-host";
  public static final String  DFS_JOURNALNODE_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_JOURNALNODE_HTTPS_PORT_DEFAULT;


  public static final String  DFS_JOURNALNODE_KEYTAB_FILE_KEY = "dfs.journalnode.keytab.file";
  public static final String  DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY = "dfs.journalnode.kerberos.principal";
  public static final String  DFS_JOURNALNODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY = "dfs.journalnode.kerberos.internal.spnego.principal";
  public static final String DFS_JOURNALNODE_ENABLE_SYNC_KEY =
      "dfs.journalnode.enable.sync";
  public static final boolean DFS_JOURNALNODE_ENABLE_SYNC_DEFAULT = true;
  public static final String DFS_JOURNALNODE_SYNC_INTERVAL_KEY =
      "dfs.journalnode.sync.interval";
  public static final long DFS_JOURNALNODE_SYNC_INTERVAL_DEFAULT = 2*60*1000L;
  public static final String DFS_JOURNALNODE_ENABLE_SYNC_FORMAT_KEY =
      "dfs.journalnode.enable.sync.format";
  public static final boolean DFS_JOURNALNODE_ENABLE_SYNC_FORMAT_DEFAULT = false;
  public static final String DFS_JOURNALNODE_EDIT_CACHE_SIZE_KEY =
      "dfs.journalnode.edit-cache-size.bytes";

  public static final String DFS_JOURNALNODE_EDIT_CACHE_SIZE_FRACTION_KEY =
          "dfs.journalnode.edit-cache-size.fraction";
  public static final float DFS_JOURNALNODE_EDIT_CACHE_SIZE_FRACTION_DEFAULT = 0.5f;

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
  public static final String  DFS_QJOURNAL_HTTP_OPEN_TIMEOUT_KEY = "dfs.qjournal.http.open.timeout.ms";
  public static final String  DFS_QJOURNAL_HTTP_READ_TIMEOUT_KEY = "dfs.qjournal.http.read.timeout.ms";
  public static final String DFS_QJOURNAL_PARALLEL_READ_NUM_THREADS_KEY =
      "dfs.qjournal.parallel-read.num-threads";
  public static final int     DFS_QJOURNAL_START_SEGMENT_TIMEOUT_DEFAULT = 20000;
  public static final int     DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_DEFAULT = 20000;
  public static final int     DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_DEFAULT = 20000;
  public static final int     DFS_QJOURNAL_HTTP_OPEN_TIMEOUT_DEFAULT = URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT;
  public static final int     DFS_QJOURNAL_HTTP_READ_TIMEOUT_DEFAULT = URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT;
  public static final int DFS_QJOURNAL_PARALLEL_READ_NUM_THREADS_DEFAULT = 5;

  public static final String DFS_MAX_NUM_BLOCKS_TO_LOG_KEY = "dfs.namenode.max-num-blocks-to-log";
  public static final long   DFS_MAX_NUM_BLOCKS_TO_LOG_DEFAULT = 1000l;
  
  public static final String DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY = "dfs.namenode.enable.retrycache";
  public static final boolean DFS_NAMENODE_ENABLE_RETRY_CACHE_DEFAULT = true;
  public static final String DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY = "dfs.namenode.retrycache.expirytime.millis";
  public static final long DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT = 600000; // 10 minutes
  public static final String DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_KEY = "dfs.namenode.retrycache.heap.percent";
  public static final float DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_DEFAULT = 0.03f;
  
  // Hidden configuration undocumented in hdfs-site. xml
  // Timeout to wait for block receiver and responder thread to stop
  public static final String DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_KEY = "dfs.datanode.xceiver.stop.timeout.millis";
  public static final long   DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT = 60000;

  // WebHDFS retry policy
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY =
      HdfsClientConfigKeys.HttpClient.RETRY_POLICY_ENABLED_KEY;
  @Deprecated
  public static final boolean DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_DEFAULT =
      HdfsClientConfigKeys.HttpClient.RETRY_POLICY_ENABLED_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_SPEC_KEY =
      HdfsClientConfigKeys.HttpClient.RETRY_POLICY_SPEC_KEY;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_SPEC_DEFAULT =
      HdfsClientConfigKeys.HttpClient.RETRY_POLICY_SPEC_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY =
      HdfsClientConfigKeys.HttpClient.FAILOVER_MAX_ATTEMPTS_KEY;
  @Deprecated
  public static final int     DFS_HTTP_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT =
      HdfsClientConfigKeys.HttpClient.FAILOVER_MAX_ATTEMPTS_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_RETRY_MAX_ATTEMPTS_KEY =
      HdfsClientConfigKeys.HttpClient.RETRY_MAX_ATTEMPTS_KEY;
  @Deprecated
  public static final int     DFS_HTTP_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT =
      HdfsClientConfigKeys.HttpClient.RETRY_MAX_ATTEMPTS_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY =
      HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_BASE_KEY;
  @Deprecated
  public static final int     DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT =
      HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_BASE_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY =
      HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_MAX_KEY;
  @Deprecated
  public static final int     DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT
      = HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_MAX_DEFAULT;

  // Handling unresolved DN topology mapping
  public static final String  DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_KEY = 
      "dfs.namenode.reject-unresolved-dn-topology-mapping";
  public static final boolean DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_DEFAULT =
      false;

  // Slow io warning log threshold settings for dfsclient and datanode.
  public static final String DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY =
      "dfs.datanode.slow.io.warning.threshold.ms";
  public static final long DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT = 300;

  // Number of parallel threads to load multiple datanode volumes
  public static final String DFS_DATANODE_PARALLEL_VOLUME_LOAD_THREADS_NUM_KEY =
      "dfs.datanode.parallel.volumes.load.threads.num";
  public static final String DFS_DATANODE_BLOCK_ID_LAYOUT_UPGRADE_THREADS_KEY =
      "dfs.datanode.block.id.layout.upgrade.threads";
  public static final int DFS_DATANODE_BLOCK_ID_LAYOUT_UPGRADE_THREADS = 6;

  public static final String DFS_NAMENODE_INOTIFY_MAX_EVENTS_PER_RPC_KEY =
      "dfs.namenode.inotify.max.events.per.rpc";
  public static final int DFS_NAMENODE_INOTIFY_MAX_EVENTS_PER_RPC_DEFAULT =
      1000;

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
  public static final String[] NNTOP_WINDOWS_MINUTES_DEFAULT = {"1", "5", "25"};
  public static final String DFS_PIPELINE_ECN_ENABLED = "dfs.pipeline.ecn";
  public static final boolean DFS_PIPELINE_ECN_ENABLED_DEFAULT = false;
  public static final String DFS_PIPELINE_SLOWNODE_ENABLED = "dfs.pipeline.slownode";
  public static final boolean DFS_PIPELINE_SLOWNODE_ENABLED_DEFAULT = false;
  public static final String DFS_PIPELINE_CONGESTION_RATIO = "dfs.pipeline.congestion.ratio";
  public static final double DFS_PIPELINE_CONGESTION_RATIO_DEFAULT = 1.5;

  // Key Provider Cache Expiry
  public static final String DFS_DATANODE_BLOCK_PINNING_ENABLED =
      "dfs.datanode.block-pinning.enabled";
  public static final boolean DFS_DATANODE_BLOCK_PINNING_ENABLED_DEFAULT =
      false;

  public static final String
      DFS_DATANODE_TRANSFER_SOCKET_SEND_BUFFER_SIZE_KEY =
      "dfs.datanode.transfer.socket.send.buffer.size";
  public static final int
      DFS_DATANODE_TRANSFER_SOCKET_SEND_BUFFER_SIZE_DEFAULT =
      HdfsConstants.DEFAULT_DATA_SOCKET_SIZE;

  public static final String
      DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY =
      "dfs.datanode.transfer.socket.recv.buffer.size";
  public static final int
      DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT =
      HdfsConstants.DEFAULT_DATA_SOCKET_SIZE;

  public static final String
      DFS_DATA_TRANSFER_SERVER_TCPNODELAY =
      "dfs.data.transfer.server.tcpnodelay";
  public static final boolean
      DFS_DATA_TRANSFER_SERVER_TCPNODELAY_DEFAULT = true;

  // Disk Balancer Keys
  public static final String DFS_DISK_BALANCER_ENABLED =
      "dfs.disk.balancer.enabled";
  public static final boolean DFS_DISK_BALANCER_ENABLED_DEFAULT = true;

  public static final String DFS_DISK_BALANCER_MAX_DISK_THROUGHPUT =
      "dfs.disk.balancer.max.disk.throughputInMBperSec";
  public static final int DFS_DISK_BALANCER_MAX_DISK_THROUGHPUT_DEFAULT =
      10;

  public static final String DFS_DISK_BALANCER_MAX_DISK_ERRORS =
      "dfs.disk.balancer.max.disk.errors";
  public static final int DFS_DISK_BALANCER_MAX_DISK_ERRORS_DEFAULT = 5;

  public static final String DFS_DISK_BALANCER_PLAN_VALID_INTERVAL =
      "dfs.disk.balancer.plan.valid.interval";
  public static final String DFS_DISK_BALANCER_PLAN_VALID_INTERVAL_DEFAULT =
      "1d";


  public static final String DFS_DISK_BALANCER_BLOCK_TOLERANCE =
      "dfs.disk.balancer.block.tolerance.percent";
  public static final int DFS_DISK_BALANCER_BLOCK_TOLERANCE_DEFAULT = 10;

  public static final String DFS_DISK_BALANCER_PLAN_THRESHOLD =
      "dfs.disk.balancer.plan.threshold.percent";
  public static final int DFS_DISK_BALANCER_PLAN_THRESHOLD_DEFAULT = 10;

  public static final String HTTPFS_BUFFER_SIZE_KEY =
      "httpfs.buffer.size";
  public static final int HTTP_BUFFER_SIZE_DEFAULT = 4096;

  public static final String DFS_USE_DFS_NETWORK_TOPOLOGY_KEY =
      "dfs.use.dfs.network.topology";
  public static final boolean DFS_USE_DFS_NETWORK_TOPOLOGY_DEFAULT = true;

  public static final String DFS_NET_TOPOLOGY_IMPL_KEY =
      "dfs.net.topology.impl";

  public static final Class<DFSNetworkTopology> DFS_NET_TOPOLOGY_IMPL_DEFAULT =
      DFSNetworkTopology.class;

  public static final String DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_KEY =
      HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_KEY;

  public static final String DFS_NAMENODE_SEND_QOP_ENABLED =
      "dfs.namenode.send.qop.enabled";
  public static final boolean DFS_NAMENODE_SEND_QOP_ENABLED_DEFAULT = false;

  public static final String DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY =
      "dfs.namenode.state.context.enabled";
  public static final boolean DFS_NAMENODE_STATE_CONTEXT_ENABLED_DEFAULT = false;

  /**
   * whether to protect the subdirectories of directories which
   * set on fs.protected.directories.
   */
  public static final String DFS_PROTECTED_SUBDIRECTORIES_ENABLE =
      "dfs.protected.subdirectories.enable";
  // Default value for DFS_PROTECTED_SUBDIRECTORIES_ENABLE.
  public static final boolean DFS_PROTECTED_SUBDIRECTORIES_ENABLE_DEFAULT =
      false;

  /**
   *  HDFS-15548 to allow DISK/ARCHIVE configured on the same disk mount.
   *  The default ratio will be applied if DISK/ARCHIVE are configured
   *  on same disk mount.
   *
   *  Beware that capacity usage might be larger than 100% if there are already
   *  data blocks exist and the configured ratio is small, which will
   *  prevent the volume from taking new blocks until capacity is balanced out.
   */
  public static final String DFS_DATANODE_ALLOW_SAME_DISK_TIERING =
      "dfs.datanode.same-disk-tiering.enabled";
  public static final boolean DFS_DATANODE_ALLOW_SAME_DISK_TIERING_DEFAULT =
      false;

  public static final String
      DFS_DATANODE_RESERVE_FOR_ARCHIVE_DEFAULT_PERCENTAGE =
      "dfs.datanode.reserve-for-archive.default.percentage";
  public static final double
      DFS_DATANODE_RESERVE_FOR_ARCHIVE_DEFAULT_PERCENTAGE_DEFAULT = 0.0;

  public static final String
      DFS_DATANODE_SAME_DISK_TIERING_CAPACITY_RATIO_PERCENTAGE =
      "dfs.datanode.same-disk-tiering.capacity-ratio.percentage";
  public static final String
      DFS_DATANODE_SAME_DISK_TIERING_CAPACITY_RATIO_PERCENTAGE_DEFAULT = "";

  public static final String
      DFS_NAMESERVICES_RESOLUTION_ENABLED =
      "dfs.datanode.nameservices.resolution-enabled";
  public static final boolean
      DFS_NAMESERVICES_RESOLUTION_ENABLED_DEFAULT = false;

  public static final String
      DFS_NAMESERVICES_RESOLVER_IMPL =
      "dfs.datanode.nameservices.resolver.impl";

  public static final String
      DFS_DATANODE_LOCKMANAGER_TRACE =
      "dfs.datanode.lockmanager.trace";

  public static final boolean
      DFS_DATANODE_LOCKMANAGER_TRACE_DEFAULT = false;

  // dfs.client.retry confs are moved to HdfsClientConfigKeys.Retry
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_POLICY_ENABLED_KEY
      = HdfsClientConfigKeys.Retry.POLICY_ENABLED_KEY;
  @Deprecated
  public static final boolean DFS_CLIENT_RETRY_POLICY_ENABLED_DEFAULT
      = HdfsClientConfigKeys.Retry.POLICY_ENABLED_DEFAULT; 
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_POLICY_SPEC_KEY
      = HdfsClientConfigKeys.Retry.POLICY_SPEC_KEY;
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_POLICY_SPEC_DEFAULT
      = HdfsClientConfigKeys.Retry.POLICY_SPEC_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_TIMES_GET_LAST_BLOCK_LENGTH
      = HdfsClientConfigKeys.Retry.TIMES_GET_LAST_BLOCK_LENGTH_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_RETRY_TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT
      = HdfsClientConfigKeys.Retry.TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_INTERVAL_GET_LAST_BLOCK_LENGTH
      = HdfsClientConfigKeys.Retry.INTERVAL_GET_LAST_BLOCK_LENGTH_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_RETRY_INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT
      = HdfsClientConfigKeys.Retry.INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_MAX_ATTEMPTS_KEY
      = HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT
      = HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_WINDOW_BASE
      = HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_RETRY_WINDOW_BASE_DEFAULT
      = HdfsClientConfigKeys.Retry.WINDOW_BASE_DEFAULT;

  // dfs.client.failover confs are moved to HdfsClientConfigKeys.Failover 
  @Deprecated
  public static final String  DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX
      = HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX;
  @Deprecated
  public static final String  DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY
      = HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT
      = HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY
      = HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT
      = HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY
      = HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT
      = HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_KEY
      = HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_DEFAULT
      = HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY
      = HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT
      = HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT;
  
  // dfs.client.write confs are moved to HdfsClientConfigKeys.Write 
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_MAX_PACKETS_IN_FLIGHT_KEY
      = HdfsClientConfigKeys.Write.MAX_PACKETS_IN_FLIGHT_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_WRITE_MAX_PACKETS_IN_FLIGHT_DEFAULT
      = HdfsClientConfigKeys.Write.MAX_PACKETS_IN_FLIGHT_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL
      = HdfsClientConfigKeys.Write.EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_KEY;
  @Deprecated
  public static final long    DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT
      = HdfsClientConfigKeys.Write.EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT; // 10 minutes, in ms
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_ENABLED_KEY
      = HdfsClientConfigKeys.Write.ByteArrayManager.ENABLED_KEY;
  @Deprecated
  public static final boolean DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_ENABLED_DEFAULT
      = HdfsClientConfigKeys.Write.ByteArrayManager.ENABLED_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_THRESHOLD_KEY
      = HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_THRESHOLD_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_THRESHOLD_DEFAULT
      = HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_THRESHOLD_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_LIMIT_KEY
      = HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_LIMIT_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_LIMIT_DEFAULT
      = HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_LIMIT_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_RESET_TIME_PERIOD_MS_KEY
      = HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_RESET_TIME_PERIOD_MS_KEY;
  @Deprecated
  public static final long    DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_RESET_TIME_PERIOD_MS_DEFAULT
      = HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_RESET_TIME_PERIOD_MS_DEFAULT;

  // dfs.client.block.write confs are moved to HdfsClientConfigKeys.BlockWrite 
  @Deprecated
  public static final String  DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY
      = HdfsClientConfigKeys.BlockWrite.RETRIES_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT
      = HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY
      = HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT
      = HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_KEY
      = HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_DEFAULT
      = HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY
      = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY;
  @Deprecated
  public static final boolean DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_DEFAULT
      = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY
      = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_DEFAULT
      = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY
      = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY;
  @Deprecated
  public static final boolean DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_DEFAULT
      = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_DEFAULT;

  // dfs.client.read confs are moved to HdfsClientConfigKeys.Read 
  @Deprecated
  public static final String  DFS_CLIENT_READ_PREFETCH_SIZE_KEY
      = HdfsClientConfigKeys.Read.PREFETCH_SIZE_KEY; 
  @Deprecated
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_KEY
      = HdfsClientConfigKeys.Read.ShortCircuit.KEY; 
  @Deprecated
  public static final boolean DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT
      = HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY
      = HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY;
  @Deprecated
  public static final boolean DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_DEFAULT
      = HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_KEY
      = HdfsClientConfigKeys.Read.ShortCircuit.BUFFER_SIZE_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_DEFAULT
      = HdfsClientConfigKeys.Read.ShortCircuit.BUFFER_SIZE_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_KEY
      = HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_SIZE_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_DEFAULT
      = HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_SIZE_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_KEY
      = HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_EXPIRY_MS_KEY;
  @Deprecated
  public static final long    DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_DEFAULT
      = HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_EXPIRY_MS_DEFAULT;

  // dfs.client.mmap confs are moved to HdfsClientConfigKeys.Mmap 
  @Deprecated
  public static final String  DFS_CLIENT_MMAP_ENABLED
      = HdfsClientConfigKeys.Mmap.ENABLED_KEY;
  @Deprecated
  public static final boolean DFS_CLIENT_MMAP_ENABLED_DEFAULT
      = HdfsClientConfigKeys.Mmap.ENABLED_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_MMAP_CACHE_SIZE
      = HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_MMAP_CACHE_SIZE_DEFAULT
      = HdfsClientConfigKeys.Mmap.CACHE_SIZE_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS
      = HdfsClientConfigKeys.Mmap.CACHE_TIMEOUT_MS_KEY;
  @Deprecated
  public static final long    DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS_DEFAULT
      = HdfsClientConfigKeys.Mmap.CACHE_TIMEOUT_MS_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_MMAP_RETRY_TIMEOUT_MS
      = HdfsClientConfigKeys.Mmap.RETRY_TIMEOUT_MS_KEY;
  @Deprecated
  public static final long    DFS_CLIENT_MMAP_RETRY_TIMEOUT_MS_DEFAULT
      = HdfsClientConfigKeys.Mmap.RETRY_TIMEOUT_MS_DEFAULT;

  // dfs.client.short.circuit confs are moved to HdfsClientConfigKeys.ShortCircuit 
  @Deprecated
  public static final String  DFS_CLIENT_SHORT_CIRCUIT_REPLICA_STALE_THRESHOLD_MS
      = HdfsClientConfigKeys.ShortCircuit.REPLICA_STALE_THRESHOLD_MS_KEY;
  @Deprecated
  public static final long    DFS_CLIENT_SHORT_CIRCUIT_REPLICA_STALE_THRESHOLD_MS_DEFAULT
      = HdfsClientConfigKeys.ShortCircuit.REPLICA_STALE_THRESHOLD_MS_DEFAULT;

  // dfs.client.hedged.read confs are moved to HdfsClientConfigKeys.HedgedRead 
  @Deprecated
  public static final String  DFS_DFSCLIENT_HEDGED_READ_THRESHOLD_MILLIS
      = HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY;
  @Deprecated
  public static final long    DEFAULT_DFSCLIENT_HEDGED_READ_THRESHOLD_MILLIS
      = HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_DEFAULT;
  @Deprecated
  public static final String  DFS_DFSCLIENT_HEDGED_READ_THREADPOOL_SIZE
      = HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY;
  @Deprecated
  public static final int     DEFAULT_DFSCLIENT_HEDGED_READ_THREADPOOL_SIZE
      = HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_DEFAULT;

  @Deprecated
  public static final String  DFS_CLIENT_SOCKET_TIMEOUT_KEY =
      HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
  @Deprecated
  public static final String  DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY =
      HdfsClientConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT =
      HdfsClientConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY =
      HdfsClientConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY;
  @Deprecated
  public static final long    DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT =
      HdfsClientConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT;

  @Deprecated
  public static final String  DFS_CLIENT_USE_DN_HOSTNAME =
      HdfsClientConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME;
  @Deprecated
  public static final boolean DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT =
      HdfsClientConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_CACHE_DROP_BEHIND_WRITES =
      HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_WRITES;
  @Deprecated
  public static final String  DFS_CLIENT_CACHE_DROP_BEHIND_READS =
      HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS;
  @Deprecated
  public static final String  DFS_CLIENT_CACHE_READAHEAD =
      HdfsClientConfigKeys.DFS_CLIENT_CACHE_READAHEAD;
  @Deprecated
  public static final String  DFS_CLIENT_CACHED_CONN_RETRY_KEY =
      HdfsClientConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT =
      HdfsClientConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT;

  @Deprecated
  public static final String  DFS_CLIENT_CONTEXT = HdfsClientConfigKeys
      .DFS_CLIENT_CONTEXT;
  @Deprecated
  public static final String  DFS_CLIENT_CONTEXT_DEFAULT =
      HdfsClientConfigKeys.DFS_CLIENT_CONTEXT_DEFAULT;

  @Deprecated
  public static final String  DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY =
      HdfsClientConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY;
  @Deprecated
  public static final long    DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT =
      HdfsClientConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT;

  public static final String  DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY;
  public static final String  DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_DEFAULT = "ssl-client.xml";
  public static final String  DFS_CLIENT_HTTPS_NEED_AUTH_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_CLIENT_HTTPS_NEED_AUTH_KEY;
  public static final boolean DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT = false;

  // Much code in hdfs is not yet updated to use these keys.
  // the initial delay (unit is ms) for locateFollowingBlock, the delay time will increase exponentially(double) for each retry.
  @Deprecated
  public static final String  DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY =
      HdfsClientConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT =
      HdfsClientConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT;

  @Deprecated
  public static final String  DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL =
      HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL;
  @Deprecated
  public static final boolean DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT
      = HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_LOCAL_INTERFACES =
      HdfsClientConfigKeys.DFS_CLIENT_LOCAL_INTERFACES;

  @Deprecated
  public static final String  DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC =
      HdfsClientConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC;
  @Deprecated
  public static final boolean DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT =
      HdfsClientConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT;

  @Deprecated
  public static final String  DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY =
      HdfsClientConfigKeys.DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_DEFAULT =
      HdfsClientConfigKeys.DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_DEFAULT;

  @Deprecated
  public static final String  DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_MS =
      HdfsClientConfigKeys.DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_MS;
  @Deprecated
  public static final long    DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT =
      HdfsClientConfigKeys.DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT;

  public static final String DFS_LEASE_HARDLIMIT_KEY =
      HdfsClientConfigKeys.DFS_LEASE_HARDLIMIT_KEY;
  public static final long DFS_LEASE_HARDLIMIT_DEFAULT =
      HdfsClientConfigKeys.DFS_LEASE_HARDLIMIT_DEFAULT;

}
