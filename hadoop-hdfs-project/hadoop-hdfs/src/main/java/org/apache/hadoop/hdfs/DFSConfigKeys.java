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

/** 
 * This class contains constants for configuration keys used
 * in hdfs.
 *
 */

@InterfaceAudience.Private
public class DFSConfigKeys extends CommonConfigurationKeys {

  public static final String  DFS_BLOCK_SIZE_KEY = "dfs.blocksize";
  public static final long    DFS_BLOCK_SIZE_DEFAULT = 64*1024*1024;
  public static final String  DFS_REPLICATION_KEY = "dfs.replication";
  public static final short   DFS_REPLICATION_DEFAULT = 3;
  public static final String  DFS_STREAM_BUFFER_SIZE_KEY = "dfs.stream-buffer-size";
  public static final int     DFS_STREAM_BUFFER_SIZE_DEFAULT = 4096;
  public static final String  DFS_BYTES_PER_CHECKSUM_KEY = "dfs.bytes-per-checksum";
  public static final int     DFS_BYTES_PER_CHECKSUM_DEFAULT = 512;
  public static final String  DFS_CHECKSUM_TYPE_KEY = "dfs.checksum.type";
  public static final String  DFS_CHECKSUM_TYPE_DEFAULT = "CRC32C";
  public static final String  DFS_CLIENT_WRITE_PACKET_SIZE_KEY = "dfs.client-write-packet-size";
  public static final int     DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64*1024;
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY = "dfs.client.block.write.replace-datanode-on-failure.enable";
  public static final boolean DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_DEFAULT = true;
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY = "dfs.client.block.write.replace-datanode-on-failure.policy";
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_DEFAULT = "DEFAULT";
  public static final String  DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY = "dfs.client.socketcache.capacity";
  public static final int     DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT = 16;
  
  public static final String  DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY = "dfs.client.socketcache.expiryMsec";
  public static final long    DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT = 2 * 60 * 1000;
  public static final String  DFS_NAMENODE_BACKUP_ADDRESS_KEY = "dfs.namenode.backup.address";
  public static final String  DFS_NAMENODE_BACKUP_ADDRESS_DEFAULT = "localhost:50100";
  public static final String  DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY = "dfs.namenode.backup.http-address";
  public static final String  DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT = "0.0.0.0:50105";
  public static final String  DFS_NAMENODE_BACKUP_SERVICE_RPC_ADDRESS_KEY = "dfs.namenode.backup.dnrpc-address";
  public static final String  DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY = "dfs.datanode.balance.bandwidthPerSec";
  public static final long    DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT = 1024*1024;
  public static final String  DFS_DATANODE_READAHEAD_BYTES_KEY = "dfs.datanode.readahead.bytes";
  public static final long    DFS_DATANODE_READAHEAD_BYTES_DEFAULT = 0;
  public static final String  DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_KEY = "dfs.datanode.drop.cache.behind.writes";
  public static final boolean DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_DEFAULT = false;
  public static final String  DFS_DATANODE_SYNC_BEHIND_WRITES_KEY = "dfs.datanode.sync.behind.writes";
  public static final boolean DFS_DATANODE_SYNC_BEHIND_WRITES_DEFAULT = false;
  public static final String  DFS_DATANODE_DROP_CACHE_BEHIND_READS_KEY = "dfs.datanode.drop.cache.behind.reads";
  public static final boolean DFS_DATANODE_DROP_CACHE_BEHIND_READS_DEFAULT = false;

  public static final String  DFS_NAMENODE_HTTP_PORT_KEY = "dfs.http.port";
  public static final int     DFS_NAMENODE_HTTP_PORT_DEFAULT = 50070;
  public static final String  DFS_NAMENODE_HTTP_ADDRESS_KEY = "dfs.namenode.http-address";
  public static final String  DFS_NAMENODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_NAMENODE_HTTP_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_RPC_ADDRESS_KEY = "dfs.namenode.rpc-address";
  public static final String  DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY = "dfs.namenode.servicerpc-address";
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
  public static final String  DFS_NAMENODE_SECONDARY_HTTPS_PORT_KEY = "dfs.namenode.secondary.https-port";
  public static final int     DFS_NAMENODE_SECONDARY_HTTPS_PORT_DEFAULT = 50490;
  public static final String  DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY = "dfs.namenode.checkpoint.check.period";
  public static final long    DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT = 60;
  public static final String  DFS_NAMENODE_CHECKPOINT_PERIOD_KEY = "dfs.namenode.checkpoint.period";
  public static final long    DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT = 3600;
  public static final String  DFS_NAMENODE_CHECKPOINT_TXNS_KEY = "dfs.namenode.checkpoint.txns";
  public static final long    DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT = 40000;
  public static final String  DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_KEY = "dfs.namenode.checkpoint.max-retries";
  public static final int     DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_DEFAULT = 3;
  public static final String  DFS_NAMENODE_UPGRADE_PERMISSION_KEY = "dfs.namenode.upgrade.permission";
  public static final int     DFS_NAMENODE_UPGRADE_PERMISSION_DEFAULT = 00777;
  public static final String  DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY = "dfs.namenode.heartbeat.recheck-interval";
  public static final int     DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT = 5*60*1000;
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
  public static final String  DFS_WEBHDFS_ENABLED_KEY = "dfs.webhdfs.enabled";
  public static final boolean DFS_WEBHDFS_ENABLED_DEFAULT = false;
  public static final String  DFS_PERMISSIONS_ENABLED_KEY = "dfs.permissions.enabled";
  public static final boolean DFS_PERMISSIONS_ENABLED_DEFAULT = true;
  public static final String  DFS_PERMISSIONS_SUPERUSERGROUP_KEY = "dfs.permissions.superusergroup";
  public static final String  DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT = "supergroup";
  public static final String  DFS_ADMIN = "dfs.cluster.administrators";
  public static final String  DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY = "dfs.https.server.keystore.resource";
  public static final String  DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_DEFAULT = "ssl-server.xml";
  public static final String  DFS_NAMENODE_NAME_DIR_RESTORE_KEY = "dfs.namenode.name.dir.restore";
  public static final boolean DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT = false;
  public static final String  DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY = "dfs.namenode.support.allow.format";
  public static final boolean DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_DEFAULT = true;
  public static final String  DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_KEY = "dfs.namenode.num.checkpoints.retained";
  public static final int     DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_DEFAULT = 2;
  
  public static final String  DFS_LIST_LIMIT = "dfs.ls.limit";
  public static final int     DFS_LIST_LIMIT_DEFAULT = 1000;
  public static final String  DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY = "dfs.datanode.failed.volumes.tolerated";
  public static final int     DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT = 0;
  public static final String  DFS_DATANODE_SYNCONCLOSE_KEY = "dfs.datanode.synconclose";
  public static final boolean DFS_DATANODE_SYNCONCLOSE_DEFAULT = false;
  public static final String  DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY = "dfs.datanode.socket.reuse.keepalive";
  public static final int     DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_DEFAULT = 1000;

  //Delegation token related keys
  public static final String  DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY = "dfs.namenode.delegation.key.update-interval";
  public static final long    DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT = 24*60*60*1000; // 1 day
  public static final String  DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY = "dfs.namenode.delegation.token.renew-interval";
  public static final long    DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT = 24*60*60*1000;  // 1 day
  public static final String  DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY = "dfs.namenode.delegation.token.max-lifetime";
  public static final long    DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT = 7*24*60*60*1000; // 7 days

  //Filesystem limit keys
  public static final String  DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY = "dfs.namenode.fs-limits.max-component-length";
  public static final int     DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT = 0; // no limit
  public static final String  DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY = "dfs.namenode.fs-limits.max-directory-items";
  public static final int     DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT = 0; // no limit

  //Following keys have no defaults
  public static final String  DFS_DATANODE_DATA_DIR_KEY = "dfs.datanode.data.dir";
  public static final String  DFS_NAMENODE_HTTPS_PORT_KEY = "dfs.https.port";
  public static final int     DFS_NAMENODE_HTTPS_PORT_DEFAULT = 50470;
  public static final String  DFS_NAMENODE_HTTPS_ADDRESS_KEY = "dfs.namenode.https-address";
  public static final String  DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_NAMENODE_HTTPS_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_NAME_DIR_KEY = "dfs.namenode.name.dir";
  public static final String  DFS_NAMENODE_EDITS_DIR_KEY = "dfs.namenode.edits.dir";
  public static final String  DFS_CLIENT_READ_PREFETCH_SIZE_KEY = "dfs.client.read.prefetch.size"; 
  public static final String  DFS_CLIENT_RETRY_WINDOW_BASE= "dfs.client.retry.window.base";
  public static final String  DFS_METRICS_SESSION_ID_KEY = "dfs.metrics.session-id";
  public static final String  DFS_DATANODE_HOST_NAME_KEY = "dfs.datanode.hostname";
  public static final String  DFS_NAMENODE_HOSTS_KEY = "dfs.namenode.hosts";
  public static final String  DFS_NAMENODE_HOSTS_EXCLUDE_KEY = "dfs.namenode.hosts.exclude";
  public static final String  DFS_CLIENT_SOCKET_TIMEOUT_KEY = "dfs.client.socket-timeout";
  public static final String  DFS_NAMENODE_CHECKPOINT_DIR_KEY = "dfs.namenode.checkpoint.dir";
  public static final String  DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY = "dfs.namenode.checkpoint.edits.dir";
  public static final String  DFS_HOSTS = "dfs.hosts";
  public static final String  DFS_HOSTS_EXCLUDE = "dfs.hosts.exclude";

  // Much code in hdfs is not yet updated to use these keys.
  public static final String  DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY = "dfs.client.block.write.locateFollowingBlock.retries";
  public static final int     DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT = 5;
  public static final String  DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY = "dfs.client.block.write.retries";
  public static final int     DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT = 3;
  public static final String  DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY = "dfs.client.max.block.acquire.failures";
  public static final int     DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT = 3;
  public static final String  DFS_CLIENT_USE_LEGACY_BLOCKREADER = "dfs.client.use.legacy.blockreader";
  public static final boolean DFS_CLIENT_USE_LEGACY_BLOCKREADER_DEFAULT = false;
  public static final String  DFS_BALANCER_MOVEDWINWIDTH_KEY = "dfs.balancer.movedWinWidth";
  public static final long    DFS_BALANCER_MOVEDWINWIDTH_DEFAULT = 5400*1000L;
  public static final String  DFS_DATANODE_ADDRESS_KEY = "dfs.datanode.address";
  public static final String  DFS_DATANODE_ADDRESS_DEFAULT = "0.0.0.0:50010";
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
  public static final int     DFS_DATANODE_HANDLER_COUNT_DEFAULT = 3;
  public static final String  DFS_DATANODE_HTTP_ADDRESS_KEY = "dfs.datanode.http.address";
  public static final String  DFS_DATANODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:50075";
  public static final String  DFS_DATANODE_MAX_RECEIVER_THREADS_KEY = "dfs.datanode.max.transfer.threads";
  public static final int     DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT = 4096;
  public static final String  DFS_DATANODE_NUMBLOCKS_KEY = "dfs.datanode.numblocks";
  public static final int     DFS_DATANODE_NUMBLOCKS_DEFAULT = 64;
  public static final String  DFS_DATANODE_SCAN_PERIOD_HOURS_KEY = "dfs.datanode.scan.period.hours";
  public static final int     DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT = 0;
  public static final String  DFS_DATANODE_TRANSFERTO_ALLOWED_KEY = "dfs.datanode.transferTo.allowed";
  public static final boolean DFS_DATANODE_TRANSFERTO_ALLOWED_DEFAULT = true;
  public static final String  DFS_DATANODE_BLOCKVOLUMECHOICEPOLICY = "dfs.datanode.block.volume.choice.policy";
  public static final String  DFS_DATANODE_BLOCKVOLUMECHOICEPOLICY_DEFAULT =
    "org.apache.hadoop.hdfs.server.datanode.RoundRobinVolumesPolicy";
  public static final String  DFS_HEARTBEAT_INTERVAL_KEY = "dfs.heartbeat.interval";
  public static final long    DFS_HEARTBEAT_INTERVAL_DEFAULT = 3;
  public static final String  DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY = "dfs.namenode.decommission.interval";
  public static final int     DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT = 30;
  public static final String  DFS_NAMENODE_DECOMMISSION_NODES_PER_INTERVAL_KEY = "dfs.namenode.decommission.nodes.per.interval";
  public static final int     DFS_NAMENODE_DECOMMISSION_NODES_PER_INTERVAL_DEFAULT = 5;
  public static final String  DFS_NAMENODE_HANDLER_COUNT_KEY = "dfs.namenode.handler.count";
  public static final int     DFS_NAMENODE_HANDLER_COUNT_DEFAULT = 10;
  public static final String  DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY = "dfs.namenode.service.handler.count";
  public static final int     DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT = 10;
  public static final String  DFS_SUPPORT_APPEND_KEY = "dfs.support.append";
  public static final boolean DFS_SUPPORT_APPEND_DEFAULT = true;
  public static final String  DFS_HTTPS_ENABLE_KEY = "dfs.https.enable";
  public static final boolean DFS_HTTPS_ENABLE_DEFAULT = false;
  public static final String  DFS_HTTPS_PORT_KEY = "dfs.https.port";
  public static final int     DFS_HTTPS_PORT_DEFAULT = 50470;
  public static final String  DFS_DEFAULT_CHUNK_VIEW_SIZE_KEY = "dfs.default.chunk.view.size";
  public static final int     DFS_DEFAULT_CHUNK_VIEW_SIZE_DEFAULT = 32*1024;
  public static final String  DFS_DATANODE_HTTPS_ADDRESS_KEY = "dfs.datanode.https.address";
  public static final String  DFS_DATANODE_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:50475";
  public static final String  DFS_DATANODE_IPC_ADDRESS_KEY = "dfs.datanode.ipc.address";
  public static final String  DFS_DATANODE_IPC_ADDRESS_DEFAULT = "0.0.0.0:50020";

  public static final String  DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY = "dfs.block.access.token.enable";
  public static final boolean DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT = false;
  public static final String  DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY = "dfs.block.access.key.update.interval";
  public static final long    DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_DEFAULT = 600L;
  public static final String  DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY = "dfs.block.access.token.lifetime";
  public static final long    DFS_BLOCK_ACCESS_TOKEN_LIFETIME_DEFAULT = 600L;

  public static final String  DFS_REPLICATION_MAX_KEY = "dfs.replication.max";
  public static final int     DFS_REPLICATION_MAX_DEFAULT = 512;
  public static final String  DFS_DF_INTERVAL_KEY = "dfs.df.interval";
  public static final int     DFS_DF_INTERVAL_DEFAULT = 60000;
  public static final String  DFS_BLOCKREPORT_INTERVAL_MSEC_KEY = "dfs.blockreport.intervalMsec";
  public static final long    DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT = 60 * 60 * 1000;
  public static final String  DFS_BLOCKREPORT_INITIAL_DELAY_KEY = "dfs.blockreport.initialDelay";
  public static final int     DFS_BLOCKREPORT_INITIAL_DELAY_DEFAULT = 0;
  public static final String  DFS_BLOCK_INVALIDATE_LIMIT_KEY = "dfs.block.invalidate.limit";
  public static final int     DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT = 1000;
  public static final String  DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED_KEY = "dfs.corruptfilesreturned.max";
  public static final int     DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED = 500;

  public static final String DFS_CLIENT_READ_SHORTCIRCUIT_KEY = "dfs.client.read.shortcircuit";
  public static final boolean DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT = false;
  public static final String DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY = "dfs.client.read.shortcircuit.skip.checksum";
  public static final boolean DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_DEFAULT = false;

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

  //Keys with no defaults
  public static final String  DFS_DATANODE_PLUGINS_KEY = "dfs.datanode.plugins";
  public static final String  DFS_DATANODE_FSDATASET_FACTORY_KEY = "dfs.datanode.fsdataset.factory";
  public static final String  DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY = "dfs.datanode.socket.write.timeout";
  public static final String  DFS_DATANODE_STARTUP_KEY = "dfs.datanode.startup";
  public static final String  DFS_NAMENODE_PLUGINS_KEY = "dfs.namenode.plugins";
  public static final String  DFS_WEB_UGI_KEY = "dfs.web.ugi";
  public static final String  DFS_NAMENODE_STARTUP_KEY = "dfs.namenode.startup";
  public static final String  DFS_DATANODE_KEYTAB_FILE_KEY = "dfs.datanode.keytab.file";
  public static final String  DFS_DATANODE_USER_NAME_KEY = "dfs.datanode.kerberos.principal";
  public static final String  DFS_NAMENODE_KEYTAB_FILE_KEY = "dfs.namenode.keytab.file";
  public static final String  DFS_NAMENODE_USER_NAME_KEY = "dfs.namenode.kerberos.principal";
  public static final String  DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY = "dfs.namenode.kerberos.https.principal";
  public static final String  DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY = "dfs.secondary.namenode.keytab.file";
  public static final String  DFS_SECONDARY_NAMENODE_USER_NAME_KEY = "dfs.secondary.namenode.kerberos.principal";
  public static final String  DFS_SECONDARY_NAMENODE_KRB_HTTPS_USER_NAME_KEY = "dfs.secondary.namenode.kerberos.https.principal";
  public static final String  DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY = "dfs.namenode.name.cache.threshold";
  public static final int     DFS_NAMENODE_NAME_CACHE_THRESHOLD_DEFAULT = 10;
  
  public static final String DFS_FEDERATION_NAMESERVICES = "dfs.federation.nameservices";
  public static final String DFS_FEDERATION_NAMESERVICE_ID = "dfs.federation.nameservice.id";
  public static final String  DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY = "dfs.namenode.resource.check.interval";
  public static final int     DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT = 5000;
  public static final String  DFS_NAMENODE_DU_RESERVED_KEY = "dfs.namenode.resource.du.reserved";
  public static final long    DFS_NAMENODE_DU_RESERVED_DEFAULT = 1024 * 1024 * 100; // 100 MB
  public static final String  DFS_NAMENODE_CHECKED_VOLUMES_KEY = "dfs.namenode.resource.checked.volumes";
  public static final String  DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY = "dfs.web.authentication.kerberos.principal";
  public static final String  DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY = "dfs.web.authentication.kerberos.keytab";
  public static final String DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY = "dfs.block.local-path-access.user";
}
