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

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Adds deprecated keys into the configuration.
 */
@InterfaceAudience.Private
public class HdfsConfiguration extends Configuration {
  static {
    addDeprecatedKeys();
  }

  public HdfsConfiguration() {
    super();
  }

  public HdfsConfiguration(boolean loadDefaults) {
    super(loadDefaults);
  }

  public HdfsConfiguration(Configuration conf) {
    super(conf);
  }

  private static void deprecate(String oldKey, String newKey) {
    Configuration.addDeprecation(oldKey, new String[]{newKey});
  }

  private static void addDeprecatedKeys() {
    deprecate("dfs.backup.address", DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY);
    deprecate("dfs.backup.http.address", DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY);
    deprecate("dfs.balance.bandwidthPerSec", DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY);
    deprecate("dfs.data.dir", DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
    deprecate("dfs.http.address", DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    deprecate("dfs.https.address", DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY);
    deprecate("dfs.max.objects", DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_KEY);
    deprecate("dfs.name.dir", DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
    deprecate("dfs.name.dir.restore", DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY);
    deprecate("dfs.name.edits.dir", DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY);
    deprecate("dfs.read.prefetch.size", DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY);
    deprecate("dfs.safemode.extension", DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY);
    deprecate("dfs.safemode.threshold.pct", DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY);
    deprecate("dfs.secondary.http.address", DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY);
    deprecate("dfs.socket.timeout", DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY);
    deprecate("fs.checkpoint.dir", DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
    deprecate("fs.checkpoint.edits.dir", DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
    deprecate("fs.checkpoint.period", DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY);
    deprecate("fs.checkpoint.size", DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_SIZE_KEY);
    deprecate("dfs.upgrade.permission", DFSConfigKeys.DFS_NAMENODE_UPGRADE_PERMISSION_KEY);
    deprecate("heartbeat.recheck.interval", DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY);
    deprecate("StorageId", DFSConfigKeys.DFS_DATANODE_STORAGEID_KEY);
    deprecate("dfs.https.client.keystore.resource", DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY);
    deprecate("dfs.https.need.client.auth", DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_KEY);
    deprecate("slave.host.name", DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);
    deprecate("session.id", DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
    deprecate("dfs.access.time.precision", DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY);
    deprecate("dfs.replication.considerLoad", DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY);
    deprecate("dfs.replication.interval", DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY);
    deprecate("dfs.replication.min", DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY);
    deprecate("dfs.replication.pending.timeout.sec", DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY);
    deprecate("dfs.max-repl-streams", DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY);
    deprecate("dfs.permissions", DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY);
    deprecate("dfs.permissions.supergroup", DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY);
    deprecate("dfs.write.packet.size", DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY);
    deprecate("dfs.block.size", DFSConfigKeys.DFS_BLOCK_SIZE_KEY);
  }
}
