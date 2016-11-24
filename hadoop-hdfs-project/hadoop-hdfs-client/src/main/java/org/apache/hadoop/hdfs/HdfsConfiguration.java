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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DeprecatedKeys;

/**
 * Adds deprecated keys into the configuration.
 */
@InterfaceAudience.Private
public class HdfsConfiguration extends Configuration {
  static {
    addDeprecatedKeys();

    // adds the default resources
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
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

  /**
   * This method is here so that when invoked, HdfsConfiguration is class-loaded
   * if it hasn't already been previously loaded.  Upon loading the class, the
   * static initializer block above will be executed to add the deprecated keys
   * and to add the default resources. It is safe for this method to be called
   * multiple times as the static initializer block will only get invoked once.
   *
   * This replaces the previously, dangerous practice of other classes calling
   * Configuration.addDefaultResource("hdfs-default.xml") directly without
   * loading this class first, thereby skipping the key deprecation.
   */
  public static void init() {
  }

  private static void addDeprecatedKeys() {
    Configuration.addDeprecations(new DeprecationDelta[]{
        new DeprecationDelta("dfs.backup.address",
            DeprecatedKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY),
        new DeprecationDelta("dfs.backup.http.address",
            DeprecatedKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY),
        new DeprecationDelta("dfs.balance.bandwidthPerSec",
            DeprecatedKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY),
        new DeprecationDelta("dfs.data.dir",
            DeprecatedKeys.DFS_DATANODE_DATA_DIR_KEY),
        new DeprecationDelta("dfs.http.address",
            HdfsClientConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY),
        new DeprecationDelta("dfs.https.address",
            HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY),
        new DeprecationDelta("dfs.max.objects",
            DeprecatedKeys.DFS_NAMENODE_MAX_OBJECTS_KEY),
        new DeprecationDelta("dfs.name.dir",
            DeprecatedKeys.DFS_NAMENODE_NAME_DIR_KEY),
        new DeprecationDelta("dfs.name.dir.restore",
            DeprecatedKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY),
        new DeprecationDelta("dfs.name.edits.dir",
            DeprecatedKeys.DFS_NAMENODE_EDITS_DIR_KEY),
        new DeprecationDelta("dfs.read.prefetch.size",
            HdfsClientConfigKeys.Read.PREFETCH_SIZE_KEY),
        new DeprecationDelta("dfs.safemode.extension",
            DeprecatedKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY),
        new DeprecationDelta("dfs.safemode.threshold.pct",
            DeprecatedKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY),
        new DeprecationDelta("dfs.secondary.http.address",
            DeprecatedKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY),
        new DeprecationDelta("dfs.socket.timeout",
            HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY),
        new DeprecationDelta("fs.checkpoint.dir",
            DeprecatedKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY),
        new DeprecationDelta("fs.checkpoint.edits.dir",
            DeprecatedKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY),
        new DeprecationDelta("fs.checkpoint.period",
            DeprecatedKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY),
        new DeprecationDelta("heartbeat.recheck.interval",
            DeprecatedKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY),
        new DeprecationDelta("dfs.https.client.keystore.resource",
            DeprecatedKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY),
        new DeprecationDelta("dfs.https.need.client.auth",
            DeprecatedKeys.DFS_CLIENT_HTTPS_NEED_AUTH_KEY),
        new DeprecationDelta("slave.host.name",
            DeprecatedKeys.DFS_DATANODE_HOST_NAME_KEY),
        new DeprecationDelta("session.id",
            DeprecatedKeys.DFS_METRICS_SESSION_ID_KEY),
        new DeprecationDelta("dfs.access.time.precision",
            DeprecatedKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY),
        new DeprecationDelta("dfs.replication.considerLoad",
            DeprecatedKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY),
        new DeprecationDelta("dfs.namenode.replication.considerLoad",
            DeprecatedKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY),
        new DeprecationDelta("dfs.namenode.replication.considerLoad.factor",
            DeprecatedKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_FACTOR),
        new DeprecationDelta("dfs.replication.interval",
            DeprecatedKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY),
        new DeprecationDelta("dfs.namenode.replication.interval",
            DeprecatedKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY),
        new DeprecationDelta("dfs.replication.min",
            DeprecatedKeys.DFS_NAMENODE_REPLICATION_MIN_KEY),
        new DeprecationDelta("dfs.replication.pending.timeout.sec",
            DeprecatedKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY),
        new DeprecationDelta("dfs.namenode.replication.pending.timeout-sec",
            DeprecatedKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY),
        new DeprecationDelta("dfs.max-repl-streams",
            DeprecatedKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY),
        new DeprecationDelta("dfs.permissions",
            DeprecatedKeys.DFS_PERMISSIONS_ENABLED_KEY),
        new DeprecationDelta("dfs.permissions.supergroup",
            DeprecatedKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY),
        new DeprecationDelta("dfs.write.packet.size",
            HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY),
        new DeprecationDelta("dfs.block.size",
            HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY),
        new DeprecationDelta("dfs.datanode.max.xcievers",
            DeprecatedKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY),
        new DeprecationDelta("io.bytes.per.checksum",
            HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY),
        new DeprecationDelta("dfs.federation.nameservices",
            HdfsClientConfigKeys.DFS_NAMESERVICES),
        new DeprecationDelta("dfs.federation.nameservice.id",
            DeprecatedKeys.DFS_NAMESERVICE_ID),
        new DeprecationDelta("dfs.encryption.key.provider.uri",
            CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH),
    });
  }

  public static void main(String[] args) {
    init();
    Configuration.dumpDeprecatedKeys();
  }
}
