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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;

import java.util.Set;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurableBase;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryLevelDBAliasMapServer;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.MetricsLoggerTask;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.TokenVerifier;
import org.apache.hadoop.hdfs.server.namenode.ha.ActiveState;
import org.apache.hadoop.hdfs.server.namenode.ha.BootstrapStandby;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.HAState;
import org.apache.hadoop.hdfs.server.namenode.ha.StandbyState;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressMetrics;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.ipc.ExternalCall;
import org.apache.hadoop.ipc.RefreshCallQueueProtocol;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.util.ExitUtil.ExitException;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.GcTimeMonitor;
import org.apache.hadoop.util.GcTimeMonitor.Builder;
import org.apache.hadoop.tracing.Tracer;
import org.apache.hadoop.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_NODES_TO_REPORT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_NODES_TO_REPORT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NN_NOT_BECOME_ACTIVE_IN_SAFEMODE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NN_NOT_BECOME_ACTIVE_IN_SAFEMODE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_IMAGE_PARALLEL_LOAD_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_IMAGE_PARALLEL_LOAD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_AUTO_FAILOVER_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_AUTO_FAILOVER_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_FENCE_METHODS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_ZKFC_PORT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_METRICS_LOGGER_PERIOD_SECONDS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_OBSERVER_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_OBSERVER_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PLUGINS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STARTUP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.FS_PROTECTED_DIRECTORIES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_GC_TIME_MONITOR_SLEEP_INTERVAL_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_GC_TIME_MONITOR_SLEEP_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_GC_TIME_MONITOR_OBSERVATION_WINDOW_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_GC_TIME_MONITOR_OBSERVATION_WINDOW_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_GC_TIME_MONITOR_ENABLE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_GC_TIME_MONITOR_ENABLE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_PLACEMENT_EC_CLASSNAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK_DEFAULT;

import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.ToolRunner.confirmPrompt;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_BACKOFF_ENABLE;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_NAMESPACE;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_BACKOFF_ENABLE_DEFAULT;

/**********************************************************
 * NameNode serves as both directory namespace manager and
 * "inode table" for the Hadoop DFS.  There is a single NameNode
 * running in any DFS deployment.  (Well, except when there
 * is a second backup/failover NameNode, or when using federated NameNodes.)
 *
 * The NameNode controls two critical tables:
 *   1)  filename{@literal ->}blocksequence (namespace)
 *   2)  block{@literal ->}machinelist ("inodes")
 *
 * The first table is stored on disk and is very precious.
 * The second table is rebuilt every time the NameNode comes up.
 *
 * 'NameNode' refers to both this class as well as the 'NameNode server'.
 * The 'FSNamesystem' class actually performs most of the filesystem
 * management.  The majority of the 'NameNode' class itself is concerned
 * with exposing the IPC interface and the HTTP server to the outside world,
 * plus some configuration management.
 *
 * NameNode implements the
 * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol} interface, which
 * allows clients to ask for DFS services.
 * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol} is not designed for
 * direct use by authors of DFS client code.  End-users should instead use the
 * {@link org.apache.hadoop.fs.FileSystem} class.
 *
 * NameNode also implements the
 * {@link org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol} interface,
 * used by DataNodes that actually store DFS data blocks.  These
 * methods are invoked repeatedly and automatically by all the
 * DataNodes in a DFS deployment.
 *
 * NameNode also implements the
 * {@link org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol} interface,
 * used by secondary namenodes or rebalancing processes to get partial
 * NameNode state, for example partial blocksMap etc.
 **********************************************************/
@InterfaceAudience.Private
@Metrics(context="dfs")
public class NameNode extends ReconfigurableBase implements
    NameNodeStatusMXBean, TokenVerifier<DelegationTokenIdentifier> {
  static{
    HdfsConfiguration.init();
  }

  private InMemoryLevelDBAliasMapServer levelDBAliasMapServer;

  /**
   * Categories of operations supported by the namenode.
   */
  public enum OperationCategory {
    /** Operations that are state agnostic */
    UNCHECKED,
    /** Read operation that does not change the namespace state */
    READ,
    /** Write operation that changes the namespace state */
    WRITE,
    /** Operations related to checkpointing */
    CHECKPOINT,
    /** Operations related to {@link JournalProtocol} */
    JOURNAL
  }
  
  /**
   * HDFS configuration can have three types of parameters:
   * <ol>
   * <li>Parameters that are common for all the name services in the cluster.</li>
   * <li>Parameters that are specific to a name service. These keys are suffixed
   * with nameserviceId in the configuration. For example,
   * "dfs.namenode.rpc-address.nameservice1".</li>
   * <li>Parameters that are specific to a single name node. These keys are suffixed
   * with nameserviceId and namenodeId in the configuration. for example,
   * "dfs.namenode.rpc-address.nameservice1.namenode1"</li>
   * </ol>
   * 
   * In the latter cases, operators may specify the configuration without
   * any suffix, with a nameservice suffix, or with a nameservice and namenode
   * suffix. The more specific suffix will take precedence.
   * 
   * These keys are specific to a given namenode, and thus may be configured
   * globally, for a nameservice, or for a specific namenode within a nameservice.
   */
  public static final String[] NAMENODE_SPECIFIC_KEYS = {
    DFS_NAMENODE_RPC_ADDRESS_KEY,
    DFS_NAMENODE_RPC_BIND_HOST_KEY,
    DFS_NAMENODE_NAME_DIR_KEY,
    DFS_NAMENODE_EDITS_DIR_KEY,
    DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
    DFS_NAMENODE_CHECKPOINT_DIR_KEY,
    DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY,
    DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY,
    DFS_NAMENODE_LIFELINE_RPC_BIND_HOST_KEY,
    DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
    DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY,
    DFS_NAMENODE_HTTP_ADDRESS_KEY,
    DFS_NAMENODE_HTTPS_ADDRESS_KEY,
    DFS_NAMENODE_HTTP_BIND_HOST_KEY,
    DFS_NAMENODE_HTTPS_BIND_HOST_KEY,
    DFS_NAMENODE_KEYTAB_FILE_KEY,
    DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
    DFS_NAMENODE_SECONDARY_HTTPS_ADDRESS_KEY,
    DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY,
    DFS_NAMENODE_BACKUP_ADDRESS_KEY,
    DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY,
    DFS_NAMENODE_BACKUP_SERVICE_RPC_ADDRESS_KEY,
    DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
    DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
    DFS_HA_FENCE_METHODS_KEY,
    DFS_HA_ZKFC_PORT_KEY
  };
  
  /**
   * @see #NAMENODE_SPECIFIC_KEYS
   * These keys are specific to a nameservice, but may not be overridden
   * for a specific namenode.
   */
  public static final String[] NAMESERVICE_SPECIFIC_KEYS = {
    DFS_HA_AUTO_FAILOVER_ENABLED_KEY
  };

  private String ipcClientRPCBackoffEnable;

  /** A list of property that are reconfigurable at runtime. */
  private final TreeSet<String> reconfigurableProperties = Sets
      .newTreeSet(Lists.newArrayList(
          DFS_HEARTBEAT_INTERVAL_KEY,
          DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
          FS_PROTECTED_DIRECTORIES,
          HADOOP_CALLER_CONTEXT_ENABLED_KEY,
          DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
          DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY,
          DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY,
          DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION,
          DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
          DFS_BLOCK_PLACEMENT_EC_CLASSNAME_KEY,
          DFS_IMAGE_PARALLEL_LOAD_KEY,
          DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_KEY,
          DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY,
          DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY,
          DFS_BLOCK_INVALIDATE_LIMIT_KEY,
          DFS_DATANODE_PEER_STATS_ENABLED_KEY,
          DFS_DATANODE_MAX_NODES_TO_REPORT_KEY,
          DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY,
          DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT,
          DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK,
          DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_KEY));

  private static final String USAGE = "Usage: hdfs namenode ["
      + StartupOption.BACKUP.getName() + "] | \n\t["
      + StartupOption.CHECKPOINT.getName() + "] | \n\t["
      + StartupOption.FORMAT.getName() + " ["
      + StartupOption.CLUSTERID.getName() + " cid ] ["
      + StartupOption.FORCE.getName() + "] ["
      + StartupOption.NONINTERACTIVE.getName() + "] ] | \n\t["
      + StartupOption.UPGRADE.getName() + 
        " [" + StartupOption.CLUSTERID.getName() + " cid]" +
        " [" + StartupOption.RENAMERESERVED.getName() + "<k-v pairs>] ] | \n\t["
      + StartupOption.UPGRADEONLY.getName() + 
        " [" + StartupOption.CLUSTERID.getName() + " cid]" +
        " [" + StartupOption.RENAMERESERVED.getName() + "<k-v pairs>] ] | \n\t["
      + StartupOption.ROLLBACK.getName() + "] | \n\t["
      + StartupOption.ROLLINGUPGRADE.getName() + " "
      + RollingUpgradeStartupOption.getAllOptionString() + " ] | \n\t["
      + StartupOption.IMPORT.getName() + "] | \n\t["
      + StartupOption.INITIALIZESHAREDEDITS.getName() + "] | \n\t["
      + StartupOption.BOOTSTRAPSTANDBY.getName() + " ["
      + StartupOption.FORCE.getName() + "] ["
      + StartupOption.NONINTERACTIVE.getName() + "] ["
      + StartupOption.SKIPSHAREDEDITSCHECK.getName() + "] ] | \n\t["
      + StartupOption.RECOVER.getName() + " [ "
      + StartupOption.FORCE.getName() + "] ] | \n\t["
      + StartupOption.METADATAVERSION.getName() + " ]";

  
  public long getProtocolVersion(String protocol, 
                                 long clientVersion) throws IOException {
    if (protocol.equals(ClientProtocol.class.getName())) {
      return ClientProtocol.versionID; 
    } else if (protocol.equals(DatanodeProtocol.class.getName())){
      return DatanodeProtocol.versionID;
    } else if (protocol.equals(NamenodeProtocol.class.getName())){
      return NamenodeProtocol.versionID;
    } else if (protocol.equals(RefreshAuthorizationPolicyProtocol.class.getName())){
      return RefreshAuthorizationPolicyProtocol.versionID;
    } else if (protocol.equals(RefreshUserMappingsProtocol.class.getName())){
      return RefreshUserMappingsProtocol.versionID;
    } else if (protocol.equals(RefreshCallQueueProtocol.class.getName())) {
      return RefreshCallQueueProtocol.versionID;
    } else if (protocol.equals(GetUserMappingsProtocol.class.getName())){
      return GetUserMappingsProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }
    
  /**
   * @deprecated Use {@link HdfsClientConfigKeys#DFS_NAMENODE_RPC_PORT_DEFAULT}
   *             instead.
   */
  @Deprecated
  public static final int DEFAULT_PORT = DFS_NAMENODE_RPC_PORT_DEFAULT;
  public static final Logger LOG =
      LoggerFactory.getLogger(NameNode.class.getName());
  public static final Logger stateChangeLog =
      LoggerFactory.getLogger("org.apache.hadoop.hdfs.StateChange");
  public static final Logger blockStateChangeLog =
      LoggerFactory.getLogger("BlockStateChange");
  public static final HAState ACTIVE_STATE = new ActiveState();
  public static final HAState STANDBY_STATE = new StandbyState();
  public static final HAState OBSERVER_STATE = new StandbyState(true);

  private static final String NAMENODE_HTRACE_PREFIX = "namenode.htrace.";

  public static final String METRICS_LOG_NAME = "NameNodeMetricsLog";

  protected FSNamesystem namesystem; 
  protected final NamenodeRole role;
  private volatile HAState state;
  private final boolean haEnabled;
  private final HAContext haContext;
  protected final boolean allowStaleStandbyReads;
  private AtomicBoolean started = new AtomicBoolean(false);
  private final boolean notBecomeActiveInSafemode;

  private final static int HEALTH_MONITOR_WARN_THRESHOLD_MS = 5000;
  
  /** httpServer */
  protected NameNodeHttpServer httpServer;
  private Thread emptier;
  /** only used for testing purposes  */
  protected boolean stopRequested = false;
  /** Registration information of this name-node  */
  protected NamenodeRegistration nodeRegistration;
  /** Activated plug-ins. */
  private List<ServicePlugin> plugins;
  
  private NameNodeRpcServer rpcServer;

  private JvmPauseMonitor pauseMonitor;
  private GcTimeMonitor gcTimeMonitor;
  private ObjectName nameNodeStatusBeanName;
  protected final Tracer tracer;
  ScheduledThreadPoolExecutor metricsLoggerTimer;

  /**
   * The namenode address that clients will use to access this namenode
   * or the name service. For HA configurations using logical URI, it
   * will be the logical address.
   */
  private String clientNamenodeAddress;
  
  /** Format a new filesystem.  Destroys any filesystem that may already
   * exist at this location.  **/
  public static void format(Configuration conf) throws IOException {
    format(conf, true, true);
  }

  static NameNodeMetrics metrics;
  private static final StartupProgress startupProgress = new StartupProgress();
  /** Return the {@link FSNamesystem} object.
   * @return {@link FSNamesystem} object.
   */
  public FSNamesystem getNamesystem() {
    return namesystem;
  }

  public NamenodeProtocols getRpcServer() {
    return rpcServer;
  }

  @VisibleForTesting
  public HttpServer2 getHttpServer() {
    return httpServer.getHttpServer();
  }

  public void queueExternalCall(ExternalCall<?> extCall)
      throws IOException, InterruptedException {
    if (rpcServer == null) {
      throw new RetriableException("Namenode is in startup mode");
    }
    rpcServer.getClientRpcServer().queueCall(extCall);
  }

  public static void initMetrics(Configuration conf, NamenodeRole role) {
    metrics = NameNodeMetrics.create(conf, role);
  }

  public static NameNodeMetrics getNameNodeMetrics() {
    return metrics;
  }

  /**
   * Try to obtain the actual client info according to the current user.
   * @param ipProxyUsers Users who can override client infos
   */
  private static String clientInfoFromContext(
      final String[] ipProxyUsers) {
    if (ipProxyUsers != null) {
      UserGroupInformation user =
          UserGroupInformation.getRealUserOrSelf(Server.getRemoteUser());
      if (user != null &&
          ArrayUtils.contains(ipProxyUsers, user.getShortUserName())) {
        CallerContext context = CallerContext.getCurrent();
        if (context != null && context.isContextValid()) {
          return context.getContext();
        }
      }
    }
    return null;
  }

  /**
   * Try to obtain the value corresponding to the key by parsing the content.
   * @param content the full content to be parsed.
   * @param key trying to obtain the value of the key.
   * @return the value corresponding to the key.
   */
  @VisibleForTesting
  public static String parseSpecialValue(String content, String key) {
    int posn = content.indexOf(key);
    if (posn != -1) {
      posn += key.length();
      int end = content.indexOf(",", posn);
      return end == -1 ? content.substring(posn)
          : content.substring(posn, end);
    }
    return null;
  }

  /**
   * Try to obtain the actual client's machine according to the current user.
   * @param ipProxyUsers Users who can override client infos.
   * @return The actual client's machine.
   */
  public static String getClientMachine(final String[] ipProxyUsers) {
    String clientMachine = null;
    String cc = clientInfoFromContext(ipProxyUsers);
    if (cc != null) {
      // if the rpc has a caller context of "clientIp:1.2.3.4,CLI",
      // return "1.2.3.4" as the client machine.
      String key = CallerContext.CLIENT_IP_STR +
          CallerContext.Builder.KEY_VALUE_SEPARATOR;
      clientMachine = parseSpecialValue(cc, key);
    }

    if (clientMachine == null) {
      clientMachine = Server.getRemoteAddress();
      if (clientMachine == null) { //not a RPC client
        clientMachine = "";
      }
    }
    return clientMachine;
  }

  /**
   * Try to obtain the actual client's id and call id
   * according to the current user.
   * @param ipProxyUsers Users who can override client infos
   * @return The actual client's id and call id.
   */
  public static Pair<byte[], Integer> getClientIdAndCallId(
      final String[] ipProxyUsers) {
    byte[] clientId = Server.getClientId();
    int callId = Server.getCallId();
    String cc = clientInfoFromContext(ipProxyUsers);
    if (cc != null) {
      String clientIdKey = CallerContext.CLIENT_ID_STR +
          CallerContext.Builder.KEY_VALUE_SEPARATOR;
      String clientIdStr = parseSpecialValue(cc, clientIdKey);
      if (clientIdStr != null) {
        clientId = StringUtils.hexStringToByte(clientIdStr);
      }
      String callIdKey = CallerContext.CLIENT_CALL_ID_STR +
          CallerContext.Builder.KEY_VALUE_SEPARATOR;
      String callIdStr = parseSpecialValue(cc, callIdKey);
      if (callIdStr != null) {
        callId = Integer.parseInt(callIdStr);
      }
    }
    return Pair.of(clientId, callId);
  }

  /**
   * Returns object used for reporting namenode startup progress.
   * 
   * @return StartupProgress for reporting namenode startup progress
   */
  public static StartupProgress getStartupProgress() {
    return startupProgress;
  }

  /**
   * Return the service name of the issued delegation token.
   *
   * @return The name service id in HA-mode, or the rpc address in non-HA mode
   */
  public String getTokenServiceName() {
    return getClientNamenodeAddress();
  }

  /**
   * Get the namenode address to be used by clients.
   * @return nn address
   */
  public String getClientNamenodeAddress() {
    return clientNamenodeAddress;
  }

  /**
   * Set the configuration property for the service rpc address
   * to address
   */
  public static void setServiceAddress(Configuration conf,
                                           String address) {
    LOG.info("Setting ADDRESS {}", address);
    conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, address);
  }
  
  /**
   * Fetches the address for services to use when connecting to namenode
   * based on the value of fallback returns null if the special
   * address is not specified or returns the default namenode address
   * to be used by both clients and services.
   * Services here are datanodes, backup node, any non client connection
   */
  public static InetSocketAddress getServiceAddress(Configuration conf,
                                                        boolean fallback) {
    String addr = conf.getTrimmed(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY);
    if (addr == null || addr.isEmpty()) {
      return fallback ? DFSUtilClient.getNNAddress(conf) : null;
    }
    return DFSUtilClient.getNNAddress(addr);
  }

  //
  // Common NameNode methods implementation for the active name-node role.
  //
  public NamenodeRole getRole() {
    return role;
  }

  boolean isRole(NamenodeRole that) {
    return role.equals(that);
  }

  public static String composeNotStartedMessage(NamenodeRole role) {
    return role + " still not started";
  }

  /**
   * Given a configuration get the address of the lifeline RPC server.
   * If the lifeline RPC is not configured returns null.
   *
   * @param conf configuration
   * @return address or null
   */
  InetSocketAddress getLifelineRpcServerAddress(Configuration conf) {
    String addr = getTrimmedOrNull(conf, DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY);
    if (addr == null) {
      return null;
    }
    return NetUtils.createSocketAddr(addr);
  }

  /**
   * Given a configuration get the address of the service rpc server
   * If the service rpc is not configured returns null
   */
  protected InetSocketAddress getServiceRpcServerAddress(Configuration conf) {
    return NameNode.getServiceAddress(conf, false);
  }

  protected InetSocketAddress getRpcServerAddress(Configuration conf) {
    return DFSUtilClient.getNNAddress(conf);
  }

  /**
   * Given a configuration get the bind host of the lifeline RPC server.
   * If the bind host is not configured returns null.
   *
   * @param conf configuration
   * @return bind host or null
   */
  String getLifelineRpcServerBindHost(Configuration conf) {
    return getTrimmedOrNull(conf, DFS_NAMENODE_LIFELINE_RPC_BIND_HOST_KEY);
  }

  /** Given a configuration get the bind host of the service rpc server
   *  If the bind host is not configured returns null.
   */
  protected String getServiceRpcServerBindHost(Configuration conf) {
    return getTrimmedOrNull(conf, DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY);
  }

  /** Given a configuration get the bind host of the client rpc server
   *  If the bind host is not configured returns null.
   */
  protected String getRpcServerBindHost(Configuration conf) {
    return getTrimmedOrNull(conf, DFS_NAMENODE_RPC_BIND_HOST_KEY);
  }

  /**
   * Gets a trimmed value from configuration, or null if no value is defined.
   *
   * @param conf configuration
   * @param key configuration key to get
   * @return trimmed value, or null if no value is defined
   */
  private static String getTrimmedOrNull(Configuration conf, String key) {
    String addr = conf.getTrimmed(key);
    if (addr == null || addr.isEmpty()) {
      return null;
    }
    return addr;
  }
   
  /**
   * Modifies the configuration to contain the lifeline RPC address setting.
   *
   * @param conf configuration to modify
   * @param lifelineRPCAddress lifeline RPC address
   */
  void setRpcLifelineServerAddress(Configuration conf,
      InetSocketAddress lifelineRPCAddress) {
    LOG.info("Setting lifeline RPC address {}", lifelineRPCAddress);
    conf.set(DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY,
        NetUtils.getHostPortString(lifelineRPCAddress));
  }

  /**
   * Modifies the configuration passed to contain the service rpc address setting
   */
  protected void setRpcServiceServerAddress(Configuration conf,
      InetSocketAddress serviceRPCAddress) {
    setServiceAddress(conf, NetUtils.getHostPortString(serviceRPCAddress));
  }

  protected void setRpcServerAddress(Configuration conf,
      InetSocketAddress rpcAddress) {
    FileSystem.setDefaultUri(conf, DFSUtilClient.getNNUri(rpcAddress));
  }

  protected InetSocketAddress getHttpServerAddress(Configuration conf) {
    return getHttpAddress(conf);
  }

  /**
   * HTTP server address for binding the endpoint. This method is
   * for use by the NameNode and its derivatives. It may return
   * a different address than the one that should be used by clients to
   * connect to the NameNode. See
   * {@link DFSConfigKeys#DFS_NAMENODE_HTTP_BIND_HOST_KEY}
   *
   * @param conf
   * @return
   */
  protected InetSocketAddress getHttpServerBindAddress(Configuration conf) {
    InetSocketAddress bindAddress = getHttpServerAddress(conf);

    // If DFS_NAMENODE_HTTP_BIND_HOST_KEY exists then it overrides the
    // host name portion of DFS_NAMENODE_HTTP_ADDRESS_KEY.
    final String bindHost = conf.getTrimmed(DFS_NAMENODE_HTTP_BIND_HOST_KEY);
    if (bindHost != null && !bindHost.isEmpty()) {
      bindAddress = new InetSocketAddress(bindHost, bindAddress.getPort());
    }

    return bindAddress;
  }

  /** @return the NameNode HTTP address. */
  public static InetSocketAddress getHttpAddress(Configuration conf) {
    return  NetUtils.createSocketAddr(
        conf.getTrimmed(DFS_NAMENODE_HTTP_ADDRESS_KEY, DFS_NAMENODE_HTTP_ADDRESS_DEFAULT));
  }

  protected void loadNamesystem(Configuration conf) throws IOException {
    this.namesystem = FSNamesystem.loadFromDisk(conf);
  }

  NamenodeRegistration getRegistration() {
    return nodeRegistration;
  }

  NamenodeRegistration setRegistration() {
    nodeRegistration = new NamenodeRegistration(
        NetUtils.getHostPortString(getNameNodeAddress()),
        NetUtils.getHostPortString(getHttpAddress()),
        getFSImage().getStorage(), getRole());
    return nodeRegistration;
  }

  /* optimize ugi lookup for RPC operations to avoid a trip through
   * UGI.getCurrentUser which is synch'ed
   */
  public static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  @Override
  public void verifyToken(DelegationTokenIdentifier id, byte[] password)
      throws IOException {
    // during startup namesystem is null, let client retry
    if (namesystem == null) {
      throw new RetriableException("Namenode is in startup mode");
    }
    namesystem.verifyToken(id, password);
  }

  /**
   * Login as the configured user for the NameNode.
   */
  void loginAsNameNodeUser(Configuration conf) throws IOException {
    InetSocketAddress socAddr = getRpcServerAddress(conf);
    SecurityUtil.login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY,
        DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, socAddr.getHostName());
  }
  
  /**
   * Initialize name-node.
   * 
   * @param conf the configuration
   */
  protected void initialize(Configuration conf) throws IOException {
    if (conf.get(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS) == null) {
      String intervals = conf.get(DFS_METRICS_PERCENTILES_INTERVALS_KEY);
      if (intervals != null) {
        conf.set(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS,
          intervals);
      }
    }

    UserGroupInformation.setConfiguration(conf);
    loginAsNameNodeUser(conf);

    NameNode.initMetrics(conf, this.getRole());
    StartupProgressMetrics.register(startupProgress);

    pauseMonitor = new JvmPauseMonitor();
    pauseMonitor.init(conf);
    pauseMonitor.start();
    metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);

    if (conf.getBoolean(DFS_NAMENODE_GC_TIME_MONITOR_ENABLE,
        DFS_NAMENODE_GC_TIME_MONITOR_ENABLE_DEFAULT)) {
      long observationWindow = conf.getTimeDuration(
          DFS_NAMENODE_GC_TIME_MONITOR_OBSERVATION_WINDOW_MS,
          DFS_NAMENODE_GC_TIME_MONITOR_OBSERVATION_WINDOW_MS_DEFAULT,
          TimeUnit.MILLISECONDS);
      long sleepInterval = conf.getTimeDuration(
          DFS_NAMENODE_GC_TIME_MONITOR_SLEEP_INTERVAL_MS,
          DFS_NAMENODE_GC_TIME_MONITOR_SLEEP_INTERVAL_MS_DEFAULT,
          TimeUnit.MILLISECONDS);
      gcTimeMonitor = new Builder().observationWindowMs(observationWindow)
          .sleepIntervalMs(sleepInterval).build();
      gcTimeMonitor.start();
      metrics.getJvmMetrics().setGcTimeMonitor(gcTimeMonitor);
    }

    if (NamenodeRole.NAMENODE == role) {
      startHttpServer(conf);
    }

    loadNamesystem(conf);
    startAliasMapServerIfNecessary(conf);

    rpcServer = createRpcServer(conf);

    initReconfigurableBackoffKey();

    if (clientNamenodeAddress == null) {
      // This is expected for MiniDFSCluster. Set it now using 
      // the RPC server's bind address.
      clientNamenodeAddress = 
          NetUtils.getHostPortString(getNameNodeAddress());
      LOG.info("Clients are to use " + clientNamenodeAddress + " to access"
          + " this namenode/service.");
    }
    if (NamenodeRole.NAMENODE == role) {
      httpServer.setNameNodeAddress(getNameNodeAddress());
      httpServer.setFSImage(getFSImage());
      if (levelDBAliasMapServer != null) {
        httpServer.setAliasMap(levelDBAliasMapServer.getAliasMap());
      }
    }

    startCommonServices(conf);
    startMetricsLogger(conf);
  }

  @VisibleForTesting
  public InMemoryLevelDBAliasMapServer getAliasMapServer() {
    return levelDBAliasMapServer;
  }

  private void startAliasMapServerIfNecessary(Configuration conf)
      throws IOException {
    if (conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED,
        DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED_DEFAULT)
        && conf.getBoolean(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED,
            DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED_DEFAULT)) {
      levelDBAliasMapServer = new InMemoryLevelDBAliasMapServer(
          InMemoryAliasMap::init, namesystem.getBlockPoolId());
      levelDBAliasMapServer.setConf(conf);
      levelDBAliasMapServer.start();
    }
  }

  private void initReconfigurableBackoffKey() {
    ipcClientRPCBackoffEnable = buildBackoffEnableKey(rpcServer
        .getClientRpcServer().getPort());
    reconfigurableProperties.add(ipcClientRPCBackoffEnable);
  }

  static String buildBackoffEnableKey(final int port) {
    // format used to construct backoff enable key, e.g. ipc.8020.backoff.enable
    String format = "%s.%d.%s";
    return String.format(format, IPC_NAMESPACE, port, IPC_BACKOFF_ENABLE);
  }

  /**
   * Start a timer to periodically write NameNode metrics to the log
   * file. This behavior can be disabled by configuration.
   * @param conf
   */
  protected void startMetricsLogger(Configuration conf) {
    long metricsLoggerPeriodSec =
        conf.getInt(DFS_NAMENODE_METRICS_LOGGER_PERIOD_SECONDS_KEY,
            DFS_NAMENODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT);

    if (metricsLoggerPeriodSec <= 0) {
      return;
    }

    // Schedule the periodic logging.
    metricsLoggerTimer = new ScheduledThreadPoolExecutor(1);
    metricsLoggerTimer.setExecuteExistingDelayedTasksAfterShutdownPolicy(
        false);
    metricsLoggerTimer.scheduleWithFixedDelay(new MetricsLoggerTask(METRICS_LOG_NAME,
        "NameNode", (short) 128),
        metricsLoggerPeriodSec,
        metricsLoggerPeriodSec,
        TimeUnit.SECONDS);
  }

  protected void stopMetricsLogger() {
    if (metricsLoggerTimer != null) {
      metricsLoggerTimer.shutdown();
      metricsLoggerTimer = null;
    }
  }
  
  /**
   * Create the RPC server implementation. Used as an extension point for the
   * BackupNode.
   */
  protected NameNodeRpcServer createRpcServer(Configuration conf)
      throws IOException {
    return new NameNodeRpcServer(conf, this);
  }

  /** Start the services common to active and standby states */
  private void startCommonServices(Configuration conf) throws IOException {
    namesystem.startCommonServices(conf, haContext);
    registerNNSMXBean();
    if (NamenodeRole.NAMENODE != role) {
      startHttpServer(conf);
      httpServer.setNameNodeAddress(getNameNodeAddress());
      httpServer.setFSImage(getFSImage());
      if (levelDBAliasMapServer != null) {
        httpServer.setAliasMap(levelDBAliasMapServer.getAliasMap());
      }
    }
    rpcServer.start();
    try {
      plugins = conf.getInstances(DFS_NAMENODE_PLUGINS_KEY,
          ServicePlugin.class);
    } catch (RuntimeException e) {
      String pluginsValue = conf.get(DFS_NAMENODE_PLUGINS_KEY);
      LOG.error("Unable to load NameNode plugins. Specified list of plugins: " +
          pluginsValue, e);
      throw e;
    }
    for (ServicePlugin p: plugins) {
      try {
        p.start(this);
      } catch (Throwable t) {
        LOG.warn("ServicePlugin " + p + " could not be started", t);
      }
    }
    LOG.info("{} RPC up at: {}.", getRole(), getNameNodeAddress());
    if (rpcServer.getServiceRpcAddress() != null) {
      LOG.info("{} service RPC up at: {}.", getRole(), rpcServer.getServiceRpcAddress());
    }
    if (rpcServer.getLifelineRpcAddress() != null) {
      LOG.info("{} lifeline RPC up at: {}.", getRole(), rpcServer.getLifelineRpcAddress());
    }
  }
  
  private void stopCommonServices() {
    if(rpcServer != null) rpcServer.stop();
    if(namesystem != null) namesystem.close();
    if (pauseMonitor != null) pauseMonitor.stop();
    if (plugins != null) {
      for (ServicePlugin p : plugins) {
        try {
          p.stop();
        } catch (Throwable t) {
          LOG.warn("ServicePlugin " + p + " could not be stopped", t);
        }
      }
    }   
    stopHttpServer();
  }
  
  private void startTrashEmptier(final Configuration conf) throws IOException {
    long trashInterval =
        conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT);
    if (trashInterval == 0) {
      return;
    } else if (trashInterval < 0) {
      throw new IOException("Cannot start trash emptier with negative interval."
          + " Set " + FS_TRASH_INTERVAL_KEY + " to a positive value.");
    }
    
    // This may be called from the transitionToActive code path, in which
    // case the current user is the administrator, not the NN. The trash
    // emptier needs to run as the NN. See HDFS-3972.
    FileSystem fs = SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<FileSystem>() {
          @Override
          public FileSystem run() throws IOException {
            FileSystem dfs = new DistributedFileSystem();
            dfs.initialize(FileSystem.getDefaultUri(conf), conf);
            return dfs;
          }
        });
    this.emptier = new Thread(new Trash(fs, conf).getEmptier(), "Trash Emptier");
    this.emptier.setDaemon(true);
    this.emptier.start();
  }
  
  private void stopTrashEmptier() {
    if (this.emptier != null) {
      emptier.interrupt();
      emptier = null;
    }
  }
  
  private void startHttpServer(final Configuration conf) throws IOException {
    httpServer = new NameNodeHttpServer(conf, this, getHttpServerBindAddress(conf));
    httpServer.start();
    httpServer.setStartupProgress(startupProgress);
  }
  
  private void stopHttpServer() {
    try {
      if (httpServer != null) httpServer.stop();
    } catch (Exception e) {
      LOG.error("Exception while stopping httpserver", e);
    }
  }

  /**
   * Start NameNode.
   * <p>
   * The name-node can be started with one of the following startup options:
   * <ul> 
   * <li>{@link StartupOption#REGULAR REGULAR} - normal name node startup</li>
   * <li>{@link StartupOption#FORMAT FORMAT} - format name node</li>
   * <li>{@link StartupOption#BACKUP BACKUP} - start backup node</li>
   * <li>{@link StartupOption#CHECKPOINT CHECKPOINT} - start checkpoint node</li>
   * <li>{@link StartupOption#UPGRADE UPGRADE} - start the cluster  
   * <li>{@link StartupOption#UPGRADEONLY UPGRADEONLY} - upgrade the cluster  
   * upgrade and create a snapshot of the current file system state</li> 
   * <li>{@link StartupOption#RECOVER RECOVERY} - recover name node
   * metadata</li>
   * <li>{@link StartupOption#ROLLBACK ROLLBACK} - roll the  
   *            cluster back to the previous state</li>
   * <li>{@link StartupOption#IMPORT IMPORT} - import checkpoint</li>
   * </ul>
   * The option is passed via configuration field: 
   * <tt>dfs.namenode.startup</tt>
   * 
   * The conf will be modified to reflect the actual ports on which 
   * the NameNode is up and running if the user passes the port as
   * <code>zero</code> in the conf.
   * 
   * @param conf  confirguration
   * @throws IOException
   */
  public NameNode(Configuration conf) throws IOException {
    this(conf, NamenodeRole.NAMENODE);
  }

  protected NameNode(Configuration conf, NamenodeRole role)
      throws IOException {
    super(conf);
    this.tracer = new Tracer.Builder("NameNode").
        conf(TraceUtils.wrapHadoopConf(NAMENODE_HTRACE_PREFIX, conf)).
        build();
    this.role = role;
    String nsId = getNameServiceId(conf);
    String namenodeId = HAUtil.getNameNodeId(conf, nsId);
    clientNamenodeAddress = NameNodeUtils.getClientNamenodeAddress(
        conf, nsId);

    if (clientNamenodeAddress != null) {
      LOG.info("Clients should use {} to access"
          + " this namenode/service.", clientNamenodeAddress);
    }
    this.haEnabled = HAUtil.isHAEnabled(conf, nsId);
    state = createHAState(conf);
    this.allowStaleStandbyReads = HAUtil.shouldAllowStandbyReads(conf);
    this.haContext = createHAContext();
    try {
      initializeGenericKeys(conf, nsId, namenodeId);
      initialize(getConf());
      state.prepareToEnterState(haContext);
      try {
        haContext.writeLock();
        state.enterState(haContext);
      } finally {
        haContext.writeUnlock();
      }
    } catch (IOException e) {
      this.stopAtException(e);
      throw e;
    } catch (HadoopIllegalArgumentException e) {
      this.stopAtException(e);
      throw e;
    }
    notBecomeActiveInSafemode = conf.getBoolean(
        DFS_HA_NN_NOT_BECOME_ACTIVE_IN_SAFEMODE,
        DFS_HA_NN_NOT_BECOME_ACTIVE_IN_SAFEMODE_DEFAULT);
    this.started.set(true);
    DefaultMetricsSystem.instance().register(this);
  }

  private void stopAtException(Exception e){
    try {
      this.stop();
    } catch (Exception ex) {
      LOG.warn("Encountered exception when handling exception ("
          + e.getMessage() + "):", ex);
    }
  }

  protected HAState createHAState(Configuration conf) {
    StartupOption startOpt = getStartupOption(conf);
    if (!haEnabled || startOpt == StartupOption.UPGRADE
        || startOpt == StartupOption.UPGRADEONLY) {
      return ACTIVE_STATE;
    } else if (conf.getBoolean(DFS_NAMENODE_OBSERVER_ENABLED_KEY,
          DFS_NAMENODE_OBSERVER_ENABLED_DEFAULT)
        || startOpt == StartupOption.OBSERVER) {
      // Set Observer state using config instead of startup option
      // This allows other startup options to be used when starting observer.
      // e.g. rollingUpgrade
      return OBSERVER_STATE;
    } else {
      return STANDBY_STATE;
    }
  }

  protected HAContext createHAContext() {
    return new NameNodeHAContext();
  }

  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  public void join() {
    try {
      rpcServer.join();
    } catch (InterruptedException ie) {
      LOG.info("Caught interrupted exception", ie);
    }
  }

  /**
   * Stop all NameNode threads and wait for all to finish.
   */
  public void stop() {
    synchronized(this) {
      if (stopRequested)
        return;
      stopRequested = true;
    }
    try {
      if (state != null) {
        state.exitState(haContext);
      }
    } catch (ServiceFailedException e) {
      LOG.warn("Encountered exception while exiting state", e);
    } finally {
      stopMetricsLogger();
      stopCommonServices();
      if (metrics != null) {
        metrics.shutdown();
      }
      if (namesystem != null) {
        namesystem.shutdown();
      }
      if (nameNodeStatusBeanName != null) {
        MBeans.unregister(nameNodeStatusBeanName);
        nameNodeStatusBeanName = null;
      }
      if (levelDBAliasMapServer != null) {
        levelDBAliasMapServer.close();
      }
    }
    started.set(false);
    tracer.close();
  }

  synchronized boolean isStopRequested() {
    return stopRequested;
  }

  /**
   * Is the cluster currently in safe mode?
   */
  public boolean isInSafeMode() {
    return namesystem.isInSafeMode();
  }

  /** get FSImage */
  @VisibleForTesting
  public FSImage getFSImage() {
    return namesystem.getFSImage();
  }

  /**
   * @return NameNode RPC address
   */
  public InetSocketAddress getNameNodeAddress() {
    return rpcServer.getRpcAddress();
  }

  /**
   * @return The auxiliary nameNode RPC addresses, or empty set if there
   * is none.
   */
  public Set<InetSocketAddress> getAuxiliaryNameNodeAddresses() {
    return rpcServer.getAuxiliaryRpcAddresses();
  }

  /**
   * @return NameNode RPC address in "host:port" string form
   */
  public String getNameNodeAddressHostPortString() {
    return NetUtils.getHostPortString(getNameNodeAddress());
  }

  /**
   * Return a host:port format string corresponds to an auxiliary
   * port configured on NameNode. If there are multiple auxiliary ports,
   * an arbitrary one is returned. If there is no auxiliary listener, returns
   * null.
   *
   * @return a string of format host:port that points to an auxiliary NameNode
   *         address, or null if there is no such address.
   */
  @VisibleForTesting
  public String getNNAuxiliaryRpcAddress() {
    Set<InetSocketAddress> auxiliaryAddrs = getAuxiliaryNameNodeAddresses();
    if (auxiliaryAddrs.isEmpty()) {
      return null;
    }
    // since set has no particular order, returning the first element of
    // from the iterator is effectively arbitrary.
    InetSocketAddress addr = auxiliaryAddrs.iterator().next();
    return NetUtils.getHostPortString(addr);
  }

  /**
   * @return NameNode service RPC address if configured, the
   *    NameNode RPC address otherwise
   */
  public InetSocketAddress getServiceRpcAddress() {
    final InetSocketAddress serviceAddr = rpcServer.getServiceRpcAddress();
    return serviceAddr == null ? getNameNodeAddress() : serviceAddr;
  }

  /**
   * @return NameNode HTTP address, used by the Web UI, image transfer,
   *    and HTTP-based file system clients like WebHDFS
   */
  public InetSocketAddress getHttpAddress() {
    return httpServer.getHttpAddress();
  }

  /**
   * @return NameNode HTTPS address, used by the Web UI, image transfer,
   *    and HTTP-based file system clients like WebHDFS
   */
  public InetSocketAddress getHttpsAddress() {
    return httpServer.getHttpsAddress();
  }

  /**
   * NameNodeHttpServer, used by unit tests to ensure a full shutdown,
   * so that no bind exception is thrown during restart.
   */
  @VisibleForTesting
  public void joinHttpServer() {
    if (httpServer != null) {
      try {
        httpServer.join();
      } catch (InterruptedException e) {
        LOG.info("Caught InterruptedException joining NameNodeHttpServer", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Verify that configured directories exist, then
   * Interactively confirm that formatting is desired 
   * for each existing directory and format them.
   * 
   * @param conf configuration to use
   * @param force if true, format regardless of whether dirs exist
   * @return true if formatting was aborted, false otherwise
   * @throws IOException
   */
  private static boolean format(Configuration conf, boolean force,
      boolean isInteractive) throws IOException {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    String namenodeId = HAUtil.getNameNodeId(conf, nsId);
    initializeGenericKeys(conf, nsId, namenodeId);
    checkAllowFormat(conf);

    if (UserGroupInformation.isSecurityEnabled()) {
      InetSocketAddress socAddr = DFSUtilClient.getNNAddress(conf);
      SecurityUtil.login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY,
          DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, socAddr.getHostName());
    }
    
    Collection<URI> nameDirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    List<URI> sharedDirs = FSNamesystem.getSharedEditsDirs(conf);
    List<URI> dirsToPrompt = new ArrayList<URI>();
    dirsToPrompt.addAll(nameDirsToFormat);
    dirsToPrompt.addAll(sharedDirs);
    List<URI> editDirsToFormat = 
                 FSNamesystem.getNamespaceEditsDirs(conf);

    // if clusterID is not provided - see if you can find the current one
    String clusterId = StartupOption.FORMAT.getClusterId();
    if(clusterId == null || clusterId.equals("")) {
      //Generate a new cluster id
      clusterId = NNStorage.newClusterID();
    }

    LOG.info("Formatting using clusterid: {}", clusterId);
    
    FSImage fsImage = new FSImage(conf, nameDirsToFormat, editDirsToFormat);
    FSNamesystem fsn = null;
    try {
      fsn = new FSNamesystem(conf, fsImage);
      fsImage.getEditLog().initJournalsForWrite();

      // Abort NameNode format if reformat is disabled and if
      // meta-dir already exists
      if (conf.getBoolean(DFSConfigKeys.DFS_REFORMAT_DISABLED,
          DFSConfigKeys.DFS_REFORMAT_DISABLED_DEFAULT)) {
        force = false;
        isInteractive = false;
        for (StorageDirectory sd : fsImage.storage.dirIterable(null)) {
          if (sd.hasSomeData()) {
            throw new NameNodeFormatException(
                "NameNode format aborted as reformat is disabled for "
                    + "this cluster.");
          }
        }
      }

      if (!fsImage.confirmFormat(force, isInteractive)) {
        return true; // aborted
      }

      fsImage.format(fsn, clusterId, force);
    } catch (IOException ioe) {
      LOG.warn("Encountered exception during format", ioe);
      throw ioe;
    } finally {
      if (fsImage != null) {
        fsImage.close();
      }
      if (fsn != null) {
        fsn.close();
      }
    }
    return false;
  }

  public static void checkAllowFormat(Configuration conf) throws IOException {
    if (!conf.getBoolean(DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY, 
        DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_DEFAULT)) {
      throw new IOException("The option " + DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY
                + " is set to false for this filesystem, so it "
                + "cannot be formatted. You will need to set "
                + DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY +" parameter "
                + "to true in order to format this filesystem");
    }
  }
  
  @VisibleForTesting
  public static boolean initializeSharedEdits(Configuration conf) throws IOException {
    return initializeSharedEdits(conf, true);
  }
  
  @VisibleForTesting
  public static boolean initializeSharedEdits(Configuration conf,
      boolean force) throws IOException {
    return initializeSharedEdits(conf, force, false);
  }

  /**
   * Clone the supplied configuration but remove the shared edits dirs.
   *
   * @param conf Supplies the original configuration.
   * @return Cloned configuration without the shared edit dirs.
   * @throws IOException on failure to generate the configuration.
   */
  private static Configuration getConfigurationWithoutSharedEdits(
      Configuration conf)
      throws IOException {
    List<URI> editsDirs = FSNamesystem.getNamespaceEditsDirs(conf, false);
    String editsDirsString = Joiner.on(",").join(editsDirs);

    Configuration confWithoutShared = new Configuration(conf);
    confWithoutShared.unset(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
    confWithoutShared.setStrings(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        editsDirsString);
    return confWithoutShared;
  }

  /**
   * Format a new shared edits dir and copy in enough edit log segments so that
   * the standby NN can start up.
   * 
   * @param conf configuration
   * @param force format regardless of whether or not the shared edits dir exists
   * @param interactive prompt the user when a dir exists
   * @return true if the command aborts, false otherwise
   */
  private static boolean initializeSharedEdits(Configuration conf,
      boolean force, boolean interactive) throws IOException {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    String namenodeId = HAUtil.getNameNodeId(conf, nsId);
    initializeGenericKeys(conf, nsId, namenodeId);
    
    if (conf.get(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY) == null) {
      LOG.error("No shared edits directory configured for namespace " +
          nsId + " namenode " + namenodeId);
      return false;
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      InetSocketAddress socAddr = DFSUtilClient.getNNAddress(conf);
      SecurityUtil.login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY,
          DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, socAddr.getHostName());
    }

    NNStorage existingStorage = null;
    FSImage sharedEditsImage = null;
    try {
      FSNamesystem fsns =
          FSNamesystem.loadFromDisk(getConfigurationWithoutSharedEdits(conf));
      
      existingStorage = fsns.getFSImage().getStorage();
      NamespaceInfo nsInfo = existingStorage.getNamespaceInfo();
      
      List<URI> sharedEditsDirs = FSNamesystem.getSharedEditsDirs(conf);
      
      sharedEditsImage = new FSImage(conf,
          Lists.<URI>newArrayList(),
          sharedEditsDirs);
      sharedEditsImage.getEditLog().initJournalsForWrite();
      
      if (!sharedEditsImage.confirmFormat(force, interactive)) {
        return true; // abort
      }
      
      NNStorage newSharedStorage = sharedEditsImage.getStorage();
      // Call Storage.format instead of FSImage.format here, since we don't
      // actually want to save a checkpoint - just prime the dirs with
      // the existing namespace info
      newSharedStorage.format(nsInfo);
      sharedEditsImage.getEditLog().formatNonFileJournals(nsInfo, force);

      // Need to make sure the edit log segments are in good shape to initialize
      // the shared edits dir.
      fsns.getFSImage().getEditLog().close();
      fsns.getFSImage().getEditLog().initJournalsForWrite();
      fsns.getFSImage().getEditLog().recoverUnclosedStreams();

      copyEditLogSegmentsToSharedDir(fsns, sharedEditsDirs, newSharedStorage,
          conf);
    } catch (IOException ioe) {
      LOG.error("Could not initialize shared edits dir", ioe);
      return true; // aborted
    } finally {
      if (sharedEditsImage != null) {
        try {
          sharedEditsImage.close();
        }  catch (IOException ioe) {
          LOG.warn("Could not close sharedEditsImage", ioe);
        }
      }
      // Have to unlock storage explicitly for the case when we're running in a
      // unit test, which runs in the same JVM as NNs.
      if (existingStorage != null) {
        try {
          existingStorage.unlockAll();
        } catch (IOException ioe) {
          LOG.warn("Could not unlock storage directories", ioe);
          return true; // aborted
        }
      }
    }
    return false; // did not abort
  }

  private static void copyEditLogSegmentsToSharedDir(FSNamesystem fsns,
      Collection<URI> sharedEditsDirs, NNStorage newSharedStorage,
      Configuration conf) throws IOException {
    Preconditions.checkArgument(!sharedEditsDirs.isEmpty(),
        "No shared edits specified");
    // Copy edit log segments into the new shared edits dir.
    List<URI> sharedEditsUris = new ArrayList<URI>(sharedEditsDirs);
    FSEditLog newSharedEditLog = new FSEditLog(conf, newSharedStorage,
        sharedEditsUris);
    newSharedEditLog.initJournalsForWrite();
    newSharedEditLog.recoverUnclosedStreams();
    
    FSEditLog sourceEditLog = fsns.getFSImage().editLog;
    
    long fromTxId = fsns.getFSImage().getMostRecentCheckpointTxId();
    
    Collection<EditLogInputStream> streams = null;
    try {
      streams = sourceEditLog.selectInputStreams(fromTxId + 1, 0);

      // Set the nextTxid to the CheckpointTxId+1
      newSharedEditLog.setNextTxId(fromTxId + 1);

      // Copy all edits after last CheckpointTxId to shared edits dir
      for (EditLogInputStream stream : streams) {
        LOG.debug("Beginning to copy stream {} to shared edits", stream);
        FSEditLogOp op;
        boolean segmentOpen = false;
        while ((op = stream.readOp()) != null) {
          LOG.trace("copying op: {}", op);
          if (!segmentOpen) {
            newSharedEditLog.startLogSegment(op.txid, false,
                fsns.getEffectiveLayoutVersion());
            segmentOpen = true;
          }

          newSharedEditLog.logEdit(op);

          if (op.opCode == FSEditLogOpCodes.OP_END_LOG_SEGMENT) {
            newSharedEditLog.endCurrentLogSegment(false);
            LOG.debug("ending log segment because of END_LOG_SEGMENT op in {}",
                stream);
            segmentOpen = false;
          }
        }

        if (segmentOpen) {
          LOG.debug("ending log segment because of end of stream in {}",
              stream);
          newSharedEditLog.logSync();
          newSharedEditLog.endCurrentLogSegment(false);
          segmentOpen = false;
        }
      }
    } finally {
      if (streams != null) {
        FSEditLog.closeAllStreams(streams);
      }
    }
  }
  
  @VisibleForTesting
  public static boolean doRollback(Configuration conf,
      boolean isConfirmationNeeded) throws IOException {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    String namenodeId = HAUtil.getNameNodeId(conf, nsId);
    initializeGenericKeys(conf, nsId, namenodeId);

    FSNamesystem nsys = new FSNamesystem(conf, new FSImage(conf));
    System.err.print(
        "\"rollBack\" will remove the current state of the file system,\n"
        + "returning you to the state prior to initiating your recent.\n"
        + "upgrade. This action is permanent and cannot be undone. If you\n"
        + "are performing a rollback in an HA environment, you should be\n"
        + "certain that no NameNode process is running on any host.");
    if (isConfirmationNeeded) {
      if (!confirmPrompt("Roll back file system state?")) {
        System.err.println("Rollback aborted.");
        return true;
      }
    }
    nsys.getFSImage().doRollback(nsys);
    return false;
  }

  private static void printUsage(PrintStream out) {
    out.println(USAGE + "\n");
  }

  @VisibleForTesting
  static StartupOption parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.FORMAT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FORMAT;
        for (i = i + 1; i < argsLen; i++) {
          if (args[i].equalsIgnoreCase(StartupOption.CLUSTERID.getName())) {
            i++;
            if (i >= argsLen) {
              // if no cluster id specified, return null
              LOG.error("Must specify a valid cluster ID after the "
                  + StartupOption.CLUSTERID.getName() + " flag");
              return null;
            }
            String clusterId = args[i];
            // Make sure an id is specified and not another flag
            if (clusterId.isEmpty() ||
                clusterId.equalsIgnoreCase(StartupOption.FORCE.getName()) ||
                clusterId.equalsIgnoreCase(
                    StartupOption.NONINTERACTIVE.getName())) {
              LOG.error("Must specify a valid cluster ID after the "
                  + StartupOption.CLUSTERID.getName() + " flag");
              return null;
            }
            startOpt.setClusterId(clusterId);
          }

          if (args[i].equalsIgnoreCase(StartupOption.FORCE.getName())) {
            startOpt.setForceFormat(true);
          }

          if (args[i].equalsIgnoreCase(StartupOption.NONINTERACTIVE.getName())) {
            startOpt.setInteractiveFormat(false);
          }
        }
      } else if (StartupOption.GENCLUSTERID.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.GENCLUSTERID;
      } else if (StartupOption.REGULAR.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else if (StartupOption.BACKUP.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.BACKUP;
      } else if (StartupOption.CHECKPOINT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.CHECKPOINT;
      } else if (StartupOption.OBSERVER.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.OBSERVER;
      } else if (StartupOption.UPGRADE.getName().equalsIgnoreCase(cmd)
          || StartupOption.UPGRADEONLY.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.UPGRADE.getName().equalsIgnoreCase(cmd) ? 
            StartupOption.UPGRADE : StartupOption.UPGRADEONLY;
        /* Can be followed by CLUSTERID with a required parameter or
         * RENAMERESERVED with an optional parameter
         */
        while (i + 1 < argsLen) {
          String flag = args[i + 1];
          if (flag.equalsIgnoreCase(StartupOption.CLUSTERID.getName())) {
            if (i + 2 < argsLen) {
              i += 2;
              startOpt.setClusterId(args[i]);
            } else {
              LOG.error("Must specify a valid cluster ID after the "
                  + StartupOption.CLUSTERID.getName() + " flag");
              return null;
            }
          } else if (flag.equalsIgnoreCase(StartupOption.RENAMERESERVED
              .getName())) {
            if (i + 2 < argsLen) {
              FSImageFormat.setRenameReservedPairs(args[i + 2]);
              i += 2;
            } else {
              FSImageFormat.useDefaultRenameReservedPairs();
              i += 1;
            }
          } else {
            LOG.error("Unknown upgrade flag: {}", flag);
            return null;
          }
        }
      } else if (StartupOption.ROLLINGUPGRADE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLINGUPGRADE;
        ++i;
        if (i >= argsLen) {
          LOG.error("Must specify a rolling upgrade startup option "
              + RollingUpgradeStartupOption.getAllOptionString());
          return null;
        }
        startOpt.setRollingUpgradeStartupOption(args[i]);
      } else if (StartupOption.ROLLBACK.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if (StartupOption.IMPORT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.IMPORT;
      } else if (StartupOption.BOOTSTRAPSTANDBY.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.BOOTSTRAPSTANDBY;
        return startOpt;
      } else if (StartupOption.INITIALIZESHAREDEDITS.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.INITIALIZESHAREDEDITS;
        for (i = i + 1 ; i < argsLen; i++) {
          if (StartupOption.NONINTERACTIVE.getName().equals(args[i])) {
            startOpt.setInteractiveFormat(false);
          } else if (StartupOption.FORCE.getName().equals(args[i])) {
            startOpt.setForceFormat(true);
          } else {
            LOG.error("Invalid argument: " + args[i]);
            return null;
          }
        }
        return startOpt;
      } else if (StartupOption.RECOVER.getName().equalsIgnoreCase(cmd)) {
        if (startOpt != StartupOption.REGULAR) {
          throw new RuntimeException("Can't combine -recover with " +
              "other startup options.");
        }
        startOpt = StartupOption.RECOVER;
        while (++i < argsLen) {
          if (args[i].equalsIgnoreCase(
                StartupOption.FORCE.getName())) {
            startOpt.setForce(MetaRecoveryContext.FORCE_FIRST_CHOICE);
          } else {
            throw new RuntimeException("Error parsing recovery options: " + 
              "can't understand option \"" + args[i] + "\"");
          }
        }
      } else if (StartupOption.METADATAVERSION.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.METADATAVERSION;
      } else {
        return null;
      }
    }
    return startOpt;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set(DFS_NAMENODE_STARTUP_KEY, opt.name());
  }

  public static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(conf.get(DFS_NAMENODE_STARTUP_KEY,
                                          StartupOption.REGULAR.toString()));
  }

  private static void doRecovery(StartupOption startOpt, Configuration conf)
      throws IOException {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    String namenodeId = HAUtil.getNameNodeId(conf, nsId);
    initializeGenericKeys(conf, nsId, namenodeId);
    if (startOpt.getForce() < MetaRecoveryContext.FORCE_ALL) {
      if (!confirmPrompt("You have selected Metadata Recovery mode.  " +
          "This mode is intended to recover lost metadata on a corrupt " +
          "filesystem.  Metadata recovery mode often permanently deletes " +
          "data from your HDFS filesystem.  Please back up your edit log " +
          "and fsimage before trying this!\n\n" +
          "Are you ready to proceed? (Y/N)\n")) {
        System.err.println("Recovery aborted at user request.\n");
        return;
      }
    }
    MetaRecoveryContext.LOG.info("starting recovery...");
    UserGroupInformation.setConfiguration(conf);
    NameNode.initMetrics(conf, startOpt.toNodeRole());
    FSNamesystem fsn = null;
    try {
      fsn = FSNamesystem.loadFromDisk(conf);
      fsn.getFSImage().saveNamespace(fsn);
      MetaRecoveryContext.LOG.info("RECOVERY COMPLETE");
    } catch (IOException e) {
      MetaRecoveryContext.LOG.info("RECOVERY FAILED: caught exception", e);
      throw e;
    } catch (RuntimeException e) {
      MetaRecoveryContext.LOG.info("RECOVERY FAILED: caught exception", e);
      throw e;
    } finally {
      if (fsn != null)
        fsn.close();
    }
  }

  /**
   * Verify that configured directories exist, then print the metadata versions
   * of the software and the image.
   *
   * @param conf configuration to use
   * @throws IOException
   */
  private static boolean printMetadataVersion(Configuration conf)
    throws IOException {
    final String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    final String namenodeId = HAUtil.getNameNodeId(conf, nsId);
    NameNode.initializeGenericKeys(conf, nsId, namenodeId);
    final FSImage fsImage = new FSImage(conf);
    final FSNamesystem fs = new FSNamesystem(conf, fsImage, false);
    return fsImage.recoverTransitionRead(
      StartupOption.METADATAVERSION, fs, null);
  }

  public static NameNode createNameNode(String argv[], Configuration conf)
      throws IOException {
    LOG.info("createNameNode " + Arrays.asList(argv));
    if (conf == null)
      conf = new HdfsConfiguration();
    // Parse out some generic args into Configuration.
    GenericOptionsParser hParser = new GenericOptionsParser(conf, argv);
    argv = hParser.getRemainingArgs();
    // Parse the rest, NN specific args.
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage(System.err);
      return null;
    }
    setStartupOption(conf, startOpt);

    boolean aborted = false;
    switch (startOpt) {
    case FORMAT:
      aborted = format(conf, startOpt.getForceFormat(),
          startOpt.getInteractiveFormat());
      terminate(aborted ? 1 : 0);
      return null; // avoid javac warning
    case GENCLUSTERID:
      String clusterID = NNStorage.newClusterID();
      LOG.info("Generated new cluster id: {}", clusterID);
      terminate(0);
      return null;
    case ROLLBACK:
      aborted = doRollback(conf, true);
      terminate(aborted ? 1 : 0);
      return null; // avoid warning
    case BOOTSTRAPSTANDBY:
      String[] toolArgs = Arrays.copyOfRange(argv, 1, argv.length);
      int rc = BootstrapStandby.run(toolArgs, conf);
      terminate(rc);
      return null; // avoid warning
    case INITIALIZESHAREDEDITS:
      aborted = initializeSharedEdits(conf,
          startOpt.getForceFormat(),
          startOpt.getInteractiveFormat());
      terminate(aborted ? 1 : 0);
      return null; // avoid warning
    case BACKUP:
    case CHECKPOINT:
      NamenodeRole role = startOpt.toNodeRole();
      DefaultMetricsSystem.initialize(role.toString().replace(" ", ""));
      return new BackupNode(conf, role);
    case RECOVER:
      NameNode.doRecovery(startOpt, conf);
      return null;
    case METADATAVERSION:
      printMetadataVersion(conf);
      terminate(0);
      return null; // avoid javac warning
    case UPGRADEONLY:
      DefaultMetricsSystem.initialize("NameNode");
      new NameNode(conf);
      terminate(0);
      return null;
    default:
      DefaultMetricsSystem.initialize("NameNode");
      return new NameNode(conf);
    }
  }

  /**
   * In federation configuration is set for a set of
   * namenode and secondary namenode/backup/checkpointer, which are
   * grouped under a logical nameservice ID. The configuration keys specific 
   * to them have suffix set to configured nameserviceId.
   * 
   * This method copies the value from specific key of format key.nameserviceId
   * to key, to set up the generic configuration. Once this is done, only
   * generic version of the configuration is read in rest of the code, for
   * backward compatibility and simpler code changes.
   * 
   * @param conf
   *          Configuration object to lookup specific key and to set the value
   *          to the key passed. Note the conf object is modified
   * @param nameserviceId name service Id (to distinguish federated NNs)
   * @param namenodeId the namenode ID (to distinguish HA NNs)
   * @see DFSUtil#setGenericConf(Configuration, String, String, String...)
   */
  public static void initializeGenericKeys(Configuration conf,
      String nameserviceId, String namenodeId) {
    if ((nameserviceId != null && !nameserviceId.isEmpty()) || 
        (namenodeId != null && !namenodeId.isEmpty())) {
      if (nameserviceId != null) {
        conf.set(DFS_NAMESERVICE_ID, nameserviceId);
      }
      if (namenodeId != null) {
        conf.set(DFS_HA_NAMENODE_ID_KEY, namenodeId);
      }
      
      DFSUtil.setGenericConf(conf, nameserviceId, namenodeId,
          NAMENODE_SPECIFIC_KEYS);
      DFSUtil.setGenericConf(conf, nameserviceId, null,
          NAMESERVICE_SPECIFIC_KEYS);
    }
    
    // If the RPC address is set use it to (re-)configure the default FS
    if (conf.get(DFS_NAMENODE_RPC_ADDRESS_KEY) != null) {
      URI defaultUri = URI.create(HdfsConstants.HDFS_URI_SCHEME + "://"
          + conf.get(DFS_NAMENODE_RPC_ADDRESS_KEY));
      conf.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
      LOG.debug("Setting {} to {}", FS_DEFAULT_NAME_KEY, defaultUri);
    }
  }
    
  /** 
   * Get the name service Id for the node
   * @return name service Id or null if federation is not configured
   */
  protected String getNameServiceId(Configuration conf) {
    return DFSUtil.getNamenodeNameServiceId(conf);
  }
  
  /**
   */
  public static void main(String argv[]) throws Exception {
    if (DFSUtil.parseHelpArgument(argv, NameNode.USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      StringUtils.startupShutdownMessage(NameNode.class, argv, LOG);
      NameNode namenode = createNameNode(argv, null);
      if (namenode != null) {
        namenode.join();
      }
    } catch (Throwable e) {
      LOG.error("Failed to start namenode.", e);
      terminate(1, e);
    }
  }

  synchronized void monitorHealth() throws IOException {
    String operationName = "monitorHealth";
    namesystem.checkSuperuserPrivilege(operationName);
    if (!haEnabled) {
      return; // no-op, if HA is not enabled
    }
    long start = Time.monotonicNow();
    getNamesystem().checkAvailableResources();
    long end = Time.monotonicNow();
    if (end - start >= HEALTH_MONITOR_WARN_THRESHOLD_MS) {
      // log a warning if it take >= 5 seconds.
      LOG.warn("Remote IP {} checking available resources took {}ms",
          Server.getRemoteIp(), end - start);
    }
    if (!getNamesystem().nameNodeHasResourcesAvailable()) {
      throw new HealthCheckFailedException(
          "The NameNode has no resources available");
    }
    if (notBecomeActiveInSafemode && isInSafeMode()) {
      throw new HealthCheckFailedException("The NameNode is configured to " +
          "report UNHEALTHY to ZKFC in Safemode.");
    }
  }
  
  synchronized void transitionToActive() throws IOException {
    String operationName = "transitionToActive";
    namesystem.checkSuperuserPrivilege(operationName);
    if (!haEnabled) {
      throw new ServiceFailedException("HA for namenode is not enabled");
    }
    if (state == OBSERVER_STATE) {
      throw new ServiceFailedException(
          "Cannot transition from '" + OBSERVER_STATE + "' to '" +
              ACTIVE_STATE + "'");
    }
    if (notBecomeActiveInSafemode && isInSafeMode()) {
      throw new ServiceFailedException(getRole() + " still not leave safemode");
    }
    state.setState(haContext, ACTIVE_STATE);
  }

  synchronized void transitionToStandby() throws IOException {
    String operationName = "transitionToStandby";
    namesystem.checkSuperuserPrivilege(operationName);
    if (!haEnabled) {
      throw new ServiceFailedException("HA for namenode is not enabled");
    }
    state.setState(haContext, STANDBY_STATE);
  }

  synchronized void transitionToObserver() throws IOException {
    String operationName = "transitionToObserver";
    namesystem.checkSuperuserPrivilege(operationName);
    if (notBecomeActiveInSafemode && isInSafeMode()) {
      throw new ServiceFailedException(getRole() + " still not leave safemode");
    }
    if (!haEnabled) {
      throw new ServiceFailedException("HA for namenode is not enabled");
    }
    // Transition from ACTIVE to OBSERVER is forbidden.
    if (state == ACTIVE_STATE) {
      throw new ServiceFailedException(
          "Cannot transition from '" + ACTIVE_STATE + "' to '" +
              OBSERVER_STATE + "'");
    }
    state.setState(haContext, OBSERVER_STATE);
  }

  synchronized HAServiceStatus getServiceStatus()
      throws ServiceFailedException, AccessControlException {
    if (!haEnabled) {
      throw new ServiceFailedException("HA for namenode is not enabled");
    }
    if (state == null) {
      return new HAServiceStatus(HAServiceState.INITIALIZING);
    }
    HAServiceState retState = state.getServiceState();
    HAServiceStatus ret = new HAServiceStatus(retState);
    if (retState == HAServiceState.STANDBY) {
      if (namesystem.isInSafeMode()) {
        ret.setNotReadyToBecomeActive("The NameNode is in safemode. " +
            namesystem.getSafeModeTip());
      } else {
        ret.setReadyToBecomeActive();
      }
    } else if (retState == HAServiceState.ACTIVE) {
      ret.setReadyToBecomeActive();
    } else {
      ret.setNotReadyToBecomeActive("State is " + state);
    }
    return ret;
  }

  synchronized HAServiceState getServiceState() {
    if (state == null) {
      return HAServiceState.INITIALIZING;
    }
    return state.getServiceState();
  }

  /**
   * Emit Namenode HA service state as an integer so that one can monitor NN HA
   * state based on this metric.
   *
   * @return  0 when not fully started
   *          1 for active or standalone (non-HA) NN
   *          2 for standby
   *          3 for observer
   *
   * These are the same integer values for the HAServiceState enum.
   */
  @Metric({"NameNodeState", "Namenode HA service state"})
  public int getNameNodeState() {
    if (!isStarted() || state == null) {
      return HAServiceState.INITIALIZING.ordinal();
    }

    return state.getServiceState().ordinal();
  }

  /**
   * Register NameNodeStatusMXBean
   */
  private void registerNNSMXBean() {
    nameNodeStatusBeanName = MBeans.register("NameNode", "NameNodeStatus", this);
  }

  @Override // NameNodeStatusMXBean
  public String getNNRole() {
    NamenodeRole role = getRole();
    return Objects.toString(role, "");
  }

  @Override // NameNodeStatusMXBean
  public String getState() {
    HAServiceState servState = getServiceState();
    return Objects.toString(servState, "");
  }

  @Override // NameNodeStatusMXBean
  public String getHostAndPort() {
    return getNameNodeAddressHostPortString();
  }

  @Override // NameNodeStatusMXBean
  public boolean isSecurityEnabled() {
    return UserGroupInformation.isSecurityEnabled();
  }

  @Override // NameNodeStatusMXBean
  public long getLastHATransitionTime() {
    return state.getLastHATransitionTime();
  }

  @Override //NameNodeStatusMXBean
  public long getBytesWithFutureGenerationStamps() {
    return getNamesystem().getBytesInFuture();
  }

  @Override
  public String getSlowPeersReport() {
    return namesystem.getBlockManager().getDatanodeManager()
        .getSlowPeersReport();
  }

  @Override //NameNodeStatusMXBean
  public String getSlowDisksReport() {
    return namesystem.getBlockManager().getDatanodeManager()
        .getSlowDisksReport();
  }

  /**
   * Shutdown the NN immediately in an ungraceful way. Used when it would be
   * unsafe for the NN to continue operating, e.g. during a failed HA state
   * transition.
   * 
   * @param t exception which warrants the shutdown. Printed to the NN log
   *          before exit.
   * @throws ExitException thrown only for testing.
   */
  protected synchronized void doImmediateShutdown(Throwable t)
      throws ExitException {
    try {
      LOG.error("Error encountered requiring NN shutdown. " +
          "Shutting down immediately.", t);
    } catch (Throwable ignored) {
      // This is unlikely to happen, but there's nothing we can do if it does.
    }
    terminate(1, t);
  }
  
  /**
   * Class used to expose {@link NameNode} as context to {@link HAState}
   */
  protected class NameNodeHAContext implements HAContext {
    @Override
    public void setState(HAState s) {
      state = s;
    }

    @Override
    public HAState getState() {
      return state;
    }

    @Override
    public void startActiveServices() throws IOException {
      try {
        namesystem.startActiveServices();
        namesystem.checkAndProvisionSnapshotTrashRoots();
        startTrashEmptier(getConf());
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }

    @Override
    public void stopActiveServices() throws IOException {
      try {
        if (namesystem != null) {
          namesystem.stopActiveServices();
        }
        stopTrashEmptier();
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }

    @Override
    public void startStandbyServices() throws IOException {
      try {
        namesystem.startStandbyServices(getConf(),
            state == NameNode.OBSERVER_STATE);
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }

    @Override
    public void prepareToStopStandbyServices() throws ServiceFailedException {
      try {
        namesystem.prepareToStopStandbyServices();
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }
    
    @Override
    public void stopStandbyServices() throws IOException {
      try {
        if (namesystem != null) {
          namesystem.stopStandbyServices();
        }
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }
    
    @Override
    public void writeLock() {
      namesystem.writeLock();
      namesystem.lockRetryCache();
    }
    
    @Override
    public void writeUnlock() {
      namesystem.unlockRetryCache();
      namesystem.writeUnlock("HAState");
    }
    
    /** Check if an operation of given category is allowed */
    @Override
    public void checkOperation(final OperationCategory op)
        throws StandbyException {
      state.checkOperation(haContext, op);
    }
    
    @Override
    public boolean allowStaleReads() {
      if (state == OBSERVER_STATE) {
        return true;
      }
      return allowStaleStandbyReads;
    }

  }
  
  public boolean isStandbyState() {
    return (state.equals(STANDBY_STATE));
  }
  
  public boolean isActiveState() {
    return (state.equals(ACTIVE_STATE));
  }

  public boolean isObserverState() {
    return state.equals(OBSERVER_STATE);
  }

  /**
   * Returns whether the NameNode is completely started
   */
  boolean isStarted() {
    return this.started.get();
  }

  /**
   * Check that a request to change this node's HA state is valid.
   * In particular, verifies that, if auto failover is enabled, non-forced
   * requests from the HAAdmin CLI are rejected, and vice versa.
   *
   * @param req the request to check
   * @throws AccessControlException if the request is disallowed
   */
  void checkHaStateChange(StateChangeRequestInfo req)
      throws AccessControlException {
    boolean autoHaEnabled = getConf().getBoolean(
        DFS_HA_AUTO_FAILOVER_ENABLED_KEY, DFS_HA_AUTO_FAILOVER_ENABLED_DEFAULT);
    switch (req.getSource()) {
    case REQUEST_BY_USER:
      if (autoHaEnabled) {
        throw new AccessControlException(
            "Manual HA control for this NameNode is disallowed, because " +
            "automatic HA is enabled.");
      }
      break;
    case REQUEST_BY_USER_FORCED:
      if (autoHaEnabled) {
        LOG.warn("Allowing manual HA control from " +
            Server.getRemoteAddress() +
            " even though automatic HA is enabled, because the user " +
            "specified the force flag");
      }
      break;
    case REQUEST_BY_ZKFC:
      if (!autoHaEnabled) {
        throw new AccessControlException(
            "Request from ZK failover controller at " +
            Server.getRemoteAddress() + " denied since automatic HA " +
            "is not enabled"); 
      }
      break;
    }
  }

  /*
   * {@inheritDoc}
   * */
  @Override // ReconfigurableBase
  public Collection<String> getReconfigurableProperties() {
    return reconfigurableProperties;
  }

  /*
   * {@inheritDoc}
   * */
  @Override // ReconfigurableBase
  protected String reconfigurePropertyImpl(String property, String newVal)
      throws ReconfigurationException {
    final DatanodeManager datanodeManager = namesystem.getBlockManager()
        .getDatanodeManager();

    if (property.equals(DFS_HEARTBEAT_INTERVAL_KEY)) {
      return reconfHeartbeatInterval(datanodeManager, property, newVal);
    } else if (property.equals(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY)) {
      return reconfHeartbeatRecheckInterval(datanodeManager, property, newVal);
    } else if (property.equals(FS_PROTECTED_DIRECTORIES)) {
      return reconfProtectedDirectories(newVal);
    } else if (property.equals(HADOOP_CALLER_CONTEXT_ENABLED_KEY)) {
      return reconfCallerContextEnabled(newVal);
    } else if (property.equals(ipcClientRPCBackoffEnable)) {
      return reconfigureIPCBackoffEnabled(newVal);
    } else if (property.equals(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY)) {
      return reconfigureSPSModeEvent(newVal, property);
    } else if (property.equals(DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY)
        || property.equals(DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY)
        || property.equals(
            DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION)
        || property.equals(DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY)) {
      return reconfReplicationParameters(newVal, property);
    } else if (property.equals(DFS_BLOCK_REPLICATOR_CLASSNAME_KEY) || property
        .equals(DFS_BLOCK_PLACEMENT_EC_CLASSNAME_KEY)) {
      reconfBlockPlacementPolicy();
      return newVal;
    } else if (property.equals(DFS_IMAGE_PARALLEL_LOAD_KEY)) {
      return reconfigureParallelLoad(newVal);
    } else if (property.equals(DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_KEY) || (property.equals(
        DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY)) || (property.equals(
        DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY)) || (property.equals(
        DFS_DATANODE_PEER_STATS_ENABLED_KEY)) || property.equals(
        DFS_DATANODE_MAX_NODES_TO_REPORT_KEY)) {
      return reconfigureSlowNodesParameters(datanodeManager, property, newVal);
    } else if (property.equals(DFS_BLOCK_INVALIDATE_LIMIT_KEY)) {
      return reconfigureBlockInvalidateLimit(datanodeManager, property, newVal);
    } else if (property.equals(DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT) ||
        (property.equals(DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK))) {
      return reconfigureDecommissionBackoffMonitorParameters(datanodeManager, property,
          newVal);
    } else if (property.equals(DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_KEY)) {
      return reconfigureMinBlocksForWrite(property, newVal);
    } else {
      throw new ReconfigurationException(property, newVal, getConf().get(
          property));
    }
  }

  private String reconfReplicationParameters(final String newVal,
      final String property) throws ReconfigurationException {
    BlockManager bm = namesystem.getBlockManager();
    int newSetting;
    namesystem.writeLock();
    try {
      if (property.equals(DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY)) {
        bm.setMaxReplicationStreams(
            adjustNewVal(DFS_NAMENODE_REPLICATION_MAX_STREAMS_DEFAULT, newVal));
        newSetting = bm.getMaxReplicationStreams();
      } else if (property.equals(
          DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY)) {
        bm.setReplicationStreamsHardLimit(
            adjustNewVal(DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT,
                newVal));
        newSetting = bm.getReplicationStreamsHardLimit();
      } else if (
          property.equals(
              DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION)) {
        bm.setBlocksReplWorkMultiplier(
            adjustNewVal(
                DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION_DEFAULT,
                newVal));
        newSetting = bm.getBlocksReplWorkMultiplier();
      } else if (
          property.equals(
              DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY)) {
        bm.setReconstructionPendingTimeout(
            adjustNewVal(
                DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_DEFAULT,
                newVal));
        newSetting = bm.getReconstructionPendingTimeout();
      } else {
        throw new IllegalArgumentException("Unexpected property " +
            property + " in reconfReplicationParameters");
      }
      LOG.info("RECONFIGURE* changed {} to {}", property, newSetting);
      return String.valueOf(newSetting);
    } catch (IllegalArgumentException e) {
      throw new ReconfigurationException(property, newVal, getConf().get(
          property), e);
    } finally {
      namesystem.writeUnlock("reconfReplicationParameters");
    }
  }

  private void reconfBlockPlacementPolicy() {
    getNamesystem().getBlockManager()
        .refreshBlockPlacementPolicy(getNewConf());
  }

  private int adjustNewVal(int defaultVal, String newVal) {
    if (newVal == null) {
      return defaultVal;
    } else {
      return Integer.parseInt(newVal);
    }
  }

  private String reconfHeartbeatInterval(final DatanodeManager datanodeManager,
      final String property, final String newVal)
      throws ReconfigurationException {
    namesystem.writeLock();
    try {
      if (newVal == null) {
        // set to default
        datanodeManager.setHeartbeatInterval(DFS_HEARTBEAT_INTERVAL_DEFAULT);
        return String.valueOf(DFS_HEARTBEAT_INTERVAL_DEFAULT);
      } else {
        long newInterval = getConf()
            .getTimeDurationHelper(DFS_HEARTBEAT_INTERVAL_KEY,
                newVal, TimeUnit.SECONDS);
        datanodeManager.setHeartbeatInterval(newInterval);
        return String.valueOf(datanodeManager.getHeartbeatInterval());
      }
    } catch (NumberFormatException nfe) {
      throw new ReconfigurationException(property, newVal, getConf().get(
          property), nfe);
    } finally {
      namesystem.writeUnlock("reconfHeartbeatInterval");
      LOG.info("RECONFIGURE* changed heartbeatInterval to "
          + datanodeManager.getHeartbeatInterval());
    }
  }

  private String reconfHeartbeatRecheckInterval(
      final DatanodeManager datanodeManager, final String property,
      final String newVal) throws ReconfigurationException {
    namesystem.writeLock();
    try {
      if (newVal == null) {
        // set to default
        datanodeManager.setHeartbeatRecheckInterval(
            DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT);
        return String.valueOf(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT);
      } else {
        datanodeManager.setHeartbeatRecheckInterval(Integer.parseInt(newVal));
        return String.valueOf(datanodeManager.getHeartbeatRecheckInterval());
      }
    } catch (NumberFormatException nfe) {
      throw new ReconfigurationException(property, newVal, getConf().get(
          property), nfe);
    } finally {
      namesystem.writeUnlock("reconfHeartbeatRecheckInterval");
      LOG.info("RECONFIGURE* changed heartbeatRecheckInterval to "
          + datanodeManager.getHeartbeatRecheckInterval());
    }
  }

  private String reconfProtectedDirectories(String newVal) {
    return getNamesystem().getFSDirectory().setProtectedDirectories(newVal);
  }

  private String reconfCallerContextEnabled(String newVal) {
    Boolean callerContextEnabled;
    if (newVal == null) {
      callerContextEnabled = HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT;
    } else {
      callerContextEnabled = Boolean.parseBoolean(newVal);
    }
    namesystem.setCallerContextEnabled(callerContextEnabled);
    return Boolean.toString(callerContextEnabled);
  }

  String reconfigureIPCBackoffEnabled(String newVal) {
    boolean clientBackoffEnabled;
    if (newVal == null) {
      clientBackoffEnabled = IPC_BACKOFF_ENABLE_DEFAULT;
    } else {
      clientBackoffEnabled = Boolean.parseBoolean(newVal);
    }
    rpcServer.getClientRpcServer()
        .setClientBackoffEnabled(clientBackoffEnabled);
    return Boolean.toString(clientBackoffEnabled);
  }

  String reconfigureSPSModeEvent(String newVal, String property)
      throws ReconfigurationException {
    if (newVal == null
        || StoragePolicySatisfierMode.fromString(newVal) == null) {
      throw new ReconfigurationException(property, newVal,
          getConf().get(property),
          new HadoopIllegalArgumentException(
              "For enabling or disabling storage policy satisfier, must "
                  + "pass either internal/external/none string value only"));
    }

    if (!isActiveState()) {
      throw new ReconfigurationException(property, newVal,
          getConf().get(property),
          new HadoopIllegalArgumentException(
              "Enabling or disabling storage policy satisfier service on "
                  + state + " NameNode is not allowed"));
    }
    StoragePolicySatisfierMode mode = StoragePolicySatisfierMode
        .fromString(newVal);
    if (mode == StoragePolicySatisfierMode.NONE) {
      // disabling sps service
      if (namesystem.getBlockManager().getSPSManager() != null) {
        namesystem.getBlockManager().getSPSManager().changeModeEvent(mode);
        namesystem.getBlockManager().disableSPS();
      }
    } else {
      // enabling sps service
      boolean spsCreated = (namesystem.getBlockManager()
          .getSPSManager() != null);
      if (!spsCreated) {
        spsCreated = namesystem.getBlockManager().createSPSManager(getConf(),
            newVal);
      }
      if (spsCreated) {
        namesystem.getBlockManager().getSPSManager().changeModeEvent(mode);
      }
    }
    return newVal;
  }

  String reconfigureParallelLoad(String newVal) {
    boolean enableParallelLoad;
    if (newVal == null) {
      enableParallelLoad = DFS_IMAGE_PARALLEL_LOAD_DEFAULT;
    } else {
      enableParallelLoad = Boolean.parseBoolean(newVal);
    }
    FSImageFormatProtobuf.refreshParallelSaveAndLoad(enableParallelLoad);
    return Boolean.toString(enableParallelLoad);
  }

  String reconfigureSlowNodesParameters(final DatanodeManager datanodeManager,
      final String property, final String newVal) throws ReconfigurationException {
    BlockManager bm = namesystem.getBlockManager();
    namesystem.writeLock();
    String result;
    try {
      switch (property) {
      case DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_KEY: {
        boolean enable = (newVal == null ?
            DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_DEFAULT :
            Boolean.parseBoolean(newVal));
        result = Boolean.toString(enable);
        datanodeManager.setAvoidSlowDataNodesForReadEnabled(enable);
        break;
      }
      case DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY: {
        boolean enable = (newVal == null ?
            DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_DEFAULT :
            Boolean.parseBoolean(newVal));
        result = Boolean.toString(enable);
        bm.setExcludeSlowNodesEnabled(enable);
        break;
      }
      case DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY: {
        int maxSlowpeerCollectNodes = (newVal == null ?
            DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_DEFAULT :
            Integer.parseInt(newVal));
        result = Integer.toString(maxSlowpeerCollectNodes);
        datanodeManager.setMaxSlowpeerCollectNodes(maxSlowpeerCollectNodes);
        break;
      }
      case DFS_DATANODE_PEER_STATS_ENABLED_KEY: {
        Timer timer = new Timer();
        if (newVal != null && !newVal.equalsIgnoreCase("true") && !newVal.equalsIgnoreCase(
            "false")) {
          throw new IllegalArgumentException(newVal + " is not boolean value");
        }
        final boolean peerStatsEnabled = newVal == null ?
            DFS_DATANODE_PEER_STATS_ENABLED_DEFAULT :
            Boolean.parseBoolean(newVal);
        result = Boolean.toString(peerStatsEnabled);
        datanodeManager.initSlowPeerTracker(getConf(), timer, peerStatsEnabled);
        break;
      }
      case DFS_DATANODE_MAX_NODES_TO_REPORT_KEY: {
        int maxSlowPeersToReport = (newVal == null
            ? DFS_DATANODE_MAX_NODES_TO_REPORT_DEFAULT : Integer.parseInt(newVal));
        result = Integer.toString(maxSlowPeersToReport);
        datanodeManager.setMaxSlowPeersToReport(maxSlowPeersToReport);
        break;
      }
      default: {
        throw new IllegalArgumentException(
            "Unexpected property " + property + " in reconfigureSlowNodesParameters");
      }
      }
      LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
      return result;
    } catch (IllegalArgumentException e) {
      throw new ReconfigurationException(property, newVal, getConf().get(
          property), e);
    } finally {
      namesystem.writeUnlock("reconfigureSlowNodesParameters");
    }
  }

  private String reconfigureBlockInvalidateLimit(final DatanodeManager datanodeManager,
      final String property, final String newVal) throws ReconfigurationException {
    namesystem.writeLock();
    try {
      if (newVal == null) {
        datanodeManager.setBlockInvalidateLimit(DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT);
      } else {
        datanodeManager.setBlockInvalidateLimit(Integer.parseInt(newVal));
      }
      final String updatedBlockInvalidateLimit =
          String.valueOf(datanodeManager.getBlockInvalidateLimit());
      LOG.info("RECONFIGURE* changed blockInvalidateLimit to {}", updatedBlockInvalidateLimit);
      return updatedBlockInvalidateLimit;
    } catch (NumberFormatException e) {
      throw new ReconfigurationException(property, newVal, getConf().get(property), e);
    } finally {
      namesystem.writeUnlock("reconfigureBlockInvalidateLimit");
    }
  }

  private String reconfigureDecommissionBackoffMonitorParameters(
      final DatanodeManager datanodeManager, final String property, final String newVal)
      throws ReconfigurationException {
    String newSetting = null;
    try {
      if (property.equals(DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT)) {
        int pendingRepLimit = (newVal == null ?
            DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT_DEFAULT :
            Integer.parseInt(newVal));
        datanodeManager.getDatanodeAdminManager().refreshPendingRepLimit(pendingRepLimit,
            DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT);
        newSetting = String.valueOf(datanodeManager.getDatanodeAdminManager().getPendingRepLimit());
      } else if (property.equals(
          DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK)) {
        int blocksPerLock = (newVal == null ?
            DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK_DEFAULT :
            Integer.parseInt(newVal));
        datanodeManager.getDatanodeAdminManager().refreshBlocksPerLock(blocksPerLock,
            DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK);
        newSetting = String.valueOf(datanodeManager.getDatanodeAdminManager().getBlocksPerLock());
      }
      LOG.info("RECONFIGURE* changed reconfigureDecommissionBackoffMonitorParameters {} to {}",
          property, newSetting);
      return newSetting;
    } catch (IllegalArgumentException e) {
      throw new ReconfigurationException(property, newVal, getConf().get(property), e);
    }
  }

  private String reconfigureMinBlocksForWrite(String property, String newValue)
      throws ReconfigurationException {
    try {
      int newSetting = adjustNewVal(
          DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_DEFAULT, newValue);
      namesystem.getBlockManager().setMinBlocksForWrite(newSetting);
      return String.valueOf(newSetting);
    } catch (IllegalArgumentException e) {
      throw new ReconfigurationException(property, newValue, getConf().get(property), e);
    }
  }

  @Override  // ReconfigurableBase
  protected Configuration getNewConf() {
    return new HdfsConfiguration();
  }
}
