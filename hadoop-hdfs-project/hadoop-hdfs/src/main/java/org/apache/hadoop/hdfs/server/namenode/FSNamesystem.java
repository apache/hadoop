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

import static org.apache.commons.text.StringEscapeUtils.escapeJava;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SEPARATOR_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SEPARATOR_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_WITH_REMOTE_PORT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_WITH_REMOTE_PORT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_PERMISSIONS_SUPERUSER_ONLY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_PERMISSIONS_SUPERUSER_ONLY_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DEFAULT_AUDIT_LOGGER_NAME;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LEASE_RECHECK_INTERVAL_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LEASE_RECHECK_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_LOCK_HOLD_TO_RELEASE_LEASE_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_LOCK_HOLD_TO_RELEASE_LEASE_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_DIFF_LISTING_LIMIT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_DIFF_LISTING_LIMIT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSUtil.isParentEntry;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.text.CaseUtils;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY;
import static org.apache.hadoop.hdfs.server.namenode.FSDirStatAndListingOp.*;
import static org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE;
import static org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY;
import static org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.OBSERVER;

import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotDeletionGc;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.hdfs.protocol.BatchedDirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsPartialListing;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.server.common.ECTopologyVerifier;
import org.apache.hadoop.hdfs.server.namenode.metrics.ReplicatedBlocksMBean;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.ipc.ObserverRetryOnActiveException;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

import static org.apache.hadoop.util.Time.now;
import static org.apache.hadoop.util.Time.monotonicNow;
import static org.apache.hadoop.hdfs.server.namenode.top.metrics.TopMetrics.TOPMETRICS_METRICS_SOURCE_NAME;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.KeyProvider.Metadata;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.UnknownCryptoProtocolVersionException;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.ReencryptAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeException;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BatchedListingKeyProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager.SecretManagerState;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockUnderConstructionFeature;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.FSDirEncryptionZoneOp.EncryptionKeyInfo;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.namenode.ha.EditLogTailer;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.StandbyCheckpointer;
import org.apache.hadoop.hdfs.server.namenode.metrics.ECBlockGroupsMBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.server.namenode.top.TopAuditLogger;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.hdfs.server.namenode.top.metrics.TopMetrics;
import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.RetryCache;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.Lists;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * FSNamesystem is a container of both transient
 * and persisted name-space state, and does all the book-keeping
 * work on a NameNode.
 *
 * Its roles are briefly described below:
 *
 * 1) Is the container for BlockManager, DatanodeManager,
 *    DelegationTokens, LeaseManager, etc. services.
 * 2) RPC calls that modify or inspect the name-space
 *    should get delegated here.
 * 3) Anything that touches only blocks (eg. block reports),
 *    it delegates to BlockManager.
 * 4) Anything that touches only file information (eg. permissions, mkdirs),
 *    it delegates to FSDirectory.
 * 5) Anything that crosses two of the above components should be
 *    coordinated here.
 * 6) Logs mutations to FSEditLog.
 *
 * This class and its contents keep:
 *
 * 1)  Valid fsname {@literal -->} blocklist  (kept on disk, logged)
 * 2)  Set of all valid blocks (inverted #1)
 * 3)  block {@literal -->} machinelist (kept in memory, rebuilt dynamically
 *     from reports)
 * 4)  machine {@literal -->} blocklist (inverted #2)
 * 5)  LRU cache of updated-heartbeat machines
 */
@InterfaceAudience.Private
@Metrics(context="dfs")
public class FSNamesystem implements Namesystem, FSNamesystemMBean,
    NameNodeMXBean, ReplicatedBlocksMBean, ECBlockGroupsMBean {

  public static final Logger LOG = LoggerFactory.getLogger(FSNamesystem.class);

  // The following are private configurations
  public static final String DFS_NAMENODE_SNAPSHOT_TRASHROOT_ENABLED =
      "dfs.namenode.snapshot.trashroot.enabled";
  public static final boolean DFS_NAMENODE_SNAPSHOT_TRASHROOT_ENABLED_DEFAULT
      = false;
  private static final FsPermission SHARED_TRASH_PERMISSION =
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL, true);

  private final MetricsRegistry registry = new MetricsRegistry("FSNamesystem");
  @Metric final MutableRatesWithAggregation detailedLockHoldTimeMetrics =
      registry.newRatesWithAggregation("detailedLockHoldTimeMetrics");

  private final String contextFieldSeparator;

  boolean isAuditEnabled() {
    return (!isDefaultAuditLogger || AUDIT_LOG.isInfoEnabled())
        && !auditLoggers.isEmpty();
  }

  void logAuditEvent(boolean succeeded, String cmd, String src)
      throws IOException {
    logAuditEvent(succeeded, cmd, src, null, null);
  }
  
  private void logAuditEvent(boolean succeeded, String cmd, String src,
      String dst, FileStatus stat) throws IOException {
    if (isAuditEnabled() && isExternalInvocation()) {
      logAuditEvent(succeeded, NameNode.getRemoteUser(), Server.getRemoteIp(),
          cmd, src, dst, stat);
    }
  }

  private void logAuditEvent(boolean succeeded, String cmd, String src,
      HdfsFileStatus stat) throws IOException {
    if (!isAuditEnabled() || !isExternalInvocation()) {
      return;
    }
    FileStatus status = null;
    if (stat != null) {
      Path symlink = stat.isSymlink()
          ? new Path(DFSUtilClient.bytes2String(stat.getSymlinkInBytes()))
          : null;
      Path path = new Path(src);
      status = new FileStatus(stat.getLen(), stat.isDirectory(),
          stat.getReplication(), stat.getBlockSize(),
          stat.getModificationTime(),
          stat.getAccessTime(), stat.getPermission(), stat.getOwner(),
          stat.getGroup(), symlink, path);
    }
    logAuditEvent(succeeded, cmd, src, null, status);
  }

  private void logAuditEvent(boolean succeeded,
      UserGroupInformation ugi, InetAddress addr, String cmd, String src,
      String dst, FileStatus status) {
    final String ugiStr = ugi.toString();
    for (AuditLogger logger : auditLoggers) {
      if (logger instanceof HdfsAuditLogger) {
        HdfsAuditLogger hdfsLogger = (HdfsAuditLogger) logger;
        if (auditLogWithRemotePort) {
          appendClientPortToCallerContextIfAbsent();
        }
        hdfsLogger.logAuditEvent(succeeded, ugiStr, addr, cmd, src, dst,
            status, CallerContext.getCurrent(), ugi, dtSecretManager);
      } else {
        logger.logAuditEvent(succeeded, ugiStr, addr, cmd, src, dst, status);
      }
    }
  }

  private void appendClientPortToCallerContextIfAbsent() {
    CallerContext ctx = CallerContext.getCurrent();
    if (isClientPortInfoAbsent(ctx)) {
      String origContext = ctx == null ? null : ctx.getContext();
      byte[] origSignature = ctx == null ? null : ctx.getSignature();
      CallerContext.setCurrent(
          new CallerContext.Builder(origContext, contextFieldSeparator)
              .append(CallerContext.CLIENT_PORT_STR, String.valueOf(Server.getRemotePort()))
              .setSignature(origSignature)
              .build());
    }
    ctx = CallerContext.getCurrent();
    if (isFromProxyUser(ctx)) {
      CallerContext.setCurrent(
          new CallerContext.Builder(ctx.getContext(), contextFieldSeparator)
              .append(CallerContext.PROXY_USER_PORT, String.valueOf(Server.getRemotePort()))
              .setSignature(ctx.getSignature())
              .build());
    }
  }

  private boolean isClientPortInfoAbsent(CallerContext ctx){
    return ctx == null || ctx.getContext() == null
        || !ctx.getContext().contains(CallerContext.CLIENT_PORT_STR);
  }

  private boolean isFromProxyUser(CallerContext ctx) {
    return ctx != null && ctx.getContext() != null &&
        ctx.getContext().contains(CallerContext.REAL_USER_STR);
  }
  /**
   * Logger for audit events, noting successful FSNamesystem operations. Emits
   * to FSNamesystem.audit at INFO. Each event causes a set of tab-separated
   * <code>key=value</code> pairs to be written for the following properties:
   * <code>
   * ugi=&lt;ugi in RPC&gt;
   * ip=&lt;remote IP&gt;
   * cmd=&lt;command&gt;
   * src=&lt;src path&gt;
   * dst=&lt;dst path (optional)&gt;
   * perm=&lt;permissions (optional)&gt;
   * </code>
   */
  public static final Logger AUDIT_LOG =
      LoggerFactory.getLogger(FSNamesystem.class.getName() + ".audit");

  private final int maxCorruptFileBlocksReturn;
  private final boolean isPermissionEnabled;
  private final boolean isStoragePolicyEnabled;
  private final boolean isStoragePolicySuperuserOnly;
  private final UserGroupInformation fsOwner;
  private final String supergroup;
  private final boolean standbyShouldCheckpoint;
  private final boolean isSnapshotTrashRootEnabled;
  private final int snapshotDiffReportLimit;

  /**
   * Whether enable checkOperation when call getBlocks.
   * It is enabled  by default.
   */
  private final boolean isGetBlocksCheckOperationEnabled;

  /** Interval between each check of lease to release. */
  private final long leaseRecheckIntervalMs;
  /** Maximum time the lock is hold to release lease. */
  private final long maxLockHoldToReleaseLeaseMs;

  // Batch size for open files response
  private final int maxListOpenFilesResponses;

  private final boolean allowOwnerSetQuota;

  // Scan interval is not configurable.
  private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL =
    TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
  final DelegationTokenSecretManager dtSecretManager;
  private final boolean alwaysUseDelegationTokensForTests;

  private static final Step STEP_AWAITING_REPORTED_BLOCKS =
    new Step(StepType.AWAITING_REPORTED_BLOCKS);

  // Tracks whether the default audit logger is the only configured audit
  // logger; this allows isAuditEnabled() to return false in case the
  // underlying logger is disabled, and avoid some unnecessary work.
  private final boolean isDefaultAuditLogger;
  private final List<AuditLogger> auditLoggers;
  private final boolean auditLogWithRemotePort;

  /** The namespace tree. */
  FSDirectory dir;
  private BlockManager blockManager;
  private final SnapshotManager snapshotManager;
  private final SnapshotDeletionGc snapshotDeletionGc;
  private final CacheManager cacheManager;
  private final DatanodeStatistics datanodeStatistics;

  private String nameserviceId;

  private volatile RollingUpgradeInfo rollingUpgradeInfo = null;
  /**
   * A flag that indicates whether the checkpointer should checkpoint a rollback
   * fsimage. The edit log tailer sets this flag. The checkpoint will create a
   * rollback fsimage if the flag is true, and then change the flag to false.
   */
  private volatile boolean needRollbackFsImage;

  final LeaseManager leaseManager = new LeaseManager(this); 

  Daemon nnrmthread = null; // NamenodeResourceMonitor thread

  Daemon nnEditLogRoller = null; // NameNodeEditLogRoller thread

  // A daemon to periodically clean up corrupt lazyPersist files
  // from the name space.
  Daemon lazyPersistFileScrubber = null;
  /**
   * Timestamp marking the end time of {@link #lazyPersistFileScrubber}'s full
   * cycle. This value can be checked by the Junit tests to verify that the
   * {@link #lazyPersistFileScrubber} has run at least one full iteration.
   */
  private final AtomicLong lazyPersistFileScrubberTS = new AtomicLong(0);


  // Executor to warm up EDEK cache
  private ExecutorService edekCacheLoader = null;
  private final int edekCacheLoaderDelay;
  private final int edekCacheLoaderInterval;

  /**
   * When an active namenode will roll its own edit log, in # edits
   */
  private final long editLogRollerThreshold;
  /**
   * Check interval of an active namenode's edit log roller thread 
   */
  private final int editLogRollerInterval;

  /**
   * How frequently we scan and unlink corrupt lazyPersist files.
   * (In seconds)
   */
  private final int lazyPersistFileScrubIntervalSec;

  private volatile boolean hasResourcesAvailable = false;
  private volatile boolean fsRunning = true;
  
  /** The start time of the namesystem. */
  private final long startTime = now();

  /** The interval of namenode checking for the disk space availability */
  private final long resourceRecheckInterval;

  // The actual resource checker instance.
  NameNodeResourceChecker nnResourceChecker;

  private final FsServerDefaults serverDefaults;
  private final ReplaceDatanodeOnFailure dtpReplaceDatanodeOnFailure;

  private final long maxFsObjects;          // maximum number of fs objects

  private final long minBlockSize;         // minimum block size
  final long maxBlocksPerFile;     // maximum # of blocks per file

  // Maximum number of paths that can be listed per batched call.
  private final int batchedListingLimit;

  private final int numCommittedAllowed;

  /** Lock to protect FSNamesystem. */
  private final FSNamesystemLock fsLock;

  /** 
   * Checkpoint lock to protect FSNamesystem modification on standby NNs.
   * Unlike fsLock, it does not affect block updates. On active NNs, this lock
   * does not provide proper protection, because there are operations that
   * modify both block and name system state.  Even on standby, fsLock is 
   * used when block state changes need to be blocked.
   */
  private final ReentrantLock cpLock;

  /**
   * Used when this NN is in standby or observer state to read from the
   * shared edit log.
   */
  private EditLogTailer editLogTailer = null;

  /**
   * Used when this NN is in standby state to perform checkpoints.
   */
  private StandbyCheckpointer standbyCheckpointer;

  /**
   * Reference to the NN's HAContext object. This is only set once
   * {@link #startCommonServices(Configuration, HAContext)} is called. 
   */
  private HAContext haContext;

  private final boolean haEnabled;

  /**
   * Whether the namenode is in the middle of starting the active service
   */
  private volatile boolean startingActiveService = false;

  private final RetryCache retryCache;

  private KeyProviderCryptoExtension provider = null;

  private volatile boolean imageLoaded = false;
  private final Condition cond;

  private final FSImage fsImage;

  private final TopConf topConf;
  private TopMetrics topMetrics;

  private INodeAttributeProvider inodeAttributeProvider;

  /**
   * If the NN is in safemode, and not due to manual / low resources, we
   * assume it must be because of startup. If the NN had low resources during
   * startup, we assume it came out of startup safemode and it is now in low
   * resources safemode.
   */
  private boolean manualSafeMode = false;
  private boolean resourceLowSafeMode = false;
  private String nameNodeHostName = null;

  /**
   * HDFS-14497: Concurrency control when many metaSave request to write
   * meta to same out stream after switch to read lock.
   */
  private final Object metaSaveLock = new Object();

  private final MessageDigest digest;

  /**
   * Notify that loading of this FSDirectory is complete, and
   * it is imageLoaded for use
   */
  void imageLoadComplete() {
    Preconditions.checkState(!imageLoaded, "FSDirectory already loaded");
    setImageLoaded();
  }

  void setImageLoaded() {
    if(imageLoaded) return;
    writeLock();
    try {
      setImageLoaded(true);
      dir.markNameCacheInitialized();
      cond.signalAll();
    } finally {
      writeUnlock("setImageLoaded");
    }
  }

  //This is for testing purposes only
  @VisibleForTesting
  boolean isImageLoaded() {
    return imageLoaded;
  }

  // exposed for unit tests
  protected void setImageLoaded(boolean flag) {
    imageLoaded = flag;
  }

  /**
   * Clear all loaded data
   */
  void clear() {
    dir.reset();
    dtSecretManager.reset();
    leaseManager.removeAllLeases();
    snapshotManager.clearSnapshottableDirs();
    cacheManager.clear();
    setImageLoaded(false);
    blockManager.clear();
    ErasureCodingPolicyManager.getInstance().clear();
  }

  @VisibleForTesting
  LeaseManager getLeaseManager() {
    return leaseManager;
  }

  /**
   * Used as ad hoc to check the time stamp of the last full cycle of {@link
   * #lazyPersistFileScrubber} daemon. This is used by the Junit tests to block
   * until {@link #lazyPersistFileScrubberTS} is updated.
   *
   * @return the current {@link #lazyPersistFileScrubberTS} if {@link
   *         #lazyPersistFileScrubber} is not null.
   */
  @VisibleForTesting
  public long getLazyPersistFileScrubberTS() {
    return lazyPersistFileScrubber == null ? -1
        : lazyPersistFileScrubberTS.get();
  }

  public boolean isHaEnabled() {
    return haEnabled;
  }

  /**
   * Check the supplied configuration for correctness.
   * @param conf Supplies the configuration to validate.
   * @throws IOException if the configuration could not be queried.
   * @throws IllegalArgumentException if the configuration is invalid.
   */
  private static void checkConfiguration(Configuration conf)
      throws IOException {

    final Collection<URI> namespaceDirs =
        FSNamesystem.getNamespaceDirs(conf);
    final Collection<URI> editsDirs =
        FSNamesystem.getNamespaceEditsDirs(conf);
    final Collection<URI> requiredEditsDirs =
        FSNamesystem.getRequiredNamespaceEditsDirs(conf);
    final Collection<URI> sharedEditsDirs =
        FSNamesystem.getSharedEditsDirs(conf);

    for (URI u : requiredEditsDirs) {
      if (u.toString().compareTo(
              DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_DEFAULT) == 0) {
        continue;
      }

      // Each required directory must also be in editsDirs or in
      // sharedEditsDirs.
      if (!editsDirs.contains(u) &&
          !sharedEditsDirs.contains(u)) {
        throw new IllegalArgumentException("Required edits directory " + u
            + " not found: "
            + DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY + "=" + editsDirs + "; "
            + DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY
            + "=" + requiredEditsDirs + "; "
            + DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY
            + "=" + sharedEditsDirs);
      }
    }

    if (namespaceDirs.size() == 1) {
      LOG.warn("Only one image storage directory ("
          + DFS_NAMENODE_NAME_DIR_KEY + ") configured. Beware of data loss"
          + " due to lack of redundant storage directories!");
    }
    if (editsDirs.size() == 1) {
      LOG.warn("Only one namespace edits storage directory ("
          + DFS_NAMENODE_EDITS_DIR_KEY + ") configured. Beware of data loss"
          + " due to lack of redundant storage directories!");
    }
  }

  /**
   * Instantiates an FSNamesystem loaded from the image and edits
   * directories specified in the passed Configuration.
   *
   * @param conf the Configuration which specifies the storage directories
   *             from which to load
   * @return an FSNamesystem which contains the loaded namespace
   * @throws IOException if loading fails
   */
  static FSNamesystem loadFromDisk(Configuration conf) throws IOException {

    checkConfiguration(conf);
    FSImage fsImage = new FSImage(conf,
        FSNamesystem.getNamespaceDirs(conf),
        FSNamesystem.getNamespaceEditsDirs(conf));
    FSNamesystem namesystem = new FSNamesystem(conf, fsImage, false);
    StartupOption startOpt = NameNode.getStartupOption(conf);
    if (startOpt == StartupOption.RECOVER) {
      namesystem.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    }

    long loadStart = monotonicNow();
    try {
      namesystem.loadFSImage(startOpt);
    } catch (IOException ioe) {
      LOG.warn("Encountered exception loading fsimage", ioe);
      fsImage.close();
      throw ioe;
    }
    long timeTakenToLoadFSImage = monotonicNow() - loadStart;
    LOG.info("Finished loading FSImage in " + timeTakenToLoadFSImage + " msecs");
    NameNodeMetrics nnMetrics = NameNode.getNameNodeMetrics();
    if (nnMetrics != null) {
      nnMetrics.setFsImageLoadTime((int) timeTakenToLoadFSImage);
    }
    namesystem.getFSDirectory().createReservedStatuses(namesystem.getCTime());
    return namesystem;
  }
  
  FSNamesystem(Configuration conf, FSImage fsImage) throws IOException {
    this(conf, fsImage, false);
  }
  
  /**
   * Create an FSNamesystem associated with the specified image.
   * 
   * Note that this does not load any data off of disk -- if you would
   * like that behavior, use {@link #loadFromDisk(Configuration)}
   *
   * @param conf configuration
   * @param fsImage The FSImage to associate with
   * @param ignoreRetryCache Whether or not should ignore the retry cache setup
   *                         step. For Secondary NN this should be set to true.
   * @throws IOException on bad configuration
   */
  FSNamesystem(Configuration conf, FSImage fsImage, boolean ignoreRetryCache)
      throws IOException {
    provider = DFSUtil.createKeyProviderCryptoExtension(conf);
    LOG.info("KeyProvider: " + provider);
    checkForAsyncLogEnabledByOldConfigs(conf);
    auditLogWithRemotePort =
        conf.getBoolean(DFS_NAMENODE_AUDIT_LOG_WITH_REMOTE_PORT_KEY,
            DFS_NAMENODE_AUDIT_LOG_WITH_REMOTE_PORT_DEFAULT);
    this.contextFieldSeparator =
        conf.get(HADOOP_CALLER_CONTEXT_SEPARATOR_KEY,
            HADOOP_CALLER_CONTEXT_SEPARATOR_DEFAULT);
    fsLock = new FSNamesystemLock(conf, detailedLockHoldTimeMetrics);
    cond = fsLock.newWriteLockCondition();
    cpLock = new ReentrantLock();

    this.fsImage = fsImage;
    try {
      resourceRecheckInterval = conf.getLong(
          DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
          DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT);

      this.fsOwner = UserGroupInformation.getCurrentUser();
      this.supergroup = conf.get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY, 
                                 DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
      this.isPermissionEnabled = conf.getBoolean(DFS_PERMISSIONS_ENABLED_KEY,
                                                 DFS_PERMISSIONS_ENABLED_DEFAULT);

      this.isStoragePolicyEnabled =
          conf.getBoolean(DFS_STORAGE_POLICY_ENABLED_KEY,
              DFS_STORAGE_POLICY_ENABLED_DEFAULT);
      this.isStoragePolicySuperuserOnly =
          conf.getBoolean(DFS_STORAGE_POLICY_PERMISSIONS_SUPERUSER_ONLY_KEY,
              DFS_STORAGE_POLICY_PERMISSIONS_SUPERUSER_ONLY_DEFAULT);

      this.snapshotDiffReportLimit =
          conf.getInt(DFS_NAMENODE_SNAPSHOT_DIFF_LISTING_LIMIT,
              DFS_NAMENODE_SNAPSHOT_DIFF_LISTING_LIMIT_DEFAULT);

      LOG.info("fsOwner                = " + fsOwner);
      LOG.info("supergroup             = " + supergroup);
      LOG.info("isPermissionEnabled    = " + isPermissionEnabled);
      LOG.info("isStoragePolicyEnabled = " + isStoragePolicyEnabled);

      // block allocation has to be persisted in HA using a shared edits directory
      // so that the standby has up-to-date namespace information
      nameserviceId = DFSUtil.getNamenodeNameServiceId(conf);
      this.haEnabled = HAUtil.isHAEnabled(conf, nameserviceId);  
      
      // Sanity check the HA-related config.
      if (nameserviceId != null) {
        LOG.info("Determined nameservice ID: " + nameserviceId);
      }
      LOG.info("HA Enabled: " + haEnabled);
      if (!haEnabled && HAUtil.usesSharedEditsDir(conf)) {
        LOG.warn("Configured NNs:\n" + DFSUtil.nnAddressesAsString(conf));
        throw new IOException("Invalid configuration: a shared edits dir " +
            "must not be specified if HA is not enabled.");
      }

      // block manager needs the haEnabled initialized
      this.blockManager = new BlockManager(this, haEnabled, conf);
      this.datanodeStatistics = blockManager.getDatanodeManager().getDatanodeStatistics();

      // Get the checksum type from config
      String checksumTypeStr = conf.get(DFS_CHECKSUM_TYPE_KEY,
          DFS_CHECKSUM_TYPE_DEFAULT);
      this.isSnapshotTrashRootEnabled = conf.getBoolean(
          DFS_NAMENODE_SNAPSHOT_TRASHROOT_ENABLED,
          DFS_NAMENODE_SNAPSHOT_TRASHROOT_ENABLED_DEFAULT);
      DataChecksum.Type checksumType;
      try {
         checksumType = DataChecksum.Type.valueOf(checksumTypeStr);
      } catch (IllegalArgumentException iae) {
         throw new IOException("Invalid checksum type in "
            + DFS_CHECKSUM_TYPE_KEY + ": " + checksumTypeStr);
      }

      try {
        digest = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new IOException("Algorithm 'MD5' not found");
      }

      this.serverDefaults = new FsServerDefaults(
          conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT),
          conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY, DFS_BYTES_PER_CHECKSUM_DEFAULT),
          conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY, DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT),
          (short) conf.getInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT),
          conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),
          conf.getBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY, DFS_ENCRYPT_DATA_TRANSFER_DEFAULT),
          conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT),
          checksumType,
          conf.getTrimmed(
              CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
              ""),
          blockManager.getStoragePolicySuite().getDefaultPolicy().getId(),
          isSnapshotTrashRootEnabled);

      this.maxFsObjects = conf.getLong(DFS_NAMENODE_MAX_OBJECTS_KEY, 
                                       DFS_NAMENODE_MAX_OBJECTS_DEFAULT);

      this.minBlockSize = conf.getLongBytes(
          DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY,
          DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_DEFAULT);
      this.maxBlocksPerFile = conf.getLong(DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY,
          DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_DEFAULT);
      this.batchedListingLimit = conf.getInt(
          DFSConfigKeys.DFS_NAMENODE_BATCHED_LISTING_LIMIT,
          DFSConfigKeys.DFS_NAMENODE_BATCHED_LISTING_LIMIT_DEFAULT);
      Preconditions.checkArgument(
          batchedListingLimit > 0,
          DFSConfigKeys.DFS_NAMENODE_BATCHED_LISTING_LIMIT +
              " must be greater than zero");
      this.numCommittedAllowed = conf.getInt(
          DFSConfigKeys.DFS_NAMENODE_FILE_CLOSE_NUM_COMMITTED_ALLOWED_KEY,
          DFSConfigKeys.DFS_NAMENODE_FILE_CLOSE_NUM_COMMITTED_ALLOWED_DEFAULT);

      this.maxCorruptFileBlocksReturn = conf.getInt(
          DFSConfigKeys.DFS_NAMENODE_MAX_CORRUPT_FILE_BLOCKS_RETURNED_KEY,
          DFSConfigKeys.DFS_NAMENODE_MAX_CORRUPT_FILE_BLOCKS_RETURNED_DEFAULT);

      this.dtpReplaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);
      
      this.standbyShouldCheckpoint = conf.getBoolean(
          DFS_HA_STANDBY_CHECKPOINTS_KEY, DFS_HA_STANDBY_CHECKPOINTS_DEFAULT);
      // # edit autoroll threshold is a multiple of the checkpoint threshold 
      this.editLogRollerThreshold = (long)
          (conf.getFloat(
              DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD,
              DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT) *
          conf.getLong(
              DFS_NAMENODE_CHECKPOINT_TXNS_KEY,
              DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT));
      this.editLogRollerInterval = conf.getInt(
          DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS,
          DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS_DEFAULT);

      this.lazyPersistFileScrubIntervalSec = conf.getInt(
          DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC,
          DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC_DEFAULT);

      if (this.lazyPersistFileScrubIntervalSec < 0) {
        throw new IllegalArgumentException(
            DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC
                + " must be zero (for disable) or greater than zero.");
      }

      this.edekCacheLoaderDelay = conf.getInt(
          DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY,
          DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_DEFAULT);
      this.edekCacheLoaderInterval = conf.getInt(
          DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INTERVAL_MS_KEY,
          DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INTERVAL_MS_DEFAULT);

      this.leaseRecheckIntervalMs = conf.getLong(
          DFS_NAMENODE_LEASE_RECHECK_INTERVAL_MS_KEY,
          DFS_NAMENODE_LEASE_RECHECK_INTERVAL_MS_DEFAULT);
      Preconditions.checkArgument(
              leaseRecheckIntervalMs > 0,
              DFSConfigKeys.DFS_NAMENODE_LEASE_RECHECK_INTERVAL_MS_KEY +
                      " must be greater than zero");
      this.maxLockHoldToReleaseLeaseMs = conf.getLong(
          DFS_NAMENODE_MAX_LOCK_HOLD_TO_RELEASE_LEASE_MS_KEY,
          DFS_NAMENODE_MAX_LOCK_HOLD_TO_RELEASE_LEASE_MS_DEFAULT);

      // For testing purposes, allow the DT secret manager to be started regardless
      // of whether security is enabled.
      alwaysUseDelegationTokensForTests = conf.getBoolean(
          DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
          DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_DEFAULT);

      this.dtSecretManager = createDelegationTokenSecretManager(conf);
      this.dir = new FSDirectory(this, conf);
      this.snapshotManager = new SnapshotManager(conf, dir);
      this.snapshotDeletionGc = snapshotManager.isSnapshotDeletionOrdered()?
          new SnapshotDeletionGc(this, conf): null;

      this.cacheManager = new CacheManager(this, conf, blockManager);
      // Init ErasureCodingPolicyManager instance.
      ErasureCodingPolicyManager.getInstance().init(conf);
      this.topConf = new TopConf(conf);
      this.auditLoggers = initAuditLoggers(conf);
      this.isDefaultAuditLogger = auditLoggers.size() == 1 &&
        auditLoggers.get(0) instanceof DefaultAuditLogger;
      this.retryCache = ignoreRetryCache ? null : initRetryCache(conf);
      Class<? extends INodeAttributeProvider> klass = conf.getClass(
          DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY,
          null, INodeAttributeProvider.class);
      if (klass != null) {
        inodeAttributeProvider = ReflectionUtils.newInstance(klass, conf);
        LOG.info("Using INode attribute provider: " + klass.getName());
      }
      this.maxListOpenFilesResponses = conf.getInt(
          DFSConfigKeys.DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES,
          DFSConfigKeys.DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES_DEFAULT
      );
      Preconditions.checkArgument(maxListOpenFilesResponses > 0,
          DFSConfigKeys.DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES +
              " must be a positive integer."
      );
      this.allowOwnerSetQuota = conf.getBoolean(
          DFSConfigKeys.DFS_PERMISSIONS_ALLOW_OWNER_SET_QUOTA_KEY,
          DFSConfigKeys.DFS_PERMISSIONS_ALLOW_OWNER_SET_QUOTA_DEFAULT);
      this.isGetBlocksCheckOperationEnabled = conf.getBoolean(
          DFSConfigKeys.DFS_NAMENODE_GETBLOCKS_CHECK_OPERATION_KEY,
          DFSConfigKeys.DFS_NAMENODE_GETBLOCKS_CHECK_OPERATION_DEFAULT);

    } catch(IOException e) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", e);
      close();
      throw e;
    } catch (RuntimeException re) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", re);
      close();
      throw re;
    }
  }

  private static void checkForAsyncLogEnabledByOldConfigs(Configuration conf) {
    // dfs.namenode.audit.log.async is no longer in use. Use log4j properties instead.
    if (conf.getBoolean("dfs.namenode.audit.log.async", false)) {
      LOG.warn("Use log4j properties to enable async log for audit logs. "
          + "dfs.namenode.audit.log.async is no longer in use.");
    }
  }

  @VisibleForTesting
  public List<AuditLogger> getAuditLoggers() {
    return auditLoggers;
  }

  @VisibleForTesting
  public RetryCache getRetryCache() {
    return retryCache;
  }

  @VisibleForTesting
  public long getLeaseRecheckIntervalMs() {
    return leaseRecheckIntervalMs;
  }

  @VisibleForTesting
  public long getMaxLockHoldToReleaseLeaseMs() {
    return maxLockHoldToReleaseLeaseMs;
  }

  public int getMaxListOpenFilesResponses() {
    return maxListOpenFilesResponses;
  }

  boolean isSnapshotTrashRootEnabled() {
    return isSnapshotTrashRootEnabled;
  }

  void lockRetryCache() {
    if (retryCache != null) {
      retryCache.lock();
    }
  }

  void unlockRetryCache() {
    if (retryCache != null) {
      retryCache.unlock();
    }
  }

  /** Whether or not retry cache is enabled */
  boolean hasRetryCache() {
    return retryCache != null;
  }
  
  void addCacheEntryWithPayload(byte[] clientId, int callId, Object payload) {
    if (retryCache != null) {
      retryCache.addCacheEntryWithPayload(clientId, callId, payload);
    }
  }
  
  void addCacheEntry(byte[] clientId, int callId) {
    if (retryCache != null) {
      retryCache.addCacheEntry(clientId, callId);
    }
  }

  @VisibleForTesting
  public KeyProviderCryptoExtension getProvider() {
    return provider;
  }

  @VisibleForTesting
  static RetryCache initRetryCache(Configuration conf) {
    boolean enable = conf.getBoolean(DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY,
                                     DFS_NAMENODE_ENABLE_RETRY_CACHE_DEFAULT);
    LOG.info("Retry cache on namenode is " + (enable ? "enabled" : "disabled"));
    if (enable) {
      float heapPercent = conf.getFloat(
          DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_KEY,
          DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_DEFAULT);
      long entryExpiryMillis = conf.getLong(
          DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY,
          DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT);
      LOG.info("Retry cache will use " + heapPercent
          + " of total heap and retry cache entry expiry time is "
          + entryExpiryMillis + " millis");
      long entryExpiryNanos = entryExpiryMillis * 1000 * 1000;
      return new RetryCache("NameNodeRetryCache", heapPercent,
          entryExpiryNanos);
    }
    return null;
  }

  /**
   * Locate DefaultAuditLogger, if any, to enable/disable CallerContext.
   *
   * @param value
   *          true, enable CallerContext, otherwise false to disable it.
   */
  void setCallerContextEnabled(final boolean value) {
    for (AuditLogger logger : auditLoggers) {
      if (logger instanceof DefaultAuditLogger) {
        ((DefaultAuditLogger) logger).setCallerContextEnabled(value);
        break;
      }
    }
  }

  /**
   * Get the value indicating if CallerContext is enabled.
   *
   * @return true, if CallerContext is enabled, otherwise false, if it's
   *         disabled.
   */
  boolean getCallerContextEnabled() {
    for (AuditLogger logger : auditLoggers) {
      if (logger instanceof DefaultAuditLogger) {
        return ((DefaultAuditLogger) logger).getCallerContextEnabled();
      }
    }
    return false;
  }

  private List<AuditLogger> initAuditLoggers(Configuration conf) {
    // Initialize the custom access loggers if configured.
    Collection<String> alClasses =
        conf.getTrimmedStringCollection(DFS_NAMENODE_AUDIT_LOGGERS_KEY);
    List<AuditLogger> auditLoggers = Lists.newArrayList();
    boolean topAuditLoggerAdded = false;
    if (alClasses != null && !alClasses.isEmpty()) {
      for (String className : alClasses) {
        try {
          AuditLogger logger;
          if (DFS_NAMENODE_DEFAULT_AUDIT_LOGGER_NAME.equals(className)) {
            logger = new FSNamesystemAuditLogger();
          } else {
            logger = (AuditLogger) Class.forName(className).newInstance();
            if (TopAuditLogger.class.getName().equals(
                    logger.getClass().getName())) {
              topAuditLoggerAdded = true;
            }
          }
          logger.initialize(conf);
          auditLoggers.add(logger);
        } catch (InstantiationException e) {
          LOG.error("{} instantiation failed.", className, e);
          throw new RuntimeException(e);
        } catch (RuntimeException re) {
          throw re;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    // Make sure there is at least one logger installed.
    if (auditLoggers.isEmpty()) {
      FSNamesystemAuditLogger fsNamesystemAuditLogger = new FSNamesystemAuditLogger();
      fsNamesystemAuditLogger.initialize(conf);
      auditLoggers.add(fsNamesystemAuditLogger);
    }

    // Add audit logger to calculate top users
    if (topConf.isEnabled && !topAuditLoggerAdded) {
      topMetrics = new TopMetrics(conf, topConf.nntopReportingPeriodsMs);
      if (DefaultMetricsSystem.instance().getSource(
          TOPMETRICS_METRICS_SOURCE_NAME) == null) {
        DefaultMetricsSystem.instance().register(TOPMETRICS_METRICS_SOURCE_NAME,
            "Top N operations by user", topMetrics);
      }
      auditLoggers.add(new TopAuditLogger(topMetrics));
    }

    return Collections.unmodifiableList(auditLoggers);
  }

  void loadFSImage(StartupOption startOpt) throws IOException {
    final FSImage fsImage = getFSImage();

    // format before starting up if requested
    if (startOpt == StartupOption.FORMAT) {
      // reuse current id
      fsImage.format(this, fsImage.getStorage().determineClusterId(), false);

      startOpt = StartupOption.REGULAR;
    }
    boolean success = false;
    writeLock();
    try {
      // We shouldn't be calling saveNamespace if we've come up in standby state.
      MetaRecoveryContext recovery = startOpt.createRecoveryContext();
      final boolean staleImage
          = fsImage.recoverTransitionRead(startOpt, this, recovery);
      if (RollingUpgradeStartupOption.ROLLBACK.matches(startOpt)) {
        rollingUpgradeInfo = null;
      }
      final boolean needToSave = staleImage && !haEnabled && !isRollingUpgrade(); 
      LOG.info("Need to save fs image? " + needToSave
          + " (staleImage=" + staleImage + ", haEnabled=" + haEnabled
          + ", isRollingUpgrade=" + isRollingUpgrade() + ")");
      if (needToSave) {
        fsImage.saveNamespace(this);
      } else {
        // No need to save, so mark the phase done.
        StartupProgress prog = NameNode.getStartupProgress();
        prog.beginPhase(Phase.SAVING_CHECKPOINT);
        prog.endPhase(Phase.SAVING_CHECKPOINT);
      }
      // This will start a new log segment and write to the seen_txid file, so
      // we shouldn't do it when coming up in standby state
      if (!haEnabled || (haEnabled && startOpt == StartupOption.UPGRADE)
          || (haEnabled && startOpt == StartupOption.UPGRADEONLY)) {
        fsImage.openEditLogForWrite(getEffectiveLayoutVersion());
      }
      success = true;
    } finally {
      if (!success) {
        fsImage.close();
      }
      writeUnlock("loadFSImage", true);
    }
    imageLoadComplete();
  }

  private void startSecretManager() {
    if (dtSecretManager != null) {
      try {
        dtSecretManager.startThreads();
      } catch (IOException e) {
        // Inability to start secret manager
        // can't be recovered from.
        throw new RuntimeException(e);
      }
    }
  }
  
  @Override
  public void startSecretManagerIfNecessary() {
    assert hasWriteLock() : "Starting secret manager needs write lock";
    boolean shouldRun = shouldUseDelegationTokens() &&
      !isInSafeMode() && getEditLog().isOpenForWrite();
    boolean running = dtSecretManager.isRunning();
    if (shouldRun && !running) {
      startSecretManager();
    }
  }

  private void stopSecretManager() {
    if (dtSecretManager != null) {
      dtSecretManager.stopThreads();
    }
  }
  
  /** 
   * Start services common to both active and standby states
   */
  void startCommonServices(Configuration conf, HAContext haContext) throws IOException {
    this.registerMBean(); // register the MBean for the FSNamesystemState
    writeLock();
    this.haContext = haContext;
    try {
      nnResourceChecker = new NameNodeResourceChecker(conf);
      checkAvailableResources();
      assert !blockManager.isPopulatingReplQueues();
      StartupProgress prog = NameNode.getStartupProgress();
      prog.beginPhase(Phase.SAFEMODE);
      long completeBlocksTotal = getCompleteBlocksTotal();
      prog.setTotal(Phase.SAFEMODE, STEP_AWAITING_REPORTED_BLOCKS,
          completeBlocksTotal);
      blockManager.activate(conf, completeBlocksTotal);
    } finally {
      writeUnlock("startCommonServices");
    }
    
    registerMXBean();
    DefaultMetricsSystem.instance().register(this);
    if (inodeAttributeProvider != null) {
      inodeAttributeProvider.start();
      dir.setINodeAttributeProvider(inodeAttributeProvider);
    }
    snapshotManager.registerMXBean();
    InetSocketAddress serviceAddress = NameNode.getServiceAddress(conf, true);
    this.nameNodeHostName = (serviceAddress != null) ?
        serviceAddress.getHostName() : "";
  }
  
  /** 
   * Stop services common to both active and standby states
   */
  void stopCommonServices() {
    writeLock();
    if (inodeAttributeProvider != null) {
      dir.setINodeAttributeProvider(null);
      inodeAttributeProvider.stop();
    }
    try {
      if (blockManager != null) blockManager.close();
    } finally {
      writeUnlock("stopCommonServices");
    }
    RetryCache.clear(retryCache);
  }
  
  /**
   * Start services required in active state
   * @throws IOException
   */
  void startActiveServices() throws IOException {
    startingActiveService = true;
    LOG.info("Starting services required for active state");
    writeLock();
    try {
      FSEditLog editLog = getFSImage().getEditLog();
      
      if (!editLog.isOpenForWrite()) {
        // During startup, we're already open for write during initialization.
        editLog.initJournalsForWrite();
        // May need to recover
        editLog.recoverUnclosedStreams(true);
        
        LOG.info("Catching up to latest edits from old active before " +
            "taking over writer role in edits logs");
        editLogTailer.catchupDuringFailover();
        
        blockManager.setPostponeBlocksFromFuture(false);
        blockManager.getDatanodeManager().markAllDatanodesStale();
        blockManager.clearQueues();
        blockManager.processAllPendingDNMessages();
        blockManager.getBlockIdManager().applyImpendingGenerationStamp();

        // Only need to re-process the queue, If not in SafeMode.
        if (!isInSafeMode()) {
          LOG.info("Reprocessing replication and invalidation queues");
          blockManager.initializeReplQueues();
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("NameNode metadata after re-processing {}"
              + "replication and invalidation queues during failover:\n",
              metaSaveAsString());
        }

        long nextTxId = getFSImage().getLastAppliedTxId() + 1;
        LOG.info("Will take over writing edit logs at txnid " + 
            nextTxId);
        editLog.setNextTxId(nextTxId);

        getFSImage().editLog.openForWrite(getEffectiveLayoutVersion());
      }

      // Initialize the quota.
      dir.updateCountForQuota();
      // Enable quota checks.
      dir.enableQuotaChecks();
      dir.ezManager.startReencryptThreads();

      if (snapshotDeletionGc != null) {
        snapshotDeletionGc.schedule();
      }

      if (haEnabled) {
        // Renew all of the leases before becoming active.
        // This is because, while we were in standby mode,
        // the leases weren't getting renewed on this NN.
        // Give them all a fresh start here.
        leaseManager.renewAllLeases();
      }
      leaseManager.startMonitor();
      startSecretManagerIfNecessary();

      //ResourceMonitor required only at ActiveNN. See HDFS-2914
      this.nnrmthread = new Daemon(new NameNodeResourceMonitor());
      nnrmthread.start();

      nnEditLogRoller = new Daemon(new NameNodeEditLogRoller(
          editLogRollerThreshold, editLogRollerInterval));
      nnEditLogRoller.start();

      if (lazyPersistFileScrubIntervalSec > 0) {
        lazyPersistFileScrubber = new Daemon(new LazyPersistFileScrubber(
            lazyPersistFileScrubIntervalSec));
        lazyPersistFileScrubber.start();
      } else {
        LOG.warn("Lazy persist file scrubber is disabled,"
            + " configured scrub interval is zero.");
      }

      cacheManager.startMonitorThread();
      blockManager.getDatanodeManager().setShouldSendCachingCommands(true);
      if (provider != null) {
        edekCacheLoader = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("Warm Up EDEK Cache Thread #%d")
                .build());
        FSDirEncryptionZoneOp.warmUpEdekCache(edekCacheLoader, dir,
            edekCacheLoaderDelay, edekCacheLoaderInterval);
      }
      if (blockManager.getSPSManager() != null) {
        blockManager.getSPSManager().start();
      }
    } finally {
      startingActiveService = false;
      blockManager.checkSafeMode();
      writeUnlock("startActiveServices");
    }
  }

  private boolean inActiveState() {
    return haContext != null &&
        haContext.getState().getServiceState() == HAServiceState.ACTIVE;
  }

  @Override
  public boolean inTransitionToActive() {
    return haEnabled && inActiveState() && startingActiveService;
  }

  private boolean shouldUseDelegationTokens() {
    return UserGroupInformation.isSecurityEnabled() ||
      alwaysUseDelegationTokensForTests;
  }

  /** 
   * Stop services required in active state
   */
  void stopActiveServices() {
    LOG.info("Stopping services started for active state");
    writeLock();
    try {
      if (blockManager != null) {
        blockManager.stopReconstructionInitializer();
        if (blockManager.getSPSManager() != null) {
          blockManager.getSPSManager().stop();
        }
      }
      stopSecretManager();
      leaseManager.stopMonitor();
      if (nnrmthread != null) {
        ((NameNodeResourceMonitor) nnrmthread.getRunnable()).stopMonitor();
        nnrmthread.interrupt();
      }
      if (edekCacheLoader != null) {
        edekCacheLoader.shutdownNow();
      }
      if (nnEditLogRoller != null) {
        ((NameNodeEditLogRoller)nnEditLogRoller.getRunnable()).stop();
        nnEditLogRoller.interrupt();
      }
      if (lazyPersistFileScrubber != null) {
        ((LazyPersistFileScrubber) lazyPersistFileScrubber.getRunnable()).stop();
        lazyPersistFileScrubber.interrupt();
      }
      if (dir != null && getFSImage() != null) {
        if (getFSImage().editLog != null) {
          getFSImage().editLog.close();
        }
        // Update the fsimage with the last txid that we wrote
        // so that the tailer starts from the right spot.
        getFSImage().updateLastAppliedTxIdFromWritten();
      }
      if (dir != null) {
        dir.ezManager.stopReencryptThread();
      }
      if (cacheManager != null) {
        cacheManager.stopMonitorThread();
        cacheManager.clearDirectiveStats();
      }
      if (blockManager != null) {
        blockManager.getDatanodeManager().clearPendingCachingCommands();
        blockManager.getDatanodeManager().setShouldSendCachingCommands(false);
        // Don't want to keep replication queues when not in Active.
        blockManager.clearQueues();
        blockManager.setInitializedReplQueues(false);
      }
    } finally {
      writeUnlock("stopActiveServices");
    }
  }
  
  /**
   * Start services required in standby or observer state
   * 
   * @throws IOException
   */
  void startStandbyServices(final Configuration conf, boolean isObserver)
      throws IOException {
    LOG.info("Starting services required for " +
        (isObserver ? "observer" : "standby") + " state");
    if (!getFSImage().editLog.isOpenForRead()) {
      // During startup, we're already open for read.
      getFSImage().editLog.initSharedJournalsForRead();
    }
    blockManager.setPostponeBlocksFromFuture(true);

    // Disable quota checks while in standby.
    dir.disableQuotaChecks();
    editLogTailer = new EditLogTailer(this, conf);
    editLogTailer.start();
    if (!isObserver && standbyShouldCheckpoint) {
      standbyCheckpointer = new StandbyCheckpointer(conf, this);
      standbyCheckpointer.start();
    }
  }

  /**
   * Called when the NN is in Standby state and the editlog tailer tails the
   * OP_ROLLING_UPGRADE_START.
   */
  void triggerRollbackCheckpoint() {
    setNeedRollbackFsImage(true);
    if (standbyCheckpointer != null) {
      standbyCheckpointer.triggerRollbackCheckpoint();
    }
  }

  /**
   * Called while the NN is in Standby state, but just about to be
   * asked to enter Active state. This cancels any checkpoints
   * currently being taken.
   */
  void prepareToStopStandbyServices() throws ServiceFailedException {
    if (standbyCheckpointer != null) {
      standbyCheckpointer.cancelAndPreventCheckpoints(
          "About to leave standby state");
    }
  }

  /** Stop services required in standby state */
  void stopStandbyServices() throws IOException {
    HAServiceState curState = getState() == OBSERVER? OBSERVER : STANDBY;
    LOG.info("Stopping services started for {} state", curState);
    if (standbyCheckpointer != null) {
      standbyCheckpointer.stop();
    }
    if (editLogTailer != null) {
      editLogTailer.stop();
    }
    if (dir != null && getFSImage() != null && getFSImage().editLog != null) {
      getFSImage().editLog.close();
    }
  }

  public void checkOperation(OperationCategory op) throws StandbyException {
    if (haContext != null) {
      // null in some unit tests
      haContext.checkOperation(op);
    }

    boolean assertsEnabled = false;
    assert assertsEnabled = true; // Intentional side effect!!!
    if (assertsEnabled && op == OperationCategory.WRITE) {
      getSnapshotManager().initThreadLocals();
    }
  }
  
  /**
   * @throws RetriableException
   *           If 1) The NameNode is in SafeMode, 2) HA is enabled, and 3)
   *           NameNode is in active state
   * @throws SafeModeException
   *           Otherwise if NameNode is in SafeMode.
   */
  void checkNameNodeSafeMode(String errorMsg)
      throws RetriableException, SafeModeException {
    if (isInSafeMode()) {
      SafeModeException se = newSafemodeException(errorMsg);
      if (haEnabled && haContext != null
          && haContext.getState().getServiceState() == HAServiceState.ACTIVE
          && isInStartupSafeMode()) {
        throw new RetriableException(se);
      } else {
        throw se;
      }
    }
  }

  private SafeModeException newSafemodeException(String errorMsg) {
    return new SafeModeException(errorMsg + ". Name node is in safe " +
        "mode.\n" + getSafeModeTip() + " NamenodeHostName:" + nameNodeHostName);
  }

  boolean isPermissionEnabled() {
    return isPermissionEnabled;
  }

  public static Collection<URI> getNamespaceDirs(Configuration conf) {
    return getStorageDirs(conf, DFS_NAMENODE_NAME_DIR_KEY);
  }

  /**
   * Get all edits dirs which are required. If any shared edits dirs are
   * configured, these are also included in the set of required dirs.
   * 
   * @param conf the HDFS configuration.
   * @return all required dirs.
   */
  public static Collection<URI> getRequiredNamespaceEditsDirs(Configuration conf) {
    Set<URI> ret = new HashSet<URI>();
    ret.addAll(getStorageDirs(conf, DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY));
    ret.addAll(getSharedEditsDirs(conf));
    return ret;
  }

  private static Collection<URI> getStorageDirs(Configuration conf,
                                                String propertyName) {
    Collection<String> dirNames = conf.getTrimmedStringCollection(propertyName);
    StartupOption startOpt = NameNode.getStartupOption(conf);
    if(startOpt == StartupOption.IMPORT) {
      // In case of IMPORT this will get rid of default directories 
      // but will retain directories specified in hdfs-site.xml
      // When importing image from a checkpoint, the name-node can
      // start with empty set of storage directories.
      Configuration cE = new HdfsConfiguration(false);
      cE.addResource("core-default.xml");
      cE.addResource("core-site.xml");
      cE.addResource("hdfs-default.xml");
      Collection<String> dirNames2 = cE.getTrimmedStringCollection(propertyName);
      dirNames.removeAll(dirNames2);
      if(dirNames.isEmpty())
        LOG.warn("!!! WARNING !!!" +
          "\n\tThe NameNode currently runs without persistent storage." +
          "\n\tAny changes to the file system meta-data may be lost." +
          "\n\tRecommended actions:" +
          "\n\t\t- shutdown and restart NameNode with configured \"" 
          + propertyName + "\" in hdfs-site.xml;" +
          "\n\t\t- use Backup Node as a persistent and up-to-date storage " +
          "of the file system meta-data.");
    } else if (dirNames.isEmpty()) {
      dirNames = Collections.singletonList(
          DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_DEFAULT);
    }
    return Util.stringCollectionAsURIs(dirNames);
  }

  /**
   * Return an ordered list of edits directories to write to.
   * The list is ordered such that all shared edits directories
   * are ordered before non-shared directories, and any duplicates
   * are removed. The order they are specified in the configuration
   * is retained.
   * @return Collection of shared edits directories.
   * @throws IOException if multiple shared edits directories are configured
   */
  public static List<URI> getNamespaceEditsDirs(Configuration conf)
      throws IOException {
    return getNamespaceEditsDirs(conf, true);
  }
  
  public static List<URI> getNamespaceEditsDirs(Configuration conf,
      boolean includeShared)
      throws IOException {
    // Use a LinkedHashSet so that order is maintained while we de-dup
    // the entries.
    LinkedHashSet<URI> editsDirs = new LinkedHashSet<URI>();
    
    if (includeShared) {
      List<URI> sharedDirs = getSharedEditsDirs(conf);
  
      // Fail until multiple shared edits directories are supported (HDFS-2782)
      if (sharedDirs.size() > 1) {
        throw new IOException(
            "Multiple shared edits directories are not yet supported");
      }
  
      // First add the shared edits dirs. It's critical that the shared dirs
      // are added first, since JournalSet syncs them in the order they are listed,
      // and we need to make sure all edits are in place in the shared storage
      // before they are replicated locally. See HDFS-2874.
      for (URI dir : sharedDirs) {
        if (!editsDirs.add(dir)) {
          LOG.warn("Edits URI " + dir + " listed multiple times in " + 
              DFS_NAMENODE_SHARED_EDITS_DIR_KEY + ". Ignoring duplicates.");
        }
      }
    }    
    // Now add the non-shared dirs.
    for (URI dir : getStorageDirs(conf, DFS_NAMENODE_EDITS_DIR_KEY)) {
      if (!editsDirs.add(dir)) {
        LOG.warn("Edits URI " + dir + " listed multiple times in " + 
            DFS_NAMENODE_SHARED_EDITS_DIR_KEY + " and " +
            DFS_NAMENODE_EDITS_DIR_KEY + ". Ignoring duplicates.");
      }
    }

    if (editsDirs.isEmpty()) {
      // If this is the case, no edit dirs have been explicitly configured.
      // Image dirs are to be used for edits too.
      return Lists.newArrayList(getNamespaceDirs(conf));
    } else {
      return Lists.newArrayList(editsDirs);
    }
  }
  
  /**
   * Returns edit directories that are shared between primary and secondary.
   * @param conf configuration
   * @return collection of edit directories from {@code conf}
   */
  public static List<URI> getSharedEditsDirs(Configuration conf) {
    // don't use getStorageDirs here, because we want an empty default
    // rather than the dir in /tmp
    Collection<String> dirNames = conf.getTrimmedStringCollection(
        DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
    return Util.stringCollectionAsURIs(dirNames);
  }

  @Override
  public void readLock() {
    this.fsLock.readLock();
  }

  @Override
  public void readLockInterruptibly() throws InterruptedException {
    this.fsLock.readLockInterruptibly();
  }

  @Override
  public void readUnlock() {
    this.fsLock.readUnlock();
  }

  @Override
  public void readUnlock(String opName) {
    this.fsLock.readUnlock(opName);
  }

  public void readUnlock(String opName,
      Supplier<String> lockReportInfoSupplier) {
    this.fsLock.readUnlock(opName, lockReportInfoSupplier);
  }

  @Override
  public void writeLock() {
    this.fsLock.writeLock();
  }

  @Override
  public void writeLockInterruptibly() throws InterruptedException {
    this.fsLock.writeLockInterruptibly();
  }

  @Override
  public void writeUnlock() {
    this.fsLock.writeUnlock();
  }

  @Override
  public void writeUnlock(String opName) {
    this.fsLock.writeUnlock(opName);
  }

  public void writeUnlock(String opName, boolean suppressWriteLockReport) {
    this.fsLock.writeUnlock(opName, suppressWriteLockReport);
  }

  public void writeUnlock(String opName,
      Supplier<String> lockReportInfoSupplier) {
    this.fsLock.writeUnlock(opName, lockReportInfoSupplier);
  }

  @Override
  public boolean hasWriteLock() {
    return this.fsLock.isWriteLockedByCurrentThread();
  }
  @Override
  public boolean hasReadLock() {
    return this.fsLock.getReadHoldCount() > 0 || hasWriteLock();
  }

  public int getReadHoldCount() {
    return this.fsLock.getReadHoldCount();
  }

  public int getWriteHoldCount() {
    return this.fsLock.getWriteHoldCount();
  }

  /** Lock the checkpoint lock */
  public void cpLock() {
    this.cpLock.lock();
  }

  /** Lock the checkpoint lock interrupibly */
  public void cpLockInterruptibly() throws InterruptedException {
    this.cpLock.lockInterruptibly();
  }

  /** Unlock the checkpoint lock */
  public void cpUnlock() {
    this.cpLock.unlock();
  }
    

  NamespaceInfo getNamespaceInfo() {
    readLock();
    try {
      return unprotectedGetNamespaceInfo();
    } finally {
      readUnlock("getNamespaceInfo");
    }
  }

  /**
   * Get the creation time of the file system.
   * Notice that this time is initialized to NameNode format time, and updated
   * to upgrade time during upgrades.
   * @return time in milliseconds.
   * See {@link org.apache.hadoop.util.Time#now()}.
   */
  @VisibleForTesting
  long getCTime() {
    return fsImage == null ? 0 : fsImage.getStorage().getCTime();
  }

  /**
   * Version of @see #getNamespaceInfo() that is not protected by a lock.
   */
  NamespaceInfo unprotectedGetNamespaceInfo() {
    return new NamespaceInfo(getFSImage().getStorage().getNamespaceID(),
        getClusterId(), getBlockPoolId(),
        getFSImage().getStorage().getCTime(), getState());
  }

  /**
   * Close down this file system manager.
   * Causes heartbeat and lease daemons to stop; waits briefly for
   * them to finish, but a short timeout returns control back to caller.
   */
  void close() {
    fsRunning = false;
    try {
      stopCommonServices();
    } finally {
      // using finally to ensure we also wait for lease daemon
      try {
        stopActiveServices();
        stopStandbyServices();
      } catch (IOException ie) {
      } finally {
        IOUtils.cleanupWithLogger(LOG, dir);
        IOUtils.cleanupWithLogger(LOG, fsImage);
      }
    }
  }

  @Override
  public boolean isRunning() {
    return fsRunning;
  }

  public boolean isInStandbyState() {
    if (haContext == null || haContext.getState() == null) {
      // We're still starting up. In this case, if HA is
      // on for the cluster, we always start in standby. Otherwise
      // start in active.
      return haEnabled;
    }

    return HAServiceState.STANDBY == haContext.getState().getServiceState() ||
        HAServiceState.OBSERVER == haContext.getState().getServiceState();
  }

  /**
   * return a list of blocks &amp; their locations on {@code datanode} whose
   * total size is {@code size}
   *
   * @param datanode on which blocks are located
   * @param size total size of blocks
   * @param minimumBlockSize
   */
  public BlocksWithLocations getBlocks(DatanodeID datanode, long size, long
      minimumBlockSize, long timeInterval) throws IOException {
    OperationCategory checkOp =
        isGetBlocksCheckOperationEnabled ? OperationCategory.READ :
            OperationCategory.UNCHECKED;
    checkOperation(checkOp);
    readLock();
    try {
      checkOperation(checkOp);
      return getBlockManager().getBlocksWithLocations(datanode, size,
          minimumBlockSize, timeInterval);
    } finally {
      readUnlock("getBlocks");
    }
  }

  /**
   * Dump all metadata into specified file
   * @param filename
   */
  void metaSave(String filename) throws IOException {
    String operationName = "metaSave";
    checkSuperuserPrivilege(operationName);
    checkOperation(OperationCategory.READ);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      synchronized(metaSaveLock) {
        File file = new File(System.getProperty("hadoop.log.dir"), filename);
        PrintWriter out = new PrintWriter(new BufferedWriter(
                new OutputStreamWriter(Files.newOutputStream(file.toPath()),
                        Charsets.UTF_8)));
        metaSave(out);
        out.flush();
        out.close();
      }
    } finally {
      readUnlock(operationName, getLockReportInfoSupplier(null));
    }
    logAuditEvent(true, operationName, null);
  }

  private void metaSave(PrintWriter out) {
    assert hasReadLock();
    long totalInodes = this.dir.totalInodes();
    long totalBlocks = this.getBlocksTotal();
    out.println(totalInodes + " files and directories, " + totalBlocks
        + " blocks = " + (totalInodes + totalBlocks)
        + " total filesystem objects");

    blockManager.metaSave(out);
  }

  /**
   * List open files in the system in batches. prevId is the cursor INode id and
   * the open files returned in a batch will have their INode ids greater than
   * this cursor. Open files can only be requested by super user and the the
   * list across batches does not represent a consistent view of all open files.
   * TODO: HDFS-12969 - to report open files by type.
   *
   * @param prevId the cursor INode id.
   * @param openFilesTypes types to filter the open files.
   * @param path path to filter the open files.
   * @throws IOException
   */
  BatchedListEntries<OpenFileEntry> listOpenFiles(long prevId,
      EnumSet<OpenFilesType> openFilesTypes, String path) throws IOException {
    INode.checkAbsolutePath(path);
    final String operationName = "listOpenFiles";
    checkSuperuserPrivilege(operationName, path);
    checkOperation(OperationCategory.READ);
    BatchedListEntries<OpenFileEntry> batchedListEntries;
    String normalizedPath = new Path(path).toString(); // normalize path.
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        if (openFilesTypes.contains(OpenFilesType.ALL_OPEN_FILES)) {
          batchedListEntries = leaseManager.getUnderConstructionFiles(prevId,
              normalizedPath);
        } else {
          if (openFilesTypes.contains(OpenFilesType.BLOCKING_DECOMMISSION)) {
            batchedListEntries = getFilesBlockingDecom(prevId, normalizedPath);
          } else {
            throw new IllegalArgumentException("Unknown OpenFileType: "
                + openFilesTypes);
          }
        }
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(null));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, null);
      throw e;
    }
    logAuditEvent(true, operationName, null);
    return batchedListEntries;
  }

  public BatchedListEntries<OpenFileEntry> getFilesBlockingDecom(long prevId,
      String path) {
    assert hasReadLock();
    final List<OpenFileEntry> openFileEntries = Lists.newArrayList();
    LightWeightHashSet<Long> openFileIds = new LightWeightHashSet<>();
    for (DatanodeDescriptor dataNode :
        blockManager.getDatanodeManager().getDatanodes()) {
      // Sort open files
      LightWeightHashSet<Long> dnOpenFiles =
          dataNode.getLeavingServiceStatus().getOpenFiles();
      Long[] dnOpenFileIds = new Long[dnOpenFiles.size()];
      Arrays.sort(dnOpenFiles.toArray(dnOpenFileIds));
      for (Long ucFileId : dnOpenFileIds) {
        INode ucFile = getFSDirectory().getInode(ucFileId);
        if (ucFile == null || ucFileId <= prevId ||
            openFileIds.contains(ucFileId)) {
          // probably got deleted or
          // part of previous batch or
          // already part of the current batch
          continue;
        }
        Preconditions.checkState(ucFile instanceof INodeFile);

        INodeFile inodeFile = ucFile.asFile();
        if (!inodeFile.isUnderConstruction()) {
          LOG.warn("The file {} is not under construction but has lease.",
              inodeFile.getFullPathName());
          continue;
        }
        openFileIds.add(ucFileId);

        String fullPathName = inodeFile.getFullPathName();
        if (org.apache.commons.lang3.StringUtils.isEmpty(path)
            || DFSUtil.isParentEntry(fullPathName, path)) {
          openFileEntries.add(new OpenFileEntry(inodeFile.getId(),
              inodeFile.getFullPathName(),
              inodeFile.getFileUnderConstructionFeature().getClientName(),
              inodeFile.getFileUnderConstructionFeature().getClientMachine()));
        }

        if (openFileIds.size() >= this.maxListOpenFilesResponses) {
          return new BatchedListEntries<>(openFileEntries, true);
        }
      }
    }
    return new BatchedListEntries<>(openFileEntries, false);
  }

  private String metaSaveAsString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    metaSave(pw);
    pw.flush();
    return sw.toString();
  }

  @VisibleForTesting
  public FsServerDefaults getServerDefaults() throws StandbyException {
    checkOperation(OperationCategory.READ);
    return serverDefaults;
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by HadoopFS clients
  //
  /////////////////////////////////////////////////////////
  /**
   * Set permissions for an existing file.
   * @param src
   * @param permission
   * @throws IOException
   */
  void setPermission(String src, FsPermission permission) throws IOException {
    final String operationName = "setPermission";
    FileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot set permission for " + src);
        auditStat = FSDirAttrOp.setPermission(dir, pc, src, permission);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, auditStat);
  }

  /**
   * Set owner for an existing file.
   * @param src
   * @param group
   * @param username
   * @throws IOException
   */
  void setOwner(String src, String username, String group)
      throws IOException {
    final String operationName = "setOwner";
    FileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot set owner for " + src);
        auditStat = FSDirAttrOp.setOwner(dir, pc, src, username, group);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, auditStat);
  }

  /**
   * Get block locations within the specified range.
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  LocatedBlocks getBlockLocations(String clientMachine, String srcArg,
      long offset, long length) throws IOException {
    final String operationName = "open";
    checkOperation(OperationCategory.READ);
    GetBlockLocationsResult res = null;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    final INode inode;
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        res = FSDirStatAndListingOp.getBlockLocations(
            dir, pc, srcArg, offset, length, true);
        inode = res.getIIp().getLastINode();
        if (isInSafeMode()) {
          for (LocatedBlock b : res.blocks.getLocatedBlocks()) {
            // if safemode & no block locations yet then throw safemodeException
            if ((b.getLocations() == null) || (b.getLocations().length == 0)) {
              SafeModeException se = newSafemodeException(
                  "Zero blocklocations for " + srcArg);
              if (haEnabled && haContext != null &&
                  (haContext.getState().getServiceState() == ACTIVE ||
                      haContext.getState().getServiceState() == OBSERVER)) {
                throw new RetriableException(se);
              } else {
                throw se;
              }
            }
          }
        } else if (isObserver()) {
          checkBlockLocationsWhenObserver(res.blocks, srcArg);
        }
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(srcArg));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, srcArg);
      throw e;
    }

    logAuditEvent(true, operationName, srcArg);

    if (!isInSafeMode() && res.updateAccessTime()) {
      String src = srcArg;
      checkOperation(OperationCategory.WRITE);
      try {
        writeLock();
        final long now = now();
        try {
          checkOperation(OperationCategory.WRITE);
          boolean updateAccessTime =
              now > inode.getAccessTime() + dir.getAccessTimePrecision();
          if (!isInSafeMode() && updateAccessTime) {
            if (!inode.isDeleted()) {
              src = inode.getFullPathName();
              final INodesInPath iip = dir.resolvePath(pc, src, DirOp.READ);
              boolean changed = FSDirAttrOp.setTimes(dir, iip, -1, now, false);
              if (changed) {
                getEditLog().logTimes(src, -1, now);
              }
            }
          }
        } finally {
          writeUnlock(operationName, getLockReportInfoSupplier(srcArg));
        }
      } catch (Throwable e) {
        LOG.warn("Failed to update the access time of " + src, e);
      }
    }

    LocatedBlocks blocks = res.blocks;
    sortLocatedBlocks(clientMachine, blocks);
    return blocks;
  }

  private void sortLocatedBlocks(String clientMachine, LocatedBlocks blocks) {
    if (blocks != null) {
      List<LocatedBlock> blkList = blocks.getLocatedBlocks();
      if (blkList == null || blkList.size() == 0) {
        // simply return, block list is empty
        return;
      }
      blockManager.getDatanodeManager().sortLocatedBlocks(clientMachine,
          blkList);

      // lastBlock is not part of getLocatedBlocks(), might need to sort it too
      LocatedBlock lastBlock = blocks.getLastLocatedBlock();
      if (lastBlock != null) {
        ArrayList<LocatedBlock> lastBlockList = Lists.newArrayList(lastBlock);
        blockManager.getDatanodeManager().sortLocatedBlocks(clientMachine,
            lastBlockList);
      }
    }
  }

  /**
   * Moves all the blocks from {@code srcs} and appends them to {@code target}
   * To avoid rollbacks we will verify validity of ALL of the args
   * before we start actual move.
   * 
   * This does not support ".inodes" relative path
   * @param target target to concat into
   * @param srcs file that will be concatenated
   * @throws IOException on error
   */
  void concat(String target, String [] srcs, boolean logRetryCache)
      throws IOException {
    final String operationName = "concat";
    FileStatus stat = null;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    checkOperation(OperationCategory.WRITE);
    String srcsStr = Arrays.toString(srcs);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot concat " + target);
        stat = FSDirConcatOp.concat(dir, pc, target, srcs, logRetryCache);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(srcsStr, target, stat));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, srcsStr, target, stat);
      throw ace;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, srcsStr, target, stat);
  }

  /**
   * stores the modification and access time for this inode. 
   * The access time is precise up to an hour. The transaction, if needed, is
   * written to the edits log but is not flushed.
   */
  void setTimes(String src, long mtime, long atime) throws IOException {
    final String operationName = "setTimes";
    FileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot set times " + src);
        auditStat = FSDirAttrOp.setTimes(dir, pc, src, mtime, atime);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, auditStat);
  }

  /**
   * Truncate file to a lower length.
   * Truncate cannot be reverted / recovered from as it causes data loss.
   * Truncation at block boundary is atomic, otherwise it requires
   * block recovery to truncate the last block of the file.
   *
   * @return true if client does not need to wait for block recovery,
   *         false if client needs to wait for block recovery.
   */
  boolean truncate(String src, long newLength, String clientName,
      String clientMachine, long mtime) throws IOException,
      UnresolvedLinkException {

    final String operationName = "truncate";
    requireEffectiveLayoutVersionForFeature(Feature.TRUNCATE);
    FSDirTruncateOp.TruncateResult r = null;
    FileStatus status;
    try {
      NameNode.stateChangeLog.info(
          "DIR* NameSystem.truncate: src={} newLength={}", src, newLength);
      if (newLength < 0) {
        throw new HadoopIllegalArgumentException(
            "Cannot truncate to a negative file size: " + newLength + ".");
      }
      checkOperation(OperationCategory.WRITE);
      final FSPermissionChecker pc = getPermissionChecker();
      FSPermissionChecker.setOperationType(operationName);
      writeLock();
      BlocksMapUpdateInfo toRemoveBlocks = new BlocksMapUpdateInfo();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot truncate for " + src);
        r = FSDirTruncateOp.truncate(this, src, newLength, clientName,
            clientMachine, mtime, toRemoveBlocks, pc);
      } finally {
        status = r != null ? r.getFileStatus() : null;
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, status));
      }
      getEditLog().logSync();
      if (!toRemoveBlocks.getToDeleteList().isEmpty()) {
        blockManager.addBLocksToMarkedDeleteQueue(
            toRemoveBlocks.getToDeleteList());
      }
      logAuditEvent(true, operationName, src, null, status);
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    assert(r != null);
    return r.getResult();
  }

  /**
   * Create a symbolic link.
   */
  void createSymlink(String target, String link,
      PermissionStatus dirPerms, boolean createParent, boolean logRetryCache)
      throws IOException {
    final String operationName = "createSymlink";
    if (!FileSystem.areSymlinksEnabled()) {
      throw new UnsupportedOperationException("Symlinks not supported");
    }
    FileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot create symlink " + link);
        auditStat = FSDirSymlinkOp.createSymlinkInt(this, target, link,
            dirPerms, createParent, logRetryCache);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(link, target, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, link, target, null);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, link, target, auditStat);
  }

  /**
   * Set replication for an existing file.
   * 
   * The NameNode sets new replication and schedules either replication of 
   * under-replicated data blocks or removal of the excessive block copies 
   * if the blocks are over-replicated.
   * 
   * @see ClientProtocol#setReplication(String, short)
   * @param src file name
   * @param replication new replication
   * @return true if successful; 
   *         false if file does not exist or is a directory
   * @throws  IOException
   */
  boolean setReplication(final String src, final short replication)
      throws IOException {
    final String operationName = "setReplication";
    boolean success = false;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot set replication for " + src);
        success = FSDirAttrOp.setReplication(dir, pc, blockManager, src,
            replication);
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(src));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    if (success) {
      getEditLog().logSync();
      logAuditEvent(true, operationName, src);
    }
    return success;
  }

  /**
   * Verify storage policies are enabled and if only super user is allowed to
   * set storage policies.
   *
   * @param operationNameReadable Name of storage policy for exception text
   * @param checkSuperUser Whether to check for super user privilege
   * @throws IOException
   */
  private void checkStoragePolicyEnabled(final String operationNameReadable,
      boolean checkSuperUser) throws IOException {
    if (!isStoragePolicyEnabled) {
      throw new IOException(String.format(
          "Failed to %s since %s is set to false.", operationNameReadable,
          DFS_STORAGE_POLICY_ENABLED_KEY));
    }
    if (checkSuperUser && isStoragePolicySuperuserOnly) {
      checkSuperuserPrivilege(
          CaseUtils.toCamelCase(operationNameReadable, false));
    }
  }

  /**
   * Set the storage policy for a file or a directory.
   *
   * @param src file/directory path
   * @param policyName storage policy name
   * @throws  IOException
   */
  void setStoragePolicy(String src, String policyName) throws IOException {
    if (policyName.equalsIgnoreCase(
            HdfsConstants.ALLNVDIMM_STORAGE_POLICY_NAME)) {
      requireEffectiveLayoutVersionForFeature(Feature.NVDIMM_SUPPORT);
    }
    final String operationName = "setStoragePolicy";
    checkOperation(OperationCategory.WRITE);
    checkStoragePolicyEnabled("set storage policy", true);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    FileStatus auditStat = null;
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot set storage policy for " + src);
        auditStat = FSDirAttrOp.setStoragePolicy(dir, pc, blockManager, src,
            policyName);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, auditStat);
  }

  /**
   * Satisfy the storage policy for a file or a directory.
   *
   * @param src file/directory path
   * @throws  IOException
   */
  void satisfyStoragePolicy(String src, boolean logRetryCache)
      throws IOException {
    final String operationName = "satisfyStoragePolicy";
    checkOperation(OperationCategory.WRITE);
    // make sure storage policy is enabled, otherwise
    // there is no need to satisfy storage policy.
    checkStoragePolicyEnabled("satisfy storage policy", false);
    FileStatus auditStat = null;
    validateStoragePolicySatisfy();
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot satisfy storage policy for " + src);
        auditStat = FSDirSatisfyStoragePolicyOp.satisfyStoragePolicy(
            dir, blockManager, src, logRetryCache);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, auditStat);
  }

  private void validateStoragePolicySatisfy()
      throws UnsupportedActionException, IOException {
    // checks sps status
    boolean disabled = (blockManager.getSPSManager() == null);
    if (disabled) {
      throw new UnsupportedActionException(
          "Cannot request to satisfy storage policy "
              + "when storage policy satisfier feature has been disabled"
              + " by admin. Seek for an admin help to enable it "
              + "or use Mover tool.");
    }
    // checks SPS Q has many outstanding requests. It will throw IOException if
    // the limit exceeds.
    blockManager.getSPSManager().verifyOutstandingPathQLimit();
  }

  /**
   * unset storage policy set for a given file or a directory.
   *
   * @param src file/directory path
   * @throws  IOException
   */
  void unsetStoragePolicy(String src) throws IOException {
    final String operationName = "unsetStoragePolicy";
    checkOperation(OperationCategory.WRITE);
    checkStoragePolicyEnabled("unset storage policy", true);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    FileStatus auditStat = null;
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot unset storage policy for " + src);
        auditStat = FSDirAttrOp.unsetStoragePolicy(dir, pc, blockManager, src);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, auditStat);
  }
  /**
   * Get the storage policy for a file or a directory.
   *
   * @param src
   *          file/directory path
   * @return storage policy object
   * @throws  IOException
   */
  BlockStoragePolicy getStoragePolicy(String src) throws IOException {
    checkOperation(OperationCategory.READ);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(null);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      return FSDirAttrOp.getStoragePolicy(dir, pc, blockManager, src);
    } finally {
      readUnlock("getStoragePolicy");
    }
  }

  /**
   * @return All the existing block storage policies
   * @throws  IOException
   */
  BlockStoragePolicy[] getStoragePolicies() throws IOException {
    checkOperation(OperationCategory.READ);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      return FSDirAttrOp.getStoragePolicies(blockManager);
    } finally {
      readUnlock("getStoragePolicies");
    }
  }

  long getPreferredBlockSize(String src) throws IOException {
    checkOperation(OperationCategory.READ);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(null);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      return FSDirAttrOp.getPreferredBlockSize(dir, pc, src);
    } finally {
      readUnlock("getPreferredBlockSize");
    }
  }

  /**
   * If the file is within an encryption zone, select the appropriate 
   * CryptoProtocolVersion from the list provided by the client. Since the
   * client may be newer, we need to handle unknown versions.
   *
   * @param zone EncryptionZone of the file
   * @param supportedVersions List of supported protocol versions
   * @return chosen protocol version
   * @throws IOException
   */
  CryptoProtocolVersion chooseProtocolVersion(
      EncryptionZone zone, CryptoProtocolVersion[] supportedVersions)
      throws UnknownCryptoProtocolVersionException, UnresolvedLinkException,
        SnapshotAccessControlException {
    Preconditions.checkNotNull(zone);
    Preconditions.checkNotNull(supportedVersions);
    // Right now, we only support a single protocol version,
    // so simply look for it in the list of provided options
    final CryptoProtocolVersion required = zone.getVersion();

    for (CryptoProtocolVersion c : supportedVersions) {
      if (c.equals(CryptoProtocolVersion.UNKNOWN)) {
        LOG.debug("Ignoring unknown CryptoProtocolVersion provided by client: {}",
            c.getUnknownValue());
        continue;
      }
      if (c.equals(required)) {
        return c;
      }
    }
    throw new UnknownCryptoProtocolVersionException(
        "No crypto protocol versions provided by the client are supported."
            + " Client provided: " + Arrays.toString(supportedVersions)
            + " NameNode supports: " + Arrays.toString(CryptoProtocolVersion
            .values()));
  }

  /**
   * Create a new file entry in the namespace.
   * 
   * For description of parameters and exceptions thrown see
   * {@link ClientProtocol#create}, except it returns valid file status upon
   * success
   */
  HdfsFileStatus startFile(String src, PermissionStatus permissions,
      String holder, String clientMachine, EnumSet<CreateFlag> flag,
      boolean createParent, short replication, long blockSize,
      CryptoProtocolVersion[] supportedVersions, String ecPolicyName,
      String storagePolicy, boolean logRetryCache) throws IOException {

    HdfsFileStatus status;
    try {
      status = startFileInt(src, permissions, holder, clientMachine, flag,
          createParent, replication, blockSize, supportedVersions, ecPolicyName,
          storagePolicy, logRetryCache);
    } catch (AccessControlException e) {
      logAuditEvent(false, "create", src);
      throw e;
    }
    logAuditEvent(true, "create", src, status);
    return status;
  }

  private HdfsFileStatus startFileInt(String src,
      PermissionStatus permissions, String holder, String clientMachine,
      EnumSet<CreateFlag> flag, boolean createParent, short replication,
      long blockSize, CryptoProtocolVersion[] supportedVersions,
      String ecPolicyName, String storagePolicy, boolean logRetryCache)
      throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder.append("DIR* NameSystem.startFile: src=").append(src)
          .append(", holder=").append(holder)
          .append(", clientMachine=").append(clientMachine)
          .append(", createParent=").append(createParent)
          .append(", replication=").append(replication)
          .append(", createFlag=").append(flag)
          .append(", blockSize=").append(blockSize)
          .append(", supportedVersions=")
          .append(Arrays.toString(supportedVersions))
          .append(", ecPolicyName=").append(ecPolicyName)
          .append(", storagePolicy=").append(storagePolicy);
      NameNode.stateChangeLog.debug(builder.toString());
    }
    if (!DFSUtil.isValidName(src) ||
        FSDirectory.isExactReservedName(src) ||
        (FSDirectory.isReservedName(src)
            && !FSDirectory.isReservedRawName(src)
            && !FSDirectory.isReservedInodesName(src))) {
      throw new InvalidPathException(src);
    }

    boolean shouldReplicate = flag.contains(CreateFlag.SHOULD_REPLICATE);
    if (shouldReplicate &&
        (!org.apache.commons.lang3.StringUtils.isEmpty(ecPolicyName))) {
      throw new HadoopIllegalArgumentException("SHOULD_REPLICATE flag and " +
          "ecPolicyName are exclusive parameters. Set both is not allowed!");
    }

    INodesInPath iip = null;
    boolean skipSync = true; // until we do something that might create edits
    HdfsFileStatus stat = null;
    BlocksMapUpdateInfo toRemoveBlocks = null;

    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(null);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot create file" + src);

      iip = FSDirWriteFileOp.resolvePathForStartFile(
          dir, pc, src, flag, createParent);

      if (blockSize < minBlockSize) {
        throw new IOException("Specified block size " + blockSize +
            " is less than configured minimum value " +
            DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY + "=" + minBlockSize);
      }

      if (shouldReplicate) {
        blockManager.verifyReplication(src, replication, clientMachine);
      } else {
        final ErasureCodingPolicy ecPolicy = FSDirErasureCodingOp
            .getErasureCodingPolicy(this, ecPolicyName, iip);
        if (ecPolicy != null && (!ecPolicy.isReplicationPolicy())) {
          checkErasureCodingSupported("createWithEC");
          if (blockSize < ecPolicy.getCellSize()) {
            throw new IOException("Specified block size (" + blockSize
                + ") is less than the cell size (" + ecPolicy.getCellSize()
                +") of the erasure coding policy (" + ecPolicy + ").");
          }
        } else {
          blockManager.verifyReplication(src, replication, clientMachine);
        }
      }

      FileEncryptionInfo feInfo = null;
      if (!iip.isRaw() && provider != null) {
        EncryptionKeyInfo ezInfo = FSDirEncryptionZoneOp.getEncryptionKeyInfo(
            this, iip, supportedVersions);
        // if the path has an encryption zone, the lock was released while
        // generating the EDEK.  re-resolve the path to ensure the namesystem
        // and/or EZ has not mutated
        if (ezInfo != null) {
          checkOperation(OperationCategory.WRITE);
          iip = FSDirWriteFileOp.resolvePathForStartFile(
              dir, pc, iip.getPath(), flag, createParent);
          feInfo = FSDirEncryptionZoneOp.getFileEncryptionInfo(
              dir, iip, ezInfo);
        }
      }

      skipSync = false; // following might generate edits
      toRemoveBlocks = new BlocksMapUpdateInfo();
      dir.writeLock();
      try {
        stat = FSDirWriteFileOp.startFile(this, iip, permissions, holder,
            clientMachine, flag, createParent, replication, blockSize, feInfo,
            toRemoveBlocks, shouldReplicate, ecPolicyName, storagePolicy,
            logRetryCache);
      } catch (IOException e) {
        skipSync = e instanceof StandbyException;
        throw e;
      } finally {
        dir.writeUnlock();
      }
    } finally {
      writeUnlock("create", getLockReportInfoSupplier(src, null, stat));
      // There might be transactions logged while trying to recover the lease.
      // They need to be sync'ed even when an exception was thrown.
      if (!skipSync) {
        getEditLog().logSync();
        if (toRemoveBlocks != null) {
          blockManager.addBLocksToMarkedDeleteQueue(
              toRemoveBlocks.getToDeleteList());
        }
      }
    }

    return stat;
  }

  /**
   * Recover lease;
   * Immediately revoke the lease of the current lease holder and start lease
   * recovery so that the file can be forced to be closed.
   * 
   * @param src the path of the file to start lease recovery
   * @param holder the lease holder's name
   * @param clientMachine the client machine's name
   * @return true if the file is already closed or
   *         if the lease can be released and the file can be closed.
   * @throws IOException
   */
  boolean recoverLease(String src, String holder, String clientMachine)
      throws IOException {
    boolean skipSync = false;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(null);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot recover the lease of " + src);
      final INodesInPath iip = dir.resolvePath(pc, src, DirOp.WRITE);
      src = iip.getPath();
      final INodeFile inode = INodeFile.valueOf(iip.getLastINode(), src);
      if (!inode.isUnderConstruction()) {
        return true;
      }
      if (isPermissionEnabled) {
        dir.checkPathAccess(pc, iip, FsAction.WRITE);
      }
  
      return recoverLeaseInternal(RecoverLeaseOp.RECOVER_LEASE,
          iip, src, holder, clientMachine, true);
    } catch (StandbyException se) {
      skipSync = true;
      throw se;
    } finally {
      writeUnlock("recoverLease");
      // There might be transactions logged while trying to recover the lease.
      // They need to be sync'ed even when an exception was thrown.
      if (!skipSync) {
        getEditLog().logSync();
      }
    }
  }

  enum RecoverLeaseOp {
    CREATE_FILE,
    APPEND_FILE,
    TRUNCATE_FILE,
    RECOVER_LEASE;
    
    public String getExceptionMessage(String src, String holder,
        String clientMachine, String reason) {
      return "Failed to " + this + " " + src + " for " + holder +
          " on " + clientMachine + " because " + reason;
    }
  }

  boolean recoverLeaseInternal(RecoverLeaseOp op, INodesInPath iip,
      String src, String holder, String clientMachine, boolean force)
      throws IOException {
    assert hasWriteLock();
    INodeFile file = iip.getLastINode().asFile();
    if (file.isUnderConstruction()) {
      //
      // If the file is under construction , then it must be in our
      // leases. Find the appropriate lease record.
      //
      Lease lease = leaseManager.getLease(holder);

      if (!force && lease != null) {
        Lease leaseFile = leaseManager.getLease(file);
        if (leaseFile != null && leaseFile.equals(lease)) {
          // We found the lease for this file but the original
          // holder is trying to obtain it again.
          throw new AlreadyBeingCreatedException(
              op.getExceptionMessage(src, holder, clientMachine,
                  holder + " is already the current lease holder."));
        }
      }
      //
      // Find the original holder.
      //
      FileUnderConstructionFeature uc = file.getFileUnderConstructionFeature();
      String clientName = uc.getClientName();
      lease = leaseManager.getLease(clientName);
      if (lease == null) {
        throw new AlreadyBeingCreatedException(
            op.getExceptionMessage(src, holder, clientMachine,
                "the file is under construction but no leases found."));
      }
      if (force) {
        // close now: no need to wait for soft lease expiration and 
        // close only the file src
        LOG.info("recoverLease: " + lease + ", src=" + src +
          " from client " + clientName);
        return internalReleaseLease(lease, src, iip, holder);
      } else {
        assert lease.getHolder().equals(clientName) :
          "Current lease holder " + lease.getHolder() +
          " does not match file creator " + clientName;
        //
        // If the original holder has not renewed in the last SOFTLIMIT 
        // period, then start lease recovery.
        //
        if (lease.expiredSoftLimit()) {
          LOG.info("startFile: recover " + lease + ", src=" + src + " client "
              + clientName);
          if (internalReleaseLease(lease, src, iip, null)) {
            return true;
          } else {
            throw new RecoveryInProgressException(
                op.getExceptionMessage(src, holder, clientMachine,
                    "lease recovery is in progress. Try again later."));
          }
        } else {
          final BlockInfo lastBlock = file.getLastBlock();
          if (lastBlock != null
              && lastBlock.getBlockUCState() == BlockUCState.UNDER_RECOVERY) {
            throw new RecoveryInProgressException(
                op.getExceptionMessage(src, holder, clientMachine,
                    "another recovery is in progress by "
                        + clientName + " on " + uc.getClientMachine()));
          } else {
            throw new AlreadyBeingCreatedException(
                op.getExceptionMessage(src, holder, clientMachine,
                    "this file lease is currently owned by "
                        + clientName + " on " + uc.getClientMachine()));
          }
        }
      }
    } else {
      return true;
     }
  }

  /**
   * Append to an existing file in the namespace.
   */
  LastBlockWithStatus appendFile(String srcArg, String holder,
      String clientMachine, EnumSet<CreateFlag> flag, boolean logRetryCache)
      throws IOException {
    final String operationName = "append";
    boolean newBlock = flag.contains(CreateFlag.NEW_BLOCK);
    if (newBlock) {
      requireEffectiveLayoutVersionForFeature(Feature.APPEND_NEW_BLOCK);
    }

    NameNode.stateChangeLog.debug("DIR* NameSystem.appendFile: src={}, holder={}, clientMachine={}",
        srcArg, holder, clientMachine);
    try {
      boolean skipSync = false;
      LastBlockWithStatus lbs = null;
      checkOperation(OperationCategory.WRITE);
      final FSPermissionChecker pc = getPermissionChecker();
      FSPermissionChecker.setOperationType(operationName);
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot append to file" + srcArg);
        lbs = FSDirAppendOp.appendFile(this, srcArg, pc, holder, clientMachine,
            newBlock, logRetryCache);
      } catch (StandbyException se) {
        skipSync = true;
        throw se;
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(srcArg));
        // There might be transactions logged while trying to recover the lease
        // They need to be sync'ed even when an exception was thrown.
        if (!skipSync) {
          getEditLog().logSync();
        }
      }
      logAuditEvent(true, operationName, srcArg);
      return lbs;
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, srcArg);
      throw e;
    }
  }

  ExtendedBlock getExtendedBlock(Block blk) {
    return new ExtendedBlock(getBlockPoolId(), blk);
  }
  
  void setBlockPoolId(String bpid) {
    blockManager.setBlockPoolId(bpid);
  }

  /**
   * The client would like to obtain an additional block for the indicated
   * filename (which is being written-to).  Return an array that consists
   * of the block, plus a set of machines.  The first on this list should
   * be where the client writes data.  Subsequent items in the list must
   * be provided in the connection to the first datanode.
   *
   * Make sure the previous blocks have been reported by datanodes and
   * are replicated.  Will return an empty 2-elt array if we want the
   * client to "try again later".
   */
  LocatedBlock getAdditionalBlock(
      String src, long fileId, String clientName, ExtendedBlock previous,
      DatanodeInfo[] excludedNodes, String[] favoredNodes,
      EnumSet<AddBlockFlag> flags) throws IOException {
    final String operationName = "getAdditionalBlock";
    NameNode.stateChangeLog.debug("BLOCK* getAdditionalBlock: {}  inodeId {} for {}",
        src, fileId, clientName);

    LocatedBlock[] onRetryBlock = new LocatedBlock[1];
    FSDirWriteFileOp.ValidateAddBlockResult r;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    readLock();
    try {
      checkOperation(OperationCategory.WRITE);
      r = FSDirWriteFileOp.validateAddBlock(this, pc, src, fileId, clientName,
                                            previous, onRetryBlock);
    } finally {
      readUnlock(operationName);
    }

    if (r == null) {
      assert onRetryBlock[0] != null : "Retry block is null";
      // This is a retry. Just return the last block.
      return onRetryBlock[0];
    }

    DatanodeStorageInfo[] targets = FSDirWriteFileOp.chooseTargetForNewBlock(
        blockManager, src, excludedNodes, favoredNodes, flags, r);

    checkOperation(OperationCategory.WRITE);
    writeLock();
    LocatedBlock lb;
    try {
      checkOperation(OperationCategory.WRITE);
      lb = FSDirWriteFileOp.storeAllocatedBlock(
          this, src, fileId, clientName, previous, targets);
    } finally {
      writeUnlock(operationName);
    }
    getEditLog().logSync();
    return lb;
  }

  /** @see ClientProtocol#getAdditionalDatanode */
  LocatedBlock getAdditionalDatanode(String src, long fileId,
      final ExtendedBlock blk, final DatanodeInfo[] existings,
      final String[] storageIDs,
      final Set<Node> excludes,
      final int numAdditionalNodes, final String clientName
      ) throws IOException {
    //check if the feature is enabled
    dtpReplaceDatanodeOnFailure.checkEnabled();

    Node clientnode = null;
    String clientMachine;
    final long preferredblocksize;
    final byte storagePolicyID;
    final List<DatanodeStorageInfo> chosen;
    final BlockType blockType;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(null);
    readLock();
    try {
      // Changing this operation category to WRITE instead of making getAdditionalDatanode as a
      // read method is aim to let Active NameNode to handle this RPC, because Active NameNode
      // contains a more complete DN selection context than Observer NameNode.
      checkOperation(OperationCategory.WRITE);
      //check safe mode
      checkNameNodeSafeMode("Cannot add datanode; src=" + src + ", blk=" + blk);
      final INodesInPath iip = dir.resolvePath(pc, src, fileId);
      src = iip.getPath();

      //check lease
      final INodeFile file = checkLease(iip, clientName, fileId);
      clientMachine = file.getFileUnderConstructionFeature().getClientMachine();
      clientnode = blockManager.getDatanodeManager().getDatanodeByHost(clientMachine);
      preferredblocksize = file.getPreferredBlockSize();
      storagePolicyID = file.getStoragePolicyID();
      blockType = file.getBlockType();

      //find datanode storages
      final DatanodeManager dm = blockManager.getDatanodeManager();
      chosen = Arrays.asList(dm.getDatanodeStorageInfos(existings, storageIDs,
          "src=%s, fileId=%d, blk=%s, clientName=%s, clientMachine=%s",
          src, fileId, blk, clientName, clientMachine));
    } finally {
      readUnlock("getAdditionalDatanode");
    }

    if (clientnode == null) {
      clientnode = FSDirWriteFileOp.getClientNode(blockManager, clientMachine);
    }

    // choose new datanodes.
    final DatanodeStorageInfo[] targets = blockManager.chooseTarget4AdditionalDatanode(
        src, numAdditionalNodes, clientnode, chosen, 
        excludes, preferredblocksize, storagePolicyID, blockType);
    final LocatedBlock lb = BlockManager.newLocatedBlock(
        blk, targets, -1, false);
    blockManager.setBlockToken(lb, BlockTokenIdentifier.AccessMode.COPY);
    return lb;
  }

  /**
   * The client would like to let go of the given block
   */
  void abandonBlock(ExtendedBlock b, long fileId, String src, String holder)
      throws IOException {
    NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: {} of file {}", b, src);
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(null);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot abandon block " + b + " for file" + src);
      FSDirWriteFileOp.abandonBlock(dir, pc, b, fileId, src, holder);
      NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.abandonBlock: {} is removed from pendingCreates", b);
    } finally {
      writeUnlock("abandonBlock");
    }
    getEditLog().logSync();
  }

  private String leaseExceptionString(String src, long fileId, String holder) {
    final Lease lease = leaseManager.getLease(holder);
    return src + " (inode " + fileId + ") " + (lease != null? lease.toString()
        : "Holder " + holder + " does not have any open files.");
  }

  INodeFile checkLease(INodesInPath iip, String holder, long fileId)
      throws LeaseExpiredException, FileNotFoundException {
    String src = iip.getPath();
    INode inode = iip.getLastINode();
    assert hasReadLock();
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: "
          + leaseExceptionString(src, fileId, holder));
    }
    if (!inode.isFile()) {
      throw new LeaseExpiredException("INode is not a regular file: "
          + leaseExceptionString(src, fileId, holder));
    }
    final INodeFile file = inode.asFile();
    if (!file.isUnderConstruction()) {
      throw new LeaseExpiredException("File is not open for writing: "
          + leaseExceptionString(src, fileId, holder));
    }
    // No further modification is allowed on a deleted file.
    // A file is considered deleted, if it is not in the inodeMap or is marked
    // as deleted in the snapshot feature.
    if (isFileDeleted(file)) {
      throw new FileNotFoundException("File is deleted: "
          + leaseExceptionString(src, fileId, holder));
    }
    final String owner = file.getFileUnderConstructionFeature().getClientName();
    if (holder != null && !owner.equals(holder)) {
      throw new LeaseExpiredException("Client (=" + holder
          + ") is not the lease owner (=" + owner + ": "
          + leaseExceptionString(src, fileId, holder));
    }
    return file;
  }
 
  /**
   * Complete in-progress write to the given file.
   * @return true if successful, false if the client should continue to retry
   *         (e.g if not all blocks have reached minimum replication yet)
   * @throws IOException on error (eg lease mismatch, file not open, file deleted)
   */
  boolean completeFile(final String src, String holder,
                       ExtendedBlock last, long fileId)
    throws IOException {
    boolean success = false;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(null);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot complete file " + src);
      success = FSDirWriteFileOp.completeFile(this, pc, src, holder, last,
                                              fileId);
    } finally {
      writeUnlock("completeFile");
    }
    getEditLog().logSync();
    if (success) {
      NameNode.stateChangeLog.info("DIR* completeFile: " + src
          + " is closed by " + holder);
    }
    return success;
  }

  /**
   * Create new block with a unique block id and a new generation stamp.
   * @param blockType is the file under striping or contiguous layout?
   */
  Block createNewBlock(BlockType blockType) throws IOException {
    assert hasWriteLock();
    Block b = new Block(nextBlockId(blockType), 0, 0);
    // Increment the generation stamp for every new block.
    b.setGenerationStamp(nextGenerationStamp(false));
    return b;
  }

  /**
   * Check that the indicated file's blocks are present and
   * replicated.  If not, return false. If checkall is true, then check
   * all blocks, otherwise check only penultimate block.
   */
  boolean checkFileProgress(String src, INodeFile v, boolean checkall) {
    assert hasReadLock();
    if (checkall) {
      return checkBlocksComplete(src, true, v.getBlocks());
    } else {
      final BlockInfo[] blocks = v.getBlocks();
      final int i = blocks.length - numCommittedAllowed - 2;
      return i < 0 || blocks[i] == null
          || checkBlocksComplete(src, false, blocks[i]);
    }
  }

  /**
   * Check if the blocks are COMPLETE;
   * it may allow the last block to be COMMITTED.
   */
  private boolean checkBlocksComplete(String src, boolean allowCommittedBlock,
      BlockInfo... blocks) {
    final int n = allowCommittedBlock? numCommittedAllowed: 0;
    for(int i = 0; i < blocks.length; i++) {
      final short min = blockManager.getMinStorageNum(blocks[i]);
      final String err = INodeFile.checkBlockComplete(blocks, i, n, min);
      if (err != null) {
        final int numNodes = blocks[i].numNodes();
        LOG.info("BLOCK* " + err + "(numNodes= " + numNodes
            + (numNodes < min ? " < " : " >= ")
            + " minimum = " + min + ") in file " + src);
        return false;
      }
    }
    return true;
  }

  /**
   * Change the indicated filename. 
   * @deprecated Use {@link #renameTo(String, String, boolean,
   * Options.Rename...)} instead.
   */
  @Deprecated
  boolean renameTo(String src, String dst, boolean logRetryCache)
      throws IOException {
    final String operationName = "rename";
    FSDirRenameOp.RenameResult ret = null;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot rename " + src);
        ret = FSDirRenameOp.renameToInt(dir, pc, src, dst, logRetryCache);
      } finally {
        FileStatus status = ret != null ? ret.auditStat : null;
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, dst, status));
      }
    } catch (AccessControlException e)  {
      logAuditEvent(false, operationName, src, dst, null);
      throw e;
    }
    assert ret != null;
    boolean success = ret.success;
    if (success) {
      getEditLog().logSync();
      logAuditEvent(true, operationName, src, dst, ret.auditStat);
    }
    return success;
  }

  void renameTo(final String src, final String dst,
                boolean logRetryCache, Options.Rename... options)
      throws IOException {
    final String operationName = "rename";
    FSDirRenameOp.RenameResult res = null;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot rename " + src);
        res = FSDirRenameOp.renameToInt(dir, pc, src, dst, logRetryCache,
            options);
      } finally {
        FileStatus status = res != null ? res.auditStat : null;
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, dst, status));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName + " (options=" +
          Arrays.toString(options) + ")", src, dst, null);
      throw e;
    }
    getEditLog().logSync();
    assert res != null;
    BlocksMapUpdateInfo collectedBlocks = res.collectedBlocks;
    if (!collectedBlocks.getToDeleteList().isEmpty()) {
      blockManager.addBLocksToMarkedDeleteQueue(
          collectedBlocks.getToDeleteList());
    }

    logAuditEvent(true, operationName + " (options=" +
        Arrays.toString(options) + ")", src, dst, res.auditStat);
  }

  /**
   * Remove the indicated file from namespace.
   * 
   * @see ClientProtocol#delete(String, boolean) for detailed description and 
   * description of exceptions
   */
  boolean delete(String src, boolean recursive, boolean logRetryCache)
      throws IOException {
    final String operationName = "delete";
    BlocksMapUpdateInfo toRemovedBlocks = null;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    boolean ret = false;
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot delete " + src);
        toRemovedBlocks = FSDirDeleteOp.delete(
            this, pc, src, recursive, logRetryCache);
        ret = toRemovedBlocks != null;
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(src));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(ret, operationName, src);
    if (toRemovedBlocks != null) {
      blockManager.addBLocksToMarkedDeleteQueue(
          toRemovedBlocks.getToDeleteList());
    }
    return ret;
  }

  FSPermissionChecker getPermissionChecker()
      throws AccessControlException {
    return dir.getPermissionChecker();
  }

  /**
   * Remove leases and inodes related to a given path
   * @param removedUCFiles INodes whose leases need to be released
   * @param removedINodes Containing the list of inodes to be removed from
   *                      inodesMap
   * @param acquireINodeMapLock Whether to acquire the lock for inode removal
   */
  void removeLeasesAndINodes(List<Long> removedUCFiles,
      List<INode> removedINodes,
      final boolean acquireINodeMapLock) {
    assert hasWriteLock();
    for(long i : removedUCFiles) {
      leaseManager.removeLease(i);
    }
    // remove inodes from inodesMap
    if (removedINodes != null) {
      if (acquireINodeMapLock) {
        dir.writeLock();
      }
      try {
        dir.removeFromInodeMap(removedINodes);
      } finally {
        if (acquireINodeMapLock) {
          dir.writeUnlock();
        }
      }
      removedINodes.clear();
    }
  }

  /**
   * Get the file info for a specific file.
   *
   * @param src The string representation of the path to the file
   * @param resolveLink whether to throw UnresolvedLinkException
   *        if src refers to a symlink
   *
   * @param needLocation Include {@link LocatedBlocks} in result.
   * @param needBlockToken Include block tokens in {@link LocatedBlocks}
   * @throws AccessControlException if access is denied
   * @throws UnresolvedLinkException if a symlink is encountered.
   *
   * @return object containing information regarding the file
   *         or null if file not found
   * @throws StandbyException
   */
  HdfsFileStatus getFileInfo(final String src, boolean resolveLink,
      boolean needLocation, boolean needBlockToken) throws IOException {
    // if the client requests block tokens, then it can read data blocks
    // and should appear in the audit log as if getBlockLocations had been
    // called
    final String operationName = needBlockToken ? "open" : "getfileinfo";
    checkOperation(OperationCategory.READ);
    HdfsFileStatus stat = null;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        stat = FSDirStatAndListingOp.getFileInfo(
            dir, pc, src, resolveLink, needLocation, needBlockToken);
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(src));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    if (needLocation && isObserver() && stat instanceof HdfsLocatedFileStatus) {
      LocatedBlocks lbs = ((HdfsLocatedFileStatus) stat).getLocatedBlocks();
      checkBlockLocationsWhenObserver(lbs, src);
    }
    logAuditEvent(true, operationName, src);
    return stat;
  }

  /**
   * Returns true if the file is closed
   */
  boolean isFileClosed(final String src) throws IOException {
    final String operationName = "isFileClosed";
    checkOperation(OperationCategory.READ);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    boolean success = false;
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        success = FSDirStatAndListingOp.isFileClosed(dir, pc, src);
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(src));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    if (success) {
      logAuditEvent(true, operationName, src);
    }
    return success;
  }

  /**
   * Create all the necessary directories
   */
  boolean mkdirs(String src, PermissionStatus permissions,
      boolean createParent) throws IOException {
    final String operationName = "mkdirs";
    FileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot create directory " + src);
        auditStat = FSDirMkdirOp.mkdirs(this, pc, src, permissions,
            createParent);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, auditStat);
    return true;
  }

  /**
   * Get the content summary for a specific file/dir.
   *
   * @param src The string representation of the path to the file
   *
   * @throws AccessControlException if access is denied
   * @throws UnresolvedLinkException if a symlink is encountered.
   * @throws FileNotFoundException if no file exists
   * @throws StandbyException
   * @throws IOException for issues with writing to the audit log
   *
   * @return object containing information regarding the file
   *         or null if file not found
   */
  ContentSummary getContentSummary(final String src) throws IOException {
    checkOperation(OperationCategory.READ);
    final String operationName = "contentSummary";
    ContentSummary cs;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        cs = FSDirStatAndListingOp.getContentSummary(dir, pc, src);
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(src));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, src);
      throw ace;
    }
    logAuditEvent(true, operationName, src);
    return cs;
  }

  /**
   * Get the quota usage for a specific file/dir.
   *
   * @param src The string representation of the path to the file
   *
   * @throws AccessControlException if access is denied
   * @throws UnresolvedLinkException if a symlink is encountered.
   * @throws FileNotFoundException if no file exists
   * @throws StandbyException
   * @throws IOException for issues with writing to the audit log
   *
   * @return object containing information regarding the file
   *         or null if file not found
   */
  QuotaUsage getQuotaUsage(final String src) throws IOException {
    checkOperation(OperationCategory.READ);
    final String operationName = "quotaUsage";
    QuotaUsage quotaUsage;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        quotaUsage = FSDirStatAndListingOp.getQuotaUsage(dir, pc, src);
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(src));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, src);
      throw ace;
    }
    logAuditEvent(true, operationName, src);
    return quotaUsage;
  }

  /**
   * Set the namespace quota and storage space quota for a directory.
   * See {@link ClientProtocol#setQuota(String, long, long, StorageType)} for the
   * contract.
   * 
   * Note: This does not support ".inodes" relative path.
   */
  void setQuota(String src, long nsQuota, long ssQuota, StorageType type)
      throws IOException {
    if (type != null) {
      requireEffectiveLayoutVersionForFeature(Feature.QUOTA_BY_STORAGE_TYPE);
    }
    if (type == StorageType.NVDIMM) {
      requireEffectiveLayoutVersionForFeature(Feature.NVDIMM_SUPPORT);
    }
    checkOperation(OperationCategory.WRITE);
    final String operationName = getQuotaCommand(nsQuota, ssQuota);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    if(!allowOwnerSetQuota) {
      checkSuperuserPrivilege(operationName, src);
    }
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot set quota on " + src);
        FSDirAttrOp.setQuota(dir, pc, src, nsQuota, ssQuota, type,
            allowOwnerSetQuota);
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(src));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, src);
      throw ace;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src);
  }

  /** Persist all metadata about this file.
   * @param src The string representation of the path
   * @param fileId The inode ID that we're fsyncing.  Older clients will pass
   *               INodeId.GRANDFATHER_INODE_ID here.
   * @param clientName The string representation of the client
   * @param lastBlockLength The length of the last block 
   *                        under construction reported from client.
   * @throws IOException if path does not exist
   */
  void fsync(String src, long fileId, String clientName, long lastBlockLength)
      throws IOException {
    NameNode.stateChangeLog.info("BLOCK* fsync: " + src + " for " + clientName);
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(null);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot fsync file " + src);
      INodesInPath iip = dir.resolvePath(pc, src, fileId);
      src = iip.getPath();
      final INodeFile pendingFile = checkLease(iip, clientName, fileId);
      if (lastBlockLength > 0) {
        pendingFile.getFileUnderConstructionFeature().updateLengthOfLastBlock(
            pendingFile, lastBlockLength);
      }
      FSDirWriteFileOp.persistBlocks(dir, src, pendingFile, false);
    } finally {
      writeUnlock("fsync");
    }
    getEditLog().logSync();
  }

  /**
   * Move a file that is being written to be immutable.
   * @param src The filename
   * @param lease The lease for the client creating the file
   * @param recoveryLeaseHolder reassign lease to this holder if the last block
   *        needs recovery; keep current holder if null.
   * @throws AlreadyBeingCreatedException if file is waiting to achieve minimal
   *         replication;<br>
   *         RecoveryInProgressException if lease recovery is in progress.<br>
   *         IOException in case of an error.
   * @return true  if file has been successfully finalized and closed or 
   *         false if block recovery has been initiated. Since the lease owner
   *         has been changed and logged, caller should call logSync().
   */
  boolean internalReleaseLease(Lease lease, String src, INodesInPath iip,
      String recoveryLeaseHolder) throws IOException {
    LOG.info("Recovering " + lease + ", src=" + src);
    assert !isInSafeMode();
    assert hasWriteLock();

    final INodeFile pendingFile = iip.getLastINode().asFile();
    int nrBlocks = pendingFile.numBlocks();
    BlockInfo[] blocks = pendingFile.getBlocks();

    int nrCompleteBlocks;
    BlockInfo curBlock = null;
    for(nrCompleteBlocks = 0; nrCompleteBlocks < nrBlocks; nrCompleteBlocks++) {
      curBlock = blocks[nrCompleteBlocks];
      if(!curBlock.isComplete())
        break;
      assert blockManager.hasMinStorage(curBlock) :
              "A COMPLETE block is not minimally replicated in " + src;
    }

    // If there are no incomplete blocks associated with this file,
    // then reap lease immediately and close the file.
    if(nrCompleteBlocks == nrBlocks) {
      finalizeINodeFileUnderConstruction(src, pendingFile,
          iip.getLatestSnapshotId(), false);
      NameNode.stateChangeLog.warn("BLOCK*" +
          " internalReleaseLease: All existing blocks are COMPLETE," +
          " lease removed, file " + src + " closed.");
      return true;  // closed!
    }

    // Only the last and the penultimate blocks may be in non COMPLETE state.
    // If the penultimate block is not COMPLETE, then it must be COMMITTED.
    if(nrCompleteBlocks < nrBlocks - 2 ||
       nrCompleteBlocks == nrBlocks - 2 &&
         curBlock != null &&
         curBlock.getBlockUCState() != BlockUCState.COMMITTED) {
      final String message = "DIR* NameSystem.internalReleaseLease: "
        + "attempt to release a create lock on "
        + src + " but file is already closed.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }

    // The last block is not COMPLETE, and
    // that the penultimate block if exists is either COMPLETE or COMMITTED
    final BlockInfo lastBlock = pendingFile.getLastBlock();
    BlockUCState lastBlockState = lastBlock.getBlockUCState();
    BlockInfo penultimateBlock = pendingFile.getPenultimateBlock();

    // If penultimate block doesn't exist then its minReplication is met
    boolean penultimateBlockMinStorage = penultimateBlock == null ||
        blockManager.hasMinStorage(penultimateBlock);

    switch(lastBlockState) {
    case COMPLETE:
      assert false : "Already checked that the last block is incomplete";
      break;
    case COMMITTED:
      // Close file if committed blocks are minimally replicated
      if(penultimateBlockMinStorage &&
          blockManager.hasMinStorage(lastBlock)) {
        finalizeINodeFileUnderConstruction(src, pendingFile,
            iip.getLatestSnapshotId(), false);
        NameNode.stateChangeLog.warn("BLOCK*" +
            " internalReleaseLease: Committed blocks are minimally" +
            " replicated, lease removed, file" + src + " closed.");
        return true;  // closed!
      }
      // Cannot close file right now, since some blocks 
      // are not yet minimally replicated.
      // This may potentially cause infinite loop in lease recovery
      // if there are no valid replicas on data-nodes.
      String message = "DIR* NameSystem.internalReleaseLease: " +
          "Failed to release lease for file " + src +
          ". Committed blocks are waiting to be minimally replicated.";
      NameNode.stateChangeLog.warn(message);
      if (!penultimateBlockMinStorage) {
        throw new AlreadyBeingCreatedException(message);
      }
      // Intentionally fall through to UNDER_RECOVERY so BLOCK_RECOVERY is
      // attempted
    case UNDER_CONSTRUCTION:
    case UNDER_RECOVERY:
      BlockUnderConstructionFeature uc =
          lastBlock.getUnderConstructionFeature();
      // determine if last block was intended to be truncated
      BlockInfo recoveryBlock = uc.getTruncateBlock();
      boolean truncateRecovery = recoveryBlock != null;
      boolean copyOnTruncate = truncateRecovery &&
          recoveryBlock.getBlockId() != lastBlock.getBlockId();
      assert !copyOnTruncate ||
          recoveryBlock.getBlockId() < lastBlock.getBlockId() &&
          recoveryBlock.getGenerationStamp() < lastBlock.getGenerationStamp() &&
          recoveryBlock.getNumBytes() > lastBlock.getNumBytes() :
            "wrong recoveryBlock";

      // setup the last block locations from the blockManager if not known
      if (uc.getNumExpectedLocations() == 0) {
        uc.setExpectedLocations(lastBlock, blockManager.getStorages(lastBlock),
            lastBlock.getBlockType());
      }

      int minLocationsNum = 1;
      if (lastBlock.isStriped()) {
        minLocationsNum = ((BlockInfoStriped) lastBlock).getRealDataBlockNum();
      }
      if (uc.getNumExpectedLocations() < minLocationsNum &&
          lastBlock.getNumBytes() == 0) {
        // There is no datanode reported to this block.
        // may be client have crashed before writing data to pipeline.
        // This blocks doesn't need any recovery.
        // We can remove this block and close the file.
        pendingFile.removeLastBlock(lastBlock);
        finalizeINodeFileUnderConstruction(src, pendingFile,
            iip.getLatestSnapshotId(), false);
        if (uc.getNumExpectedLocations() == 0) {
          // If uc.getNumExpectedLocations() is 0, regardless of whether it
          // is a striped block or not, we should consider it as an empty block.
          NameNode.stateChangeLog.warn("BLOCK* internalReleaseLease: "
              + "Removed empty last block and closed file " + src);
        } else {
          // If uc.getNumExpectedLocations() is greater than 0, it means that
          // minLocationsNum must be greater than 1, so this must be a striped
          // block.
          NameNode.stateChangeLog.warn("BLOCK* internalReleaseLease: "
              + "Removed last unrecoverable block group and closed file " + src);
        }
        return true;
      }
      // Start recovery of the last block for this file
      // Only do so if there is no ongoing recovery for this block,
      // or the previous recovery for this block timed out.
      if (blockManager.addBlockRecoveryAttempt(lastBlock)) {
        long blockRecoveryId = nextGenerationStamp(
            blockManager.isLegacyBlock(lastBlock));
        if(copyOnTruncate) {
          lastBlock.setGenerationStamp(blockRecoveryId);
        } else if(truncateRecovery) {
          recoveryBlock.setGenerationStamp(blockRecoveryId);
        }
        uc.initializeBlockRecovery(lastBlock, blockRecoveryId, true);

        // Cannot close file right now, since the last block requires recovery.
        // This may potentially cause infinite loop in lease recovery
        // if there are no valid replicas on data-nodes.
        NameNode.stateChangeLog.warn(
            "DIR* NameSystem.internalReleaseLease: " +
                "File " + src + " has not been closed." +
                " Lease recovery is in progress. " +
                "RecoveryId = " + blockRecoveryId + " for block " + lastBlock);
      }
      lease = reassignLease(lease, src, recoveryLeaseHolder, pendingFile);
      if (recoveryLeaseHolder == null) {
        leaseManager.renewLease(lease);
      }
      break;
    }
    return false;
  }

  private Lease reassignLease(Lease lease, String src, String newHolder,
      INodeFile pendingFile) {
    assert hasWriteLock();
    if(newHolder == null)
      return lease;
    // The following transaction is not synced. Make sure it's sync'ed later.
    logReassignLease(lease.getHolder(), src, newHolder);
    return reassignLeaseInternal(lease, newHolder, pendingFile);
  }
  
  Lease reassignLeaseInternal(Lease lease, String newHolder, INodeFile pendingFile) {
    assert hasWriteLock();
    pendingFile.getFileUnderConstructionFeature().setClientName(newHolder);
    return leaseManager.reassignLease(lease, pendingFile, newHolder);
  }

  void commitOrCompleteLastBlock(
      final INodeFile fileINode, final INodesInPath iip,
      final Block commitBlock) throws IOException {
    assert hasWriteLock();
    Preconditions.checkArgument(fileINode.isUnderConstruction());
    blockManager.commitOrCompleteLastBlock(fileINode, commitBlock, iip);
  }

  void addCommittedBlocksToPending(final INodeFile pendingFile) {
    final BlockInfo[] blocks = pendingFile.getBlocks();
    int i = blocks.length - numCommittedAllowed;
    if (i < 0) {
      i = 0;
    }
    for(; i < blocks.length; i++) {
      final BlockInfo b = blocks[i];
      if (b != null && b.getBlockUCState() == BlockUCState.COMMITTED) {
        // b is COMMITTED but not yet COMPLETE, add it to pending replication.
        blockManager.addExpectedReplicasToPending(b);
      }
    }
  }

  void finalizeINodeFileUnderConstruction(String src, INodeFile pendingFile,
      int latestSnapshot, boolean allowCommittedBlock) throws IOException {
    assert hasWriteLock();

    FileUnderConstructionFeature uc = pendingFile.getFileUnderConstructionFeature();
    if (uc == null) {
      throw new IOException("Cannot finalize file " + src
          + " because it is not under construction");
    }

    pendingFile.recordModification(latestSnapshot);

    // The file is no longer pending.
    // Create permanent INode, update blocks. No need to replace the inode here
    // since we just remove the uc feature from pendingFile
    pendingFile.toCompleteFile(now(),
        allowCommittedBlock? numCommittedAllowed: 0,
        blockManager.getMinReplication());

    leaseManager.removeLease(uc.getClientName(), pendingFile);

    // close file and persist block allocations for this file
    closeFile(src, pendingFile);

    blockManager.checkRedundancy(pendingFile);
  }

  @VisibleForTesting
  BlockInfo getStoredBlock(Block block) {
    return blockManager.getStoredBlock(block);
  }

  @Override
  public boolean isInSnapshot(long blockCollectionID) {
    assert hasReadLock();
    final INodeFile bc = getBlockCollection(blockCollectionID);
    if (bc == null || !bc.isUnderConstruction()) {
      return false;
    }

    String fullName = bc.getName();
    try {
      if (fullName != null && fullName.startsWith(Path.SEPARATOR)
          && dir.getINode(fullName, DirOp.READ) == bc) {
        // If file exists in normal path then no need to look in snapshot
        return false;
      }
    } catch (IOException e) {
      // the snapshot path and current path may contain symlinks, ancestor
      // dirs replaced by files, etc.
      LOG.error("Error while resolving the path : " + fullName, e);
      return false;
    }
    /*
     * 1. if bc is under construction and also with snapshot, and
     * bc is not in the current fsdirectory tree, bc must represent a snapshot
     * file. 
     * 2. if fullName is not an absolute path, bc cannot be existent in the 
     * current fsdirectory tree. 
     * 3. if bc is not the current node associated with fullName, bc must be a
     * snapshot inode.
     */
    return true;
  }

  INodeFile getBlockCollection(BlockInfo b) {
    return getBlockCollection(b.getBlockCollectionId());
  }

  @Override
  public INodeFile getBlockCollection(long id) {
    assert hasReadLock() : "Accessing INode id = " + id + " without read lock";
    INode inode = getFSDirectory().getInode(id);
    return inode == null ? null : inode.asFile();
  }

  void commitBlockSynchronization(ExtendedBlock oldBlock,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets,
      String[] newtargetstorages) throws IOException {
    LOG.info("commitBlockSynchronization(oldBlock=" + oldBlock
             + ", newgenerationstamp=" + newgenerationstamp
             + ", newlength=" + newlength
             + ", newtargets=" + Arrays.asList(newtargets)
             + ", closeFile=" + closeFile
             + ", deleteBlock=" + deleteblock
             + ")");
    checkOperation(OperationCategory.WRITE);
    final String src;
    writeLock();
    boolean copyTruncate = false;
    BlockInfo truncatedBlock = null;
    try {
      checkOperation(OperationCategory.WRITE);
      // If a DN tries to commit to the standby, the recovery will
      // fail, and the next retry will succeed on the new NN.
  
      checkNameNodeSafeMode(
          "Cannot commitBlockSynchronization while in safe mode");
      final BlockInfo storedBlock = getStoredBlock(
          ExtendedBlock.getLocalBlock(oldBlock));
      if (storedBlock == null) {
        if (deleteblock) {
          // This may be a retry attempt so ignore the failure
          // to locate the block.
          LOG.debug("Block (={}) not found", oldBlock);
          return;
        } else {
          throw new IOException("Block (=" + oldBlock + ") not found");
        }
      }
      final long oldGenerationStamp = storedBlock.getGenerationStamp();
      final long oldNumBytes = storedBlock.getNumBytes();
      //
      // The implementation of delete operation (see @deleteInternal method)
      // first removes the file paths from namespace, and delays the removal
      // of blocks to later time for better performance. When
      // commitBlockSynchronization (this method) is called in between, the
      // blockCollection of storedBlock could have been assigned to null by
      // the delete operation, throw IOException here instead of NPE; if the
      // file path is already removed from namespace by the delete operation,
      // throw FileNotFoundException here, so not to proceed to the end of
      // this method to add a CloseOp to the edit log for an already deleted
      // file (See HDFS-6825).
      //
      if (storedBlock.isDeleted()) {
        throw new IOException("The blockCollection of " + storedBlock
            + " is null, likely because the file owning this block was"
            + " deleted and the block removal is delayed");
      }
      final INodeFile iFile = getBlockCollection(storedBlock);
      src = iFile.getFullPathName();
      if (isFileDeleted(iFile)) {
        throw new FileNotFoundException("File not found: "
            + src + ", likely due to delayed block removal");
      }
      if ((!iFile.isUnderConstruction() || storedBlock.isComplete()) &&
          iFile.getLastBlock().isComplete()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unexpected block (={}) since the file (={}) is not under construction",
              oldBlock, iFile.getLocalName());
        }

        return;
      }

      truncatedBlock = iFile.getLastBlock();
      final long recoveryId = truncatedBlock.getUnderConstructionFeature()
          .getBlockRecoveryId();
      copyTruncate = truncatedBlock.getBlockId() != storedBlock.getBlockId();
      if(recoveryId != newgenerationstamp) {
        throw new IOException("The recovery id " + newgenerationstamp
                              + " does not match current recovery id "
                              + recoveryId + " for block " + oldBlock);
      }

      if (deleteblock) {
        Block blockToDel = ExtendedBlock.getLocalBlock(oldBlock);
        boolean remove = iFile.removeLastBlock(blockToDel) != null;
        if (remove) {
          blockManager.removeBlock(storedBlock);
        }
      } else {
        // update last block
        if(!copyTruncate) {
          storedBlock.setGenerationStamp(newgenerationstamp);
          storedBlock.setNumBytes(newlength);
        }

        // Find the target DatanodeStorageInfos. If not found because of invalid
        // or empty DatanodeID/StorageID, the slot of same offset in dsInfos is
        // null
        final DatanodeStorageInfo[] dsInfos = blockManager.getDatanodeManager().
            getDatanodeStorageInfos(newtargets, newtargetstorages,
                "src=%s, oldBlock=%s, newgenerationstamp=%d, newlength=%d",
                src, oldBlock, newgenerationstamp, newlength);

        if (closeFile && dsInfos != null) {
          // the file is getting closed. Insert block locations into blockManager.
          // Otherwise fsck will report these blocks as MISSING, especially if the
          // blocksReceived from Datanodes take a long time to arrive.
          for (int i = 0; i < dsInfos.length; i++) {
            if (dsInfos[i] != null) {
              if(copyTruncate) {
                dsInfos[i].addBlock(truncatedBlock, truncatedBlock);
              } else {
                Block bi = new Block(storedBlock);
                if (storedBlock.isStriped()) {
                  bi.setBlockId(bi.getBlockId() + i);
                }
                dsInfos[i].addBlock(storedBlock, bi);
              }
            }
          }
        }

        // add pipeline locations into the INodeUnderConstruction
        if(copyTruncate) {
          iFile.convertLastBlockToUC(truncatedBlock, dsInfos);
        } else {
          iFile.convertLastBlockToUC(storedBlock, dsInfos);
          if (closeFile) {
            blockManager.markBlockReplicasAsCorrupt(oldBlock.getLocalBlock(),
                storedBlock, oldGenerationStamp, oldNumBytes,
                dsInfos);
          }
        }
      }

      if (closeFile) {
        if(copyTruncate) {
          closeFileCommitBlocks(src, iFile, truncatedBlock);
          if(!iFile.isBlockInLatestSnapshot(storedBlock)) {
            blockManager.removeBlock(storedBlock);
          }
        } else {
          closeFileCommitBlocks(src, iFile, storedBlock);
        }
      } else {
        // If this commit does not want to close the file, persist blocks
        FSDirWriteFileOp.persistBlocks(dir, src, iFile, false);
      }
      blockManager.successfulBlockRecovery(storedBlock);
    } finally {
      writeUnlock("commitBlockSynchronization");
    }
    getEditLog().logSync();
    if (closeFile) {
      LOG.info("commitBlockSynchronization(oldBlock=" + oldBlock
          + ", file=" + src
          + (copyTruncate ? ", newBlock=" + truncatedBlock
              : ", newgenerationstamp=" + newgenerationstamp)
          + ", newlength=" + newlength
          + ", newtargets=" + Arrays.asList(newtargets) + ") successful");
    } else {
      LOG.info("commitBlockSynchronization(" + oldBlock + ") successful");
    }
  }

  /**
   * @param pendingFile open file that needs to be closed
   * @param storedBlock last block
   * @throws IOException on error
   */
  @VisibleForTesting
  void closeFileCommitBlocks(String src, INodeFile pendingFile,
      BlockInfo storedBlock) throws IOException {
    final INodesInPath iip = INodesInPath.fromINode(pendingFile);

    // commit the last block and complete it if it has minimum replicas
    commitOrCompleteLastBlock(pendingFile, iip, storedBlock);

    //remove lease, close file
    int s = Snapshot.findLatestSnapshot(pendingFile, Snapshot.CURRENT_STATE_ID);
    finalizeINodeFileUnderConstruction(src, pendingFile, s, false);
  }

  /**
   * Renew the lease(s) held by the given client
   */
  void renewLease(String holder) throws IOException {
    checkOperation(OperationCategory.WRITE);
    checkNameNodeSafeMode("Cannot renew lease for " + holder);
    // fsn is not mutated so lock is not required.  the leaseManger is also
    // thread-safe.
    leaseManager.renewLease(holder);
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * @param src the directory name
   * @param startAfter the name to start after
   * @param needLocation if blockLocations need to be returned
   * @return a partial listing starting after startAfter
   * 
   * @throws AccessControlException if access is denied
   * @throws UnresolvedLinkException if symbolic link is encountered
   * @throws IOException if other I/O error occurred
   */
  DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) 
      throws IOException {
    checkOperation(OperationCategory.READ);
    final String operationName = "listStatus";
    DirectoryListing dl = null;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      readLock();
      try {
        checkOperation(NameNode.OperationCategory.READ);
        dl = getListingInt(dir, pc, src, startAfter, needLocation);
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(src));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    if (dl != null && needLocation && isObserver()) {
      for (HdfsFileStatus fs : dl.getPartialListing()) {
        if (fs instanceof HdfsLocatedFileStatus) {
          LocatedBlocks lbs = ((HdfsLocatedFileStatus) fs).getLocatedBlocks();
          checkBlockLocationsWhenObserver(lbs, fs.toString());
        }
      }
    }
    logAuditEvent(true, operationName, src);
    return dl;
  }

  public byte[] getSrcPathsHash(String[] srcs) {
    synchronized (digest) {
      for (String src : srcs) {
        digest.update(src.getBytes(Charsets.UTF_8));
      }
      byte[] result = digest.digest();
      digest.reset();
      return result;
    }
  }

  BatchedDirectoryListing getBatchedListing(String[] srcs, byte[] startAfter,
      boolean needLocation) throws IOException {

    if (srcs.length > this.batchedListingLimit) {
      String msg = String.format("Too many source paths (%d > %d)",
          srcs.length, batchedListingLimit);
      throw new IllegalArgumentException(msg);
    }

    // Parse the startAfter key if present
    int srcsIndex = 0;
    byte[] indexStartAfter = new byte[0];

    if (startAfter.length > 0) {
      BatchedListingKeyProto startAfterProto =
          BatchedListingKeyProto.parseFrom(startAfter);
      // Validate that the passed paths match the checksum from key
      Preconditions.checkArgument(
          Arrays.equals(
              startAfterProto.getChecksum().toByteArray(),
              getSrcPathsHash(srcs)));
      srcsIndex = startAfterProto.getPathIndex();
      indexStartAfter = startAfterProto.getStartAfter().toByteArray();
      // Special case: if the indexStartAfter key is an empty array, it
      // means the last element we listed was a file, not a directory.
      // Skip it so we don't list it twice.
      if (indexStartAfter.length == 0) {
        srcsIndex++;
      }
    }
    final int startSrcsIndex = srcsIndex;
    final String operationName = "listStatus";
    final FSPermissionChecker pc = getPermissionChecker();

    BatchedDirectoryListing bdl;

    checkOperation(OperationCategory.READ);
    readLock();
    try {
      checkOperation(NameNode.OperationCategory.READ);

      // List all directories from the starting index until we've reached
      // ls limit OR finished listing all srcs.
      LinkedHashMap<Integer, HdfsPartialListing> listings =
          Maps.newLinkedHashMap();
      DirectoryListing lastListing = null;
      int numEntries = 0;
      for (; srcsIndex < srcs.length; srcsIndex++) {
        String src = srcs[srcsIndex];
        HdfsPartialListing listing;
        try {
          DirectoryListing dirListing =
              getListingInt(dir, pc, src, indexStartAfter, needLocation);
          if (dirListing == null) {
            throw new FileNotFoundException("Path " + src + " does not exist");
          }
          listing = new HdfsPartialListing(
              srcsIndex, Lists.newArrayList(dirListing.getPartialListing()));
          numEntries += listing.getPartialListing().size();
          lastListing = dirListing;
        } catch (Exception e) {
          if (e instanceof AccessControlException) {
            logAuditEvent(false, operationName, src);
          }
          listing = new HdfsPartialListing(
              srcsIndex,
              new RemoteException(
                  e.getClass().getCanonicalName(),
                  e.getMessage()));
          lastListing = null;
          LOG.info("Exception listing src {}", src, e);
        }

        listings.put(srcsIndex, listing);
        // Null out the indexStartAfter after the first time.
        // If we get a partial result, we're done iterating because we're also
        // over the list limit.
        if (indexStartAfter.length != 0) {
          indexStartAfter = new byte[0];
        }
        // Terminate if we've reached the maximum listing size
        if (numEntries >= dir.getListLimit()) {
          break;
        }
      }

      HdfsPartialListing[] partialListingArray =
          listings.values().toArray(new HdfsPartialListing[] {});

      // Check whether there are more dirs/files to be listed, and if so setting
      // up the index to start within the first dir to be listed next time.
      if (srcsIndex >= srcs.length) {
        // If the loop finished normally, there are no more srcs and we're done.
        bdl = new BatchedDirectoryListing(
            partialListingArray,
            false,
            new byte[0]);
      } else if (srcsIndex == srcs.length-1 &&
          lastListing != null &&
          !lastListing.hasMore()) {
        // If we're on the last srcsIndex, then we might be done exactly on an
        // lsLimit boundary.
        bdl = new BatchedDirectoryListing(
            partialListingArray,
            false,
            new byte[0]
        );
      } else {
        byte[] lastName = lastListing != null && lastListing.getLastName() !=
            null ? lastListing.getLastName() : new byte[0];
        BatchedListingKeyProto proto = BatchedListingKeyProto.newBuilder()
            .setChecksum(ByteString.copyFrom(getSrcPathsHash(srcs)))
            .setPathIndex(srcsIndex)
            .setStartAfter(ByteString.copyFrom(lastName))
            .build();
        byte[] returnedStartAfter = proto.toByteArray();

        // Set the startAfter key if the last listing has more entries
        bdl = new BatchedDirectoryListing(
            partialListingArray,
            true,
            returnedStartAfter);
      }
    } finally {
      readUnlock(operationName,
          getLockReportInfoSupplier(Arrays.toString(srcs)));
    }
    for (int i = startSrcsIndex; i < srcsIndex; i++) {
      logAuditEvent(true, operationName, srcs[i]);
    }
    return bdl;
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by datanodes
  //
  /////////////////////////////////////////////////////////
  /**
   * Register Datanode.
   * <p>
   * The purpose of registration is to identify whether the new datanode
   * serves a new data storage, and will report new data block copies,
   * which the namenode was not aware of; or the datanode is a replacement
   * node for the data storage that was previously served by a different
   * or the same (in terms of host:port) datanode.
   * The data storages are distinguished by their storageIDs. When a new
   * data storage is reported the namenode issues a new unique storageID.
   * <p>
   * Finally, the namenode returns its namespaceID as the registrationID
   * for the datanodes. 
   * namespaceID is a persistent attribute of the name space.
   * The registrationID is checked every time the datanode is communicating
   * with the namenode. 
   * Datanodes with inappropriate registrationID are rejected.
   * If the namenode stops, and then restarts it can restore its 
   * namespaceID and will continue serving the datanodes that has previously
   * registered with the namenode without restarting the whole cluster.
   * 
   * @see org.apache.hadoop.hdfs.server.datanode.DataNode
   */
  void registerDatanode(DatanodeRegistration nodeReg) throws IOException {
    writeLock();
    try {
      blockManager.registerDatanode(nodeReg);
    } finally {
      writeUnlock("registerDatanode");
    }
  }
  
  /**
   * Get registrationID for datanodes based on the namespaceID.
   * 
   * @see #registerDatanode(DatanodeRegistration)
   * @return registration ID
   */
  String getRegistrationID() {
    return Storage.getRegistrationID(getFSImage().getStorage());
  }

  /**
   * The given node has reported in.  This method should:
   * 1) Record the heartbeat, so the datanode isn't timed out
   * 2) Adjust usage stats for future block allocation
   *
   * If a substantial amount of time passed since the last datanode
   * heartbeat then request an immediate block report.
   *
   * @return an array of datanode commands
   * @throws IOException
   */
  HeartbeatResponse handleHeartbeat(DatanodeRegistration nodeReg,
      StorageReport[] reports, long cacheCapacity, long cacheUsed,
      int xceiverCount, int xmitsInProgress, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary,
      boolean requestFullBlockReportLease,
      @Nonnull SlowPeerReports slowPeers,
      @Nonnull SlowDiskReports slowDisks)
          throws IOException {
    readLock();
    try {
      //get datanode commands
      DatanodeCommand[] cmds = blockManager.getDatanodeManager().handleHeartbeat(
          nodeReg, reports, getBlockPoolId(), cacheCapacity, cacheUsed,
          xceiverCount, xmitsInProgress, failedVolumes, volumeFailureSummary,
          slowPeers, slowDisks);
      long blockReportLeaseId = 0;
      if (requestFullBlockReportLease) {
        blockReportLeaseId =  blockManager.requestBlockReportLeaseId(nodeReg);
      }

      //create ha status
      final NNHAStatusHeartbeat haState = new NNHAStatusHeartbeat(
          haContext.getState().getServiceState(),
          getFSImage().getCorrectLastAppliedOrWrittenTxId());

      Set<String> slownodes = DatanodeManager.getSlowNodesUuidSet();
      boolean isSlownode = slownodes.contains(nodeReg.getDatanodeUuid());

      return new HeartbeatResponse(cmds, haState, rollingUpgradeInfo,
          blockReportLeaseId, isSlownode);
    } finally {
      readUnlock("handleHeartbeat");
    }
  }

  /**
   * Handles a lifeline message sent by a DataNode.  This method updates contact
   * information and statistics for the DataNode, so that it doesn't time out.
   * Unlike a heartbeat, this method does not dispatch any commands back to the
   * DataNode for local execution.  This method also cannot request a lease for
   * sending a full block report.  Lifeline messages are used only as a fallback
   * in case something prevents successful delivery of heartbeat messages.
   * Therefore, the implementation of this method must remain lightweight
   * compared to heartbeat handling.  It should avoid lock contention and
   * expensive computation.
   *
   * @param nodeReg registration info for DataNode sending the lifeline
   * @param reports storage reports from DataNode
   * @param cacheCapacity cache capacity at DataNode
   * @param cacheUsed cache used at DataNode
   * @param xceiverCount estimated count of transfer threads running at DataNode
   * @param xmitsInProgress count of transfers running at DataNode
   * @param failedVolumes count of failed volumes at DataNode
   * @param volumeFailureSummary info on failed volumes at DataNode
   * @throws IOException if there is an error
   */
  void handleLifeline(DatanodeRegistration nodeReg, StorageReport[] reports,
      long cacheCapacity, long cacheUsed, int xceiverCount, int xmitsInProgress,
      int failedVolumes, VolumeFailureSummary volumeFailureSummary)
      throws IOException {
    blockManager.getDatanodeManager().handleLifeline(nodeReg, reports,
        cacheCapacity, cacheUsed, xceiverCount,
        failedVolumes, volumeFailureSummary);
  }

  /**
   * Returns whether or not there were available resources at the last check of
   * resources.
   *
   * @return true if there were sufficient resources available, false otherwise.
   */
  boolean nameNodeHasResourcesAvailable() {
    return hasResourcesAvailable;
  }

  /**
   * Perform resource checks and cache the results.
   */
  void checkAvailableResources() {
    long resourceCheckTime = monotonicNow();
    Preconditions.checkState(nnResourceChecker != null,
        "nnResourceChecker not initialized");
    hasResourcesAvailable = nnResourceChecker.hasAvailableDiskSpace();
    resourceCheckTime = monotonicNow() - resourceCheckTime;
    NameNode.getNameNodeMetrics().addResourceCheckTime(resourceCheckTime);
  }

  /**
   * Close file.
   * @param path
   * @param file
   */
  private void closeFile(String path, INodeFile file) {
    assert hasWriteLock();
    // file is closed
    getEditLog().logCloseFile(path, file);
    NameNode.stateChangeLog.debug("closeFile: {} with {} blocks is persisted to the file system",
        path, file.getBlocks().length);
  }

  /**
   * Periodically calls hasAvailableResources of NameNodeResourceChecker, and if
   * there are found to be insufficient resources available, causes the NN to
   * enter safe mode. If resources are later found to have returned to
   * acceptable levels, this daemon will cause the NN to exit safe mode.
   */
  class NameNodeResourceMonitor implements Runnable  {
    boolean shouldNNRmRun = true;
    @Override
    public void run () {
      try {
        while (fsRunning && shouldNNRmRun) {
          checkAvailableResources();
          if(!nameNodeHasResourcesAvailable()) {
            String lowResourcesMsg = "NameNode low on available disk space. ";
            if (!isInSafeMode()) {
              LOG.warn(lowResourcesMsg + "Entering safe mode.");
            } else {
              LOG.warn(lowResourcesMsg + "Already in safe mode.");
            }
            enterSafeMode(true);
          }
          try {
            Thread.sleep(resourceRecheckInterval);
          } catch (InterruptedException ie) {
            // Deliberately ignore
          }
        }
      } catch (Exception e) {
        FSNamesystem.LOG.error("Exception in NameNodeResourceMonitor: ", e);
      }
    }

    public void stopMonitor() {
      shouldNNRmRun = false;
    }
 }

  class NameNodeEditLogRoller implements Runnable {

    private boolean shouldRun = true;
    private final long rollThreshold;
    private final long sleepIntervalMs;

    public NameNodeEditLogRoller(long rollThreshold, int sleepIntervalMs) {
        this.rollThreshold = rollThreshold;
        this.sleepIntervalMs = sleepIntervalMs;
    }

    @Override
    public void run() {
      while (fsRunning && shouldRun) {
        try {
          long numEdits = getCorrectTransactionsSinceLastLogRoll();
          if (numEdits > rollThreshold) {
            FSNamesystem.LOG.info("NameNode rolling its own edit log because"
                + " number of edits in open segment exceeds threshold of "
                + rollThreshold);
            rollEditLog();
          }
        } catch (Exception e) {
          FSNamesystem.LOG.error("Swallowing exception in "
              + NameNodeEditLogRoller.class.getSimpleName() + ":", e);
        }
        try {
          Thread.sleep(sleepIntervalMs);
        } catch (InterruptedException e) {
          FSNamesystem.LOG.info(NameNodeEditLogRoller.class.getSimpleName()
              + " was interrupted, exiting");
          break;
        }
      }
    }

    public void stop() {
      shouldRun = false;
    }
  }

  /**
   * Daemon to periodically scan the namespace for lazyPersist files
   * with missing blocks and unlink them.
   */
  class LazyPersistFileScrubber implements Runnable {
    private volatile boolean shouldRun = true;
    final int scrubIntervalSec;
    public LazyPersistFileScrubber(final int scrubIntervalSec) {
      this.scrubIntervalSec = scrubIntervalSec;
    }

    /**
     * Periodically go over the list of lazyPersist files with missing
     * blocks and unlink them from the namespace.
     */
    private void clearCorruptLazyPersistFiles()
        throws IOException {

      BlockStoragePolicy lpPolicy = blockManager.getStoragePolicy("LAZY_PERSIST");

      List<BlockCollection> filesToDelete = new ArrayList<>();
      boolean changed = false;
      writeLock();
      try {
        final Iterator<BlockInfo> it =
            blockManager.getCorruptReplicaBlockIterator();

        while (it.hasNext()) {
          Block b = it.next();
          BlockInfo blockInfo = blockManager.getStoredBlock(b);
          if (blockInfo == null || blockInfo.isDeleted()) {
            LOG.info("Cannot find block info for block " + b);
          } else {
            BlockCollection bc = getBlockCollection(blockInfo);
            if (bc.getStoragePolicyID() == lpPolicy.getId()) {
              filesToDelete.add(bc);
            }
          }
        }

        for (BlockCollection bc : filesToDelete) {
          LOG.warn("Removing lazyPersist file " + bc.getName() + " with no replicas.");
          BlocksMapUpdateInfo toRemoveBlocks =
              FSDirDeleteOp.deleteInternal(
                  FSNamesystem.this,
                  INodesInPath.fromINode((INodeFile) bc), false);
          changed |= toRemoveBlocks != null;
          if (toRemoveBlocks != null) {
            blockManager.addBLocksToMarkedDeleteQueue(
                toRemoveBlocks.getToDeleteList());
          }
        }
      } finally {
        writeUnlock("clearCorruptLazyPersistFiles");
      }
      if (changed) {
        getEditLog().logSync();
      }
    }

    @Override
    public void run() {
      while (fsRunning && shouldRun) {
        try {
          if (!isInSafeMode()) {
            clearCorruptLazyPersistFiles();
            // set the timeStamp of last Cycle.
            lazyPersistFileScrubberTS.set(Time.monotonicNow());
          } else {
            if (FSNamesystem.LOG.isDebugEnabled()) {
              FSNamesystem.LOG.debug("Namenode is in safemode, skipping "
                  + "scrubbing of corrupted lazy-persist files.");
            }
          }
        } catch (Exception e) {
          FSNamesystem.LOG.warn(
              "LazyPersistFileScrubber encountered an exception while "
                  + "scanning for lazyPersist files with missing blocks. "
                  + "Scanning will retry in {} seconds.",
              scrubIntervalSec, e);
        }

        try {
          Thread.sleep(scrubIntervalSec * 1000);
        } catch (InterruptedException e) {
          FSNamesystem.LOG.info(
              "LazyPersistFileScrubber was interrupted, exiting");
          break;
        }
      }
    }

    public void stop() {
      shouldRun = false;
    }
  }

  public FSImage getFSImage() {
    return fsImage;
  }

  public FSEditLog getEditLog() {
    return getFSImage().getEditLog();
  }

  private NNStorage getNNStorage() {
    return getFSImage().getStorage();
  }

  @Metric({"MissingBlocks", "Number of missing blocks"})
  public long getMissingBlocksCount() {
    // not locking
    return blockManager.getMissingBlocksCount();
  }

  @Metric({"MissingReplOneBlocks", "Number of missing blocks " +
      "with replication factor 1"})
  public long getMissingReplOneBlocksCount() {
    // not locking
    return blockManager.getMissingReplOneBlocksCount();
  }
  
  @Metric(value = {"ExpiredHeartbeats", "Number of expired heartbeats"},
      type = Metric.Type.COUNTER)
  public int getExpiredHeartbeats() {
    return datanodeStatistics.getExpiredHeartbeats();
  }
  
  @Metric({"TransactionsSinceLastCheckpoint",
      "Number of transactions since last checkpoint"})
  public long getTransactionsSinceLastCheckpoint() {
    return getFSImage().getLastAppliedOrWrittenTxId() -
        getNNStorage().getMostRecentCheckpointTxId();
  }
  
  @Metric({"TransactionsSinceLastLogRoll",
      "Number of transactions since last edit log roll"})
  public long getTransactionsSinceLastLogRoll() {
    if (isInStandbyState() || !getEditLog().isSegmentOpenWithoutLock()) {
      return 0;
    } else {
      return getEditLog().getLastWrittenTxIdWithoutLock() -
          getEditLog().getCurSegmentTxIdWithoutLock() + 1;
    }
  }

  /**
   * Get the correct number of transactions since last edit log roll.
   * This method holds a lock of FSEditLog and must not be used for metrics.
   */
  private long getCorrectTransactionsSinceLastLogRoll() {
    if (isInStandbyState() || !getEditLog().isSegmentOpen()) {
      return 0;
    } else {
      return getEditLog().getLastWrittenTxId() -
          getEditLog().getCurSegmentTxId() + 1;
    }
  }

  @Metric({"LastWrittenTransactionId", "Transaction ID written to the edit log"})
  public long getLastWrittenTransactionId() {
    return getEditLog().getLastWrittenTxIdWithoutLock();
  }
  
  @Metric({"LastCheckpointTime",
      "Time in milliseconds since the epoch of the last checkpoint"})
  public long getLastCheckpointTime() {
    return getNNStorage().getMostRecentCheckpointTime();
  }

  /** @see ClientProtocol#getStats() */
  long[] getStats() {
    final long[] stats = datanodeStatistics.getStats();
    stats[ClientProtocol.GET_STATS_LOW_REDUNDANCY_IDX] =
        getLowRedundancyBlocks();
    stats[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX] =
        getCorruptReplicaBlocks();
    stats[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX] =
        getMissingBlocksCount();
    stats[ClientProtocol.GET_STATS_MISSING_REPL_ONE_BLOCKS_IDX] =
        getMissingReplOneBlocksCount();
    stats[ClientProtocol.GET_STATS_BYTES_IN_FUTURE_BLOCKS_IDX] =
        blockManager.getBytesInFuture();
    stats[ClientProtocol.GET_STATS_PENDING_DELETION_BLOCKS_IDX] =
        blockManager.getPendingDeletionBlocksCount();
    return stats;
  }

  /**
   * Get statistics pertaining to blocks of type {@link BlockType#CONTIGUOUS}
   * in the filesystem.
   * <p>
   * @see ClientProtocol#getReplicatedBlockStats()
   */
  ReplicatedBlockStats getReplicatedBlockStats() {
    return new ReplicatedBlockStats(getLowRedundancyReplicatedBlocks(),
        getCorruptReplicatedBlocks(), getMissingReplicatedBlocks(),
        getMissingReplicationOneBlocks(), getBytesInFutureReplicatedBlocks(),
        getPendingDeletionReplicatedBlocks(),
        getHighestPriorityLowRedundancyReplicatedBlocks());
  }

  /**
   * Get statistics pertaining to blocks of type {@link BlockType#STRIPED}
   * in the filesystem.
   * <p>
   * @see ClientProtocol#getECBlockGroupStats()
   */
  ECBlockGroupStats getECBlockGroupStats() {
    return new ECBlockGroupStats(getLowRedundancyECBlockGroups(),
        getCorruptECBlockGroups(), getMissingECBlockGroups(),
        getBytesInFutureECBlockGroups(), getPendingDeletionECBlocks(),
        getHighestPriorityLowRedundancyECBlocks());
  }

  @Override // FSNamesystemMBean
  @Metric({"CapacityTotal",
      "Total raw capacity of data nodes in bytes"})
  public long getCapacityTotal() {
    return datanodeStatistics.getCapacityTotal();
  }

  @Metric({"CapacityTotalGB",
      "Total raw capacity of data nodes in GB"})
  public float getCapacityTotalGB() {
    return DFSUtil.roundBytesToGB(getCapacityTotal());
  }

  @Override // FSNamesystemMBean
  @Metric({"CapacityUsed",
      "Total used capacity across all data nodes in bytes"})
  public long getCapacityUsed() {
    return datanodeStatistics.getCapacityUsed();
  }

  @Metric({"CapacityUsedGB",
      "Total used capacity across all data nodes in GB"})
  public float getCapacityUsedGB() {
    return DFSUtil.roundBytesToGB(getCapacityUsed());
  }

  @Override // FSNamesystemMBean
  @Metric({"CapacityRemaining", "Remaining capacity in bytes"})
  public long getCapacityRemaining() {
    return datanodeStatistics.getCapacityRemaining();
  }

  @Override // FSNamesystemMBean
  @Metric({"ProvidedCapacityTotal",
      "Total space used in PROVIDED storage in bytes" })
  public long getProvidedCapacityTotal() {
    return datanodeStatistics.getProvidedCapacity();
  }

  @Metric({"CapacityRemainingGB", "Remaining capacity in GB"})
  public float getCapacityRemainingGB() {
    return DFSUtil.roundBytesToGB(getCapacityRemaining());
  }

  @Metric({"CapacityUsedNonDFS",
      "Total space used by data nodes for non DFS purposes in bytes"})
  public long getCapacityUsedNonDFS() {
    return datanodeStatistics.getCapacityUsedNonDFS();
  }

  /**
   * Total number of connections.
   */
  @Override // FSNamesystemMBean
  @Metric
  public int getTotalLoad() {
    return datanodeStatistics.getXceiverCount();
  }
  
  @Metric({ "SnapshottableDirectories", "Number of snapshottable directories" })
  public int getNumSnapshottableDirs() {
    return this.snapshotManager.getNumSnapshottableDirs();
  }

  @Metric({ "Snapshots", "The number of snapshots" })
  public int getNumSnapshots() {
    return this.snapshotManager.getNumSnapshots();
  }

  @Override
  public String getSnapshotStats() {
    Map<String, Object> info = new HashMap<String, Object>();
    info.put("SnapshottableDirectories", this.getNumSnapshottableDirs());
    info.put("Snapshots", this.getNumSnapshots());
    return JSON.toString(info);
  }

  @Override // FSNamesystemMBean
  @Metric({ "NumEncryptionZones", "The number of encryption zones" })
  public int getNumEncryptionZones() {
    return dir.ezManager.getNumEncryptionZones();
  }

  @Override // FSNamesystemMBean
  @Metric({ "CurrentTokensCount", "The number of delegation tokens"})
  public long getCurrentTokensCount() {
    return dtSecretManager != null ?
        dtSecretManager.getCurrentTokensSize() : -1;
  }

  @Override
  @Metric({"PendingSPSPaths", "The number of paths to be processed by storage policy satisfier"})
  public int getPendingSPSPaths() {
    return blockManager.getPendingSPSPaths();
  }

  /**
   * Get the progress of the reconstruction queues initialisation.
   */
  @Override // FSNamesystemMBean
  @Metric
  public float getReconstructionQueuesInitProgress() {
    return blockManager.getReconstructionQueuesInitProgress();
  }

  /**
   * Returns the length of the wait Queue for the FSNameSystemLock.
   *
   * A larger number here indicates lots of threads are waiting for
   * FSNameSystemLock.
   *
   * @return int - Number of Threads waiting to acquire FSNameSystemLock
   */
  @Override
  @Metric({"LockQueueLength", "Number of threads waiting to " +
      "acquire FSNameSystemLock"})
  public int getFsLockQueueLength() {
    return fsLock.getQueueLength();
  }

  @Metric(value = {"ReadLockLongHoldCount", "The number of time " +
          "the read lock has been held for longer than the threshold"},
          type = Metric.Type.COUNTER)
  public long getNumOfReadLockLongHold() {
    return fsLock.getNumOfReadLockLongHold();
  }

  @Metric(value = {"WriteLockLongHoldCount", "The number of time " +
          "the write lock has been held for longer than the threshold"},
          type = Metric.Type.COUNTER)
  public long getNumOfWriteLockLongHold() {
    return fsLock.getNumOfWriteLockLongHold();
  }

  int getNumberOfDatanodes(DatanodeReportType type) {
    readLock();
    try {
      return getBlockManager().getDatanodeManager().getDatanodeListForReport(
          type).size(); 
    } finally {
      readUnlock("getNumberOfDatanodes");
    }
  }

  DatanodeInfo[] slowDataNodesReport() throws IOException {
    String operationName = "slowDataNodesReport";
    DatanodeInfo[] datanodeInfos;
    checkOperation(OperationCategory.UNCHECKED);
    readLock();
    try {
      checkOperation(OperationCategory.UNCHECKED);
      final DatanodeManager dm = getBlockManager().getDatanodeManager();
      final List<DatanodeDescriptor> results = dm.getAllSlowDataNodes();
      datanodeInfos = getDatanodeInfoFromDescriptors(results);
    } finally {
      readUnlock(operationName, getLockReportInfoSupplier(null));
    }
    logAuditEvent(true, operationName, null);
    return datanodeInfos;
  }

  private DatanodeInfo[] getDatanodeInfoFromDescriptors(List<DatanodeDescriptor> results) {
    DatanodeInfo[] datanodeInfos = new DatanodeInfo[results.size()];
    for (int i = 0; i < datanodeInfos.length; i++) {
      datanodeInfos[i] = new DatanodeInfoBuilder().setFrom(results.get(i)).build();
      datanodeInfos[i].setNumBlocks(results.get(i).numBlocks());
    }
    return datanodeInfos;
  }

  DatanodeInfo[] datanodeReport(final DatanodeReportType type)
      throws IOException {
    String operationName = "datanodeReport";
    DatanodeInfo[] arr;
    checkSuperuserPrivilege(operationName);
    checkOperation(OperationCategory.UNCHECKED);
    readLock();
    try {
      checkOperation(OperationCategory.UNCHECKED);
      final DatanodeManager dm = getBlockManager().getDatanodeManager();      
      final List<DatanodeDescriptor> results = dm.getDatanodeListForReport(type);
      arr = getDatanodeInfoFromDescriptors(results);
    } finally {
      readUnlock(operationName, getLockReportInfoSupplier(null));
    }
    logAuditEvent(true, operationName, null);
    return arr;
  }

  DatanodeStorageReport[] getDatanodeStorageReport(final DatanodeReportType type
      ) throws IOException {
    String operationName = "getDatanodeStorageReport";
    DatanodeStorageReport[] reports;
    checkSuperuserPrivilege(operationName);
    checkOperation(OperationCategory.UNCHECKED);
    readLock();
    try {
      checkOperation(OperationCategory.UNCHECKED);
      final DatanodeManager dm = getBlockManager().getDatanodeManager();      
      reports = dm.getDatanodeStorageReport(type);
    } finally {
      readUnlock(operationName, getLockReportInfoSupplier(null));
    }
    logAuditEvent(true, operationName, null);
    return reports;
  }

  /**
   * Save namespace image.
   * This will save current namespace into fsimage file and empty edits file.
   * Requires superuser privilege and safe mode.
   */
  boolean saveNamespace(final long timeWindow, final long txGap)
      throws IOException {
    String operationName = "saveNamespace";
    checkOperation(OperationCategory.UNCHECKED);
    checkSuperuserPrivilege(operationName);

    boolean saved = false;
    cpLock();  // Block if a checkpointing is in progress on standby.
    readLock();
    try {
      checkOperation(OperationCategory.UNCHECKED);

      if (!isInSafeMode()) {
        throw new IOException("Safe mode should be turned ON "
            + "in order to create namespace image.");
      }
      saved = getFSImage().saveNamespace(timeWindow, txGap, this);
    } finally {
      readUnlock(operationName, getLockReportInfoSupplier(null));
      cpUnlock();
    }
    if (saved) {
      LOG.info("New namespace image has been created");
    }
    logAuditEvent(true, operationName, null);
    return saved;
  }
  
  /**
   * Enables/Disables/Checks restoring failed storage replicas if the storage becomes available again.
   * Requires superuser privilege.
   * 
   * @throws AccessControlException if superuser privilege is violated.
   */
  boolean restoreFailedStorage(String arg) throws IOException {
    String operationName = getFailedStorageCommand(arg);
    boolean val = false;
    checkSuperuserPrivilege(operationName);
    checkOperation(OperationCategory.UNCHECKED);
    cpLock();  // Block if a checkpointing is in progress on standby.
    writeLock();
    try {
      checkOperation(OperationCategory.UNCHECKED);
      
      // if it is disabled - enable it and vice versa.
      if(arg.equals("check")) {
        val = getNNStorage().getRestoreFailedStorage();
      } else {
        val = arg.equals("true");  // false if not
        getNNStorage().setRestoreFailedStorage(val);
      }
    } finally {
      writeUnlock(operationName, getLockReportInfoSupplier(null));
      cpUnlock();
    }
    logAuditEvent(true, operationName, null);
    return val;
  }

  Date getStartTime() {
    return new Date(startTime); 
  }
    
  void finalizeUpgrade() throws IOException {
    String operationName = "finalizeUpgrade";
    checkSuperuserPrivilege(operationName);
    checkOperation(OperationCategory.UNCHECKED);
    cpLock();  // Block if a checkpointing is in progress on standby.
    writeLock();
    try {
      checkOperation(OperationCategory.UNCHECKED);
      getFSImage().finalizeUpgrade(this.isHaEnabled() && inActiveState());
    } finally {
      writeUnlock(operationName, getLockReportInfoSupplier(null));
      cpUnlock();
    }
    logAuditEvent(true, operationName, null);
  }

  void refreshNodes() throws IOException {
    String operationName = "refreshNodes";
    checkOperation(OperationCategory.UNCHECKED);
    checkSuperuserPrivilege(operationName);
    getBlockManager().getDatanodeManager().refreshNodes(new HdfsConfiguration());
    logAuditEvent(true, operationName, null);
  }

  void setBalancerBandwidth(long bandwidth) throws IOException {
    String operationName = "setBalancerBandwidth";
    checkOperation(OperationCategory.WRITE);
    checkSuperuserPrivilege(operationName);
    getBlockManager().getDatanodeManager().setBalancerBandwidth(bandwidth);
    logAuditEvent(true, operationName, null);
  }

  boolean setSafeMode(SafeModeAction action) throws IOException {
    String operationName = action.toString().toLowerCase();
    boolean error = false;
    if (action != SafeModeAction.SAFEMODE_GET) {
      checkSuperuserPrivilege(operationName);
      switch(action) {
      case SAFEMODE_LEAVE: // leave safe mode
        leaveSafeMode(false);
        break;
      case SAFEMODE_ENTER: // enter safe mode
        enterSafeMode(false);
        break;
      case SAFEMODE_FORCE_EXIT:
        leaveSafeMode(true);
        break;
      default:
        LOG.error("Unexpected safe mode action");
        error = true;
      }
    }
    if (!error) {
      logAuditEvent(true, operationName, null);
    }
    return isInSafeMode();
  }

  /**
   * Get the total number of blocks in the system. 
   */
  @Override // FSNamesystemMBean
  @Metric
  public long getBlocksTotal() {
    return blockManager.getTotalBlocks();
  }

  /**
   * Get the number of files under construction in the system.
   */
  @Metric({ "NumFilesUnderConstruction",
      "Number of files under construction" })
  public long getNumFilesUnderConstruction() {
    return leaseManager.countPath();
  }

  /**
   * Get the total number of active clients holding lease in the system.
   */
  @Metric({ "NumActiveClients", "Number of active clients holding lease" })
  public long getNumActiveClients() {
    return leaseManager.countLease();
  }

  /**
   * Get the total number of COMPLETE blocks in the system.
   * For safe mode only complete blocks are counted.
   * This is invoked only during NN startup and checkpointing.
   */
  public long getCompleteBlocksTotal() {
    // Calculate number of blocks under construction
    long numUCBlocks = 0;
    readLock();
    try {
      numUCBlocks = leaseManager.getNumUnderConstructionBlocks();
      return getBlocksTotal() - numUCBlocks;
    } finally {
      readUnlock("getCompleteBlocksTotal");
    }
  }


  @Override
  public boolean isInSafeMode() {
    return isInManualOrResourceLowSafeMode() || blockManager.isInSafeMode();
  }

  @Override
  public boolean isInStartupSafeMode() {
    return !isInManualOrResourceLowSafeMode() && blockManager.isInSafeMode();
  }

  /**
   * Enter safe mode. If resourcesLow is false, then we assume it is manual
   * @throws IOException
   */
  void enterSafeMode(boolean resourcesLow) throws IOException {
    writeLock();
    try {
      // Stop the secret manager, since rolling the master key would
      // try to write to the edit log
      stopSecretManager();

      // Ensure that any concurrent operations have been fully synced
      // before entering safe mode. This ensures that the FSImage
      // is entirely stable on disk as soon as we're in safe mode.
      boolean isEditlogOpenForWrite = getEditLog().isOpenForWrite();
      // Before Editlog is in OpenForWrite mode, editLogStream will be null. So,
      // logSyncAll call can be called only when Edlitlog is in OpenForWrite mode
      if (isEditlogOpenForWrite) {
        getEditLog().logSyncAll();
      }
      setManualAndResourceLowSafeMode(!resourcesLow, resourcesLow);
      NameNode.stateChangeLog.info("STATE* Safe mode is ON.\n" +
          getSafeModeTip());
    } finally {
      writeUnlock("enterSafeMode", getLockReportInfoSupplier(null));
    }
  }

  /**
   * Leave safe mode.
   * @param force true if to leave safe mode forcefully with -forceExit option
   */
  void leaveSafeMode(boolean force) {
    writeLock();
    try {
      if (!isInSafeMode()) {
        NameNode.stateChangeLog.info("STATE* Safe mode is already OFF"); 
        return;
      }
      if (blockManager.leaveSafeMode(force)) {
        setManualAndResourceLowSafeMode(false, false);
        startSecretManagerIfNecessary();
      }
    } finally {
      writeUnlock("leaveSafeMode", getLockReportInfoSupplier(null));
    }
  }

  String getSafeModeTip() {
    String cmd = "Use \"hdfs dfsadmin -safemode leave\" to turn safe mode off.";
    synchronized (this) {
      if (resourceLowSafeMode) {
        return "Resources are low on NN. Please add or free up more resources"
            + "then turn off safe mode manually. NOTE:  If you turn off safe "
            + "mode before adding resources, the NN will immediately return to "
            + "safe mode. " + cmd;
      } else if (manualSafeMode) {
        return "It was turned on manually. " + cmd;
      }
    }

    return blockManager.getSafeModeTip();
  }

  /**
   * @return true iff it is in manual safe mode or resource low safe mode.
   */
  private synchronized boolean isInManualOrResourceLowSafeMode() {
    return manualSafeMode || resourceLowSafeMode;
  }

  private synchronized void setManualAndResourceLowSafeMode(boolean manual,
      boolean resourceLow) {
    this.manualSafeMode = manual;
    this.resourceLowSafeMode = resourceLow;
  }

  CheckpointSignature rollEditLog() throws IOException {
    String operationName = "rollEditLog";
    CheckpointSignature result = null;
    checkSuperuserPrivilege(operationName);
    checkOperation(OperationCategory.JOURNAL);
    writeLock();
    try {
      checkOperation(OperationCategory.JOURNAL);
      checkNameNodeSafeMode("Log not rolled");
      if (Server.isRpcInvocation()) {
        LOG.info("Roll Edit Log from " + Server.getRemoteAddress());
      }
      result = getFSImage().rollEditLog(getEffectiveLayoutVersion());
    } finally {
      writeUnlock(operationName, getLockReportInfoSupplier(null));
    }
    logAuditEvent(true, operationName, null);
    return result;
  }

  NamenodeCommand startCheckpoint(NamenodeRegistration backupNode,
      NamenodeRegistration activeNamenode) throws IOException {
    checkOperation(OperationCategory.CHECKPOINT);
    writeLock();
    try {
      checkOperation(OperationCategory.CHECKPOINT);
      checkNameNodeSafeMode("Checkpoint not started");
      
      LOG.info("Start checkpoint for " + backupNode.getAddress());
      NamenodeCommand cmd = getFSImage().startCheckpoint(backupNode,
          activeNamenode, getEffectiveLayoutVersion());
      getEditLog().logSync();
      return cmd;
    } finally {
      writeUnlock("startCheckpoint");
    }
  }

  public void processIncrementalBlockReport(final DatanodeID nodeID,
      final StorageReceivedDeletedBlocks srdb)
      throws IOException {
    writeLock();
    try {
      blockManager.processIncrementalBlockReport(nodeID, srdb);
    } finally {
      writeUnlock("processIncrementalBlockReport");
    }
  }
  
  void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException {
    checkOperation(OperationCategory.CHECKPOINT);
    readLock();
    try {
      checkOperation(OperationCategory.CHECKPOINT);
      checkNameNodeSafeMode("Checkpoint not ended");
      LOG.info("End checkpoint for " + registration.getAddress());
      getFSImage().endCheckpoint(sig);
    } finally {
      readUnlock("endCheckpoint");
    }
  }

  PermissionStatus createFsOwnerPermissions(FsPermission permission) {
    return new PermissionStatus(fsOwner.getShortUserName(), supergroup, permission);
  }

  /**
   * This method is retained for backward compatibility.
   * Please use {@link #checkSuperuserPrivilege(String)} instead.
   *
   * @throws AccessControlException if user is not a super user.
   */
  void checkSuperuserPrivilege() throws AccessControlException {
    if (isPermissionEnabled) {
      FSPermissionChecker pc = getPermissionChecker();
      pc.checkSuperuserPrivilege(null);
    }
  }

  void checkSuperuserPrivilege(String operationName)
      throws IOException {
    checkSuperuserPrivilege(operationName, null);
  }

  /**
   * Check to see if we have exceeded the limit on the number
   * of inodes.
   */
  void checkFsObjectLimit() throws IOException {
    if (maxFsObjects != 0 &&
        maxFsObjects <= dir.totalInodes() + getBlocksTotal()) {
      throw new IOException("Exceeded the configured number of objects " +
                             maxFsObjects + " in the filesystem.");
    }
  }

  @Override // FSNamesystemMBean
  public long getMaxObjects() {
    return maxFsObjects;
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getFilesTotal() {
    // There is no need to take fSNamesystem's lock as
    // FSDirectory has its own lock.
    return this.dir.totalInodes();
  }

  /**
   * Get aggregated count of all blocks pending to be reconstructed.
   */
  @Override // FSNamesystemMBean
  @Metric
  @Deprecated
  public long getPendingReplicationBlocks() {
    return blockManager.getPendingReconstructionBlocksCount();
  }

  /**
   * Get aggregated count of all blocks pending to be reconstructed.
   */
  @Override // FSNamesystemMBean
  @Metric
  public long getPendingReconstructionBlocks() {
    return blockManager.getPendingReconstructionBlocksCount();
  }

  /**
   * Get aggregated count of all blocks with low redundancy.
   * @deprecated - Use {@link #getLowRedundancyBlocks()} instead.
   */
  @Override // FSNamesystemMBean
  @Metric
  @Deprecated
  public long getUnderReplicatedBlocks() {
    return blockManager.getLowRedundancyBlocksCount();
  }

  /**
   * Get aggregated count of all blocks with low redundancy.
   */
  @Override // FSNamesystemMBean
  @Metric
  public long getLowRedundancyBlocks() {
    return blockManager.getLowRedundancyBlocksCount();
  }

  /** Returns number of blocks with corrupt replicas */
  @Metric({"CorruptBlocks", "Number of blocks with corrupt replicas"})
  public long getCorruptReplicaBlocks() {
    return blockManager.getCorruptReplicaBlocksCount();
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getScheduledReplicationBlocks() {
    return blockManager.getScheduledReplicationBlocksCount();
  }

  @Override
  @Metric
  public long getPendingDeletionBlocks() {
    return blockManager.getPendingDeletionBlocksCount();
  }

  @Override // ReplicatedBlocksMBean
  @Metric({"LowRedundancyReplicatedBlocks",
      "Number of low redundancy replicated blocks"})
  public long getLowRedundancyReplicatedBlocks() {
    return blockManager.getLowRedundancyBlocks();
  }

  @Override // ReplicatedBlocksMBean
  @Metric({"CorruptReplicatedBlocks", "Number of corrupted replicated blocks"})
  public long getCorruptReplicatedBlocks() {
    return blockManager.getCorruptBlocks();
  }

  @Override // ReplicatedBlocksMBean
  @Metric({"MissingReplicatedBlocks", "Number of missing replicated blocks"})
  public long getMissingReplicatedBlocks() {
    return blockManager.getMissingBlocks();
  }

  @Override // ReplicatedBlocksMBean
  @Metric({"MissingReplicationOneBlocks", "Number of missing replicated " +
      "blocks with replication factor 1"})
  public long getMissingReplicationOneBlocks() {
    return blockManager.getMissingReplicationOneBlocks();
  }

  @Override // ReplicatedBlocksMBean
  @Metric({"HighestPriorityLowRedundancyReplicatedBlocks", "Number of " +
      "replicated blocks which have the highest risk of loss."})
  public long getHighestPriorityLowRedundancyReplicatedBlocks() {
    return blockManager.getHighestPriorityReplicatedBlockCount();
  }

  @Override // ReplicatedBlocksMBean
  @Metric({"HighestPriorityLowRedundancyECBlocks", "Number of erasure coded " +
      "blocks which have the highest risk of loss."})
  public long getHighestPriorityLowRedundancyECBlocks() {
    return blockManager.getHighestPriorityECBlockCount();
  }

  @Override // ReplicatedBlocksMBean
  @Metric({"BytesInFutureReplicatedBlocks", "Total bytes in replicated " +
      "blocks with future generation stamp"})
  public long getBytesInFutureReplicatedBlocks() {
    return blockManager.getBytesInFutureReplicatedBlocks();
  }

  @Override // ReplicatedBlocksMBean
  @Metric({"PendingDeletionReplicatedBlocks", "Number of replicated blocks " +
      "that are pending deletion"})
  public long getPendingDeletionReplicatedBlocks() {
    return blockManager.getPendingDeletionReplicatedBlocks();
  }

  @Override // ReplicatedBlocksMBean
  @Metric({"TotalReplicatedBlocks", "Total number of replicated blocks"})
  public long getTotalReplicatedBlocks() {
    return blockManager.getTotalReplicatedBlocks();
  }

  @Override // ECBlockGroupsMBean
  @Metric({"LowRedundancyECBlockGroups", "Number of erasure coded block " +
      "groups with low redundancy"})
  public long getLowRedundancyECBlockGroups() {
    return blockManager.getLowRedundancyECBlockGroups();
  }

  @Override // ECBlockGroupsMBean
  @Metric({"CorruptECBlockGroups", "Number of erasure coded block groups that" +
      " are corrupt"})
  public long getCorruptECBlockGroups() {
    return blockManager.getCorruptECBlockGroups();
  }

  @Override // ECBlockGroupsMBean
  @Metric({"MissingECBlockGroups", "Number of erasure coded block groups that" +
      " are missing"})
  public long getMissingECBlockGroups() {
    return blockManager.getMissingECBlockGroups();
  }

  @Override // ECBlockGroupsMBean
  @Metric({"BytesInFutureECBlockGroups", "Total bytes in erasure coded block " +
      "groups with future generation stamp"})
  public long getBytesInFutureECBlockGroups() {
    return blockManager.getBytesInFutureECBlockGroups();
  }

  @Override // ECBlockGroupsMBean
  @Metric({"PendingDeletionECBlocks", "Number of erasure coded blocks " +
      "that are pending deletion"})
  public long getPendingDeletionECBlocks() {
    return blockManager.getPendingDeletionECBlocks();
  }

  @Override // ECBlockGroupsMBean
  @Metric({"TotalECBlockGroups", "Total number of erasure coded block groups"})
  public long getTotalECBlockGroups() {
    return blockManager.getTotalECBlockGroups();
  }

  /**
   * Get the enabled erasure coding policies separated with comma.
   */
  @Override // ECBlockGroupsMBean
  @Metric({"EnabledEcPolicies", "Enabled erasure coding policies"})
  public String getEnabledEcPolicies() {
    return getErasureCodingPolicyManager().getEnabledPoliciesMetric();
  }

  @Override
  public long getBlockDeletionStartTime() {
    return startTime + blockManager.getStartupDelayBlockDeletionInMs();
  }

  @Metric
  public long getExcessBlocks() {
    return blockManager.getExcessBlocksCount();
  }

  @Metric
  public long getNumTimedOutPendingReconstructions() {
    return blockManager.getNumTimedOutPendingReconstructions();
  }
  
  // HA-only metric
  @Metric
  public long getPostponedMisreplicatedBlocks() {
    return blockManager.getPostponedMisreplicatedBlocksCount();
  }

  // HA-only metric
  @Metric
  public int getPendingDataNodeMessageCount() {
    return blockManager.getPendingDataNodeMessageCount();
  }
  
  // HA-only metric
  @Metric
  public String getHAState() {
    return haContext.getState().toString();
  }

  // HA-only metric
  @Metric
  public long getMillisSinceLastLoadedEdits() {
    if (isInStandbyState() && editLogTailer != null) {
      return monotonicNow() - editLogTailer.getLastLoadTimeMs();
    } else {
      return 0;
    }
  }

  @Metric
  public int getBlockCapacity() {
    return blockManager.getCapacity();
  }

  public HAServiceState getState() {
    return haContext == null ? null : haContext.getState().getServiceState();
  }

  @Override // FSNamesystemMBean
  public String getFSState() {
    return isInSafeMode() ? "safeMode" : "Operational";
  }
  
  private ObjectName namesystemMBeanName, replicatedBlocksMBeanName,
      ecBlockGroupsMBeanName, namenodeMXBeanName;

  /**
   * Register following MBeans with their respective names.
   * FSNamesystemMBean:
   *        "hadoop:service=NameNode,name=FSNamesystemState"
   * ReplicatedBlocksMBean:
   *        "hadoop:service=NameNode,name=ReplicatedBlocksState"
   * ECBlockGroupsMBean:
   *        "hadoop:service=NameNode,name=ECBlockGroupsState"
   */
  private void registerMBean() {
    // We can only implement one MXBean interface, so we keep the old one.
    try {
      StandardMBean namesystemBean = new StandardMBean(
          this, FSNamesystemMBean.class);
      StandardMBean replicaBean = new StandardMBean(
          this, ReplicatedBlocksMBean.class);
      StandardMBean ecBean = new StandardMBean(
          this, ECBlockGroupsMBean.class);
      namesystemMBeanName = MBeans.register(
          "NameNode", "FSNamesystemState", namesystemBean);
      replicatedBlocksMBeanName = MBeans.register(
          "NameNode", "ReplicatedBlocksState", replicaBean);
      ecBlockGroupsMBeanName = MBeans.register(
          "NameNode", "ECBlockGroupsState", ecBean);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad MBean setup", e);
    }
    LOG.info("Registered FSNamesystemState, ReplicatedBlocksState and " +
        "ECBlockGroupsState MBeans.");
  }

  /**
   * Shutdown FSNamesystem.
   */
  void shutdown() {
    if (snapshotDeletionGc != null) {
      snapshotDeletionGc.cancel();
    }
    if (snapshotManager != null) {
      snapshotManager.shutdown();
    }
    if (namesystemMBeanName != null) {
      MBeans.unregister(namesystemMBeanName);
      namesystemMBeanName = null;
    }
    if (replicatedBlocksMBeanName != null) {
      MBeans.unregister(replicatedBlocksMBeanName);
      replicatedBlocksMBeanName = null;
    }
    if (ecBlockGroupsMBeanName != null) {
      MBeans.unregister(ecBlockGroupsMBeanName);
      ecBlockGroupsMBeanName = null;
    }
    if (namenodeMXBeanName != null) {
      MBeans.unregister(namenodeMXBeanName);
      namenodeMXBeanName = null;
    }
    if (dir != null) {
      dir.shutdown();
    }
    if (blockManager != null) {
      blockManager.shutdown();
    }
    if (provider != null) {
      try {
        provider.close();
      } catch (IOException e) {
        LOG.error("Failed to close provider.", e);
      }
    }
  }

  @Override // FSNamesystemMBean
  @Metric({"NumLiveDataNodes", "Number of datanodes which are currently live"})
  public int getNumLiveDataNodes() {
    return getBlockManager().getDatanodeManager().getNumLiveDataNodes();
  }

  @Override // FSNamesystemMBean
  @Metric({"NumDeadDataNodes", "Number of datanodes which are currently dead"})
  public int getNumDeadDataNodes() {
    return getBlockManager().getDatanodeManager().getNumDeadDataNodes();
  }
  
  @Override // FSNamesystemMBean
  @Metric({"NumDecomLiveDataNodes",
      "Number of datanodes which have been decommissioned and are now live"})
  public int getNumDecomLiveDataNodes() {
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    getBlockManager().getDatanodeManager().fetchDatanodes(live, null, false);
    int liveDecommissioned = 0;
    for (DatanodeDescriptor node : live) {
      liveDecommissioned += node.isDecommissioned() ? 1 : 0;
    }
    return liveDecommissioned;
  }

  @Override // FSNamesystemMBean
  @Metric({"NumDecomDeadDataNodes",
      "Number of datanodes which have been decommissioned and are now dead"})
  public int getNumDecomDeadDataNodes() {
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    getBlockManager().getDatanodeManager().fetchDatanodes(null, dead, false);
    int deadDecommissioned = 0;
    for (DatanodeDescriptor node : dead) {
      deadDecommissioned += node.isDecommissioned() ? 1 : 0;
    }
    return deadDecommissioned;
  }

  @Override // FSNamesystemMBean
  @Metric({"NumInServiceLiveDataNodes",
      "Number of live datanodes which are currently in service"})
  public int getNumInServiceLiveDataNodes() {
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    getBlockManager().getDatanodeManager().fetchDatanodes(live, null, true);
    int liveInService = live.size();
    for (DatanodeDescriptor node : live) {
      liveInService -= node.isInMaintenance() ? 1 : 0;
    }
    return liveInService;
  }

  @Override // FSNamesystemMBean
  @Metric({"VolumeFailuresTotal",
      "Total number of volume failures across all Datanodes"})
  public int getVolumeFailuresTotal() {
    List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    getBlockManager().getDatanodeManager().fetchDatanodes(live, null, false);
    int volumeFailuresTotal = 0;
    for (DatanodeDescriptor node: live) {
      volumeFailuresTotal += node.getVolumeFailures();
    }
    return volumeFailuresTotal;
  }

  @Override // FSNamesystemMBean
  @Metric({"EstimatedCapacityLostTotal",
      "An estimate of the total capacity lost due to volume failures"})
  public long getEstimatedCapacityLostTotal() {
    List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    getBlockManager().getDatanodeManager().fetchDatanodes(live, null, false);
    long estimatedCapacityLostTotal = 0;
    for (DatanodeDescriptor node: live) {
      VolumeFailureSummary volumeFailureSummary = node.getVolumeFailureSummary();
      if (volumeFailureSummary != null) {
        estimatedCapacityLostTotal +=
            volumeFailureSummary.getEstimatedCapacityLostTotal();
      }
    }
    return estimatedCapacityLostTotal;
  }

  @Override // FSNamesystemMBean
  @Metric({"NumDecommissioningDataNodes",
      "Number of datanodes in decommissioning state"})
  public int getNumDecommissioningDataNodes() {
    return getBlockManager().getDatanodeManager().getDecommissioningNodes()
        .size();
  }

  @Override // FSNamesystemMBean
  @Metric({"StaleDataNodes", 
    "Number of datanodes marked stale due to delayed heartbeat"})
  public int getNumStaleDataNodes() {
    return getBlockManager().getDatanodeManager().getNumStaleNodes();
  }

  /**
   * Storages are marked as "content stale" after NN restart or fails over and
   * before NN receives the first Heartbeat followed by the first Blockreport.
   */
  @Override // FSNamesystemMBean
  @Metric({"NumStaleStorages",
      "Number of storages marked as content stale"})
  public int getNumStaleStorages() {
    return getBlockManager().getDatanodeManager().getNumStaleStorages();
  }

  @Override // FSNamesystemMBean
  public String getTopUserOpCounts() {
    if (!topConf.isEnabled) {
      return null;
    }

    Date now = new Date();
    final List<RollingWindowManager.TopWindow> topWindows =
        topMetrics.getTopWindows();
    Map<String, Object> topMap = new TreeMap<String, Object>();
    topMap.put("windows", topWindows);
    topMap.put("timestamp", DFSUtil.dateToIso8601String(now));
    try {
      return JsonUtil.toJsonString(topMap);
    } catch (IOException e) {
      LOG.warn("Failed to fetch TopUser metrics", e);
    }
    return null;
  }

  /**
   * Increments, logs and then returns the stamp.
   */
  long nextGenerationStamp(boolean legacyBlock)
      throws IOException {
    assert hasWriteLock();
    checkNameNodeSafeMode("Cannot get next generation stamp");

    long gs = blockManager.nextGenerationStamp(legacyBlock);
    if (legacyBlock) {
      getEditLog().logLegacyGenerationStamp(gs);
    } else {
      getEditLog().logGenerationStamp(gs);
    }

    // NB: callers sync the log
    return gs;
  }

  /**
   * Increments, logs and then returns the block ID
   * @param blockType is the file under striping or contiguous layout?
   */
  private long nextBlockId(BlockType blockType) throws IOException {
    assert hasWriteLock();
    checkNameNodeSafeMode("Cannot get next block ID");
    final long blockId = blockManager.nextBlockId(blockType);
    getEditLog().logAllocateBlockId(blockId);
    // NB: callers sync the log
    return blockId;
  }

  boolean isFileDeleted(INodeFile file) {
    assert hasReadLock();
    // Not in the inodeMap or in the snapshot but marked deleted.
    if (dir.getInode(file.getId()) == null) {
      return true;
    }

    // look at the path hierarchy to see if one parent is deleted by recursive
    // deletion
    INode tmpChild = file;
    INodeDirectory tmpParent = file.getParent();
    while (true) {
      if (tmpParent == null) {
        return true;
      }

      INode childINode = tmpParent.getChild(tmpChild.getLocalNameBytes(),
          Snapshot.CURRENT_STATE_ID);
      if (childINode == null || !childINode.equals(tmpChild)) {
        // a newly created INode with the same name as an already deleted one
        // would be a different INode than the deleted one
        return true;
      }

      if (tmpParent.isRoot()) {
        break;
      }

      tmpChild = tmpParent;
      tmpParent = tmpParent.getParent();
    }

    if (file.isWithSnapshot() &&
        file.getFileWithSnapshotFeature().isCurrentFileDeleted()) {
      return true;
    }
    return false;
  }

  private INodeFile checkUCBlock(ExtendedBlock block,
      String clientName) throws IOException {
    assert hasWriteLock();
    checkNameNodeSafeMode("Cannot get a new generation stamp and an "
        + "access token for block " + block);
    
    // check stored block state
    BlockInfo storedBlock = getStoredBlock(ExtendedBlock.getLocalBlock(block));
    if (storedBlock == null) {
      throw new IOException(block + " does not exist.");
    }
    if (storedBlock.getBlockUCState() != BlockUCState.UNDER_CONSTRUCTION) {
      throw new IOException("Unexpected BlockUCState: " + block
          + " is " + storedBlock.getBlockUCState()
          + " but not " + BlockUCState.UNDER_CONSTRUCTION);
    }
    
    // check file inode
    final INodeFile file = getBlockCollection(storedBlock);
    if (file == null || !file.isUnderConstruction() || isFileDeleted(file)) {
      throw new IOException("The file " + storedBlock + 
          " belonged to does not exist or it is not under construction.");
    }
    
    // check lease
    if (clientName == null
        || !clientName.equals(file.getFileUnderConstructionFeature()
            .getClientName())) {
      throw new LeaseExpiredException("Lease mismatch: " + block + 
          " is accessed by a non lease holder " + clientName); 
    }

    return file;
  }
  
  /**
   * Client is reporting some bad block locations.
   */
  void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      for (int i = 0; i < blocks.length; i++) {
        ExtendedBlock blk = blocks[i].getBlock();
        DatanodeInfo[] nodes = blocks[i].getLocations();
        String[] storageIDs = blocks[i].getStorageIDs();
        for (int j = 0; j < nodes.length; j++) {
          NameNode.stateChangeLog.info("*DIR* reportBadBlocks for block: {} on"
              + " datanode: {}", blk, nodes[j].getXferAddr());
          blockManager.findAndMarkBlockAsCorrupt(blk, nodes[j],
              storageIDs == null ? null: storageIDs[j],
              "client machine reported it");
        }
      }
    } finally {
      writeUnlock("reportBadBlocks");
    }
  }

  /**
   * Get a new generation stamp together with an access token for 
   * a block under construction.
   * 
   * This method is called for recovering a failed write or setting up
   * a block for appended.
   * 
   * @param block a block
   * @param clientName the name of a client
   * @return a located block with a new generation stamp and an access token
   * @throws IOException if any error occurs
   */
  LocatedBlock bumpBlockGenerationStamp(ExtendedBlock block,
      String clientName) throws IOException {
    final LocatedBlock locatedBlock;
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      // check vadility of parameters
      final INodeFile file = checkUCBlock(block, clientName);
  
      // get a new generation stamp and an access token
      block.setGenerationStamp(nextGenerationStamp(
          blockManager.isLegacyBlock(block.getLocalBlock())));

      BlockInfo lastBlockInfo = file.getLastBlock();
      locatedBlock = BlockManager.newLocatedBlock(block, lastBlockInfo,
          null, -1);
      if (lastBlockInfo.isStriped() &&
          ((BlockInfoStriped) lastBlockInfo).getTotalBlockNum() >
              ((LocatedStripedBlock) locatedBlock).getBlockIndices().length) {
        // The location info in BlockUnderConstructionFeature may not be
        // complete after a failover, so we just return all block tokens for a
        // striped block. This will disrupt the correspondence between
        // LocatedStripedBlock.blockIndices and LocatedStripedBlock.locs,
        // which is not used in client side. The correspondence between
        // LocatedStripedBlock.blockIndices and LocatedBlock.blockToken is
        // ensured.
        byte[] indices =
            new byte[((BlockInfoStriped) lastBlockInfo).getTotalBlockNum()];
        for (int i = 0; i < indices.length; ++i) {
          indices[i] = (byte) i;
        }
        ((LocatedStripedBlock) locatedBlock).setBlockIndices(indices);
      }
      blockManager.setBlockToken(locatedBlock,
          BlockTokenIdentifier.AccessMode.WRITE);
    } finally {
      writeUnlock("bumpBlockGenerationStamp");
    }
    // Ensure we record the new generation stamp
    getEditLog().logSync();
    LOG.info("bumpBlockGenerationStamp({}, client={}) success",
        locatedBlock.getBlock(), clientName);
    return locatedBlock;
  }
  
  /**
   * Update a pipeline for a block under construction.
   * 
   * @param clientName the name of the client
   * @param oldBlock and old block
   * @param newBlock a new block with a new generation stamp and length
   * @param newNodes datanodes in the pipeline
   * @throws IOException if any error occurs
   */
  void updatePipeline(
      String clientName, ExtendedBlock oldBlock, ExtendedBlock newBlock,
      DatanodeID[] newNodes, String[] newStorageIDs, boolean logRetryCache)
      throws IOException {
    checkOperation(OperationCategory.WRITE);
    LOG.info("updatePipeline(" + oldBlock.getLocalBlock()
             + ", newGS=" + newBlock.getGenerationStamp()
             + ", newLength=" + newBlock.getNumBytes()
             + ", newNodes=" + Arrays.asList(newNodes)
             + ", client=" + clientName
             + ")");
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Pipeline not updated");
      assert newBlock.getBlockId()==oldBlock.getBlockId() : newBlock + " and "
        + oldBlock + " has different block identifier";
      updatePipelineInternal(clientName, oldBlock, newBlock, newNodes,
          newStorageIDs, logRetryCache);
    } finally {
      writeUnlock("updatePipeline");
    }
    getEditLog().logSync();
    LOG.info("updatePipeline(" + oldBlock.getLocalBlock() + " => "
        + newBlock.getLocalBlock() + ") success");
  }

  private void updatePipelineInternal(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs,
      boolean logRetryCache)
      throws IOException {
    assert hasWriteLock();
    // check the vadility of the block and lease holder name
    final INodeFile pendingFile = checkUCBlock(oldBlock, clientName);
    final String src = pendingFile.getFullPathName();
    final BlockInfo lastBlock = pendingFile.getLastBlock();
    assert !lastBlock.isComplete();

    // check new GS & length: this is not expected
    if (newBlock.getGenerationStamp() <= lastBlock.getGenerationStamp()) {
      final String msg = "Update " + oldBlock + " but the new block " + newBlock
          + " does not have a larger generation stamp than the last block "
          + lastBlock;
      LOG.warn(msg);
      throw new IOException(msg);
    }
    if (newBlock.getNumBytes() < lastBlock.getNumBytes()) {
      final String msg = "Update " + oldBlock + " (size="
          + oldBlock.getNumBytes() + ") to a smaller size block " + newBlock
          + " (size=" + newBlock.getNumBytes() + ")";
      LOG.warn(msg);
      throw new IOException(msg);
    }

    // Update old block with the new generation stamp and new length
    blockManager.updateLastBlock(lastBlock, newBlock);

    // find the DatanodeDescriptor objects
    final DatanodeStorageInfo[] storages = blockManager.getDatanodeManager()
        .getDatanodeStorageInfos(newNodes, newStorageIDs,
            "src=%s, oldBlock=%s, newBlock=%s, clientName=%s",
            src, oldBlock, newBlock, clientName);
    lastBlock.getUnderConstructionFeature().setExpectedLocations(lastBlock,
        storages, lastBlock.getBlockType());

    FSDirWriteFileOp.persistBlocks(dir, src, pendingFile, logRetryCache);
  }

  /**
   * Register a Backup name-node, verifying that it belongs
   * to the correct namespace, and adding it to the set of
   * active journals if necessary.
   * 
   * @param bnReg registration of the new BackupNode
   * @param nnReg registration of this NameNode
   * @throws IOException if the namespace IDs do not match
   */
  void registerBackupNode(NamenodeRegistration bnReg,
      NamenodeRegistration nnReg) throws IOException {
    writeLock();
    try {
      if(getNNStorage().getNamespaceID()
         != bnReg.getNamespaceID())
        throw new IOException("Incompatible namespaceIDs: "
            + " Namenode namespaceID = "
            + getNNStorage().getNamespaceID() + "; "
            + bnReg.getRole() +
            " node namespaceID = " + bnReg.getNamespaceID());
      if (bnReg.getRole() == NamenodeRole.BACKUP) {
        getFSImage().getEditLog().registerBackupNode(
            bnReg, nnReg);
      }
    } finally {
      writeUnlock("registerBackupNode");
    }
  }

  /**
   * Release (unregister) backup node.
   * <p>
   * Find and remove the backup stream corresponding to the node.
   * @throws IOException
   */
  void releaseBackupNode(NamenodeRegistration registration)
    throws IOException {
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      if(getNNStorage().getNamespaceID()
         != registration.getNamespaceID())
        throw new IOException("Incompatible namespaceIDs: "
            + " Namenode namespaceID = "
            + getNNStorage().getNamespaceID() + "; "
            + registration.getRole() +
            " node namespaceID = " + registration.getNamespaceID());
      getEditLog().releaseBackupStream(registration);
    } finally {
      writeUnlock("releaseBackupNode");
    }
  }

  static class CorruptFileBlockInfo {
    final String path;
    final Block block;
    
    public CorruptFileBlockInfo(String p, Block b) {
      path = p;
      block = b;
    }
    
    @Override
    public String toString() {
      return block.getBlockName() + "\t" + path;
    }
  }
  /**
   * @param path Restrict corrupt files to this portion of namespace.
   * @param cookieTab Support for continuation; cookieTab  tells where
   *                  to start from
   * @return a list in which each entry describes a corrupt file/block
   * @throws IOException
   */
  Collection<CorruptFileBlockInfo> listCorruptFileBlocks(String path,
  String[] cookieTab) throws IOException {
    final String operationName = "listCorruptFileBlocks";
    checkSuperuserPrivilege(operationName, path);
    checkOperation(OperationCategory.READ);

    int count = 0;
    ArrayList<CorruptFileBlockInfo> corruptFiles =
        new ArrayList<CorruptFileBlockInfo>();
    if (cookieTab == null) {
      cookieTab = new String[] { null };
    }

    // Do a quick check if there are any corrupt files without taking the lock
    if (blockManager.getMissingBlocksCount() == 0) {
      if (cookieTab[0] == null) {
        cookieTab[0] = String.valueOf(getIntCookie(cookieTab[0]));
      }
      LOG.debug("there are no corrupt file blocks.");
      return corruptFiles;
    }

    readLock();
    try {
      checkOperation(OperationCategory.READ);
      if (!blockManager.isPopulatingReplQueues()) {
        throw new IOException("Cannot run listCorruptFileBlocks because " +
                              "replication queues have not been initialized.");
      }
      // print a limited # of corrupt files per call

      final Iterator<BlockInfo> blkIterator =
          blockManager.getCorruptReplicaBlockIterator();

      int skip = getIntCookie(cookieTab[0]);
      for (int i = 0; i < skip && blkIterator.hasNext(); i++) {
        blkIterator.next();
      }

      while (blkIterator.hasNext()) {
        BlockInfo blk = blkIterator.next();
        final INodeFile inode = getBlockCollection(blk);
        skip++;
        if (inode != null) {
          String src = inode.getFullPathName();
          if (isParentEntry(src, path)) {
            corruptFiles.add(new CorruptFileBlockInfo(src, blk));
            count++;
            if (count >= maxCorruptFileBlocksReturn)
              break;
          }
        }
      }
      cookieTab[0] = String.valueOf(skip);
      LOG.debug("list corrupt file blocks returned: {}", count);
      return corruptFiles;
    } finally {
      readUnlock("listCorruptFileBlocks");
    }
  }

  /**
   * Convert string cookie to integer.
   */
  private static int getIntCookie(String cookie){
    int c;
    if(cookie == null){
      c = 0;
    } else {
      try{
        c = Integer.parseInt(cookie);
      }catch (NumberFormatException e) {
        c = 0;
      }
    }
    c = Math.max(0, c);
    return c;
  }

  /**
   * Create delegation token secret manager
   */
  private DelegationTokenSecretManager createDelegationTokenSecretManager(
      Configuration conf) {
    return new DelegationTokenSecretManager(conf.getLong(
        DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY,
        DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT),
        conf.getLong(DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY,
            DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT),
        conf.getLong(DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
            DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT),
        DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL,
        conf.getBoolean(DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY,
            DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT),
        this);
  }

  /**
   * Returns the DelegationTokenSecretManager instance in the namesystem.
   * @return delegation token secret manager object
   */
  DelegationTokenSecretManager getDelegationTokenSecretManager() {
    return dtSecretManager;
  }

  /**
   * @param renewer Renewer information
   * @return delegation toek
   * @throws IOException on error
   */
  Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    final String operationName = "getDelegationToken";
    String tokenId = null;
    Token<DelegationTokenIdentifier> token;
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot issue delegation token");
      if (!isAllowedDelegationTokenOp()) {
        throw new IOException(
          "Delegation Token can be issued only with kerberos or web authentication");
      }
      if (dtSecretManager == null || !dtSecretManager.isRunning()) {
        LOG.warn("trying to get DT with no secret manager running");
        return null;
      }

      UserGroupInformation ugi = getRemoteUser();
      String user = ugi.getUserName();
      Text owner = new Text(user);
      Text realUser = null;
      if (ugi.getRealUser() != null) {
        realUser = new Text(ugi.getRealUser().getUserName());
      }
      DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(owner,
        renewer, realUser);
      token = new Token<DelegationTokenIdentifier>(
        dtId, dtSecretManager);
      long expiryTime = dtSecretManager.getTokenExpiryTime(dtId);
      getEditLog().logGetDelegationToken(dtId, expiryTime);
      tokenId = dtId.toStringStable();
    } finally {
      writeUnlock(operationName, getLockReportInfoSupplier(tokenId));
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, tokenId);
    return token;
  }

  /**
   * 
   * @param token token to renew
   * @return new expiryTime of the token
   * @throws InvalidToken if {@code token} is invalid
   * @throws IOException on other errors
   */
  long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    final String operationName = "renewDelegationToken";
    String tokenId = null;
    long expiryTime;
    checkOperation(OperationCategory.WRITE);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);

        checkNameNodeSafeMode("Cannot renew delegation token");
        if (!isAllowedDelegationTokenOp()) {
          throw new IOException(
              "Delegation Token can be renewed only with kerberos or web "
                  + "authentication");
        }
        String renewer = getRemoteUser().getShortUserName();
        expiryTime = dtSecretManager.renewToken(token, renewer);
        final DelegationTokenIdentifier id = DFSUtil.decodeDelegationToken(
            token);
        getEditLog().logRenewDelegationToken(id, expiryTime);
        tokenId = id.toStringStable();
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(tokenId));
      }
    } catch (AccessControlException ace) {
      final DelegationTokenIdentifier id = DFSUtil.decodeDelegationToken(token);
      tokenId = id.toStringStable();
      logAuditEvent(false, operationName, tokenId);
      throw ace;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, tokenId);
    return expiryTime;
  }

  /**
   * 
   * @param token token to cancel
   * @throws IOException on error
   */
  void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    final String operationName = "cancelDelegationToken";
    String tokenId = null;
    checkOperation(OperationCategory.WRITE);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot cancel delegation token");
        String canceller = getRemoteUser().getUserName();
        DelegationTokenIdentifier id = dtSecretManager
            .cancelToken(token, canceller);
        getEditLog().logCancelDelegationToken(id);
        tokenId = id.toStringStable();
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(tokenId));
      }
    } catch (AccessControlException ace) {
      final DelegationTokenIdentifier id = DFSUtil.decodeDelegationToken(token);
      tokenId = id.toStringStable();
      logAuditEvent(false, operationName, tokenId);
      throw ace;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, tokenId);
  }

  /**
   * @param out save state of the secret manager
   * @param sdPath String storage directory path
   */
  void saveSecretManagerStateCompat(DataOutputStream out, String sdPath)
      throws IOException {
    dtSecretManager.saveSecretManagerStateCompat(out, sdPath);
  }

  SecretManagerState saveSecretManagerState() {
    return dtSecretManager.saveSecretManagerState();
  }

  /**
   * @param in load the state of secret manager from input stream
   */
  void loadSecretManagerStateCompat(DataInput in) throws IOException {
    dtSecretManager.loadSecretManagerStateCompat(in);
  }

  void loadSecretManagerState(SecretManagerSection s,
      List<SecretManagerSection.DelegationKey> keys,
      List<SecretManagerSection.PersistToken> tokens,
      StartupProgress.Counter counter) throws IOException {
    dtSecretManager.loadSecretManagerState(new SecretManagerState(s, keys, tokens),
            counter);
  }

  /**
   * Log the updateMasterKey operation to edit logs.
   * 
   * @param key new delegation key.
   */
  public void logUpdateMasterKey(DelegationKey key) {
    
    assert !isInSafeMode() :
      "this should never be called while in safemode, since we stop " +
      "the DT manager before entering safemode!";
    // edit log rolling is not thread-safe and must be protected by the
    // fsn lock.  not updating namespace so read lock is sufficient.
    assert hasReadLock();
    getEditLog().logUpdateMasterKey(key);
    getEditLog().logSync();
  }
  
  /**
   * Log the cancellation of expired tokens to edit logs.
   * 
   * @param id token identifier to cancel
   */
  public void logExpireDelegationToken(DelegationTokenIdentifier id) {
    assert !isInSafeMode() :
      "this should never be called while in safemode, since we stop " +
      "the DT manager before entering safemode!";
    // edit log rolling is not thread-safe and must be protected by the
    // fsn lock.  not updating namespace so read lock is sufficient.
    assert hasReadLock();
    // do not logSync so expiration edits are batched
    getEditLog().logCancelDelegationToken(id);
  }  
  
  private void logReassignLease(String leaseHolder, String src,
      String newHolder) {
    assert hasWriteLock();
    getEditLog().logReassignLease(leaseHolder, src, newHolder);
  }
  
  /**
   * 
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp() throws IOException {
    AuthenticationMethod authMethod = getConnectionAuthenticationMethod();
    if (UserGroupInformation.isSecurityEnabled()
        && (authMethod != AuthenticationMethod.KERBEROS)
        && (authMethod != AuthenticationMethod.KERBEROS_SSL)
        && (authMethod != AuthenticationMethod.CERTIFICATE)) {
      return false;
    }
    return true;
  }
  
  /**
   * Returns authentication method used to establish the connection.
   * @return AuthenticationMethod used to establish connection
   * @throws IOException
   */
  private AuthenticationMethod getConnectionAuthenticationMethod()
      throws IOException {
    UserGroupInformation ugi = getRemoteUser();
    AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
    if (authMethod == AuthenticationMethod.PROXY) {
      authMethod = ugi.getRealUser().getAuthenticationMethod();
    }
    return authMethod;
  }
  
  /**
   * Client invoked methods are invoked over RPC and will be in 
   * RPC call context even if the client exits.
   */
  boolean isExternalInvocation() {
    return Server.isRpcInvocation();
  }

  // optimize ugi lookup for RPC operations to avoid a trip through
  // UGI.getCurrentUser which is synch'ed
  private static UserGroupInformation getRemoteUser() throws IOException {
    return NameNode.getRemoteUser();
  }

  /**
   * Log fsck event in the audit log.
   *
   * @param succeeded Whether authorization succeeded.
   * @param src Path of affected source file.
   * @param remoteAddress Remote address of the request.
   * @throws IOException if {@link #getRemoteUser()} fails.
   */
  void logFsckEvent(boolean succeeded, String src, InetAddress remoteAddress)
      throws IOException {
    if (isAuditEnabled()) {
      logAuditEvent(succeeded, getRemoteUser(),
                    remoteAddress,
                    "fsck", src, null, null);
    }
  }

  /**
   * Register NameNodeMXBean.
   */
  private void registerMXBean() {
    namenodeMXBeanName = MBeans.register("NameNode", "NameNodeInfo", this);
  }

  /**
   * Class representing Namenode information for JMX interfaces.
   */
  @Override // NameNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion() + ", r" + VersionInfo.getRevision();
  }

  @Override // NameNodeMXBean
  public long getUsed() {
    return this.getCapacityUsed();
  }

  @Override // NameNodeMXBean
  public long getFree() {
    return this.getCapacityRemaining();
  }

  @Override // NameNodeMXBean
  public long getTotal() {
    return this.getCapacityTotal();
  }

  @Override // NameNodeMXBean
  public long getProvidedCapacity() {
    return this.getProvidedCapacityTotal();
  }

  @Override // NameNodeMXBean
  public String getSafemode() {
    if (!this.isInSafeMode())
      return "";
    return "Safe mode is ON. " + this.getSafeModeTip();
  }

  @Override // NameNodeMXBean
  public boolean isUpgradeFinalized() {
    return this.getFSImage().isUpgradeFinalized();
  }

  @Override // NameNodeMXBean
  public long getNonDfsUsedSpace() {
    return datanodeStatistics.getCapacityUsedNonDFS();
  }

  @Override // NameNodeMXBean
  public float getPercentUsed() {
    return datanodeStatistics.getCapacityUsedPercent();
  }

  @Override // NameNodeMXBean
  public long getBlockPoolUsedSpace() {
    return datanodeStatistics.getBlockPoolUsed();
  }

  @Override // NameNodeMXBean
  public float getPercentBlockPoolUsed() {
    return datanodeStatistics.getPercentBlockPoolUsed();
  }

  @Override // NameNodeMXBean
  public float getPercentRemaining() {
    return datanodeStatistics.getCapacityRemainingPercent();
  }

  @Override // NameNodeMXBean
  public long getCacheCapacity() {
    return datanodeStatistics.getCacheCapacity();
  }

  @Override // NameNodeMXBean
  public long getCacheUsed() {
    return datanodeStatistics.getCacheUsed();
  }

  @Override // NameNodeMXBean
  public long getTotalBlocks() {
    return getBlocksTotal();
  }

  @Override // NameNodeMXBean
  public long getNumberOfMissingBlocks() {
    return getMissingBlocksCount();
  }
  
  @Override // NameNodeMXBean
  public long getNumberOfMissingBlocksWithReplicationFactorOne() {
    return getMissingReplOneBlocksCount();
  }

  @Override // NameNodeMXBean
  public int getThreads() {
    return ManagementFactory.getThreadMXBean().getThreadCount();
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of live node attribute keys to its values.
   */
  @Override // NameNodeMXBean
  public String getLiveNodes() {
    final Map<String, Map<String,Object>> info = 
      new HashMap<String, Map<String,Object>>();
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    blockManager.getDatanodeManager().fetchDatanodes(live, null, false);
    for (DatanodeDescriptor node : live) {
      ImmutableMap.Builder<String, Object> innerinfo =
          ImmutableMap.<String,Object>builder();
      innerinfo
          .put("infoAddr", node.getInfoAddr())
          .put("infoSecureAddr", node.getInfoSecureAddr())
          .put("xferaddr", node.getXferAddr())
          .put("location", node.getNetworkLocation())
          .put("lastContact", getLastContact(node))
          .put("usedSpace", getDfsUsed(node))
          .put("adminState", node.getAdminState().toString())
          .put("nonDfsUsedSpace", node.getNonDfsUsed())
          .put("capacity", node.getCapacity())
          .put("numBlocks", node.numBlocks())
          .put("version", node.getSoftwareVersion())
          .put("used", node.getDfsUsed())
          .put("remaining", node.getRemaining())
          .put("blockScheduled", node.getBlocksScheduled())
          .put("blockPoolUsed", node.getBlockPoolUsed())
          .put("blockPoolUsedPercent", node.getBlockPoolUsedPercent())
          .put("volfails", node.getVolumeFailures())
          // Block report time in minutes
          .put("lastBlockReport", getLastBlockReport(node));
      VolumeFailureSummary volumeFailureSummary = node.getVolumeFailureSummary();
      if (volumeFailureSummary != null) {
        innerinfo
            .put("failedStorageIDs",
                volumeFailureSummary.getFailedStorageLocations())
            .put("lastVolumeFailureDate",
                volumeFailureSummary.getLastVolumeFailureDate())
            .put("estimatedCapacityLostTotal",
                volumeFailureSummary.getEstimatedCapacityLostTotal());
      }
      if (node.getUpgradeDomain() != null) {
        innerinfo.put("upgradeDomain", node.getUpgradeDomain());
      }
      StorageReport[] storageReports = node.getStorageReports();
      innerinfo.put("blockPoolUsedPercentStdDev",
          Util.getBlockPoolUsedPercentStdDev(storageReports));
      info.put(node.getXferAddrWithHostname(), innerinfo.build());
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of dead node attribute keys to its values.
   */
  @Override // NameNodeMXBean
  public String getDeadNodes() {
    final Map<String, Map<String, Object>> info = 
      new HashMap<String, Map<String, Object>>();
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    blockManager.getDatanodeManager().fetchDatanodes(null, dead, false);
    for (DatanodeDescriptor node : dead) {
      Map<String, Object> innerinfo = ImmutableMap.<String, Object>builder()
          .put("lastContact", getLastContact(node))
          .put("decommissioned", node.isDecommissioned())
          .put("adminState", node.getAdminState().toString())
          .put("xferaddr", node.getXferAddr())
          .put("location", node.getNetworkLocation())
          .build();
      info.put(node.getXferAddrWithHostname(), innerinfo);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of decommissioning node attribute keys to its
   * values.
   */
  @Override // NameNodeMXBean
  public String getDecomNodes() {
    final Map<String, Map<String, Object>> info = 
      new HashMap<String, Map<String, Object>>();
    final List<DatanodeDescriptor> decomNodeList = blockManager.getDatanodeManager(
        ).getDecommissioningNodes();
    for (DatanodeDescriptor node : decomNodeList) {
      Map<String, Object> innerinfo = ImmutableMap
          .<String, Object> builder()
          .put("xferaddr", node.getXferAddr())
          .put("location", node.getNetworkLocation())
          .put("underReplicatedBlocks",
          node.getLeavingServiceStatus().getUnderReplicatedBlocks())
          .put("decommissionOnlyReplicas",
          node.getLeavingServiceStatus().getOutOfServiceOnlyReplicas())
          .put("underReplicateInOpenFiles",
          node.getLeavingServiceStatus().getUnderReplicatedInOpenFiles())
          .put("decommissionDuration",
              monotonicNow() - node.getLeavingServiceStatus().getStartTime())
          .build();
      info.put(node.getXferAddrWithHostname(), innerinfo);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name of
   * nodes entering maintenance as the key and value as a map of various node
   * attributes to its values.
   */
  @Override // NameNodeMXBean
  public String getEnteringMaintenanceNodes() {
    final Map<String, Map<String, Object>> nodesMap =
        new HashMap<String, Map<String, Object>>();
    final List<DatanodeDescriptor> enteringMaintenanceNodeList =
        blockManager.getDatanodeManager().getEnteringMaintenanceNodes();
    for (DatanodeDescriptor node : enteringMaintenanceNodeList) {
      Map<String, Object> attrMap = ImmutableMap
          .<String, Object> builder()
          .put("xferaddr", node.getXferAddr())
          .put("location", node.getNetworkLocation())
          .put("underReplicatedBlocks",
              node.getLeavingServiceStatus().getUnderReplicatedBlocks())
          .put("maintenanceOnlyReplicas",
              node.getLeavingServiceStatus().getOutOfServiceOnlyReplicas())
          .put("underReplicateInOpenFiles",
              node.getLeavingServiceStatus().getUnderReplicatedInOpenFiles())
          .build();
      nodesMap.put(node.getXferAddrWithHostname(), attrMap);
    }
    return JSON.toString(nodesMap);
  }

  private long getLastContact(DatanodeDescriptor alivenode) {
    return (monotonicNow() - alivenode.getLastUpdateMonotonic())/1000;
  }

  private Object getLastBlockReport(DatanodeDescriptor node) {
    return (monotonicNow() - node.getLastBlockReportMonotonic()) / 60000;
  }

  private long getDfsUsed(DatanodeDescriptor alivenode) {
    return alivenode.getDfsUsed();
  }

  @Override  // NameNodeMXBean
  public String getClusterId() {
    return getNNStorage().getClusterID();
  }
  
  @Override  // NameNodeMXBean
  public String getBlockPoolId() {
    return getBlockManager().getBlockPoolId();
  }
  
  @Override  // NameNodeMXBean
  public String getNameDirStatuses() {
    Map<String, Map<File, StorageDirType>> statusMap =
      new HashMap<String, Map<File, StorageDirType>>();
    
    Map<File, StorageDirType> activeDirs = new HashMap<File, StorageDirType>();
    for (Iterator<StorageDirectory> it
        = getNNStorage().dirIterator(); it.hasNext();) {
      StorageDirectory st = it.next();
      activeDirs.put(st.getRoot(), st.getStorageDirType());
    }
    statusMap.put("active", activeDirs);
    
    List<Storage.StorageDirectory> removedStorageDirs
        = getNNStorage().getRemovedStorageDirs();
    Map<File, StorageDirType> failedDirs = new HashMap<File, StorageDirType>();
    for (StorageDirectory st : removedStorageDirs) {
      failedDirs.put(st.getRoot(), st.getStorageDirType());
    }
    statusMap.put("failed", failedDirs);
    
    return JSON.toString(statusMap);
  }

  @Override // NameNodeMXBean
  public String getNodeUsage() {
    float median = 0;
    float max = 0;
    float min = 0;
    float dev = 0;

    final Map<String, Map<String,Object>> info =
        new HashMap<String, Map<String,Object>>();
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    blockManager.getDatanodeManager().fetchDatanodes(live, null, true);
    for (Iterator<DatanodeDescriptor> it = live.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      if (!node.isInService()) {
        it.remove();
      }
    }

    if (live.size() > 0) {
      float totalDfsUsed = 0;
      float[] usages = new float[live.size()];
      int i = 0;
      for (DatanodeDescriptor dn : live) {
        usages[i++] = dn.getDfsUsedPercent();
        totalDfsUsed += dn.getDfsUsedPercent();
      }
      totalDfsUsed /= live.size();
      Arrays.sort(usages);
      median = usages[usages.length / 2];
      max = usages[usages.length - 1];
      min = usages[0];

      for (i = 0; i < usages.length; i++) {
        dev += (usages[i] - totalDfsUsed) * (usages[i] - totalDfsUsed);
      }
      dev = (float) Math.sqrt(dev / usages.length);
    }

    final Map<String, Object> innerInfo = new HashMap<String, Object>();
    innerInfo.put("min", StringUtils.format("%.2f%%", min));
    innerInfo.put("median", StringUtils.format("%.2f%%", median));
    innerInfo.put("max", StringUtils.format("%.2f%%", max));
    innerInfo.put("stdDev", StringUtils.format("%.2f%%", dev));
    info.put("nodeUsage", innerInfo);

    return JSON.toString(info);
  }

  @Override  // NameNodeMXBean
  public String getNameJournalStatus() {
    List<Map<String, String>> jasList = new ArrayList<Map<String, String>>();
    FSEditLog log = getFSImage().getEditLog();
    if (log != null) {
      // This flag can be false because we cannot hold a lock of FSEditLog
      // for metrics.
      boolean openForWrite = log.isOpenForWriteWithoutLock();
      for (JournalAndStream jas : log.getJournals()) {
        final Map<String, String> jasMap = new HashMap<String, String>();
        String manager = jas.getManager().toString();

        jasMap.put("required", String.valueOf(jas.isRequired()));
        jasMap.put("disabled", String.valueOf(jas.isDisabled()));
        jasMap.put("manager", manager);

        if (jas.isDisabled()) {
          jasMap.put("stream", "Failed");
        } else if (openForWrite) {
          EditLogOutputStream elos = jas.getCurrentStream();
          if (elos != null) {
            jasMap.put("stream", elos.generateReport());
          } else {
            jasMap.put("stream", "not currently writing");
          }
        } else {
          jasMap.put("stream", "open for read");
        }
        jasList.add(jasMap);
      }
    }
    return JSON.toString(jasList);
  }

  @Override // NameNodeMxBean
  public String getJournalTransactionInfo() {
    Map<String, String> txnIdMap = new HashMap<String, String>();
    txnIdMap.put("LastAppliedOrWrittenTxId",
        Long.toString(this.getFSImage().getLastAppliedOrWrittenTxId()));
    txnIdMap.put("MostRecentCheckpointTxId",
        Long.toString(this.getFSImage().getMostRecentCheckpointTxId()));
    return JSON.toString(txnIdMap);
  }
  
  @Override // NameNodeMXBean
  public long getNNStartedTimeInMillis() {
    return startTime;
  }

  @Override  // NameNodeMXBean
  public String getCompileInfo() {
    return VersionInfo.getDate() + " by " + VersionInfo.getUser() +
        " from " + VersionInfo.getBranch();
  }

  /** @return the block manager. */
  public BlockManager getBlockManager() {
    return blockManager;
  }

  @VisibleForTesting
  public void setBlockManagerForTesting(BlockManager bm) {
    this.blockManager = bm;
  }

  /** @return the FSDirectory. */
  @Override
  public FSDirectory getFSDirectory() {
    return dir;
  }

  /** Set the FSDirectory. */
  @VisibleForTesting
  public void setFSDirectory(FSDirectory dir) {
    this.dir = dir;
  }

  /** @return the cache manager. */
  @Override
  public CacheManager getCacheManager() {
    return cacheManager;
  }

  /** @return the ErasureCodingPolicyManager. */
  public ErasureCodingPolicyManager getErasureCodingPolicyManager() {
    return ErasureCodingPolicyManager.getInstance();
  }

  @Override
  public HAContext getHAContext() {
    return haContext;
  }

  @Override  // NameNodeMXBean
  public String getCorruptFiles() {
    return JSON.toString(getCorruptFilesList());
  }

  @Override // NameNodeMXBean
  public int getCorruptFilesCount() {
    return getCorruptFilesList().size();
  }

  private List<String> getCorruptFilesList() {
    List<String> list = new ArrayList<String>();
    Collection<FSNamesystem.CorruptFileBlockInfo> corruptFileBlocks;
    try {
      corruptFileBlocks = listCorruptFileBlocks("/", null);
      int corruptFileCount = corruptFileBlocks.size();
      if (corruptFileCount != 0) {
        for (FSNamesystem.CorruptFileBlockInfo c : corruptFileBlocks) {
          list.add(c.toString());
        }
      }
    } catch (StandbyException e) {
      LOG.debug("Get corrupt file blocks returned error: {}", e.getMessage());
    } catch (IOException e) {
      LOG.warn("Get corrupt file blocks returned error", e);
    }
    return list;
  }

  @Override  // NameNodeMXBean
  public long getNumberOfSnapshottableDirs() {
    return snapshotManager.getNumSnapshottableDirs();
  }

  /**
   * Get the list of corrupt blocks and corresponding full file path
   * including snapshots in given snapshottable directories.
   * @param path Restrict corrupt files to this portion of namespace.
   * @param snapshottableDirs Snapshottable directories. Passing in null
   *                          will only return corrupt blocks in non-snapshots.
   * @param cookieTab Support for continuation; cookieTab tells where
   *                  to start from.
   * @return a list in which each entry describes a corrupt file/block
   * @throws IOException
   */
  List<String> listCorruptFileBlocksWithSnapshot(String path,
      List<String> snapshottableDirs, String[] cookieTab) throws IOException {
    final Collection<CorruptFileBlockInfo> corruptFileBlocks =
        listCorruptFileBlocks(path, cookieTab);
    List<String> list = new ArrayList<String>();

    // Precalculate snapshottableFeature list
    List<DirectorySnapshottableFeature> lsf = new ArrayList<>();
    if (snapshottableDirs != null) {
      for (String snap : snapshottableDirs) {
        final INode isnap = getFSDirectory().getINode(snap, DirOp.READ_LINK);
        final DirectorySnapshottableFeature sf =
            isnap.asDirectory().getDirectorySnapshottableFeature();
        if (sf == null) {
          throw new SnapshotException(
              "Directory is not a snapshottable directory: " + snap);
        }
        lsf.add(sf);
      }
    }

    for (CorruptFileBlockInfo c : corruptFileBlocks) {
      if (getFileInfo(c.path, true, false, false) != null) {
        list.add(c.toString());
      }
      final Collection<String> snaps = FSDirSnapshotOp
          .getSnapshotFiles(getFSDirectory(), lsf, c.path);
      if (snaps != null) {
        for (String snap : snaps) {
          // follow the syntax of CorruptFileBlockInfo#toString()
          list.add(c.block.getBlockName() + "\t" + snap);
        }
      }
    }
    return list;
  }

  @Override  //NameNodeMXBean
  public int getDistinctVersionCount() {
    return blockManager.getDatanodeManager().getDatanodesSoftwareVersions()
      .size();
  }

  @Override  //NameNodeMXBean
  public Map<String, Integer> getDistinctVersions() {
    return blockManager.getDatanodeManager().getDatanodesSoftwareVersions();
  }

  @Override  //NameNodeMXBean
  public String getSoftwareVersion() {
    return VersionInfo.getVersion();
  }

  @Override // NameNodeStatusMXBean
  public String getNameDirSize() {
    return getNNStorage().getNNDirectorySize();
  }

  /**
   * Verifies that the given identifier and password are valid and match.
   * @param identifier Token identifier.
   * @param password Password in the token.
   */
  public synchronized void verifyToken(DelegationTokenIdentifier identifier,
      byte[] password) throws InvalidToken, RetriableException {
    try {
      getDelegationTokenSecretManager().verifyToken(identifier, password);
    } catch (InvalidToken it) {
      if (inTransitionToActive()) {
        throw new RetriableException(it);
      }
      throw it;
    }
  }

  @VisibleForTesting
  public EditLogTailer getEditLogTailer() {
    return editLogTailer;
  }
  
  @VisibleForTesting
  public void setEditLogTailerForTests(EditLogTailer tailer) {
    this.editLogTailer = tailer;
  }
  
  @VisibleForTesting
  void setFsLockForTests(ReentrantReadWriteLock lock) {
    this.fsLock.coarseLock = lock;
  }
  
  @VisibleForTesting
  public ReentrantReadWriteLock getFsLockForTests() {
    return fsLock.coarseLock;
  }
  
  @VisibleForTesting
  public ReentrantLock getCpLockForTests() {
    return cpLock;
  }
  
  @VisibleForTesting
  public void setNNResourceChecker(NameNodeResourceChecker nnResourceChecker) {
    this.nnResourceChecker = nnResourceChecker;
  }

  public SnapshotManager getSnapshotManager() {
    return snapshotManager;
  }
  
  /** Allow snapshot on a directory. */
  void allowSnapshot(String path) throws IOException {
    checkOperation(OperationCategory.WRITE);
    final String operationName = "allowSnapshot";
    checkSuperuserPrivilege(operationName, path);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot allow snapshot for " + path);
      FSDirSnapshotOp.allowSnapshot(dir, snapshotManager, path);
    } finally {
      writeUnlock(operationName, getLockReportInfoSupplier(path));
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, path, null, null);
  }
  
  /** Disallow snapshot on a directory. */
  void disallowSnapshot(String path) throws IOException {
    checkOperation(OperationCategory.WRITE);
    final String operationName = "disallowSnapshot";
    checkSuperuserPrivilege(operationName, path);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot disallow snapshot for " + path);
      FSDirSnapshotOp.disallowSnapshot(dir, snapshotManager, path);
    } finally {
      writeUnlock(operationName, getLockReportInfoSupplier(path));
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, path, null, null);
  }
  
  /**
   * Create a snapshot
   * @param snapshotRoot The directory path where the snapshot is taken
   * @param snapshotName The name of the snapshot
   */
  String createSnapshot(String snapshotRoot, String snapshotName,
                        boolean logRetryCache) throws IOException {
    checkOperation(OperationCategory.WRITE);
    final String operationName = "createSnapshot";
    String snapshotPath = null;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot create snapshot for " + snapshotRoot);
        snapshotPath = FSDirSnapshotOp.createSnapshot(dir, pc,
            snapshotManager, snapshotRoot, snapshotName, logRetryCache);
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(snapshotRoot));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, snapshotRoot);
      throw ace;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, snapshotRoot,
        snapshotPath, null);
    return snapshotPath;
  }
  
  /**
   * Rename a snapshot
   * @param path The directory path where the snapshot was taken
   * @param snapshotOldName Old snapshot name
   * @param snapshotNewName New snapshot name
   * @throws SafeModeException
   * @throws IOException 
   */
  void renameSnapshot(
      String path, String snapshotOldName, String snapshotNewName,
      boolean logRetryCache) throws IOException {
    checkOperation(OperationCategory.WRITE);
    final String operationName = "renameSnapshot";
    String oldSnapshotRoot = Snapshot.getSnapshotPath(path, snapshotOldName);
    String newSnapshotRoot = Snapshot.getSnapshotPath(path, snapshotNewName);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot rename snapshot for " + path);
        FSDirSnapshotOp.renameSnapshot(dir, pc, snapshotManager, path,
            snapshotOldName, snapshotNewName, logRetryCache);
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(oldSnapshotRoot,
            newSnapshotRoot));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, oldSnapshotRoot,
          newSnapshotRoot, null);
      throw ace;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, oldSnapshotRoot,
        newSnapshotRoot, null);
  }

  /**
   * Get the list of snapshottable directories that are owned 
   * by the current user. Return all the snapshottable directories if the 
   * current user is a super user.
   * @return The list of all the current snapshottable directories.
   * @throws IOException If an I/O error occurred.
   */
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    final String operationName = "listSnapshottableDirectory";
    SnapshottableDirectoryStatus[] status = null;
    checkOperation(OperationCategory.READ);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        status = FSDirSnapshotOp.getSnapshottableDirListing(dir, pc,
            snapshotManager);
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(null));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, null, null, null);
      throw ace;
    }
    logAuditEvent(true, operationName, null, null, null);
    return status;
  }

  /**
   * Get the list of snapshots for a given snapshottable directory.
   *
   * @return The list of all the snapshots for a snapshottable directory
   * @throws IOException
   */
  public SnapshotStatus[] getSnapshotListing(String snapshotRoot)
      throws IOException {
    final String operationName = "ListSnapshot";
    SnapshotStatus[] status;
    checkOperation(OperationCategory.READ);
    boolean success = false;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        status = FSDirSnapshotOp.getSnapshotListing(dir, pc, snapshotManager,
            snapshotRoot);
        success = true;
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(null));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(success, operationName, snapshotRoot);
      throw ace;
    }
    logAuditEvent(success, operationName, snapshotRoot);
    return status;
  }
  /**
   * Get the difference between two snapshots (or between a snapshot and the
   * current status) of a snapshottable directory.
   *
   * @param path The full path of the snapshottable directory.
   * @param fromSnapshot Name of the snapshot to calculate the diff from. Null
   *          or empty string indicates the current tree.
   * @param toSnapshot Name of the snapshot to calculated the diff to. Null or
   *          empty string indicates the current tree.
   * @return A report about the difference between {@code fromSnapshot} and
   *         {@code toSnapshot}. Modified/deleted/created/renamed files and
   *         directories belonging to the snapshottable directories are listed
   *         and labeled as M/-/+/R respectively.
   * @throws IOException
   */
  SnapshotDiffReport getSnapshotDiffReport(String path,
      String fromSnapshot, String toSnapshot) throws IOException {
    long begTime = Time.monotonicNow();
    final String operationName = "computeSnapshotDiff";
    SnapshotDiffReport diffs = null;
    checkOperation(OperationCategory.READ);
    String fromSnapshotRoot = (fromSnapshot == null || fromSnapshot.isEmpty()) ?
        path : Snapshot.getSnapshotPath(path, fromSnapshot);
    String toSnapshotRoot = (toSnapshot == null || toSnapshot.isEmpty()) ?
        path : Snapshot.getSnapshotPath(path, toSnapshot);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    long actualTime = Time.monotonicNow();
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        diffs = FSDirSnapshotOp.getSnapshotDiffReport(dir, pc, snapshotManager,
            path, fromSnapshot, toSnapshot);
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(fromSnapshotRoot,
            toSnapshotRoot));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, fromSnapshotRoot,
          toSnapshotRoot, null);
      throw ace;
    }
    if (diffs != null) {
      SnapshotDiffReport.DiffStats dstat = diffs.getStats();
      LOG.info("SnapshotDiffReport '"
          + fromSnapshot
          + "' to '" + toSnapshot + "'. Total comparison dirs: "
          + dstat.getTotalDirsCompared()
          + "/" + dstat.getTotalDirsProcessed()
          + ", files: "
          + dstat.getTotalFilesCompared()
          + "/" + dstat.getTotalFilesProcessed()
          + ". Time snapChildrenListing: "
          + dstat.getTotalChildrenListingTime() / 1000.0 + "s, actual: "
          + ((Time.monotonicNow() - actualTime) / 1000.0) + "s, total: "
          + ((Time.monotonicNow() - begTime) / 1000.0) + "s.");
    }

    logAuditEvent(true, operationName, fromSnapshotRoot,
        toSnapshotRoot, null);
    return diffs;
  }

  /**
   * Get the difference between two snapshots (or between a snapshot and the
   * current status) of a snapshottable directory.
   *
   * @param path The full path of the snapshottable directory.
   * @param fromSnapshot Name of the snapshot to calculate the diff from. Null
   *          or empty string indicates the current tree.
   * @param toSnapshot Name of the snapshot to calculated the diff to. Null or
   *          empty string indicates the current tree.
   * @param startPath
   *          path relative to the snapshottable root directory from where the
   *          snapshotdiff computation needs to start across multiple rpc calls
   * @param index
   *           index in the created or deleted list of the directory at which
   *           the snapshotdiff computation stopped during the last rpc call
   *           as the no of entries exceeded the snapshotdiffentry limit. -1
   *           indicates, the snapshotdiff compuatation needs to start right
   *           from the startPath provided.
   * @return A partial report about the difference between {@code fromSnapshot}
   *         and {@code toSnapshot}. Modified/deleted/created/renamed files and
   *         directories belonging to the snapshottable directories are listed
   *         and labeled as M/-/+/R respectively.
   * @throws IOException
   */
  SnapshotDiffReportListing getSnapshotDiffReportListing(String path,
      String fromSnapshot, String toSnapshot, byte[] startPath, int index)
      throws IOException {
    final String operationName = "computeSnapshotDiff";
    SnapshotDiffReportListing diffs = null;
    checkOperation(OperationCategory.READ);
    String fromSnapshotRoot =
        (fromSnapshot == null || fromSnapshot.isEmpty()) ? path :
            Snapshot.getSnapshotPath(path, fromSnapshot);
    String toSnapshotRoot =
        (toSnapshot == null || toSnapshot.isEmpty()) ? path :
            Snapshot.getSnapshotPath(path, toSnapshot);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        diffs = FSDirSnapshotOp
            .getSnapshotDiffReportListing(dir, pc, snapshotManager, path,
                fromSnapshot, toSnapshot, startPath, index,
                snapshotDiffReportLimit);
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(fromSnapshotRoot,
            toSnapshotRoot));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, fromSnapshotRoot, toSnapshotRoot,
          null);
      throw ace;
    }
    logAuditEvent(true, operationName, fromSnapshotRoot, toSnapshotRoot,
        null);
    return diffs;
  }
  
  /**
   * Delete a snapshot of a snapshottable directory
   * @param snapshotRoot The snapshottable directory
   * @param snapshotName The name of the to-be-deleted snapshot
   * @throws SafeModeException
   * @throws IOException
   */
  void deleteSnapshot(String snapshotRoot, String snapshotName,
      boolean logRetryCache) throws IOException {
    final String operationName = "deleteSnapshot";
    String rootPath = null;
    BlocksMapUpdateInfo blocksToBeDeleted = null;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    checkOperation(OperationCategory.WRITE);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot delete snapshot for " + snapshotRoot);
        rootPath = Snapshot.getSnapshotPath(snapshotRoot, snapshotName);
        blocksToBeDeleted = FSDirSnapshotOp.deleteSnapshot(dir, pc,
            snapshotManager, snapshotRoot, snapshotName, logRetryCache);
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(rootPath));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, rootPath, null, null);
      throw ace;
    }
    getEditLog().logSync();

    // Breaking the pattern as removing blocks have to happen outside of the
    // global lock
    if (blocksToBeDeleted != null) {
      blockManager.addBLocksToMarkedDeleteQueue(
          blocksToBeDeleted.getToDeleteList());
    }
    logAuditEvent(true, operationName, rootPath, null, null);
  }

  public void gcDeletedSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    final String operationName = "gcDeletedSnapshot";
    String rootPath = null;
    final INode.BlocksMapUpdateInfo blocksToBeDeleted;

    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      rootPath = Snapshot.getSnapshotPath(snapshotRoot, snapshotName);
      checkNameNodeSafeMode("Cannot gcDeletedSnapshot for " + rootPath);

      final long now = Time.now();
      final INodesInPath iip = dir.resolvePath(null, snapshotRoot, DirOp.WRITE);
      snapshotManager.assertMarkedAsDeleted(iip, snapshotName);
      blocksToBeDeleted = FSDirSnapshotOp.deleteSnapshot(
          dir, snapshotManager, iip, snapshotName, now, snapshotRoot, false);
    } finally {
      writeUnlock(operationName, getLockReportInfoSupplier(rootPath));
    }
    blockManager.addBLocksToMarkedDeleteQueue(
        blocksToBeDeleted.getToDeleteList());
  }

  /**
   * Remove a list of INodeDirectorySnapshottable from the SnapshotManager
   * @param toRemove the list of INodeDirectorySnapshottable to be removed
   */
  void removeSnapshottableDirs(List<INodeDirectory> toRemove) {
    if (snapshotManager != null) {
      snapshotManager.removeSnapshottable(toRemove);
    }
  }

  RollingUpgradeInfo queryRollingUpgrade() throws IOException {
    final String operationName = "queryRollingUpgrade";
    checkSuperuserPrivilege(operationName);
    checkOperation(OperationCategory.READ);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      if (!isRollingUpgrade()) {
        return null;
      }
      Preconditions.checkNotNull(rollingUpgradeInfo);
      boolean hasRollbackImage = this.getFSImage().hasRollbackFSImage();
      rollingUpgradeInfo.setCreatedRollbackImages(hasRollbackImage);
    } finally {
      readUnlock(operationName, getLockReportInfoSupplier(null));
    }
    logAuditEvent(true, operationName, null, null, null);
    return rollingUpgradeInfo;
  }

  RollingUpgradeInfo startRollingUpgrade() throws IOException {
    final String operationName = "startRollingUpgrade";
    checkSuperuserPrivilege(operationName);
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      if (isRollingUpgrade()) {
        return rollingUpgradeInfo;
      }
      long startTime = now();
      if (!haEnabled) { // for non-HA, we require NN to be in safemode
        startRollingUpgradeInternalForNonHA(startTime);
      } else { // for HA, NN cannot be in safemode
        checkNameNodeSafeMode("Failed to start rolling upgrade");
        startRollingUpgradeInternal(startTime);
      }

      getEditLog().logStartRollingUpgrade(rollingUpgradeInfo.getStartTime());
      if (haEnabled) {
        // roll the edit log to make sure the standby NameNode can tail
        getFSImage().rollEditLog(getEffectiveLayoutVersion());
      }
    } finally {
      writeUnlock(operationName, getLockReportInfoSupplier(null));
    }

    getEditLog().logSync();
    logAuditEvent(true, operationName, null, null, null);
    return rollingUpgradeInfo;
  }

  /**
   * Update internal state to indicate that a rolling upgrade is in progress.
   * @param startTime rolling upgrade start time
   */
  void startRollingUpgradeInternal(long startTime)
      throws IOException {
    checkRollingUpgrade("start rolling upgrade");
    getFSImage().checkUpgrade();
    setRollingUpgradeInfo(false, startTime);
  }

  /**
   * Update internal state to indicate that a rolling upgrade is in progress for
   * non-HA setup. This requires the namesystem is in SafeMode and after doing a
   * checkpoint for rollback the namesystem will quit the safemode automatically 
   */
  private void startRollingUpgradeInternalForNonHA(long startTime)
      throws IOException {
    Preconditions.checkState(!haEnabled);
    if (!isInSafeMode()) {
      throw new IOException("Safe mode should be turned ON "
          + "in order to create namespace image.");
    }
    checkRollingUpgrade("start rolling upgrade");
    getFSImage().checkUpgrade();
    // in non-HA setup, we do an extra checkpoint to generate a rollback image
    getFSImage().saveNamespace(this, NameNodeFile.IMAGE_ROLLBACK, null);
    LOG.info("Successfully saved namespace for preparing rolling upgrade.");

    // leave SafeMode automatically
    setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    setRollingUpgradeInfo(true, startTime);
  }

  void setRollingUpgradeInfo(boolean createdRollbackImages, long startTime) {
    rollingUpgradeInfo = new RollingUpgradeInfo(getBlockPoolId(),
        createdRollbackImages, startTime, 0L);
  }

  public void setCreatedRollbackImages(boolean created) {
    if (rollingUpgradeInfo != null) {
      rollingUpgradeInfo.setCreatedRollbackImages(created);
    }
  }

  public RollingUpgradeInfo getRollingUpgradeInfo() {
    return rollingUpgradeInfo;
  }

  public boolean isNeedRollbackFsImage() {
    return needRollbackFsImage;
  }

  public void setNeedRollbackFsImage(boolean needRollbackFsImage) {
    this.needRollbackFsImage = needRollbackFsImage;
  }

  @Override  // NameNodeMXBean
  public RollingUpgradeInfo.Bean getRollingUpgradeStatus() {
    if (!isRollingUpgrade()) {
      return null;
    }
    RollingUpgradeInfo upgradeInfo = getRollingUpgradeInfo();
    if (upgradeInfo.createdRollbackImages()) {
      return new RollingUpgradeInfo.Bean(upgradeInfo);
    }
    readLock();
    try {
      // check again after acquiring the read lock.
      upgradeInfo = getRollingUpgradeInfo();
      if (upgradeInfo == null) {
        return null;
      }
      if (!upgradeInfo.createdRollbackImages()) {
        boolean hasRollbackImage = this.getFSImage().hasRollbackFSImage();
        upgradeInfo.setCreatedRollbackImages(hasRollbackImage);
      }
    } catch (IOException ioe) {
      LOG.warn("Encountered exception setting Rollback Image", ioe);
    } finally {
      readUnlock("getRollingUpgradeStatus");
    }
    return new RollingUpgradeInfo.Bean(upgradeInfo);
  }

  /** Is rolling upgrade in progress? */
  public boolean isRollingUpgrade() {
    return rollingUpgradeInfo != null && !rollingUpgradeInfo.isFinalized();
  }

  /**
   * Returns the layout version in effect.  Under normal operation, this is the
   * same as the software's current layout version, defined in
   * {@link NameNodeLayoutVersion#CURRENT_LAYOUT_VERSION}.  During a rolling
   * upgrade, this can retain the layout version that was persisted to metadata
   * prior to starting the rolling upgrade, back to a lower bound defined in
   * {@link NameNodeLayoutVersion#MINIMUM_COMPATIBLE_LAYOUT_VERSION}.  New
   * fsimage files and edit log segments will continue to be written with this
   * older layout version, so that the files are still readable by the old
   * software version if the admin chooses to downgrade.
   *
   * @return layout version in effect
   */
  public int getEffectiveLayoutVersion() {
    return getEffectiveLayoutVersion(isRollingUpgrade(),
        fsImage.getStorage().getLayoutVersion(),
        NameNodeLayoutVersion.MINIMUM_COMPATIBLE_LAYOUT_VERSION,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
  }

  @VisibleForTesting
  static int getEffectiveLayoutVersion(boolean isRollingUpgrade, int storageLV,
      int minCompatLV, int currentLV) {
    if (isRollingUpgrade) {
      if (storageLV <= minCompatLV) {
        // The prior layout version satisfies the minimum compatible layout
        // version of the current software.  Keep reporting the prior layout
        // as the effective one.  Downgrade is possible.
        return storageLV;
      }
    }
    // The current software cannot satisfy the layout version of the prior
    // software.  Proceed with using the current layout version.
    return currentLV;
  }

  /**
   * Performs a pre-condition check that the layout version in effect is
   * sufficient to support the requested {@link Feature}.  If not, then the
   * method throws {@link HadoopIllegalArgumentException} to deny the operation.
   * This exception class is registered as a terse exception, so it prevents
   * verbose stack traces in the NameNode log.  During a rolling upgrade, this
   * method is used to restrict usage of new features.  This prevents writing
   * new edit log operations that would be unreadable by the old software
   * version if the admin chooses to downgrade.
   *
   * @param f feature to check
   * @throws HadoopIllegalArgumentException if the current layout version in
   *     effect is insufficient to support the feature
   */
  private void requireEffectiveLayoutVersionForFeature(Feature f)
      throws HadoopIllegalArgumentException {
    int lv = getEffectiveLayoutVersion();
    if (!NameNodeLayoutVersion.supports(f, lv)) {
      throw new HadoopIllegalArgumentException(String.format(
          "Feature %s unsupported at NameNode layout version %d.  If a " +
          "rolling upgrade is in progress, then it must be finalized before " +
          "using this feature.", f, lv));
    }
  }

  void checkRollingUpgrade(String action) throws RollingUpgradeException {
    if (isRollingUpgrade()) {
      throw new RollingUpgradeException("Failed to " + action
          + " since a rolling upgrade is already in progress."
          + " Existing rolling upgrade info:\n" + rollingUpgradeInfo);
    }
  }

  RollingUpgradeInfo finalizeRollingUpgrade() throws IOException {
    final String operationName = "finalizeRollingUpgrade";
    checkSuperuserPrivilege(operationName);
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      if (!isRollingUpgrade()) {
        return null;
      }
      checkNameNodeSafeMode("Failed to finalize rolling upgrade");

      finalizeRollingUpgradeInternal(now());
      getEditLog().logFinalizeRollingUpgrade(rollingUpgradeInfo.getFinalizeTime());
      if (haEnabled) {
        // roll the edit log to make sure the standby NameNode can tail
        getFSImage().rollEditLog(getEffectiveLayoutVersion());
      }
      getFSImage().updateStorageVersion();
      getFSImage().renameCheckpoint(NameNodeFile.IMAGE_ROLLBACK,
          NameNodeFile.IMAGE);
    } finally {
      writeUnlock(operationName, getLockReportInfoSupplier(null));
    }

    if (!haEnabled) {
      // Sync not needed for ha since the edit was rolled after logging.
      getEditLog().logSync();
    }
    logAuditEvent(true, operationName, null, null, null);
    return rollingUpgradeInfo;
  }

  void finalizeRollingUpgradeInternal(long finalizeTime) {
    // Set the finalize time
    rollingUpgradeInfo.finalize(finalizeTime);
  }

  long addCacheDirective(CacheDirectiveInfo directive,
                         EnumSet<CacheFlag> flags, boolean logRetryCache)
      throws IOException {
    final String operationName = "addCacheDirective";
    CacheDirectiveInfo effectiveDirective = null;
    String effectiveDirectiveStr;
    if (!flags.contains(CacheFlag.FORCE)) {
      cacheManager.waitForRescanIfNeeded();
    }
    checkOperation(OperationCategory.WRITE);
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot add cache directive");
        effectiveDirective = FSNDNCacheOp.addCacheDirective(this, cacheManager,
            directive, flags, logRetryCache);
      } finally {
        effectiveDirectiveStr = effectiveDirective != null ?
            effectiveDirective.toString() : null;
        writeUnlock(operationName,
            getLockReportInfoSupplier(effectiveDirectiveStr));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, null);
      throw ace;
    }
    getEditLog().logSync();
    assert effectiveDirective != null;
    effectiveDirectiveStr = effectiveDirective.toString();
    logAuditEvent(true, operationName, effectiveDirectiveStr);
    return effectiveDirective.getId();
  }

  void modifyCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags, boolean logRetryCache) throws IOException {
    final String operationName = "modifyCacheDirective";
    final String idStr = "{id: " + directive.getId() + "}";
    if (!flags.contains(CacheFlag.FORCE)) {
      cacheManager.waitForRescanIfNeeded();
    }
    FSPermissionChecker.setOperationType(operationName);
    checkOperation(OperationCategory.WRITE);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot add cache directive");
        FSNDNCacheOp.modifyCacheDirective(this, cacheManager, directive, flags,
            logRetryCache);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(idStr, directive.toString()));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, idStr,
          directive.toString(), null);
      throw ace;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, idStr,
        directive.toString(), null);
  }

  void removeCacheDirective(long id, boolean logRetryCache) throws IOException {
    final String operationName = "removeCacheDirective";
    String idStr = "{id: " + Long.toString(id) + "}";
    checkOperation(OperationCategory.WRITE);
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot remove cache directives");
        FSNDNCacheOp.removeCacheDirective(this, cacheManager, id,
            logRetryCache);
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(idStr));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, idStr, null, null);
      throw ace;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, idStr, null, null);
  }

  BatchedListEntries<CacheDirectiveEntry> listCacheDirectives(
      long startId, CacheDirectiveInfo filter) throws IOException {
    final String operationName = "listCacheDirectives";
    checkOperation(OperationCategory.READ);
    FSPermissionChecker.setOperationType(operationName);
    BatchedListEntries<CacheDirectiveEntry> results;
    cacheManager.waitForRescanIfNeeded();
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        results = FSNDNCacheOp.listCacheDirectives(this, cacheManager, startId,
            filter);
      } finally {
        readUnlock(operationName,
            getLockReportInfoSupplier(filter.toString()));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, filter.toString());
      throw ace;
    }
    logAuditEvent(true, operationName, filter.toString());
    return results;
  }

  void addCachePool(CachePoolInfo req, boolean logRetryCache)
      throws IOException {
    final String operationName = "addCachePool";
    checkOperation(OperationCategory.WRITE);
    String poolInfoStr = null;
    String poolName = req == null ? null : req.getPoolName();
    checkSuperuserPrivilege(operationName, poolName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot add cache pool"
            + poolName);
        CachePoolInfo info = FSNDNCacheOp.addCachePool(this, cacheManager, req,
            logRetryCache);
        poolInfoStr = info.toString();
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(poolInfoStr));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, poolInfoStr);
      throw ace;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, poolInfoStr);
  }

  void modifyCachePool(CachePoolInfo req, boolean logRetryCache)
      throws IOException {
    final String operationName = "modifyCachePool";
    checkOperation(OperationCategory.WRITE);
    String poolNameStr = "{poolName: " +
        (req == null ? null : req.getPoolName()) + "}";
    checkSuperuserPrivilege(operationName, poolNameStr);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot modify cache pool"
            + (req == null ? null : req.getPoolName()));
        FSNDNCacheOp.modifyCachePool(this, cacheManager, req, logRetryCache);
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(poolNameStr,
            req == null ? null : req.toString()));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, poolNameStr,
          req == null ? null : req.toString(), null);
      throw ace;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, poolNameStr,
        req == null ? null : req.toString(), null);
  }

  void removeCachePool(String cachePoolName, boolean logRetryCache)
      throws IOException {
    final String operationName = "removeCachePool";
    checkOperation(OperationCategory.WRITE);
    String poolNameStr = "{poolName: " + cachePoolName + "}";
    checkSuperuserPrivilege(operationName, poolNameStr);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot modify cache pool" + cachePoolName);
        FSNDNCacheOp.removeCachePool(this, cacheManager, cachePoolName,
            logRetryCache);
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(poolNameStr));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, poolNameStr);
      throw ace;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, poolNameStr);
  }

  BatchedListEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    final String operationName = "listCachePools";
    BatchedListEntries<CachePoolEntry> results;
    checkOperation(OperationCategory.READ);
    FSPermissionChecker.setOperationType(operationName);
    cacheManager.waitForRescanIfNeeded();
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        results = FSNDNCacheOp.listCachePools(this, cacheManager, prevKey);
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(null));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, null);
      throw ace;
    }
    logAuditEvent(true, operationName, null);
    return results;
  }

  void modifyAclEntries(final String src, List<AclEntry> aclSpec)
      throws IOException {
    final String operationName = "modifyAclEntries";
    FileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot modify ACL entries on " + src);
        auditStat = FSDirAclOp.modifyAclEntries(dir, pc, src, aclSpec);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, auditStat);
  }

  void removeAclEntries(final String src, List<AclEntry> aclSpec)
      throws IOException {
    final String operationName = "removeAclEntries";
    checkOperation(OperationCategory.WRITE);
    FileStatus auditStat = null;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot remove ACL entries on " + src);
        auditStat = FSDirAclOp.removeAclEntries(dir, pc, src, aclSpec);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, auditStat);
  }

  void removeDefaultAcl(final String src) throws IOException {
    final String operationName = "removeDefaultAcl";
    FileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot remove default ACL entries on " + src);
        auditStat = FSDirAclOp.removeDefaultAcl(dir, pc, src);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, auditStat);
  }

  void removeAcl(final String src) throws IOException {
    final String operationName = "removeAcl";
    FileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot remove ACL on " + src);
        auditStat = FSDirAclOp.removeAcl(dir, pc, src);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, auditStat);
  }

  void setAcl(final String src, List<AclEntry> aclSpec) throws IOException {
    final String operationName = "setAcl";
    FileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot set ACL on " + src);
        auditStat = FSDirAclOp.setAcl(dir, pc, src, aclSpec);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, auditStat);
  }

  AclStatus getAclStatus(String src) throws IOException {
    final String operationName = "getAclStatus";
    checkOperation(OperationCategory.READ);
    final AclStatus ret;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        ret = FSDirAclOp.getAclStatus(dir, pc, src);
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(src));
      }
    } catch(AccessControlException ace) {
      logAuditEvent(false, operationName, src);
      throw ace;
    }
    logAuditEvent(true, operationName, src);
    return ret;
  }

  /**
   * Create an encryption zone on directory src using the specified key.
   *
   * @param src     the path of a directory which will be the root of the
   *                encryption zone. The directory must be empty.
   * @param keyName name of a key which must be present in the configured
   *                KeyProvider.
   * @throws AccessControlException  if the caller is not the superuser.
   * @throws UnresolvedLinkException if the path can't be resolved.
   * @throws SafeModeException       if the Namenode is in safe mode.
   */
  void createEncryptionZone(final String src, final String keyName,
      boolean logRetryCache) throws IOException, UnresolvedLinkException,
          SafeModeException, AccessControlException {
    final String operationName = "createEncryptionZone";
    FileStatus resultingStat = null;
    checkSuperuserPrivilege(operationName, src);
    try {
      Metadata metadata = FSDirEncryptionZoneOp.ensureKeyIsInitialized(dir,
          keyName, src);
      final FSPermissionChecker pc = getPermissionChecker();
      checkOperation(OperationCategory.WRITE);
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot create encryption zone on " + src);
        resultingStat = FSDirEncryptionZoneOp.createEncryptionZone(dir, src,
            pc, metadata.getCipher(), keyName, logRetryCache);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, resultingStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, resultingStat);
  }

  /**
   * Get the encryption zone for the specified path.
   *
   * @param srcArg the path of a file or directory to get the EZ for.
   * @return the EZ of the of the path or null if none.
   * @throws AccessControlException  if the caller is not the superuser.
   * @throws UnresolvedLinkException if the path can't be resolved.
   */
  EncryptionZone getEZForPath(final String srcArg)
    throws AccessControlException, UnresolvedLinkException, IOException {
    final String operationName = "getEZForPath";
    FileStatus resultingStat = null;
    EncryptionZone encryptionZone;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    checkOperation(OperationCategory.READ);
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        Entry<EncryptionZone, FileStatus> ezForPath = FSDirEncryptionZoneOp
            .getEZForPath(dir, srcArg, pc);
        resultingStat = ezForPath.getValue();
        encryptionZone = ezForPath.getKey();
      } finally {
        readUnlock(operationName,
            getLockReportInfoSupplier(srcArg, null, resultingStat));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, srcArg, null, resultingStat);
      throw ace;
    }
    logAuditEvent(true, operationName, srcArg, null, resultingStat);
    return encryptionZone;
  }

  BatchedListEntries<EncryptionZone> listEncryptionZones(long prevId)
      throws IOException {
    final String operationName = "listEncryptionZones";
    boolean success = false;
    checkOperation(OperationCategory.READ);
    checkSuperuserPrivilege(operationName, dir.rootDir.getFullPathName());
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      final BatchedListEntries<EncryptionZone> ret =
          FSDirEncryptionZoneOp.listEncryptionZones(dir, prevId);
      success = true;
      return ret;
    } finally {
      readUnlock(operationName, getLockReportInfoSupplier(null));
      logAuditEvent(success, operationName, null);
    }
  }

  void reencryptEncryptionZone(final String zone, final ReencryptAction action,
      final boolean logRetryCache) throws IOException {
    final String operationName = "reencryptEncryptionZone";
    boolean success = false;
    checkSuperuserPrivilege(operationName, zone);
    try {
      Preconditions.checkNotNull(zone, "zone is null.");
      checkOperation(OperationCategory.WRITE);
      final FSPermissionChecker pc = dir.getPermissionChecker();
      checkNameNodeSafeMode("NameNode in safemode, cannot " + action
          + " re-encryption on zone " + zone);
      reencryptEncryptionZoneInt(pc, zone, action, logRetryCache);
      success = true;
    } finally {
      logAuditEvent(success, action + "reencryption", zone, null, null);
    }
  }

  BatchedListEntries<ZoneReencryptionStatus> listReencryptionStatus(
      final long prevId) throws IOException {
    final String operationName = "listReencryptionStatus";
    boolean success = false;
    checkOperation(OperationCategory.READ);
    checkSuperuserPrivilege(operationName, dir.rootDir.getFullPathName());
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      final BatchedListEntries<ZoneReencryptionStatus> ret =
          FSDirEncryptionZoneOp.listReencryptionStatus(dir, prevId);
      success = true;
      return ret;
    } finally {
      readUnlock(operationName, getLockReportInfoSupplier(null));
      logAuditEvent(success, operationName, null);
    }
  }

  private void reencryptEncryptionZoneInt(final FSPermissionChecker pc,
      final String zone, final ReencryptAction action,
      final boolean logRetryCache) throws IOException {
    if (getProvider() == null) {
      throw new IOException("No key provider configured, re-encryption "
          + "operation is rejected");
    }
    String keyVersionName = null;
    if (action == ReencryptAction.START) {
      // get zone's latest key version name out of the lock.
      keyVersionName =
          FSDirEncryptionZoneOp.getCurrentKeyVersion(dir, pc, zone);
      if (keyVersionName == null) {
        throw new IOException("Failed to get key version name for " + zone);
      }
      LOG.info("Re-encryption using key version " + keyVersionName
          + " for zone " + zone);
    }
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("NameNode in safemode, cannot " + action
          + " re-encryption on zone " + zone);
      List<XAttr> xattrs;
      dir.writeLock();
      try {
        final INodesInPath iip = dir.resolvePath(pc, zone, DirOp.WRITE);
        if (iip.getLastINode() == null) {
          throw new FileNotFoundException(zone + " does not exist.");
        }
        switch (action) {
        case START:
          xattrs = FSDirEncryptionZoneOp
              .reencryptEncryptionZone(dir, iip, keyVersionName);
          break;
        case CANCEL:
          xattrs =
              FSDirEncryptionZoneOp.cancelReencryptEncryptionZone(dir, iip);
          break;
        default:
          throw new IOException(
              "Re-encryption action " + action + " is not supported");
        }
      } finally {
        dir.writeUnlock();
      }
      if (xattrs != null && !xattrs.isEmpty()) {
        getEditLog().logSetXAttrs(zone, xattrs, logRetryCache);
      }
    } finally {
      writeUnlock(action + "reencryption", getLockReportInfoSupplier(zone));
    }
    getEditLog().logSync();
  }

  /**
   * Set an erasure coding policy on the given path.
   * @param srcArg  The path of the target directory.
   * @param ecPolicyName The erasure coding policy to set on the target
   *                    directory.
   * @throws AccessControlException  if the caller is not the superuser.
   * @throws UnresolvedLinkException if the path can't be resolved.
   * @throws SafeModeException       if the Namenode is in safe mode.
   */
  void setErasureCodingPolicy(final String srcArg, final String ecPolicyName,
      final boolean logRetryCache) throws IOException,
      UnresolvedLinkException, SafeModeException, AccessControlException {
    final String operationName = "setErasureCodingPolicy";
    checkOperation(OperationCategory.WRITE);
    checkErasureCodingSupported(operationName);
    FileStatus resultingStat = null;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot set erasure coding policy on " + srcArg);
      resultingStat = FSDirErasureCodingOp.setErasureCodingPolicy(this,
          srcArg, ecPolicyName, pc, logRetryCache);
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, srcArg);
      throw ace;
    } finally {
      writeUnlock(operationName,
          getLockReportInfoSupplier(srcArg, null, resultingStat));
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, srcArg, null, resultingStat);
  }

  /**
   * Add multiple erasure coding policies to the ErasureCodingPolicyManager.
   * @param policies The policies to add.
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *                      rebuilding
   * @return The according result of add operation.
   */
  AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies, final boolean logRetryCache)
      throws IOException {
    final String operationName = "addErasureCodingPolicies";
    List<String> addECPolicyNames = new ArrayList<>(policies.length);
    checkOperation(OperationCategory.WRITE);
    checkErasureCodingSupported(operationName);
    List<AddErasureCodingPolicyResponse> responses =
        new ArrayList<>(policies.length);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot add erasure coding policy");
      for (ErasureCodingPolicy policy : policies) {
        try {
          ErasureCodingPolicy newPolicy =
              FSDirErasureCodingOp.addErasureCodingPolicy(this, policy,
                  logRetryCache);
          addECPolicyNames.add(newPolicy.getName());
          responses.add(new AddErasureCodingPolicyResponse(newPolicy));
        } catch (HadoopIllegalArgumentException e) {
          responses.add(new AddErasureCodingPolicyResponse(policy, e));
        }
      }
    } finally {
      writeUnlock(operationName,
          getLockReportInfoSupplier(addECPolicyNames.toString()));
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, addECPolicyNames.toString());
    return responses.toArray(new AddErasureCodingPolicyResponse[0]);
  }

  /**
   * Remove an erasure coding policy.
   * @param ecPolicyName the name of the policy to be removed
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *                      rebuilding
   * @throws IOException
   */
  void removeErasureCodingPolicy(String ecPolicyName,
      final boolean logRetryCache) throws IOException {
    final String operationName = "removeErasureCodingPolicy";
    checkOperation(OperationCategory.WRITE);
    checkErasureCodingSupported(operationName);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot remove erasure coding policy "
          + ecPolicyName);
      FSDirErasureCodingOp.removeErasureCodingPolicy(this, ecPolicyName,
          logRetryCache);
    } finally {
      writeUnlock(operationName, getLockReportInfoSupplier(ecPolicyName));
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, ecPolicyName, null, null);
  }

  /**
   * Enable an erasure coding policy.
   * @param ecPolicyName the name of the policy to be enabled
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *                      rebuilding
   * @return
   * @throws IOException
   */
  boolean enableErasureCodingPolicy(String ecPolicyName,
      final boolean logRetryCache) throws IOException {
    final String operationName = "enableErasureCodingPolicy";
    checkOperation(OperationCategory.WRITE);
    checkErasureCodingSupported(operationName);
    boolean success = false;
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot enable erasure coding policy "
            + ecPolicyName);
        success = FSDirErasureCodingOp.enableErasureCodingPolicy(this,
            ecPolicyName, logRetryCache);
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(ecPolicyName));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, ecPolicyName);
      throw ace;
    }
    if (success) {
      getEditLog().logSync();
      logAuditEvent(true, operationName, ecPolicyName);
    }
    return success;
  }

  /**
   * Disable an erasure coding policy.
   * @param ecPolicyName the name of the policy to be disabled
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *                      rebuilding
   * @throws IOException
   */
  boolean disableErasureCodingPolicy(String ecPolicyName,
      final boolean logRetryCache) throws IOException {
    final String operationName = "disableErasureCodingPolicy";
    checkOperation(OperationCategory.WRITE);
    checkErasureCodingSupported(operationName);
    boolean success = false;
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot disable erasure coding policy "
            + ecPolicyName);
        success = FSDirErasureCodingOp.disableErasureCodingPolicy(this,
            ecPolicyName, logRetryCache);
      } finally {
        writeUnlock(operationName, getLockReportInfoSupplier(ecPolicyName));
      }
    } catch (AccessControlException ace) {
      logAuditEvent(false, operationName, ecPolicyName);
      throw ace;
    }
    if (success) {
      getEditLog().logSync();
      logAuditEvent(true, operationName, ecPolicyName);
    }
    return success;
  }

  /**
   * Unset an erasure coding policy from the given path.
   * @param srcArg  The path of the target directory.
   * @throws AccessControlException  if the caller is not the superuser.
   * @throws UnresolvedLinkException if the path can't be resolved.
   * @throws SafeModeException       if the Namenode is in safe mode.
   */
  void unsetErasureCodingPolicy(final String srcArg,
      final boolean logRetryCache) throws IOException,
      UnresolvedLinkException, SafeModeException, AccessControlException {
    final String operationName = "unsetErasureCodingPolicy";
    checkOperation(OperationCategory.WRITE);
    checkErasureCodingSupported(operationName);
    FileStatus resultingStat = null;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot unset erasure coding policy on " + srcArg);
      resultingStat = FSDirErasureCodingOp.unsetErasureCodingPolicy(this,
          srcArg, pc, logRetryCache);
    } finally {
      writeUnlock(operationName,
          getLockReportInfoSupplier(srcArg, null, resultingStat));
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, srcArg, null, resultingStat);
  }

  /**
   * Verifies if the given policies are supported in the given cluster setup.
   * If not policy is specified checks for all enabled policies.
   * @param policyNames name of policies.
   * @return the result if the given policies are supported in the cluster setup
   * @throws IOException
   */
  public ECTopologyVerifierResult getECTopologyResultForPolicies(
      String[] policyNames) throws IOException {
    String operationName = "getECTopologyResultForPolicies";
    checkSuperuserPrivilege(operationName);
    checkOperation(OperationCategory.UNCHECKED);
    ECTopologyVerifierResult result;
    readLock();
    try {
      checkOperation(OperationCategory.UNCHECKED);
      // If no policy name is specified return the result
      // for all enabled policies.
      if (policyNames == null || policyNames.length == 0) {
        result = getEcTopologyVerifierResultForEnabledPolicies();
      } else {
        Collection<ErasureCodingPolicy> policies =
            new ArrayList<ErasureCodingPolicy>();
        for (int i = 0; i < policyNames.length; i++) {
          policies.add(FSDirErasureCodingOp
              .getErasureCodingPolicyByName(this, policyNames[i]));
        }
        int numOfDataNodes =
            getBlockManager().getDatanodeManager().getNumOfDataNodes();
        int numOfRacks =
            getBlockManager().getDatanodeManager().getNetworkTopology()
                .getNumOfNonEmptyRacks();
        result = ECTopologyVerifier
            .getECTopologyVerifierResult(numOfRacks, numOfDataNodes, policies);
      }
    } finally {
      readUnlock(operationName, getLockReportInfoSupplier(null));
    }
    logAuditEvent(true, operationName, null);
    return result;
  }

  /**
   * Get the erasure coding policy information for specified path.
   */
  ErasureCodingPolicy getErasureCodingPolicy(String src)
      throws AccessControlException, UnresolvedLinkException, IOException {
    final String operationName = "getErasureCodingPolicy";
    boolean success = false;
    checkOperation(OperationCategory.READ);
    checkErasureCodingSupported(operationName);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      final ErasureCodingPolicy ret =
          FSDirErasureCodingOp.getErasureCodingPolicy(this, src, pc);
      success = true;
      return ret;
    } finally {
      readUnlock(operationName, getLockReportInfoSupplier(src));
      logAuditEvent(success, operationName, src);
    }
  }

  /**
   * Get all erasure coding polices.
   */
  ErasureCodingPolicyInfo[] getErasureCodingPolicies() throws IOException {
    final String operationName = "getErasureCodingPolicies";
    boolean success = false;
    checkOperation(OperationCategory.READ);
    checkErasureCodingSupported(operationName);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      final ErasureCodingPolicyInfo[] ret =
          FSDirErasureCodingOp.getErasureCodingPolicies(this);
      success = true;
      return ret;
    } finally {
      readUnlock(operationName, getLockReportInfoSupplier(null));
      logAuditEvent(success, operationName, null);
    }
  }

  /**
   * Get available erasure coding codecs and corresponding coders.
   */
  Map<String, String> getErasureCodingCodecs() throws IOException {
    final String operationName = "getErasureCodingCodecs";
    boolean success = false;
    checkOperation(OperationCategory.READ);
    checkErasureCodingSupported(operationName);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      final Map<String, String> ret =
          FSDirErasureCodingOp.getErasureCodingCodecs(this);
      success = true;
      return ret;
    } finally {
      readUnlock(operationName, getLockReportInfoSupplier(null));
      logAuditEvent(success, operationName, null);
    }
  }

  void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag,
                boolean logRetryCache)
      throws IOException {
    final String operationName = "setXAttr";
    FileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot set XAttr on " + src);
        auditStat = FSDirXAttrOp.setXAttr(dir, pc, src, xAttr, flag,
            logRetryCache);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, auditStat);
  }

  List<XAttr> getXAttrs(final String src, List<XAttr> xAttrs)
      throws IOException {
    final String operationName = "getXAttrs";
    checkOperation(OperationCategory.READ);
    List<XAttr> fsXattrs;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        fsXattrs = FSDirXAttrOp.getXAttrs(dir, pc, src, xAttrs);
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(src));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    logAuditEvent(true, operationName, src);
    return fsXattrs;
  }

  List<XAttr> listXAttrs(String src) throws IOException {
    final String operationName = "listXAttrs";
    checkOperation(OperationCategory.READ);
    List<XAttr> fsXattrs;
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        fsXattrs = FSDirXAttrOp.listXAttrs(dir, pc, src);
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(src));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    logAuditEvent(true, operationName, src);
    return fsXattrs;
  }

  void removeXAttr(String src, XAttr xAttr, boolean logRetryCache)
      throws IOException {
    final String operationName = "removeXAttr";
    FileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot remove XAttr entry on " + src);
        auditStat = FSDirXAttrOp.removeXAttr(dir, pc, src, xAttr,
            logRetryCache);
      } finally {
        writeUnlock(operationName,
            getLockReportInfoSupplier(src, null, auditStat));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    getEditLog().logSync();
    logAuditEvent(true, operationName, src, null, auditStat);
  }

  @Override
  public void removeXattr(long id, String xattrName) throws IOException {
    writeLock();
    try {
      final INode inode = dir.getInode(id);
      if (inode == null) {
        return;
      }
      final XAttrFeature xaf = inode.getXAttrFeature();
      if (xaf == null) {
        return;
      }
      final XAttr spsXAttr = xaf.getXAttr(xattrName);

      if (spsXAttr != null) {
        FSDirSatisfyStoragePolicyOp.removeSPSXattr(dir, inode, spsXAttr);
      }
    } finally {
      writeUnlock("removeXAttr");
    }
    getEditLog().logSync();
  }

  void checkAccess(String src, FsAction mode) throws IOException {
    final String operationName = "checkAccess";
    checkOperation(OperationCategory.READ);
    final FSPermissionChecker pc = getPermissionChecker();
    FSPermissionChecker.setOperationType(operationName);
    try {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        final INodesInPath iip = dir.resolvePath(pc, src, DirOp.READ);
        src = iip.getPath();
        INode inode = iip.getLastINode();
        if (inode == null) {
          throw new FileNotFoundException("Path not found: " + src);
        }
        if (isPermissionEnabled) {
          dir.checkPathAccess(pc, iip, mode);
        }
      } finally {
        readUnlock(operationName, getLockReportInfoSupplier(src));
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, operationName, src);
      throw e;
    }
    logAuditEvent(true, operationName, src);
  }

  /**
   * Check if snapshot roots are created for all existing snapshottable
   * directories. Create them if not.
   * Only the active NameNode needs to execute this in HA setup once it is out
   * of safe mode.
   *
   * The function gets called while exiting safe mode or post starting the
   * services in Active NameNode, but comes into effect post whichever event
   * happens later.
   */
  @Override
  public synchronized void checkAndProvisionSnapshotTrashRoots() {
    if (isSnapshotTrashRootEnabled && (haEnabled && inActiveState()
        || !haEnabled) && !blockManager.isInSafeMode()) {
      SnapshottableDirectoryStatus dirStatus = null;
      try {
        SnapshottableDirectoryStatus[] dirStatusList =
            getSnapshottableDirListing();
        if (dirStatusList == null) {
          return;
        }
        for (SnapshottableDirectoryStatus status : dirStatusList) {
          dirStatus = status;
          String currDir = dirStatus.getFullPath().toString();
          if (!currDir.endsWith(Path.SEPARATOR)) {
            currDir += Path.SEPARATOR;
          }
          String trashPath = currDir + FileSystem.TRASH_PREFIX;
          HdfsFileStatus fileStatus =
              getFileInfo(trashPath, false, false, false);
          if (fileStatus == null) {
            LOG.info("Trash doesn't exist for snapshottable directory {}. "
                + "Creating trash at {}", currDir, trashPath);
            PermissionStatus permissionStatus =
                new PermissionStatus(getRemoteUser().getShortUserName(), null,
                    SHARED_TRASH_PERMISSION);
            mkdirs(trashPath, permissionStatus, false);
          }
        }
      } catch (IOException e) {
        if (dirStatus == null) {
          LOG.error("Failed to get snapshottable directory list", e);
        } else {
          LOG.error("Could not provision Trash directory for existing "
              + "snapshottable directory {}", dirStatus, e);
        }
      }
    }
  }

  private Supplier<String> getLockReportInfoSupplier(String src) {
    return getLockReportInfoSupplier(src, null);
  }

  private Supplier<String> getLockReportInfoSupplier(String src, String dst) {
    return getLockReportInfoSupplier(src, dst, (FileStatus) null);
  }

  private Supplier<String> getLockReportInfoSupplier(String src, String dst,
      HdfsFileStatus stat) {
    FileStatus status = null;
    if (stat != null) {
      Path symlink = stat.isSymlink()
          ? new Path(DFSUtilClient.bytes2String(stat.getSymlinkInBytes()))
          : null;
      Path path = new Path(src);
      status = new FileStatus(stat.getLen(), stat.isDirectory(),
        stat.getReplication(), stat.getBlockSize(),
        stat.getModificationTime(),
        stat.getAccessTime(), stat.getPermission(), stat.getOwner(),
        stat.getGroup(), symlink, path);
    }
    return getLockReportInfoSupplier(src, dst, status);
  }

  private Supplier<String> getLockReportInfoSupplier(String src, String dst,
      FileStatus status) {
    return () -> {
      UserGroupInformation ugi = Server.getRemoteUser();
      String userName = ugi != null ? ugi.toString() : null;
      InetAddress addr = Server.getRemoteIp();
      StringBuilder sb = new StringBuilder();
      String s = escapeJava(src);
      String d = escapeJava(dst);
      sb.append("ugi=").append(userName).append(",")
          .append("ip=").append(addr).append(",")
          .append("src=").append(s).append(",")
          .append("dst=").append(d).append(",");
      if (null == status) {
        sb.append("perm=null");
      } else {
        sb.append("perm=")
            .append(status.getOwner()).append(":")
            .append(status.getGroup()).append(":")
            .append(status.getPermission());
      }
      return sb.toString();
    };
  }

  /**
   * FSNamesystem Default AuditLogger implementation;used when no access logger
   * is defined in the config file. It can also be explicitly listed in the
   * config file.
   */
  @VisibleForTesting
  static class FSNamesystemAuditLogger extends DefaultAuditLogger {

    @Override
    public void initialize(Configuration conf) {
      isCallerContextEnabled = conf.getBoolean(
          HADOOP_CALLER_CONTEXT_ENABLED_KEY,
          HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT);
      callerContextMaxLen = conf.getInt(
          HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY,
          HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT);
      callerSignatureMaxLen = conf.getInt(
          HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY,
          HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT);
      logTokenTrackingId = conf.getBoolean(
          DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY,
          DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT);

      debugCmdSet.addAll(Arrays.asList(conf.getTrimmedStrings(
          DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_DEBUG_CMDLIST)));
    }

    @Override
    public void logAuditEvent(boolean succeeded, String userName,
        InetAddress addr, String cmd, String src, String dst,
        FileStatus status, CallerContext callerContext, UserGroupInformation ugi,
        DelegationTokenSecretManager dtSecretManager) {

      if (AUDIT_LOG.isDebugEnabled() ||
          (AUDIT_LOG.isInfoEnabled() && !debugCmdSet.contains(cmd))) {
        final StringBuilder sb = STRING_BUILDER.get();
        src = escapeJava(src);
        dst = escapeJava(dst);
        sb.setLength(0);
        String ipAddr = addr != null ? "/" + addr.getHostAddress() : "null";
        sb.append("allowed=").append(succeeded).append("\t")
            .append("ugi=").append(userName).append("\t")
            .append("ip=").append(ipAddr).append("\t")
            .append("cmd=").append(cmd).append("\t")
            .append("src=").append(src).append("\t")
            .append("dst=").append(dst).append("\t");
        if (null == status) {
          sb.append("perm=null");
        } else {
          sb.append("perm=")
              .append(status.getOwner()).append(":")
              .append(status.getGroup()).append(":")
              .append(status.getPermission());
        }
        if (logTokenTrackingId) {
          sb.append("\t").append("trackingId=");
          String trackingId = null;
          if (ugi != null && dtSecretManager != null
              && ugi.getAuthenticationMethod() == AuthenticationMethod.TOKEN) {
            for (TokenIdentifier tid: ugi.getTokenIdentifiers()) {
              if (tid instanceof DelegationTokenIdentifier) {
                DelegationTokenIdentifier dtid =
                    (DelegationTokenIdentifier)tid;
                trackingId = dtSecretManager.getTokenTrackingId(dtid);
                break;
              }
            }
          }
          sb.append(trackingId);
        }
        sb.append("\t").append("proto=")
            .append(Server.getProtocol());
        if (isCallerContextEnabled &&
            callerContext != null &&
            callerContext.isContextValid()) {
          sb.append("\t").append("callerContext=");
          String context = escapeJava(callerContext.getContext());
          if (context.length() > callerContextMaxLen) {
            sb.append(context, 0, callerContextMaxLen);
          } else {
            sb.append(context);
          }
          if (callerContext.getSignature() != null &&
              callerContext.getSignature().length > 0 &&
              callerContext.getSignature().length <= callerSignatureMaxLen) {
            sb.append(":")
                .append(escapeJava(new String(callerContext.getSignature(),
                CallerContext.SIGNATURE_ENCODING)));
          }
        }
        logAuditMessage(sb.toString());
      }
    }

    @Override
    public void logAuditEvent(boolean succeeded, String userName,
        InetAddress addr, String cmd, String src, String dst,
        FileStatus status, UserGroupInformation ugi,
        DelegationTokenSecretManager dtSecretManager) {
      this.logAuditEvent(succeeded, userName, addr, cmd, src, dst, status,
              null /*CallerContext*/, ugi, dtSecretManager);
    }

    public void logAuditMessage(String message) {
      AUDIT_LOG.info(message);
    }
  }

  /**
   * Return total number of Sync Operations on FSEditLog.
   */
  @Override
  @Metric({"TotalSyncCount",
              "Total number of sync operations performed on edit logs"})
  public long getTotalSyncCount() {
    return fsImage.editLog.getTotalSyncCount();
  }

  /**
   * Return total time spent doing sync operations on FSEditLog.
   */
  @Override
  @Metric({"TotalSyncTimes",
              "Total time spend in sync operation on various edit logs"})
  public String getTotalSyncTimes() {
    JournalSet journalSet = fsImage.editLog.getJournalSet();
    if (journalSet != null) {
      return journalSet.getSyncTimes();
    } else {
      return "";
    }
  }

  /**
   * Gets number of bytes in the blocks in future generation stamps.
   *
   * @return number of bytes that can be deleted if exited from safe mode.
   */
  public long getBytesInFuture() {
    return blockManager.getBytesInFuture();
  }


  @Override // FSNamesystemMBean
  @Metric({"NumInMaintenanceLiveDataNodes",
      "Number of live Datanodes which are in maintenance state"})
  public int getNumInMaintenanceLiveDataNodes() {
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    getBlockManager().getDatanodeManager().fetchDatanodes(live, null, true);
    int liveInMaintenance = 0;
    for (DatanodeDescriptor node : live) {
      liveInMaintenance += node.isInMaintenance() ? 1 : 0;
    }
    return liveInMaintenance;
  }

  @Override // FSNamesystemMBean
  @Metric({"NumInMaintenanceDeadDataNodes",
      "Number of dead Datanodes which are in maintenance state"})
  public int getNumInMaintenanceDeadDataNodes() {
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    getBlockManager().getDatanodeManager().fetchDatanodes(null, dead, true);
    int deadInMaintenance = 0;
    for (DatanodeDescriptor node : dead) {
      deadInMaintenance += node.isInMaintenance() ? 1 : 0;
    }
    return deadInMaintenance;
  }

  @Override // FSNamesystemMBean
  @Metric({"NumEnteringMaintenanceDataNodes",
      "Number of Datanodes that are entering the maintenance state"})
  public int getNumEnteringMaintenanceDataNodes() {
    return getBlockManager().getDatanodeManager().getEnteringMaintenanceNodes()
        .size();
  }

  @Override // NameNodeMXBean
  public String getVerifyECWithTopologyResult() {
    ECTopologyVerifierResult result =
        getEcTopologyVerifierResultForEnabledPolicies();

    Map<String, String> resultMap = new HashMap<String, String>();
    resultMap.put("isSupported", Boolean.toString(result.isSupported()));
    resultMap.put("resultMessage", result.getResultMessage());
    return JSON.toString(resultMap);
  }

  private ECTopologyVerifierResult getEcTopologyVerifierResultForEnabledPolicies() {
    int numOfDataNodes =
        getBlockManager().getDatanodeManager().getNumOfDataNodes();
    int numOfRacks = getBlockManager().getDatanodeManager().getNetworkTopology()
        .getNumOfNonEmptyRacks();
    ErasureCodingPolicy[] enabledEcPolicies =
        getErasureCodingPolicyManager().getCopyOfEnabledPolicies();
    return ECTopologyVerifier
        .getECTopologyVerifierResult(numOfRacks, numOfDataNodes,
            Arrays.asList(enabledEcPolicies));
  }

  // This method logs operationName without super user privilege.
  // It should be called without holding FSN lock.
  void checkSuperuserPrivilege(String operationName, String path)
      throws IOException {
    if (isPermissionEnabled) {
      try {
        FSPermissionChecker.setOperationType(operationName);
        FSPermissionChecker pc = getPermissionChecker();
        pc.checkSuperuserPrivilege(path);
      } catch(AccessControlException ace){
        logAuditEvent(false, operationName, path);
        throw ace;
      }
    }
  }

  String getQuotaCommand(long nsQuota, long dsQuota) {
    if (nsQuota == HdfsConstants.QUOTA_RESET
        && dsQuota == HdfsConstants.QUOTA_DONT_SET) {
      return "clearQuota";
    } else if (nsQuota == HdfsConstants.QUOTA_DONT_SET
        && dsQuota == HdfsConstants.QUOTA_RESET) {
      return "clearSpaceQuota";
    } else if (dsQuota == HdfsConstants.QUOTA_DONT_SET) {
      return "setQuota";
    } else {
      return "setSpaceQuota";
    }
  }

  String getFailedStorageCommand(String mode) {
    if(mode.equals("check")) {
      return "checkRestoreFailedStorage";
    } else if (mode.equals("true")) {
      return "enableRestoreFailedStorage";
    } else {
      return "disableRestoreFailedStorage";
    }
  }

  /**
   * Check whether operation is supported.
   * @param operationName the name of operation.
   * @throws UnsupportedActionException throws UAE if not supported.
   */
  public void checkErasureCodingSupported(String operationName)
      throws UnsupportedActionException {
    if (!NameNodeLayoutVersion.supports(
        NameNodeLayoutVersion.Feature.ERASURE_CODING,
        getEffectiveLayoutVersion())) {
      throw new UnsupportedActionException(operationName + " not supported.");
    }
  }

  private boolean isObserver() {
    return haEnabled && haContext != null && haContext.getState().getServiceState() == OBSERVER;
  }

  private void checkBlockLocationsWhenObserver(LocatedBlocks blocks, String src)
      throws ObserverRetryOnActiveException {
    if (blocks == null) {
      return;
    }
    List<LocatedBlock> locatedBlockList = blocks.getLocatedBlocks();
    if (locatedBlockList != null) {
      for (LocatedBlock b : locatedBlockList) {
        if (b.getLocations() == null || b.getLocations().length == 0) {
          throw new ObserverRetryOnActiveException("Zero blocklocations for " + src);
        }
      }
    }
  }
}
