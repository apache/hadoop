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

import static org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_ASYNC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_ASYNC_KEY;
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
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SUPPORT_APPEND_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SUPPORT_APPEND_KEY;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.SECURITY_XATTR_UNREADABLE_BY_SUPERUSER;
import static org.apache.hadoop.util.Time.now;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.UnknownCryptoProtocolVersionException;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeException;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager.AccessMode;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager.SecretManagerState;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
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
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.ha.EditLogTailer;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.StandbyCheckpointer;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Status;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.server.namenode.top.TopAuditLogger;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.hdfs.server.namenode.top.metrics.TopMetrics;
import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.RetryCache;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.ChunkedArrayList;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.log4j.Appender;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.mortbay.util.ajax.JSON;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/***************************************************
 * FSNamesystem does the actual bookkeeping work for the
 * DataNode.
 *
 * It tracks several important tables.
 *
 * 1)  valid fsname --> blocklist  (kept on disk, logged)
 * 2)  Set of all valid blocks (inverted #1)
 * 3)  block --> machinelist (kept in memory, rebuilt dynamically from reports)
 * 4)  machine --> blocklist (inverted #2)
 * 5)  LRU cache of updated-heartbeat machines
 ***************************************************/
@InterfaceAudience.Private
@Metrics(context="dfs")
public class FSNamesystem implements Namesystem, FSNamesystemMBean,
  NameNodeMXBean {
  public static final Log LOG = LogFactory.getLog(FSNamesystem.class);

  private static final ThreadLocal<StringBuilder> auditBuffer =
    new ThreadLocal<StringBuilder>() {
      @Override
      protected StringBuilder initialValue() {
        return new StringBuilder();
      }
  };

  private final BlockIdManager blockIdManager;

  @VisibleForTesting
  public boolean isAuditEnabled() {
    return !isDefaultAuditLogger || auditLog.isInfoEnabled();
  }

  private void logAuditEvent(boolean succeeded, String cmd, String src)
      throws IOException {
    logAuditEvent(succeeded, cmd, src, null, null);
  }
  
  private void logAuditEvent(boolean succeeded, String cmd, String src,
      String dst, HdfsFileStatus stat) throws IOException {
    if (isAuditEnabled() && isExternalInvocation()) {
      logAuditEvent(succeeded, getRemoteUser(), getRemoteIp(),
                    cmd, src, dst, stat);
    }
  }

  private void logAuditEvent(boolean succeeded,
      UserGroupInformation ugi, InetAddress addr, String cmd, String src,
      String dst, HdfsFileStatus stat) {
    FileStatus status = null;
    if (stat != null) {
      Path symlink = stat.isSymlink() ? new Path(stat.getSymlink()) : null;
      Path path = dst != null ? new Path(dst) : new Path(src);
      status = new FileStatus(stat.getLen(), stat.isDir(),
          stat.getReplication(), stat.getBlockSize(), stat.getModificationTime(),
          stat.getAccessTime(), stat.getPermission(), stat.getOwner(),
          stat.getGroup(), symlink, path);
    }
    for (AuditLogger logger : auditLoggers) {
      if (logger instanceof HdfsAuditLogger) {
        HdfsAuditLogger hdfsLogger = (HdfsAuditLogger) logger;
        hdfsLogger.logAuditEvent(succeeded, ugi.toString(), addr, cmd, src, dst,
            status, ugi, dtSecretManager);
      } else {
        logger.logAuditEvent(succeeded, ugi.toString(), addr,
            cmd, src, dst, status);
      }
    }
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
  public static final Log auditLog = LogFactory.getLog(
      FSNamesystem.class.getName() + ".audit");

  static final int DEFAULT_MAX_CORRUPT_FILEBLOCKS_RETURNED = 100;
  static int BLOCK_DELETION_INCREMENT = 1000;
  private final boolean isPermissionEnabled;
  private final UserGroupInformation fsOwner;
  private final String supergroup;
  private final boolean standbyShouldCheckpoint;
  
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

  /** The namespace tree. */
  FSDirectory dir;
  private final BlockManager blockManager;
  private final SnapshotManager snapshotManager;
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

  // Block pool ID used by this namenode
  private String blockPoolId;

  final LeaseManager leaseManager = new LeaseManager(this); 

  volatile Daemon smmthread = null;  // SafeModeMonitor thread
  
  Daemon nnrmthread = null; // NamenodeResourceMonitor thread

  Daemon nnEditLogRoller = null; // NameNodeEditLogRoller thread

  // A daemon to periodically clean up corrupt lazyPersist files
  // from the name space.
  Daemon lazyPersistFileScrubber = null;
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
  private final boolean supportAppends;
  private final ReplaceDatanodeOnFailure dtpReplaceDatanodeOnFailure;

  private volatile SafeModeInfo safeMode;  // safe mode information

  private final long maxFsObjects;          // maximum number of fs objects

  private final long minBlockSize;         // minimum block size
  private final long maxBlocksPerFile;     // maximum # of blocks per file

  // precision of access times.
  private final long accessTimePrecision;

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
   * Used when this NN is in standby state to read from the shared edit log.
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

  /** flag indicating whether replication queues have been initialized */
  boolean initializedReplQueues = false;

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
      writeUnlock();
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
   * Block until the object is imageLoaded to be used.
   */
  void waitForLoadingFSImage() {
    if (!imageLoaded) {
      writeLock();
      try {
        while (!imageLoaded) {
          try {
            cond.await(5000, TimeUnit.MILLISECONDS);
          } catch (InterruptedException ignored) {
          }
        }
      } finally {
        writeUnlock();
      }
    }
  }

  /**
   * Clear all loaded data
   */
  void clear() {
    dir.reset();
    dtSecretManager.reset();
    blockIdManager.clear();
    leaseManager.removeAllLeases();
    snapshotManager.clearSnapshottableDirs();
    cacheManager.clear();
    setImageLoaded(false);
    blockManager.clear();
  }

  @VisibleForTesting
  LeaseManager getLeaseManager() {
    return leaseManager;
  }
  
  boolean isHaEnabled() {
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
        throw new IllegalArgumentException(
            "Required edits directory " + u.toString() + " not present in " +
            DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY + ". " +
            DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY + "=" +
            editsDirs.toString() + "; " +
            DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY + "=" +
            requiredEditsDirs.toString() + ". " +
            DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY + "=" +
            sharedEditsDirs.toString() + ".");
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
    if (provider == null) {
      LOG.info("No KeyProvider found.");
    } else {
      LOG.info("Found KeyProvider: " + provider.toString());
    }
    if (conf.getBoolean(DFS_NAMENODE_AUDIT_LOG_ASYNC_KEY,
                        DFS_NAMENODE_AUDIT_LOG_ASYNC_DEFAULT)) {
      LOG.info("Enabling async auditlog");
      enableAsyncAuditLog();
    }
    boolean fair = conf.getBoolean("dfs.namenode.fslock.fair", true);
    LOG.info("fsLock is fair:" + fair);
    fsLock = new FSNamesystemLock(fair);
    cond = fsLock.writeLock().newCondition();
    cpLock = new ReentrantLock();

    this.fsImage = fsImage;
    try {
      resourceRecheckInterval = conf.getLong(
          DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
          DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT);

      this.blockManager = new BlockManager(this, conf);
      this.datanodeStatistics = blockManager.getDatanodeManager().getDatanodeStatistics();
      this.blockIdManager = new BlockIdManager(blockManager);

      this.fsOwner = UserGroupInformation.getCurrentUser();
      this.supergroup = conf.get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY, 
                                 DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
      this.isPermissionEnabled = conf.getBoolean(DFS_PERMISSIONS_ENABLED_KEY,
                                                 DFS_PERMISSIONS_ENABLED_DEFAULT);
      LOG.info("fsOwner             = " + fsOwner);
      LOG.info("supergroup          = " + supergroup);
      LOG.info("isPermissionEnabled = " + isPermissionEnabled);

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

      // Get the checksum type from config
      String checksumTypeStr = conf.get(DFS_CHECKSUM_TYPE_KEY, DFS_CHECKSUM_TYPE_DEFAULT);
      DataChecksum.Type checksumType;
      try {
         checksumType = DataChecksum.Type.valueOf(checksumTypeStr);
      } catch (IllegalArgumentException iae) {
         throw new IOException("Invalid checksum type in "
            + DFS_CHECKSUM_TYPE_KEY + ": " + checksumTypeStr);
      }

      this.serverDefaults = new FsServerDefaults(
          conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT),
          conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY, DFS_BYTES_PER_CHECKSUM_DEFAULT),
          conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY, DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT),
          (short) conf.getInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT),
          conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),
          conf.getBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY, DFS_ENCRYPT_DATA_TRANSFER_DEFAULT),
          conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT),
          checksumType);
      
      this.maxFsObjects = conf.getLong(DFS_NAMENODE_MAX_OBJECTS_KEY, 
                                       DFS_NAMENODE_MAX_OBJECTS_DEFAULT);

      this.minBlockSize = conf.getLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY,
          DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_DEFAULT);
      this.maxBlocksPerFile = conf.getLong(DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY,
          DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_DEFAULT);
      this.accessTimePrecision = conf.getLong(DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
          DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT);
      this.supportAppends = conf.getBoolean(DFS_SUPPORT_APPEND_KEY, DFS_SUPPORT_APPEND_DEFAULT);
      LOG.info("Append Enabled: " + supportAppends);

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

      if (this.lazyPersistFileScrubIntervalSec == 0) {
        throw new IllegalArgumentException(
            DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC + " must be non-zero.");
      }

      // For testing purposes, allow the DT secret manager to be started regardless
      // of whether security is enabled.
      alwaysUseDelegationTokensForTests = conf.getBoolean(
          DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
          DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_DEFAULT);
      
      this.dtSecretManager = createDelegationTokenSecretManager(conf);
      this.dir = new FSDirectory(this, conf);
      this.snapshotManager = new SnapshotManager(dir);
      this.cacheManager = new CacheManager(this, conf, blockManager);
      this.safeMode = new SafeModeInfo(conf);
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

  @VisibleForTesting
  public List<AuditLogger> getAuditLoggers() {
    return auditLoggers;
  }

  @VisibleForTesting
  public RetryCache getRetryCache() {
    return retryCache;
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

  private List<AuditLogger> initAuditLoggers(Configuration conf) {
    // Initialize the custom access loggers if configured.
    Collection<String> alClasses = conf.getStringCollection(DFS_NAMENODE_AUDIT_LOGGERS_KEY);
    List<AuditLogger> auditLoggers = Lists.newArrayList();
    if (alClasses != null && !alClasses.isEmpty()) {
      for (String className : alClasses) {
        try {
          AuditLogger logger;
          if (DFS_NAMENODE_DEFAULT_AUDIT_LOGGER_NAME.equals(className)) {
            logger = new DefaultAuditLogger();
          } else {
            logger = (AuditLogger) Class.forName(className).newInstance();
          }
          logger.initialize(conf);
          auditLoggers.add(logger);
        } catch (RuntimeException re) {
          throw re;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    // Make sure there is at least one logger installed.
    if (auditLoggers.isEmpty()) {
      auditLoggers.add(new DefaultAuditLogger());
    }

    // Add audit logger to calculate top users
    if (topConf.isEnabled) {
      topMetrics = new TopMetrics(conf, topConf.nntopReportingPeriodsMs);
      auditLoggers.add(new TopAuditLogger(topMetrics));
    }

    return Collections.unmodifiableList(auditLoggers);
  }

  private void loadFSImage(StartupOption startOpt) throws IOException {
    final FSImage fsImage = getFSImage();

    // format before starting up if requested
    if (startOpt == StartupOption.FORMAT) {
      
      fsImage.format(this, fsImage.getStorage().determineClusterId());// reuse current id

      startOpt = StartupOption.REGULAR;
    }
    boolean success = false;
    writeLock();
    try {
      // We shouldn't be calling saveNamespace if we've come up in standby state.
      MetaRecoveryContext recovery = startOpt.createRecoveryContext();
      final boolean staleImage
          = fsImage.recoverTransitionRead(startOpt, this, recovery);
      if (RollingUpgradeStartupOption.ROLLBACK.matches(startOpt) ||
          RollingUpgradeStartupOption.DOWNGRADE.matches(startOpt)) {
        rollingUpgradeInfo = null;
      }
      final boolean needToSave = staleImage && !haEnabled && !isRollingUpgrade(); 
      LOG.info("Need to save fs image? " + needToSave
          + " (staleImage=" + staleImage + ", haEnabled=" + haEnabled
          + ", isRollingUpgrade=" + isRollingUpgrade() + ")");
      if (needToSave) {
        fsImage.saveNamespace(this);
      } else {
        updateStorageVersionForRollingUpgrade(fsImage.getLayoutVersion(),
            startOpt);
        // No need to save, so mark the phase done.
        StartupProgress prog = NameNode.getStartupProgress();
        prog.beginPhase(Phase.SAVING_CHECKPOINT);
        prog.endPhase(Phase.SAVING_CHECKPOINT);
      }
      // This will start a new log segment and write to the seen_txid file, so
      // we shouldn't do it when coming up in standby state
      if (!haEnabled || (haEnabled && startOpt == StartupOption.UPGRADE)
          || (haEnabled && startOpt == StartupOption.UPGRADEONLY)) {
        fsImage.openEditLogForWrite();
      }
      success = true;
    } finally {
      if (!success) {
        fsImage.close();
      }
      writeUnlock();
    }
    imageLoadComplete();
  }

  private void updateStorageVersionForRollingUpgrade(final long layoutVersion,
      StartupOption startOpt) throws IOException {
    boolean rollingStarted = RollingUpgradeStartupOption.STARTED
        .matches(startOpt) && layoutVersion > HdfsConstants
        .NAMENODE_LAYOUT_VERSION;
    boolean rollingRollback = RollingUpgradeStartupOption.ROLLBACK
        .matches(startOpt);
    if (rollingRollback || rollingStarted) {
      fsImage.updateStorageVersion();
    }
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
  
  private void startSecretManagerIfNecessary() {
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
      assert safeMode != null && !isPopulatingReplQueues();
      StartupProgress prog = NameNode.getStartupProgress();
      prog.beginPhase(Phase.SAFEMODE);
      prog.setTotal(Phase.SAFEMODE, STEP_AWAITING_REPORTED_BLOCKS,
        getCompleteBlocksTotal());
      setBlockTotal();
      blockManager.activate(conf);
    } finally {
      writeUnlock();
    }
    
    registerMXBean();
    DefaultMetricsSystem.instance().register(this);
    if (inodeAttributeProvider != null) {
      inodeAttributeProvider.start();
      dir.setINodeAttributeProvider(inodeAttributeProvider);
    }
    snapshotManager.registerMXBean();
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
      writeUnlock();
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
        editLog.recoverUnclosedStreams();
        
        LOG.info("Catching up to latest edits from old active before " +
            "taking over writer role in edits logs");
        editLogTailer.catchupDuringFailover();
        
        blockManager.setPostponeBlocksFromFuture(false);
        blockManager.getDatanodeManager().markAllDatanodesStale();
        blockManager.clearQueues();
        blockManager.processAllPendingDNMessages();

        // Only need to re-process the queue, If not in SafeMode.
        if (!isInSafeMode()) {
          LOG.info("Reprocessing replication and invalidation queues");
          initializeReplQueues();
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("NameNode metadata after re-processing " +
              "replication and invalidation queues during failover:\n" +
              metaSaveAsString());
        }
        
        long nextTxId = getFSImage().getLastAppliedTxId() + 1;
        LOG.info("Will take over writing edit logs at txnid " + 
            nextTxId);
        editLog.setNextTxId(nextTxId);

        getFSImage().editLog.openForWrite();
      }

      // Enable quota checks.
      dir.enableQuotaChecks();
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
      }

      cacheManager.startMonitorThread();
      blockManager.getDatanodeManager().setShouldSendCachingCommands(true);
    } finally {
      startingActiveService = false;
      checkSafeMode();
      writeUnlock();
    }
  }

  /**
   * Initialize replication queues.
   */
  private void initializeReplQueues() {
    LOG.info("initializing replication queues");
    blockManager.processMisReplicatedBlocks();
    initializedReplQueues = true;
  }

  private boolean inActiveState() {
    return haContext != null &&
        haContext.getState().getServiceState() == HAServiceState.ACTIVE;
  }

  /**
   * @return Whether the namenode is transitioning to active state and is in the
   *         middle of the {@link #startActiveServices()}
   */
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
      stopSecretManager();
      leaseManager.stopMonitor();
      if (nnrmthread != null) {
        ((NameNodeResourceMonitor) nnrmthread.getRunnable()).stopMonitor();
        nnrmthread.interrupt();
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
      if (cacheManager != null) {
        cacheManager.stopMonitorThread();
        cacheManager.clearDirectiveStats();
      }
      blockManager.getDatanodeManager().clearPendingCachingCommands();
      blockManager.getDatanodeManager().setShouldSendCachingCommands(false);
      // Don't want to keep replication queues when not in Active.
      blockManager.clearQueues();
      initializedReplQueues = false;
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Start services required in standby state 
   * 
   * @throws IOException
   */
  void startStandbyServices(final Configuration conf) throws IOException {
    LOG.info("Starting services required for standby state");
    if (!getFSImage().editLog.isOpenForRead()) {
      // During startup, we're already open for read.
      getFSImage().editLog.initSharedJournalsForRead();
    }
    
    blockManager.setPostponeBlocksFromFuture(true);

    // Disable quota checks while in standby.
    dir.disableQuotaChecks();
    editLogTailer = new EditLogTailer(this, conf);
    editLogTailer.start();
    if (standbyShouldCheckpoint) {
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
    LOG.info("Stopping services started for standby state");
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
  
  @Override
  public void checkOperation(OperationCategory op) throws StandbyException {
    if (haContext != null) {
      // null in some unit tests
      haContext.checkOperation(op);
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
      SafeModeException se = new SafeModeException(errorMsg, safeMode);
      if (haEnabled && haContext != null
          && haContext.getState().getServiceState() == HAServiceState.ACTIVE
          && shouldRetrySafeMode(this.safeMode)) {
        throw new RetriableException(se);
      } else {
        throw se;
      }
    }
  }

  boolean isPermissionEnabled() {
    return isPermissionEnabled;
  }

  /**
   * We already know that the safemode is on. We will throw a RetriableException
   * if the safemode is not manual or caused by low resource.
   */
  private boolean shouldRetrySafeMode(SafeModeInfo safeMode) {
    if (safeMode == null) {
      return false;
    } else {
      return !safeMode.isManual() && !safeMode.areResourcesLow();
    }
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
    this.fsLock.readLock().lock();
  }
  @Override
  public void readUnlock() {
    this.fsLock.readLock().unlock();
  }
  @Override
  public void writeLock() {
    this.fsLock.writeLock().lock();
  }
  @Override
  public void writeLockInterruptibly() throws InterruptedException {
    this.fsLock.writeLock().lockInterruptibly();
  }
  @Override
  public void writeUnlock() {
    this.fsLock.writeLock().unlock();
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
      readUnlock();
    }
  }

  /**
   * Version of @see #getNamespaceInfo() that is not protected by a lock.
   */
  NamespaceInfo unprotectedGetNamespaceInfo() {
    return new NamespaceInfo(getFSImage().getStorage().getNamespaceID(),
        getClusterId(), getBlockPoolId(),
        getFSImage().getStorage().getCTime());
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
      if (smmthread != null) smmthread.interrupt();
    } finally {
      // using finally to ensure we also wait for lease daemon
      try {
        stopActiveServices();
        stopStandbyServices();
      } catch (IOException ie) {
      } finally {
        IOUtils.cleanup(LOG, dir);
        IOUtils.cleanup(LOG, fsImage);
      }
    }
  }

  @Override
  public boolean isRunning() {
    return fsRunning;
  }
  
  @Override
  public boolean isInStandbyState() {
    if (haContext == null || haContext.getState() == null) {
      // We're still starting up. In this case, if HA is
      // on for the cluster, we always start in standby. Otherwise
      // start in active.
      return haEnabled;
    }

    return HAServiceState.STANDBY == haContext.getState().getServiceState();
  }

  /**
   * Dump all metadata into specified file
   */
  void metaSave(String filename) throws IOException {
    checkSuperuserPrivilege();
    checkOperation(OperationCategory.UNCHECKED);
    writeLock();
    try {
      checkOperation(OperationCategory.UNCHECKED);
      File file = new File(System.getProperty("hadoop.log.dir"), filename);
      PrintWriter out = new PrintWriter(new BufferedWriter(
          new OutputStreamWriter(new FileOutputStream(file), Charsets.UTF_8)));
      metaSave(out);
      out.flush();
      out.close();
    } finally {
      writeUnlock();
    }
  }

  private void metaSave(PrintWriter out) {
    assert hasWriteLock();
    long totalInodes = this.dir.totalInodes();
    long totalBlocks = this.getBlocksTotal();
    out.println(totalInodes + " files and directories, " + totalBlocks
        + " blocks = " + (totalInodes + totalBlocks) + " total");

    blockManager.metaSave(out);
  }

  private String metaSaveAsString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    metaSave(pw);
    pw.flush();
    return sw.toString();
  }

  FsServerDefaults getServerDefaults() throws StandbyException {
    checkOperation(OperationCategory.READ);
    return serverDefaults;
  }

  long getAccessTimePrecision() {
    return accessTimePrecision;
  }

  private boolean isAccessTimeSupported() {
    return accessTimePrecision > 0;
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by HadoopFS clients
  //
  /////////////////////////////////////////////////////////
  /**
   * Set permissions for an existing file.
   * @throws IOException
   */
  void setPermission(String src, FsPermission permission) throws IOException {
    HdfsFileStatus auditStat;
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot set permission for " + src);
      auditStat = FSDirAttrOp.setPermission(dir, src, permission);
    } catch (AccessControlException e) {
      logAuditEvent(false, "setPermission", src);
      throw e;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(true, "setPermission", src, null, auditStat);
  }

  /**
   * Set owner for an existing file.
   * @throws IOException
   */
  void setOwner(String src, String username, String group)
      throws IOException {
    HdfsFileStatus auditStat;
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot set owner for " + src);
      auditStat = FSDirAttrOp.setOwner(dir, src, username, group);
    } catch (AccessControlException e) {
      logAuditEvent(false, "setOwner", src);
      throw e;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(true, "setOwner", src, null, auditStat);
  }

  static class GetBlockLocationsResult {
    final boolean updateAccessTime;
    final LocatedBlocks blocks;
    boolean updateAccessTime() {
      return updateAccessTime;
    }
    private GetBlockLocationsResult(
        boolean updateAccessTime, LocatedBlocks blocks) {
      this.updateAccessTime = updateAccessTime;
      this.blocks = blocks;
    }
  }

  /**
   * Get block locations within the specified range.
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  LocatedBlocks getBlockLocations(String clientMachine, String srcArg,
      long offset, long length) throws IOException {
    checkOperation(OperationCategory.READ);
    GetBlockLocationsResult res = null;
    FSPermissionChecker pc = getPermissionChecker();
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      res = getBlockLocations(pc, srcArg, offset, length, true, true);
    } catch (AccessControlException e) {
      logAuditEvent(false, "open", srcArg);
      throw e;
    } finally {
      readUnlock();
    }

    logAuditEvent(true, "open", srcArg);

    if (res.updateAccessTime()) {
      byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(
          srcArg);
      String src = srcArg;
      writeLock();
      final long now = now();
      try {
        checkOperation(OperationCategory.WRITE);
        /**
         * Resolve the path again and update the atime only when the file
         * exists.
         *
         * XXX: Races can still occur even after resolving the path again.
         * For example:
         *
         * <ul>
         *   <li>Get the block location for "/a/b"</li>
         *   <li>Rename "/a/b" to "/c/b"</li>
         *   <li>The second resolution still points to "/a/b", which is
         *   wrong.</li>
         * </ul>
         *
         * The behavior is incorrect but consistent with the one before
         * HDFS-7463. A better fix is to change the edit log of SetTime to
         * use inode id instead of a path.
         */
        src = dir.resolvePath(pc, srcArg, pathComponents);
        final INodesInPath iip = dir.getINodesInPath(src, true);
        INode inode = iip.getLastINode();
        boolean updateAccessTime = inode != null &&
            now > inode.getAccessTime() + getAccessTimePrecision();
        if (!isInSafeMode() && updateAccessTime) {
          boolean changed = FSDirAttrOp.setTimes(dir,
              inode, -1, now, false, iip.getLatestSnapshotId());
          if (changed) {
            getEditLog().logTimes(src, -1, now);
          }
        }
      } catch (Throwable e) {
        LOG.warn("Failed to update the access time of " + src, e);
      } finally {
        writeUnlock();
      }
    }

    LocatedBlocks blocks = res.blocks;
    if (blocks != null) {
      blockManager.getDatanodeManager().sortLocatedBlocks(
          clientMachine, blocks.getLocatedBlocks());

      // lastBlock is not part of getLocatedBlocks(), might need to sort it too
      LocatedBlock lastBlock = blocks.getLastLocatedBlock();
      if (lastBlock != null) {
        ArrayList<LocatedBlock> lastBlockList = Lists.newArrayList(lastBlock);
        blockManager.getDatanodeManager().sortLocatedBlocks(
            clientMachine, lastBlockList);
      }
    }
    return blocks;
  }

  /**
   * Get block locations within the specified range.
   * @see ClientProtocol#getBlockLocations(String, long, long)
   * @throws IOException
   */
  GetBlockLocationsResult getBlockLocations(
      FSPermissionChecker pc, String src, long offset, long length,
      boolean needBlockToken, boolean checkSafeMode) throws IOException {
    if (offset < 0) {
      throw new HadoopIllegalArgumentException(
          "Negative offset is not supported. File: " + src);
    }
    if (length < 0) {
      throw new HadoopIllegalArgumentException(
          "Negative length is not supported. File: " + src);
    }
    final GetBlockLocationsResult ret = getBlockLocationsInt(
        pc, src, offset, length, needBlockToken);

    if (checkSafeMode && isInSafeMode()) {
      for (LocatedBlock b : ret.blocks.getLocatedBlocks()) {
        // if safemode & no block locations yet then throw safemodeException
        if ((b.getLocations() == null) || (b.getLocations().length == 0)) {
          SafeModeException se = new SafeModeException(
              "Zero blocklocations for " + src, safeMode);
          if (haEnabled && haContext != null &&
              haContext.getState().getServiceState() == HAServiceState.ACTIVE) {
            throw new RetriableException(se);
          } else {
            throw se;
          }
        }
      }
    }
    return ret;
  }

  private GetBlockLocationsResult getBlockLocationsInt(
      FSPermissionChecker pc, final String srcArg, long offset, long length,
      boolean needBlockToken)
      throws IOException {
    String src = srcArg;
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = dir.resolvePath(pc, srcArg, pathComponents);
    final INodesInPath iip = dir.getINodesInPath(src, true);
    final INodeFile inode = INodeFile.valueOf(iip.getLastINode(), src);
    if (isPermissionEnabled) {
      dir.checkPathAccess(pc, iip, FsAction.READ);
      checkUnreadableBySuperuser(pc, inode, iip.getPathSnapshotId());
    }

    final long fileSize = iip.isSnapshot()
        ? inode.computeFileSize(iip.getPathSnapshotId())
        : inode.computeFileSizeNotIncludingLastUcBlock();
    boolean isUc = inode.isUnderConstruction();
    if (iip.isSnapshot()) {
      // if src indicates a snapshot file, we need to make sure the returned
      // blocks do not exceed the size of the snapshot file.
      length = Math.min(length, fileSize - offset);
      isUc = false;
    }

    final FileEncryptionInfo feInfo =
        FSDirectory.isReservedRawName(srcArg) ? null
            : dir.getFileEncryptionInfo(inode, iip.getPathSnapshotId(), iip);

    final LocatedBlocks blocks = blockManager.createLocatedBlocks(
        inode.getBlocks(iip.getPathSnapshotId()), fileSize,
        isUc, offset, length, needBlockToken, iip.isSnapshot(), feInfo);

    // Set caching information for the located blocks.
    for (LocatedBlock lb : blocks.getLocatedBlocks()) {
      cacheManager.setCachedLocations(lb);
    }

    final long now = now();
    boolean updateAccessTime = isAccessTimeSupported() && !isInSafeMode()
        && !iip.isSnapshot()
        && now > inode.getAccessTime() + getAccessTimePrecision();
    return new GetBlockLocationsResult(updateAccessTime, blocks);
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
    waitForLoadingFSImage();
    HdfsFileStatus stat = null;
    boolean success = false;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot concat " + target);
      stat = FSDirConcatOp.concat(dir, target, srcs, logRetryCache);
      success = true;
    } finally {
      writeUnlock();
      if (success) {
        getEditLog().logSync();
      }
      logAuditEvent(success, "concat", Arrays.toString(srcs), target, stat);
    }
  }

  /**
   * stores the modification and access time for this inode. 
   * The access time is precise up to an hour. The transaction, if needed, is
   * written to the edits log but is not flushed.
   */
  void setTimes(String src, long mtime, long atime) throws IOException {
    HdfsFileStatus auditStat;
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot set times " + src);
      auditStat = FSDirAttrOp.setTimes(dir, src, mtime, atime);
    } catch (AccessControlException e) {
      logAuditEvent(false, "setTimes", src);
      throw e;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(true, "setTimes", src, null, auditStat);
  }

  /**
   * Create a symbolic link.
   */
  @SuppressWarnings("deprecation")
  void createSymlink(String target, String link,
      PermissionStatus dirPerms, boolean createParent, boolean logRetryCache)
      throws IOException {
    if (!FileSystem.areSymlinksEnabled()) {
      throw new UnsupportedOperationException("Symlinks not supported");
    }
    HdfsFileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot create symlink " + link);
      auditStat = FSDirSymlinkOp.createSymlinkInt(this, target, link, dirPerms,
                                                  createParent, logRetryCache);
    } catch (AccessControlException e) {
      logAuditEvent(false, "createSymlink", link, target, null);
      throw e;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(true, "createSymlink", link, target, auditStat);
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
   */
  boolean setReplication(final String src, final short replication)
      throws IOException {
    boolean success = false;
    waitForLoadingFSImage();
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot set replication for " + src);
      success = FSDirAttrOp.setReplication(dir, blockManager, src, replication);
    } catch (AccessControlException e) {
      logAuditEvent(false, "setReplication", src);
      throw e;
    } finally {
      writeUnlock();
    }
    if (success) {
      getEditLog().logSync();
      logAuditEvent(true, "setReplication", src);
    }
    return success;
  }

  /**
   * Truncate file to a lower length.
   * Truncate cannot be reverted / recovered from as it causes data loss.
   * Truncation at block boundary is atomic, otherwise it requires
   * block recovery to truncate the last block of the file.
   *
   * @return true if client does not need to wait for block recovery,
   * false if client needs to wait for block recovery.
   */
  boolean truncate(String src, long newLength,
                   String clientName, String clientMachine,
                   long mtime)
      throws IOException, UnresolvedLinkException {
    boolean ret;
    try {
      ret = truncateInt(src, newLength, clientName, clientMachine, mtime);
    } catch (AccessControlException e) {
      logAuditEvent(false, "truncate", src);
      throw e;
    }
    return ret;
  }

  boolean truncateInt(String srcArg, long newLength,
                      String clientName, String clientMachine,
                      long mtime)
      throws IOException, UnresolvedLinkException {
    String src = srcArg;
    NameNode.stateChangeLog.debug(
        "DIR* NameSystem.truncate: src={} newLength={}", src, newLength);
    if (newLength < 0) {
      throw new HadoopIllegalArgumentException(
          "Cannot truncate to a negative file size: " + newLength + ".");
    }
    HdfsFileStatus stat = null;
    FSPermissionChecker pc = getPermissionChecker();
    checkOperation(OperationCategory.WRITE);
    boolean res;
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    writeLock();
    BlocksMapUpdateInfo toRemoveBlocks = new BlocksMapUpdateInfo();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot truncate for " + src);
      src = dir.resolvePath(pc, src, pathComponents);
      res = truncateInternal(src, newLength, clientName,
          clientMachine, mtime, pc, toRemoveBlocks);
      stat = dir.getAuditFileInfo(dir.getINodesInPath4Write(src, false));
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (!toRemoveBlocks.getToDeleteList().isEmpty()) {
      removeBlocks(toRemoveBlocks);
      toRemoveBlocks.clear();
    }
    logAuditEvent(true, "truncate", src, null, stat);
    return res;
  }

  /**
   * Truncate a file to a given size
   * Update the count at each ancestor directory with quota
   */
  boolean truncateInternal(String src, long newLength,
                           String clientName, String clientMachine,
                           long mtime, FSPermissionChecker pc,
                           BlocksMapUpdateInfo toRemoveBlocks)
      throws IOException, UnresolvedLinkException {
    assert hasWriteLock();
    INodesInPath iip = dir.getINodesInPath4Write(src, true);
    if (isPermissionEnabled) {
      dir.checkPathAccess(pc, iip, FsAction.WRITE);
    }
    INodeFile file = INodeFile.valueOf(iip.getLastINode(), src);
    final BlockStoragePolicy lpPolicy =
        blockManager.getStoragePolicy("LAZY_PERSIST");

    if (lpPolicy != null &&
        lpPolicy.getId() == file.getStoragePolicyID()) {
      throw new UnsupportedOperationException(
          "Cannot truncate lazy persist file " + src);
    }

    // Check if the file is already being truncated with the same length
    final BlockInfoContiguous last = file.getLastBlock();
    if (last != null && last.getBlockUCState() == BlockUCState.UNDER_RECOVERY) {
      final Block truncateBlock
          = ((BlockInfoContiguousUnderConstruction)last).getTruncateBlock();
      if (truncateBlock != null) {
        final long truncateLength = file.computeFileSize(false, false)
            + truncateBlock.getNumBytes();
        if (newLength == truncateLength) {
          return false;
        }
      }
    }

    // Opening an existing file for truncate. May need lease recovery.
    recoverLeaseInternal(RecoverLeaseOp.TRUNCATE_FILE,
        iip, src, clientName, clientMachine, false);
    // Truncate length check.
    long oldLength = file.computeFileSize();
    if(oldLength == newLength) {
      return true;
    }
    if(oldLength < newLength) {
      throw new HadoopIllegalArgumentException(
          "Cannot truncate to a larger file size. Current size: " + oldLength +
              ", truncate size: " + newLength + ".");
    }
    // Perform INodeFile truncation.
    final QuotaCounts delta = new QuotaCounts.Builder().build();
    boolean onBlockBoundary = dir.truncate(iip, newLength, toRemoveBlocks,
        mtime, delta);
    Block truncateBlock = null;
    if(!onBlockBoundary) {
      // Open file for write, but don't log into edits
      long lastBlockDelta = file.computeFileSize() - newLength;
      assert lastBlockDelta > 0 : "delta is 0 only if on block bounday";
      truncateBlock = prepareFileForTruncate(iip, clientName, clientMachine,
          lastBlockDelta, null);
    }

    // update the quota: use the preferred block size for UC block
    dir.writeLock();
    try {
      dir.updateCountNoQuotaCheck(iip, iip.length() - 1, delta);
    } finally {
      dir.writeUnlock();
    }

    getEditLog().logTruncate(src, clientName, clientMachine, newLength, mtime,
        truncateBlock);
    return onBlockBoundary;
  }

  /**
   * Convert current INode to UnderConstruction.
   * Recreate lease.
   * Create new block for the truncated copy.
   * Schedule truncation of the replicas.
   *
   * @return the returned block will be written to editLog and passed back into
   * this method upon loading.
   */
  Block prepareFileForTruncate(INodesInPath iip,
                               String leaseHolder,
                               String clientMachine,
                               long lastBlockDelta,
                               Block newBlock)
      throws IOException {
    INodeFile file = iip.getLastINode().asFile();
    String src = iip.getPath();
    file.recordModification(iip.getLatestSnapshotId());
    file.toUnderConstruction(leaseHolder, clientMachine);
    assert file.isUnderConstruction() : "inode should be under construction.";
    leaseManager.addLease(
        file.getFileUnderConstructionFeature().getClientName(), src);
    boolean shouldRecoverNow = (newBlock == null);
    BlockInfoContiguous oldBlock = file.getLastBlock();
    boolean shouldCopyOnTruncate = shouldCopyOnTruncate(file, oldBlock);
    if(newBlock == null) {
      newBlock = (shouldCopyOnTruncate) ? createNewBlock() :
          new Block(oldBlock.getBlockId(), oldBlock.getNumBytes(),
              nextGenerationStamp(blockIdManager.isLegacyBlock(oldBlock)));
    }

    BlockInfoContiguousUnderConstruction truncatedBlockUC;
    if(shouldCopyOnTruncate) {
      // Add new truncateBlock into blocksMap and
      // use oldBlock as a source for copy-on-truncate recovery
      truncatedBlockUC = new BlockInfoContiguousUnderConstruction(newBlock,
          file.getBlockReplication());
      truncatedBlockUC.setNumBytes(oldBlock.getNumBytes() - lastBlockDelta);
      truncatedBlockUC.setTruncateBlock(oldBlock);
      file.setLastBlock(truncatedBlockUC, blockManager.getStorages(oldBlock));
      getBlockManager().addBlockCollection(truncatedBlockUC, file);

      NameNode.stateChangeLog.debug(
          "BLOCK* prepareFileForTruncate: Scheduling copy-on-truncate to new" +
          " size {}  new block {} old block {}", truncatedBlockUC.getNumBytes(),
          newBlock, truncatedBlockUC.getTruncateBlock());
    } else {
      // Use new generation stamp for in-place truncate recovery
      blockManager.convertLastBlockToUnderConstruction(file, lastBlockDelta);
      oldBlock = file.getLastBlock();
      assert !oldBlock.isComplete() : "oldBlock should be under construction";
      truncatedBlockUC = (BlockInfoContiguousUnderConstruction) oldBlock;
      truncatedBlockUC.setTruncateBlock(new Block(oldBlock));
      truncatedBlockUC.getTruncateBlock().setNumBytes(
          oldBlock.getNumBytes() - lastBlockDelta);
      truncatedBlockUC.getTruncateBlock().setGenerationStamp(
          newBlock.getGenerationStamp());

      NameNode.stateChangeLog.debug(
          "BLOCK* prepareFileForTruncate: {} Scheduling in-place block " +
          "truncate to new size {}",
          truncatedBlockUC.getTruncateBlock().getNumBytes(), truncatedBlockUC);
    }
    if (shouldRecoverNow) {
      truncatedBlockUC.initializeBlockRecovery(newBlock.getGenerationStamp());
    }

    return newBlock;
  }

  /**
   * Defines if a replica needs to be copied on truncate or
   * can be truncated in place.
   */
  boolean shouldCopyOnTruncate(INodeFile file, BlockInfoContiguous blk) {
    if(!isUpgradeFinalized()) {
      return true;
    }
    if (isRollingUpgrade()) {
      return true;
    }
    return file.isBlockInLatestSnapshot(blk);
  }

  /**
   * Set the storage policy for a file or a directory.
   *
   * @param src file/directory path
   * @param policyName storage policy name
   */
  void setStoragePolicy(String src, String policyName) throws IOException {
    HdfsFileStatus auditStat;
    waitForLoadingFSImage();
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot set storage policy for " + src);
      auditStat = FSDirAttrOp.setStoragePolicy(
          dir, blockManager, src, policyName);
    } catch (AccessControlException e) {
      logAuditEvent(false, "setStoragePolicy", src);
      throw e;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(true, "setStoragePolicy", src, null, auditStat);
  }

  /**
   * @return All the existing block storage policies
   */
  BlockStoragePolicy[] getStoragePolicies() throws IOException {
    checkOperation(OperationCategory.READ);
    waitForLoadingFSImage();
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      return FSDirAttrOp.getStoragePolicies(blockManager);
    } finally {
      readUnlock();
    }
  }

  long getPreferredBlockSize(String src) throws IOException {
    checkOperation(OperationCategory.READ);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      return FSDirAttrOp.getPreferredBlockSize(dir, src);
    } finally {
      readUnlock();
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
  private CryptoProtocolVersion chooseProtocolVersion(EncryptionZone zone,
      CryptoProtocolVersion[] supportedVersions)
      throws UnknownCryptoProtocolVersionException, UnresolvedLinkException,
        SnapshotAccessControlException {
    Preconditions.checkNotNull(zone);
    Preconditions.checkNotNull(supportedVersions);
    // Right now, we only support a single protocol version,
    // so simply look for it in the list of provided options
    final CryptoProtocolVersion required = zone.getVersion();

    for (CryptoProtocolVersion c : supportedVersions) {
      if (c.equals(CryptoProtocolVersion.UNKNOWN)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ignoring unknown CryptoProtocolVersion provided by " +
              "client: " + c.getUnknownValue());
        }
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
   * Invoke KeyProvider APIs to generate an encrypted data encryption key for an
   * encryption zone. Should not be called with any locks held.
   *
   * @param ezKeyName key name of an encryption zone
   * @return New EDEK, or null if ezKeyName is null
   * @throws IOException
   */
  private EncryptedKeyVersion generateEncryptedDataEncryptionKey(String
      ezKeyName) throws IOException {
    if (ezKeyName == null) {
      return null;
    }
    EncryptedKeyVersion edek = null;
    try {
      edek = provider.generateEncryptedKey(ezKeyName);
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
    Preconditions.checkNotNull(edek);
    return edek;
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
      CryptoProtocolVersion[] supportedVersions, boolean logRetryCache)
      throws AccessControlException, SafeModeException,
      FileAlreadyExistsException, UnresolvedLinkException,
      FileNotFoundException, ParentNotDirectoryException, IOException {

    HdfsFileStatus status = null;
    try {
      status = startFileInt(src, permissions, holder, clientMachine, flag,
          createParent, replication, blockSize, supportedVersions,
          logRetryCache);
    } catch (AccessControlException e) {
      logAuditEvent(false, "create", src);
      throw e;
    }
    return status;
  }

  private HdfsFileStatus startFileInt(final String srcArg,
      PermissionStatus permissions, String holder, String clientMachine,
      EnumSet<CreateFlag> flag, boolean createParent, short replication,
      long blockSize, CryptoProtocolVersion[] supportedVersions,
      boolean logRetryCache)
      throws AccessControlException, SafeModeException,
      FileAlreadyExistsException, UnresolvedLinkException,
      FileNotFoundException, ParentNotDirectoryException, IOException {
    String src = srcArg;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder.append("DIR* NameSystem.startFile: src=" + src
              + ", holder=" + holder
              + ", clientMachine=" + clientMachine
              + ", createParent=" + createParent
              + ", replication=" + replication
              + ", createFlag=" + flag.toString()
              + ", blockSize=" + blockSize);
      builder.append(", supportedVersions=");
      if (supportedVersions != null) {
        builder.append(Arrays.toString(supportedVersions));
      } else {
        builder.append("null");
      }
      NameNode.stateChangeLog.debug(builder.toString());
    }
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException(src);
    }
    blockManager.verifyReplication(src, replication, clientMachine);

    boolean skipSync = false;
    HdfsFileStatus stat = null;
    FSPermissionChecker pc = getPermissionChecker();
    if (blockSize < minBlockSize) {
      throw new IOException("Specified block size is less than configured" +
          " minimum value (" + DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY
          + "): " + blockSize + " < " + minBlockSize);
    }
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    boolean create = flag.contains(CreateFlag.CREATE);
    boolean overwrite = flag.contains(CreateFlag.OVERWRITE);
    boolean isLazyPersist = flag.contains(CreateFlag.LAZY_PERSIST);

    waitForLoadingFSImage();

    /**
     * If the file is in an encryption zone, we optimistically create an
     * EDEK for the file by calling out to the configured KeyProvider.
     * Since this typically involves doing an RPC, we take the readLock
     * initially, then drop it to do the RPC.
     * 
     * Since the path can flip-flop between being in an encryption zone and not
     * in the meantime, we need to recheck the preconditions when we retake the
     * lock to do the create. If the preconditions are not met, we throw a
     * special RetryStartFileException to ask the DFSClient to try the create
     * again later.
     */
    CryptoProtocolVersion protocolVersion = null;
    CipherSuite suite = null;
    String ezKeyName = null;
    EncryptedKeyVersion edek = null;

    if (provider != null) {
      readLock();
      try {
        src = dir.resolvePath(pc, src, pathComponents);
        INodesInPath iip = dir.getINodesInPath4Write(src);
        // Nothing to do if the path is not within an EZ
        final EncryptionZone zone = dir.getEZForPath(iip);
        if (zone != null) {
          protocolVersion = chooseProtocolVersion(zone, supportedVersions);
          suite = zone.getSuite();
          ezKeyName = zone.getKeyName();

          Preconditions.checkNotNull(protocolVersion);
          Preconditions.checkNotNull(suite);
          Preconditions.checkArgument(!suite.equals(CipherSuite.UNKNOWN),
              "Chose an UNKNOWN CipherSuite!");
          Preconditions.checkNotNull(ezKeyName);
        }
      } finally {
        readUnlock();
      }

      Preconditions.checkState(
          (suite == null && ezKeyName == null) ||
              (suite != null && ezKeyName != null),
          "Both suite and ezKeyName should both be null or not null");

      // Generate EDEK if necessary while not holding the lock
      edek = generateEncryptedDataEncryptionKey(ezKeyName);
      EncryptionFaultInjector.getInstance().startFileAfterGenerateKey();
    }

    // Proceed with the create, using the computed cipher suite and 
    // generated EDEK
    BlocksMapUpdateInfo toRemoveBlocks = null;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot create file" + src);
      dir.writeLock();
      try {
        src = dir.resolvePath(pc, src, pathComponents);
        final INodesInPath iip = dir.getINodesInPath4Write(src);
        toRemoveBlocks = startFileInternal(
            pc, iip, permissions, holder,
            clientMachine, create, overwrite,
            createParent, replication, blockSize,
            isLazyPersist, suite, protocolVersion, edek,
            logRetryCache);
        stat = FSDirStatAndListingOp.getFileInfo(
            dir, src, false, FSDirectory.isReservedRawName(srcArg), true);
      } finally {
        dir.writeUnlock();
      }
    } catch (StandbyException se) {
      skipSync = true;
      throw se;
    } finally {
      writeUnlock();
      // There might be transactions logged while trying to recover the lease.
      // They need to be sync'ed even when an exception was thrown.
      if (!skipSync) {
        getEditLog().logSync();
        if (toRemoveBlocks != null) {
          removeBlocks(toRemoveBlocks);
          toRemoveBlocks.clear();
        }
      }
    }

    logAuditEvent(true, "create", srcArg, null, stat);
    return stat;
  }

  /**
   * Create a new file or overwrite an existing file<br>
   * 
   * Once the file is create the client then allocates a new block with the next
   * call using {@link ClientProtocol#addBlock}.
   * <p>
   * For description of parameters and exceptions thrown see
   * {@link ClientProtocol#create}
   */
  private BlocksMapUpdateInfo startFileInternal(FSPermissionChecker pc, 
      INodesInPath iip, PermissionStatus permissions, String holder,
      String clientMachine, boolean create, boolean overwrite, 
      boolean createParent, short replication, long blockSize, 
      boolean isLazyPersist, CipherSuite suite, CryptoProtocolVersion version,
      EncryptedKeyVersion edek, boolean logRetryEntry)
      throws IOException {
    assert hasWriteLock();
    // Verify that the destination does not exist as a directory already.
    final INode inode = iip.getLastINode();
    final String src = iip.getPath();
    if (inode != null && inode.isDirectory()) {
      throw new FileAlreadyExistsException(src +
          " already exists as a directory");
    }

    final INodeFile myFile = INodeFile.valueOf(inode, src, true);
    if (isPermissionEnabled) {
      if (overwrite && myFile != null) {
        dir.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      /*
       * To overwrite existing file, need to check 'w' permission 
       * of parent (equals to ancestor in this case)
       */
      dir.checkAncestorAccess(pc, iip, FsAction.WRITE);
    }
    if (!createParent) {
      dir.verifyParentDir(iip, src);
    }

    FileEncryptionInfo feInfo = null;

    final EncryptionZone zone = dir.getEZForPath(iip);
    if (zone != null) {
      // The path is now within an EZ, but we're missing encryption parameters
      if (suite == null || edek == null) {
        throw new RetryStartFileException();
      }
      // Path is within an EZ and we have provided encryption parameters.
      // Make sure that the generated EDEK matches the settings of the EZ.
      final String ezKeyName = zone.getKeyName();
      if (!ezKeyName.equals(edek.getEncryptionKeyName())) {
        throw new RetryStartFileException();
      }
      feInfo = new FileEncryptionInfo(suite, version,
          edek.getEncryptedKeyVersion().getMaterial(),
          edek.getEncryptedKeyIv(),
          ezKeyName, edek.getEncryptionKeyVersionName());
    }

    try {
      BlocksMapUpdateInfo toRemoveBlocks = null;
      if (myFile == null) {
        if (!create) {
          throw new FileNotFoundException("Can't overwrite non-existent " +
              src + " for client " + clientMachine);
        }
      } else {
        if (overwrite) {
          toRemoveBlocks = new BlocksMapUpdateInfo();
          List<INode> toRemoveINodes = new ChunkedArrayList<INode>();
          long ret = FSDirDeleteOp.delete(dir, iip, toRemoveBlocks,
                                          toRemoveINodes, now());
          if (ret >= 0) {
            iip = INodesInPath.replace(iip, iip.length() - 1, null);
            FSDirDeleteOp.incrDeletedFileCount(ret);
            removeLeasesAndINodes(src, toRemoveINodes, true);
          }
        } else {
          // If lease soft limit time is expired, recover the lease
          recoverLeaseInternal(RecoverLeaseOp.CREATE_FILE,
              iip, src, holder, clientMachine, false);
          throw new FileAlreadyExistsException(src + " for client " +
              clientMachine + " already exists");
        }
      }

      checkFsObjectLimit();
      INodeFile newNode = null;

      // Always do an implicit mkdirs for parent directory tree.
      Map.Entry<INodesInPath, String> parent = FSDirMkdirOp
          .createAncestorDirectories(dir, iip, permissions);
      if (parent != null) {
        iip = dir.addFile(parent.getKey(), parent.getValue(), permissions,
            replication, blockSize, holder, clientMachine);
        newNode = iip != null ? iip.getLastINode().asFile() : null;
      }

      if (newNode == null) {
        throw new IOException("Unable to add " + src +  " to namespace");
      }
      leaseManager.addLease(newNode.getFileUnderConstructionFeature()
          .getClientName(), src);

      // Set encryption attributes if necessary
      if (feInfo != null) {
        dir.setFileEncryptionInfo(src, feInfo);
        newNode = dir.getInode(newNode.getId()).asFile();
      }

      setNewINodeStoragePolicy(newNode, iip, isLazyPersist);

      // record file record in log, record new generation stamp
      getEditLog().logOpenFile(src, newNode, overwrite, logRetryEntry);
      NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: added {}" +
          " inode {} holder {}", src, newNode.getId(), holder);
      return toRemoveBlocks;
    } catch (IOException ie) {
      NameNode.stateChangeLog.warn("DIR* NameSystem.startFile: " + src + " " +
          ie.getMessage());
      throw ie;
    }
  }

  private void setNewINodeStoragePolicy(INodeFile inode,
                                        INodesInPath iip,
                                        boolean isLazyPersist)
      throws IOException {

    if (isLazyPersist) {
      BlockStoragePolicy lpPolicy =
          blockManager.getStoragePolicy("LAZY_PERSIST");

      // Set LAZY_PERSIST storage policy if the flag was passed to
      // CreateFile.
      if (lpPolicy == null) {
        throw new HadoopIllegalArgumentException(
            "The LAZY_PERSIST storage policy has been disabled " +
            "by the administrator.");
      }
      inode.setStoragePolicyID(lpPolicy.getId(),
                                 iip.getLatestSnapshotId());
    } else {
      BlockStoragePolicy effectivePolicy =
          blockManager.getStoragePolicy(inode.getStoragePolicyID());

      if (effectivePolicy != null &&
          effectivePolicy.isCopyOnCreateFile()) {
        // Copy effective policy from ancestor directory to current file.
        inode.setStoragePolicyID(effectivePolicy.getId(),
                                 iip.getLatestSnapshotId());
      }
    }
  }

  /**
   * Append to an existing file for append.
   * <p>
   * 
   * The method returns the last block of the file if this is a partial block,
   * which can still be used for writing more data. The client uses the returned
   * block locations to form the data pipeline for this block.<br>
   * The method returns null if the last block is full. The client then
   * allocates a new block with the next call using
   * {@link ClientProtocol#addBlock}.
   * <p>
   * 
   * For description of parameters and exceptions thrown see
   * {@link ClientProtocol#append(String, String, EnumSetWritable)}
   *
   * @return the last block locations if the block is partial or null otherwise
   */
  private LocatedBlock appendFileInternal(FSPermissionChecker pc,
      INodesInPath iip, String holder, String clientMachine, boolean newBlock,
      boolean logRetryCache) throws IOException {
    assert hasWriteLock();
    // Verify that the destination does not exist as a directory already.
    final INode inode = iip.getLastINode();
    final String src = iip.getPath();
    if (inode != null && inode.isDirectory()) {
      throw new FileAlreadyExistsException("Cannot append to directory " + src
          + "; already exists as a directory.");
    }
    if (isPermissionEnabled) {
      dir.checkPathAccess(pc, iip, FsAction.WRITE);
    }

    try {
      if (inode == null) {
        throw new FileNotFoundException("failed to append to non-existent file "
          + src + " for client " + clientMachine);
      }
      INodeFile myFile = INodeFile.valueOf(inode, src, true);
      final BlockStoragePolicy lpPolicy =
          blockManager.getStoragePolicy("LAZY_PERSIST");
      if (lpPolicy != null &&
          lpPolicy.getId() == myFile.getStoragePolicyID()) {
        throw new UnsupportedOperationException(
            "Cannot append to lazy persist file " + src);
      }
      // Opening an existing file for append - may need to recover lease.
      recoverLeaseInternal(RecoverLeaseOp.APPEND_FILE,
          iip, src, holder, clientMachine, false);
      
      final BlockInfoContiguous lastBlock = myFile.getLastBlock();
      // Check that the block has at least minimum replication.
      if(lastBlock != null && lastBlock.isComplete() &&
          !getBlockManager().isSufficientlyReplicated(lastBlock)) {
        throw new IOException("append: lastBlock=" + lastBlock +
            " of src=" + src + " is not sufficiently replicated yet.");
      }
      return prepareFileForAppend(src, iip, holder, clientMachine, newBlock,
          true, logRetryCache);
    } catch (IOException ie) {
      NameNode.stateChangeLog.warn("DIR* NameSystem.append: " +ie.getMessage());
      throw ie;
    }
  }
  
  /**
   * Convert current node to under construction.
   * Recreate in-memory lease record.
   * 
   * @param src path to the file
   * @param leaseHolder identifier of the lease holder on this file
   * @param clientMachine identifier of the client machine
   * @param newBlock if the data is appended to a new block
   * @param writeToEditLog whether to persist this change to the edit log
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *                      rebuilding
   * @return the last block locations if the block is partial or null otherwise
   * @throws UnresolvedLinkException
   * @throws IOException
   */
  LocatedBlock prepareFileForAppend(String src, INodesInPath iip,
      String leaseHolder, String clientMachine, boolean newBlock,
      boolean writeToEditLog, boolean logRetryCache) throws IOException {
    final INodeFile file = iip.getLastINode().asFile();
    final QuotaCounts delta = verifyQuotaForUCBlock(file, iip);

    file.recordModification(iip.getLatestSnapshotId());
    file.toUnderConstruction(leaseHolder, clientMachine);

    leaseManager.addLease(
        file.getFileUnderConstructionFeature().getClientName(), src);

    LocatedBlock ret = null;
    if (!newBlock) {
      ret = blockManager.convertLastBlockToUnderConstruction(file, 0);
      if (ret != null && delta != null) {
        Preconditions.checkState(delta.getStorageSpace() >= 0,
            "appending to a block with size larger than the preferred block size");
        dir.writeLock();
        try {
          dir.updateCountNoQuotaCheck(iip, iip.length() - 1, delta);
        } finally {
          dir.writeUnlock();
        }
      }
    } else {
      BlockInfoContiguous lastBlock = file.getLastBlock();
      if (lastBlock != null) {
        ExtendedBlock blk = new ExtendedBlock(this.getBlockPoolId(), lastBlock);
        ret = new LocatedBlock(blk, new DatanodeInfo[0]);
      }
    }

    if (writeToEditLog) {
      getEditLog().logAppendFile(src, file, newBlock, logRetryCache);
    }
    return ret;
  }

  /**
   * Verify quota when using the preferred block size for UC block. This is
   * usually used by append and truncate
   * @throws QuotaExceededException when violating the storage quota
   * @return expected quota usage update. null means no change or no need to
   *         update quota usage later
   */
  private QuotaCounts verifyQuotaForUCBlock(INodeFile file, INodesInPath iip)
      throws QuotaExceededException {
    if (!isImageLoaded() || dir.shouldSkipQuotaChecks()) {
      // Do not check quota if editlog is still being processed
      return null;
    }
    if (file.getLastBlock() != null) {
      final QuotaCounts delta = computeQuotaDeltaForUCBlock(file);
      dir.readLock();
      try {
        FSDirectory.verifyQuota(iip, iip.length() - 1, delta, null);
        return delta;
      } finally {
        dir.readUnlock();
      }
    }
    return null;
  }

  /** Compute quota change for converting a complete block to a UC block */
  private QuotaCounts computeQuotaDeltaForUCBlock(INodeFile file) {
    final QuotaCounts delta = new QuotaCounts.Builder().build();
    final BlockInfoContiguous lastBlock = file.getLastBlock();
    if (lastBlock != null) {
      final long diff = file.getPreferredBlockSize() - lastBlock.getNumBytes();
      final short repl = file.getBlockReplication();
      delta.addStorageSpace(diff * repl);
      final BlockStoragePolicy policy = dir.getBlockStoragePolicySuite()
          .getPolicy(file.getStoragePolicyID());
      List<StorageType> types = policy.chooseStorageTypes(repl);
      for (StorageType t : types) {
        if (t.supportTypeQuota()) {
          delta.addTypeSpace(t, diff);
        }
      }
    }
    return delta;
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
    if (!DFSUtil.isValidName(src)) {
      throw new IOException("Invalid file name: " + src);
    }
  
    boolean skipSync = false;
    FSPermissionChecker pc = getPermissionChecker();
    checkOperation(OperationCategory.WRITE);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot recover the lease of " + src);
      src = dir.resolvePath(pc, src, pathComponents);
      final INodesInPath iip = dir.getINodesInPath4Write(src);
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
      writeUnlock();
      // There might be transactions logged while trying to recover the lease.
      // They need to be sync'ed even when an exception was thrown.
      if (!skipSync) {
        getEditLog().logSync();
      }
    }
  }

  private enum RecoverLeaseOp {
    CREATE_FILE,
    APPEND_FILE,
    TRUNCATE_FILE,
    RECOVER_LEASE;
    
    private String getExceptionMessage(String src, String holder,
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
        Lease leaseFile = leaseManager.getLeaseByPath(src);
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
          final BlockInfoContiguous lastBlock = file.getLastBlock();
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
  LastBlockWithStatus appendFile(String src, String holder,
      String clientMachine, EnumSet<CreateFlag> flag, boolean logRetryCache)
      throws IOException {
    try {
      return appendFileInt(src, holder, clientMachine,
          flag.contains(CreateFlag.NEW_BLOCK), logRetryCache);
    } catch (AccessControlException e) {
      logAuditEvent(false, "append", src);
      throw e;
    }
  }

  private LastBlockWithStatus appendFileInt(final String srcArg, String holder,
      String clientMachine, boolean newBlock, boolean logRetryCache)
      throws IOException {
    String src = srcArg;
    NameNode.stateChangeLog.debug(
        "DIR* NameSystem.appendFile: src={}, holder={}, clientMachine={}",
        src, holder, clientMachine);
    boolean skipSync = false;
    if (!supportAppends) {
      throw new UnsupportedOperationException(
          "Append is not enabled on this NameNode. Use the " +
          DFS_SUPPORT_APPEND_KEY + " configuration option to enable it.");
    }

    LocatedBlock lb = null;
    HdfsFileStatus stat = null;
    FSPermissionChecker pc = getPermissionChecker();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot append to file" + src);
      src = dir.resolvePath(pc, src, pathComponents);
      final INodesInPath iip = dir.getINodesInPath4Write(src);
      lb = appendFileInternal(pc, iip, holder, clientMachine, newBlock,
          logRetryCache);
      stat = FSDirStatAndListingOp.getFileInfo(dir, src, false,
          FSDirectory.isReservedRawName(srcArg), true);
    } catch (StandbyException se) {
      skipSync = true;
      throw se;
    } finally {
      writeUnlock();
      // There might be transactions logged while trying to recover the lease.
      // They need to be sync'ed even when an exception was thrown.
      if (!skipSync) {
        getEditLog().logSync();
      }
    }
    if (lb != null) {
      NameNode.stateChangeLog.debug(
          "DIR* NameSystem.appendFile: file {} for {} at {} block {} block" +
          " size {}", src, holder, clientMachine, lb.getBlock(),
          lb.getBlock().getNumBytes());
    }
    logAuditEvent(true, "append", srcArg);
    return new LastBlockWithStatus(lb, stat);
  }

  ExtendedBlock getExtendedBlock(Block blk) {
    return new ExtendedBlock(blockPoolId, blk);
  }
  
  void setBlockPoolId(String bpid) {
    blockPoolId = bpid;
    blockManager.setBlockPoolId(blockPoolId);
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
  LocatedBlock getAdditionalBlock(String src, long fileId, String clientName,
      ExtendedBlock previous, Set<Node> excludedNodes, 
      List<String> favoredNodes) throws IOException {
    LocatedBlock[] onRetryBlock = new LocatedBlock[1];
    DatanodeStorageInfo targets[] = getNewBlockTargets(src, fileId,
        clientName, previous, excludedNodes, favoredNodes, onRetryBlock);
    if (targets == null) {
      assert onRetryBlock[0] != null : "Retry block is null";
      // This is a retry. Just return the last block.
      return onRetryBlock[0];
    }
    LocatedBlock newBlock = storeAllocatedBlock(
        src, fileId, clientName, previous, targets);
    return newBlock;
  }

  /**
   * Part I of getAdditionalBlock().
   * Analyze the state of the file under read lock to determine if the client
   * can add a new block, detect potential retries, lease mismatches,
   * and minimal replication of the penultimate block.
   * 
   * Generate target DataNode locations for the new block,
   * but do not create the new block yet.
   */
  DatanodeStorageInfo[] getNewBlockTargets(String src, long fileId,
      String clientName, ExtendedBlock previous, Set<Node> excludedNodes,
      List<String> favoredNodes, LocatedBlock[] onRetryBlock) throws IOException {
    final long blockSize;
    final int replication;
    final byte storagePolicyID;
    Node clientNode = null;
    String clientMachine = null;

    NameNode.stateChangeLog.debug("BLOCK* getAdditionalBlock: {}  inodeId {}" +
        " for {}", src, fileId, clientName);

    checkOperation(OperationCategory.READ);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    FSPermissionChecker pc = getPermissionChecker();
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      src = dir.resolvePath(pc, src, pathComponents);
      FileState fileState = analyzeFileState(
          src, fileId, clientName, previous, onRetryBlock);
      if (onRetryBlock[0] != null && onRetryBlock[0].getLocations().length > 0) {
        // This is a retry. No need to generate new locations.
        // Use the last block if it has locations.
        return null;
      }

      final INodeFile pendingFile = fileState.inode;
      if (!checkFileProgress(src, pendingFile, false)) {
        throw new NotReplicatedYetException("Not replicated yet: " + src);
      }
      src = fileState.path;

      if (pendingFile.getBlocks().length >= maxBlocksPerFile) {
        throw new IOException("File has reached the limit on maximum number of"
            + " blocks (" + DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY
            + "): " + pendingFile.getBlocks().length + " >= "
            + maxBlocksPerFile);
      }
      blockSize = pendingFile.getPreferredBlockSize();
      clientMachine = pendingFile.getFileUnderConstructionFeature()
          .getClientMachine();
      clientNode = blockManager.getDatanodeManager().getDatanodeByHost(
          clientMachine);
      replication = pendingFile.getFileReplication();
      storagePolicyID = pendingFile.getStoragePolicyID();
    } finally {
      readUnlock();
    }

    if (clientNode == null) {
      clientNode = getClientNode(clientMachine);
    }

    // choose targets for the new block to be allocated.
    return getBlockManager().chooseTarget4NewBlock( 
        src, replication, clientNode, excludedNodes, blockSize, favoredNodes,
        storagePolicyID);
  }

  /**
   * Part II of getAdditionalBlock().
   * Should repeat the same analysis of the file state as in Part 1,
   * but under the write lock.
   * If the conditions still hold, then allocate a new block with
   * the new targets, add it to the INode and to the BlocksMap.
   */
  LocatedBlock storeAllocatedBlock(String src, long fileId, String clientName,
      ExtendedBlock previous, DatanodeStorageInfo[] targets) throws IOException {
    Block newBlock = null;
    long offset;
    checkOperation(OperationCategory.WRITE);
    waitForLoadingFSImage();
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      // Run the full analysis again, since things could have changed
      // while chooseTarget() was executing.
      LocatedBlock[] onRetryBlock = new LocatedBlock[1];
      FileState fileState = 
          analyzeFileState(src, fileId, clientName, previous, onRetryBlock);
      final INodeFile pendingFile = fileState.inode;
      src = fileState.path;

      if (onRetryBlock[0] != null) {
        if (onRetryBlock[0].getLocations().length > 0) {
          // This is a retry. Just return the last block if having locations.
          return onRetryBlock[0];
        } else {
          // add new chosen targets to already allocated block and return
          BlockInfoContiguous lastBlockInFile = pendingFile.getLastBlock();
          ((BlockInfoContiguousUnderConstruction) lastBlockInFile)
              .setExpectedLocations(targets);
          offset = pendingFile.computeFileSize();
          return makeLocatedBlock(lastBlockInFile, targets, offset);
        }
      }

      // commit the last block and complete it if it has minimum replicas
      commitOrCompleteLastBlock(pendingFile, fileState.iip,
                                ExtendedBlock.getLocalBlock(previous));

      // allocate new block, record block locations in INode.
      newBlock = createNewBlock();
      INodesInPath inodesInPath = INodesInPath.fromINode(pendingFile);
      saveAllocatedBlock(src, inodesInPath, newBlock, targets);

      persistNewBlock(src, pendingFile);
      offset = pendingFile.computeFileSize();
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();

    // Return located block
    return makeLocatedBlock(newBlock, targets, offset);
  }

  /*
   * Resolve clientmachine address to get a network location path
   */
  private Node getClientNode(String clientMachine) {
    List<String> hosts = new ArrayList<String>(1);
    hosts.add(clientMachine);
    List<String> rName = getBlockManager().getDatanodeManager()
        .resolveNetworkLocation(hosts);
    Node clientNode = null;
    if (rName != null) {
      // Able to resolve clientMachine mapping.
      // Create a temp node to findout the rack local nodes
      clientNode = new NodeBase(rName.get(0) + NodeBase.PATH_SEPARATOR_STR
          + clientMachine);
    }
    return clientNode;
  }

  static class FileState {
    public final INodeFile inode;
    public final String path;
    public final INodesInPath iip;

    public FileState(INodeFile inode, String fullPath, INodesInPath iip) {
      this.inode = inode;
      this.path = fullPath;
      this.iip = iip;
    }
  }

  FileState analyzeFileState(String src,
                                long fileId,
                                String clientName,
                                ExtendedBlock previous,
                                LocatedBlock[] onRetryBlock)
          throws IOException  {
    assert hasReadLock();

    checkBlock(previous);
    onRetryBlock[0] = null;
    checkNameNodeSafeMode("Cannot add block to " + src);

    // have we exceeded the configured limit of fs objects.
    checkFsObjectLimit();

    Block previousBlock = ExtendedBlock.getLocalBlock(previous);
    final INode inode;
    final INodesInPath iip;
    if (fileId == INodeId.GRANDFATHER_INODE_ID) {
      // Older clients may not have given us an inode ID to work with.
      // In this case, we have to try to resolve the path and hope it
      // hasn't changed or been deleted since the file was opened for write.
      iip = dir.getINodesInPath4Write(src);
      inode = iip.getLastINode();
    } else {
      // Newer clients pass the inode ID, so we can just get the inode
      // directly.
      inode = dir.getInode(fileId);
      iip = INodesInPath.fromINode(inode);
      if (inode != null) {
        src = iip.getPath();
      }
    }
    final INodeFile pendingFile = checkLease(src, clientName, inode, fileId);
    BlockInfoContiguous lastBlockInFile = pendingFile.getLastBlock();
    if (!Block.matchingIdAndGenStamp(previousBlock, lastBlockInFile)) {
      // The block that the client claims is the current last block
      // doesn't match up with what we think is the last block. There are
      // four possibilities:
      // 1) This is the first block allocation of an append() pipeline
      //    which started appending exactly at or exceeding the block boundary.
      //    In this case, the client isn't passed the previous block,
      //    so it makes the allocateBlock() call with previous=null.
      //    We can distinguish this since the last block of the file
      //    will be exactly a full block.
      // 2) This is a retry from a client that missed the response of a
      //    prior getAdditionalBlock() call, perhaps because of a network
      //    timeout, or because of an HA failover. In that case, we know
      //    by the fact that the client is re-issuing the RPC that it
      //    never began to write to the old block. Hence it is safe to
      //    to return the existing block.
      // 3) This is an entirely bogus request/bug -- we should error out
      //    rather than potentially appending a new block with an empty
      //    one in the middle, etc
      // 4) This is a retry from a client that timed out while
      //    the prior getAdditionalBlock() is still being processed,
      //    currently working on chooseTarget(). 
      //    There are no means to distinguish between the first and 
      //    the second attempts in Part I, because the first one hasn't
      //    changed the namesystem state yet.
      //    We run this analysis again in Part II where case 4 is impossible.

      BlockInfoContiguous penultimateBlock = pendingFile.getPenultimateBlock();
      if (previous == null &&
          lastBlockInFile != null &&
          lastBlockInFile.getNumBytes() >= pendingFile.getPreferredBlockSize() &&
          lastBlockInFile.isComplete()) {
        // Case 1
        NameNode.stateChangeLog.debug(
            "BLOCK* NameSystem.allocateBlock: handling block allocation" +
            " writing to a file with a complete previous block: src={}" +
            " lastBlock={}", src, lastBlockInFile);
      } else if (Block.matchingIdAndGenStamp(penultimateBlock, previousBlock)) {
        if (lastBlockInFile.getNumBytes() != 0) {
          throw new IOException(
              "Request looked like a retry to allocate block " +
              lastBlockInFile + " but it already contains " +
              lastBlockInFile.getNumBytes() + " bytes");
        }

        // Case 2
        // Return the last block.
        NameNode.stateChangeLog.info("BLOCK* allocateBlock: " +
            "caught retry for allocation of a new block in " +
            src + ". Returning previously allocated block " + lastBlockInFile);
        long offset = pendingFile.computeFileSize();
        onRetryBlock[0] = makeLocatedBlock(lastBlockInFile,
            ((BlockInfoContiguousUnderConstruction)lastBlockInFile).getExpectedStorageLocations(),
            offset);
        return new FileState(pendingFile, src, iip);
      } else {
        // Case 3
        throw new IOException("Cannot allocate block in " + src + ": " +
            "passed 'previous' block " + previous + " does not match actual " +
            "last block in file " + lastBlockInFile);
      }
    }
    return new FileState(pendingFile, src, iip);
  }

  LocatedBlock makeLocatedBlock(Block blk, DatanodeStorageInfo[] locs,
                                        long offset) throws IOException {
    LocatedBlock lBlk = new LocatedBlock(
        getExtendedBlock(blk), locs, offset, false);
    getBlockManager().setBlockToken(
        lBlk, BlockTokenSecretManager.AccessMode.WRITE);
    return lBlk;
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
    checkOperation(OperationCategory.READ);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    FSPermissionChecker pc = getPermissionChecker();
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      //check safe mode
      checkNameNodeSafeMode("Cannot add datanode; src=" + src + ", blk=" + blk);
      src = dir.resolvePath(pc, src, pathComponents);

      //check lease
      final INode inode;
      if (fileId == INodeId.GRANDFATHER_INODE_ID) {
        // Older clients may not have given us an inode ID to work with.
        // In this case, we have to try to resolve the path and hope it
        // hasn't changed or been deleted since the file was opened for write.
        inode = dir.getINode(src);
      } else {
        inode = dir.getInode(fileId);
        if (inode != null) src = inode.getFullPathName();
      }
      final INodeFile file = checkLease(src, clientName, inode, fileId);
      clientMachine = file.getFileUnderConstructionFeature().getClientMachine();
      clientnode = blockManager.getDatanodeManager().getDatanodeByHost(clientMachine);
      preferredblocksize = file.getPreferredBlockSize();
      storagePolicyID = file.getStoragePolicyID();

      //find datanode storages
      final DatanodeManager dm = blockManager.getDatanodeManager();
      chosen = Arrays.asList(dm.getDatanodeStorageInfos(existings, storageIDs,
          "src=%s, fileId=%d, blk=%s, clientName=%s, clientMachine=%s",
          src, fileId, blk, clientName, clientMachine));
    } finally {
      readUnlock();
    }

    if (clientnode == null) {
      clientnode = getClientNode(clientMachine);
    }

    // choose new datanodes.
    final DatanodeStorageInfo[] targets = blockManager.chooseTarget4AdditionalDatanode(
        src, numAdditionalNodes, clientnode, chosen, 
        excludes, preferredblocksize, storagePolicyID);
    final LocatedBlock lb = new LocatedBlock(blk, targets);
    blockManager.setBlockToken(lb, AccessMode.COPY);
    return lb;
  }

  /**
   * The client would like to let go of the given block
   */
  boolean abandonBlock(ExtendedBlock b, long fileId, String src, String holder)
      throws IOException {
    NameNode.stateChangeLog.debug(
        "BLOCK* NameSystem.abandonBlock: {} of file {}", b, src);
    checkOperation(OperationCategory.WRITE);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    FSPermissionChecker pc = getPermissionChecker();
    waitForLoadingFSImage();
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot abandon block " + b + " for file" + src);
      src = dir.resolvePath(pc, src, pathComponents);

      final INode inode;
      final INodesInPath iip;
      if (fileId == INodeId.GRANDFATHER_INODE_ID) {
        // Older clients may not have given us an inode ID to work with.
        // In this case, we have to try to resolve the path and hope it
        // hasn't changed or been deleted since the file was opened for write.
        iip = dir.getINodesInPath(src, true);
        inode = iip.getLastINode();
      } else {
        inode = dir.getInode(fileId);
        iip = INodesInPath.fromINode(inode);
        if (inode != null) {
          src = iip.getPath();
        }
      }
      final INodeFile file = checkLease(src, holder, inode, fileId);

      // Remove the block from the pending creates list
      boolean removed = dir.removeBlock(src, iip, file,
          ExtendedBlock.getLocalBlock(b));
      if (!removed) {
        return true;
      }
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: {} is " +
          "removed from pendingCreates", b);
      persistBlocks(src, file, false);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();

    return true;
  }

  private INodeFile checkLease(String src, String holder, INode inode,
      long fileId) throws LeaseExpiredException, FileNotFoundException {
    assert hasReadLock();
    final String ident = src + " (inode " + fileId + ")";
    if (inode == null) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException(
          "No lease on " + ident + ": File does not exist. "
          + (lease != null ? lease.toString()
              : "Holder " + holder + " does not have any open files."));
    }
    if (!inode.isFile()) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException(
          "No lease on " + ident + ": INode is not a regular file. "
              + (lease != null ? lease.toString()
              : "Holder " + holder + " does not have any open files."));
    }
    final INodeFile file = inode.asFile();
    if (!file.isUnderConstruction()) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException(
          "No lease on " + ident + ": File is not open for writing. "
          + (lease != null ? lease.toString()
              : "Holder " + holder + " does not have any open files."));
    }
    // No further modification is allowed on a deleted file.
    // A file is considered deleted, if it is not in the inodeMap or is marked
    // as deleted in the snapshot feature.
    if (isFileDeleted(file)) {
      throw new FileNotFoundException(src);
    }
    String clientName = file.getFileUnderConstructionFeature().getClientName();
    if (holder != null && !clientName.equals(holder)) {
      throw new LeaseExpiredException("Lease mismatch on " + ident +
          " owned by " + clientName + " but is accessed by " + holder);
    }
    return file;
  }
 
  /**
   * Complete in-progress write to the given file.
   * @return true if successful, false if the client should continue to retry
   *         (e.g if not all blocks have reached minimum replication yet)
   * @throws IOException on error (eg lease mismatch, file not open, file deleted)
   */
  boolean completeFile(final String srcArg, String holder,
                       ExtendedBlock last, long fileId)
    throws SafeModeException, UnresolvedLinkException, IOException {
    String src = srcArg;
    NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: {} for {}",
        src, holder);
    checkBlock(last);
    boolean success = false;
    checkOperation(OperationCategory.WRITE);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    FSPermissionChecker pc = getPermissionChecker();
    waitForLoadingFSImage();
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot complete file " + src);
      src = dir.resolvePath(pc, src, pathComponents);
      success = completeFileInternal(src, holder,
        ExtendedBlock.getLocalBlock(last), fileId);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (success) {
      NameNode.stateChangeLog.info("DIR* completeFile: " + srcArg
          + " is closed by " + holder);
    }
    return success;
  }

  private boolean completeFileInternal(String src, String holder, Block last,
      long fileId) throws IOException {
    assert hasWriteLock();
    final INodeFile pendingFile;
    final INodesInPath iip;
    INode inode = null;
    try {
      if (fileId == INodeId.GRANDFATHER_INODE_ID) {
        // Older clients may not have given us an inode ID to work with.
        // In this case, we have to try to resolve the path and hope it
        // hasn't changed or been deleted since the file was opened for write.
        iip = dir.getINodesInPath(src, true);
        inode = iip.getLastINode();
      } else {
        inode = dir.getInode(fileId);
        iip = INodesInPath.fromINode(inode);
        if (inode != null) {
          src = iip.getPath();
        }
      }
      pendingFile = checkLease(src, holder, inode, fileId);
    } catch (LeaseExpiredException lee) {
      if (inode != null && inode.isFile() &&
          !inode.asFile().isUnderConstruction()) {
        // This could be a retry RPC - i.e the client tried to close
        // the file, but missed the RPC response. Thus, it is trying
        // again to close the file. If the file still exists and
        // the client's view of the last block matches the actual
        // last block, then we'll treat it as a successful close.
        // See HDFS-3031.
        final Block realLastBlock = inode.asFile().getLastBlock();
        if (Block.matchingIdAndGenStamp(last, realLastBlock)) {
          NameNode.stateChangeLog.info("DIR* completeFile: " +
              "request from " + holder + " to complete inode " + fileId +
              "(" + src + ") which is already closed. But, it appears to be " +
              "an RPC retry. Returning success");
          return true;
        }
      }
      throw lee;
    }
    // Check the state of the penultimate block. It should be completed
    // before attempting to complete the last one.
    if (!checkFileProgress(src, pendingFile, false)) {
      return false;
    }

    // commit the last block and complete it if it has minimum replicas
    commitOrCompleteLastBlock(pendingFile, iip, last);

    if (!checkFileProgress(src, pendingFile, true)) {
      return false;
    }

    finalizeINodeFileUnderConstruction(src, pendingFile,
        Snapshot.CURRENT_STATE_ID);
    return true;
  }

  /**
   * Save allocated block at the given pending filename
   * 
   * @param src path to the file
   * @param inodesInPath representing each of the components of src.
   *                     The last INode is the INode for {@code src} file.
   * @param newBlock newly allocated block to be save
   * @param targets target datanodes where replicas of the new block is placed
   * @throws QuotaExceededException If addition of block exceeds space quota
   */
  BlockInfoContiguous saveAllocatedBlock(String src, INodesInPath inodesInPath,
      Block newBlock, DatanodeStorageInfo[] targets)
          throws IOException {
    assert hasWriteLock();
    BlockInfoContiguous b = dir.addBlock(src, inodesInPath, newBlock, targets);
    NameNode.stateChangeLog.info("BLOCK* allocate " + b + " for " + src);
    DatanodeStorageInfo.incrementBlocksScheduled(targets);
    return b;
  }

  /**
   * Create new block with a unique block id and a new generation stamp.
   */
  Block createNewBlock() throws IOException {
    assert hasWriteLock();
    Block b = new Block(nextBlockId(), 0, 0);
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
    if (checkall) {
      // check all blocks of the file.
      for (BlockInfoContiguous block: v.getBlocks()) {
        if (!isCompleteBlock(src, block, blockManager.minReplication)) {
          return false;
        }
      }
    } else {
      // check the penultimate block of this file
      BlockInfoContiguous b = v.getPenultimateBlock();
      if (b != null
          && !isCompleteBlock(src, b, blockManager.minReplication)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isCompleteBlock(String src, BlockInfoContiguous b, int minRepl) {
    if (!b.isComplete()) {
      final BlockInfoContiguousUnderConstruction uc = (BlockInfoContiguousUnderConstruction)b;
      final int numNodes = b.numNodes();
      LOG.info("BLOCK* " + b + " is not COMPLETE (ucState = "
          + uc.getBlockUCState() + ", replication# = " + numNodes
          + (numNodes < minRepl? " < ": " >= ")
          + " minimum = " + minRepl + ") in file " + src);
      return false;
    }
    return true;
  }

  ////////////////////////////////////////////////////////////////
  // Here's how to handle block-copy failure during client write:
  // -- As usual, the client's write should result in a streaming
  // backup write to a k-machine sequence.
  // -- If one of the backup machines fails, no worries.  Fail silently.
  // -- Before client is allowed to close and finalize file, make sure
  // that the blocks are backed up.  Namenode may have to issue specific backup
  // commands to make up for earlier datanode failures.  Once all copies
  // are made, edit namespace and return to client.
  ////////////////////////////////////////////////////////////////

  /** 
   * Change the indicated filename. 
   * @deprecated Use {@link #renameTo(String, String, boolean,
   * Options.Rename...)} instead.
   */
  @Deprecated
  boolean renameTo(String src, String dst, boolean logRetryCache)
      throws IOException {
    waitForLoadingFSImage();
    FSDirRenameOp.RenameOldResult ret = null;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot rename " + src);
      ret = FSDirRenameOp.renameToInt(dir, src, dst, logRetryCache);
    } catch (AccessControlException e)  {
      logAuditEvent(false, "rename", src, dst, null);
      throw e;
    } finally {
      writeUnlock();
    }
    boolean success = ret != null && ret.success;
    if (success) {
      getEditLog().logSync();
    }
    logAuditEvent(success, "rename", src, dst,
        ret == null ? null : ret.auditStat);
    return success;
  }

  void renameTo(final String src, final String dst,
                boolean logRetryCache, Options.Rename... options)
      throws IOException {
    waitForLoadingFSImage();
    Map.Entry<BlocksMapUpdateInfo, HdfsFileStatus> res = null;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot rename " + src);
      res = FSDirRenameOp.renameToInt(dir, src, dst, logRetryCache, options);
    } catch (AccessControlException e) {
      logAuditEvent(false, "rename (options=" + Arrays.toString(options) +
          ")", src, dst, null);
      throw e;
    } finally {
      writeUnlock();
    }

    getEditLog().logSync();

    BlocksMapUpdateInfo collectedBlocks = res.getKey();
    HdfsFileStatus auditStat = res.getValue();
    if (!collectedBlocks.getToDeleteList().isEmpty()) {
      removeBlocks(collectedBlocks);
      collectedBlocks.clear();
    }

    logAuditEvent(true, "rename (options=" + Arrays.toString(options) +
        ")", src, dst, auditStat);
  }

  /**
   * Remove the indicated file from namespace.
   * 
   * @see ClientProtocol#delete(String, boolean) for detailed description and 
   * description of exceptions
   */
  boolean delete(String src, boolean recursive, boolean logRetryCache)
      throws IOException {
    waitForLoadingFSImage();
    BlocksMapUpdateInfo toRemovedBlocks = null;
    writeLock();
    boolean ret = false;
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot delete " + src);
      toRemovedBlocks = FSDirDeleteOp.delete(
          this, src, recursive, logRetryCache);
      ret = toRemovedBlocks != null;
    } catch (AccessControlException e) {
      logAuditEvent(false, "delete", src);
      throw e;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (toRemovedBlocks != null) {
      removeBlocks(toRemovedBlocks); // Incremental deletion of blocks
    }
    logAuditEvent(true, "delete", src);
    return ret;
  }

  FSPermissionChecker getPermissionChecker()
      throws AccessControlException {
    return dir.getPermissionChecker();
  }

  /**
   * From the given list, incrementally remove the blocks from blockManager
   * Writelock is dropped and reacquired every BLOCK_DELETION_INCREMENT to
   * ensure that other waiters on the lock can get in. See HDFS-2938
   * 
   * @param blocks
   *          An instance of {@link BlocksMapUpdateInfo} which contains a list
   *          of blocks that need to be removed from blocksMap
   */
  void removeBlocks(BlocksMapUpdateInfo blocks) {
    List<Block> toDeleteList = blocks.getToDeleteList();
    Iterator<Block> iter = toDeleteList.iterator();
    while (iter.hasNext()) {
      writeLock();
      try {
        for (int i = 0; i < BLOCK_DELETION_INCREMENT && iter.hasNext(); i++) {
          blockManager.removeBlock(iter.next());
        }
      } finally {
        writeUnlock();
      }
    }
  }
  
  /**
   * Remove leases and inodes related to a given path
   * @param src The given path
   * @param removedINodes Containing the list of inodes to be removed from
   *                      inodesMap
   * @param acquireINodeMapLock Whether to acquire the lock for inode removal
   */
  void removeLeasesAndINodes(String src, List<INode> removedINodes,
      final boolean acquireINodeMapLock) {
    assert hasWriteLock();
    leaseManager.removeLeaseWithPrefixPath(src);
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
   * Removes the blocks from blocksmap and updates the safemode blocks total
   * 
   * @param blocks
   *          An instance of {@link BlocksMapUpdateInfo} which contains a list
   *          of blocks that need to be removed from blocksMap
   */
  void removeBlocksAndUpdateSafemodeTotal(BlocksMapUpdateInfo blocks) {
    assert hasWriteLock();
    // In the case that we are a Standby tailing edits from the
    // active while in safe-mode, we need to track the total number
    // of blocks and safe blocks in the system.
    boolean trackBlockCounts = isSafeModeTrackingBlocks();
    int numRemovedComplete = 0, numRemovedSafe = 0;

    for (Block b : blocks.getToDeleteList()) {
      if (trackBlockCounts) {
        BlockInfoContiguous bi = getStoredBlock(b);
        if (bi.isComplete()) {
          numRemovedComplete++;
          if (bi.numNodes() >= blockManager.minReplication) {
            numRemovedSafe++;
          }
        }
      }
      blockManager.removeBlock(b);
    }
    if (trackBlockCounts) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adjusting safe-mode totals for deletion."
            + "decreasing safeBlocks by " + numRemovedSafe
            + ", totalBlocks by " + numRemovedComplete);
      }
      adjustSafeModeBlockTotals(-numRemovedSafe, -numRemovedComplete);
    }
  }

  /**
   * @see SafeModeInfo#shouldIncrementallyTrackBlocks
   */
  private boolean isSafeModeTrackingBlocks() {
    if (!haEnabled) {
      // Never track blocks incrementally in non-HA code.
      return false;
    }
    SafeModeInfo sm = this.safeMode;
    return sm != null && sm.shouldIncrementallyTrackBlocks();
  }

  /**
   * Get the file info for a specific file.
   *
   * @param src The string representation of the path to the file
   * @param resolveLink whether to throw UnresolvedLinkException
   *        if src refers to a symlink
   *
   * @throws AccessControlException if access is denied
   * @throws UnresolvedLinkException if a symlink is encountered.
   *
   * @return object containing information regarding the file
   *         or null if file not found
   * @throws StandbyException
   */
  HdfsFileStatus getFileInfo(final String src, boolean resolveLink)
    throws IOException {
    checkOperation(OperationCategory.READ);
    HdfsFileStatus stat = null;
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      stat = FSDirStatAndListingOp.getFileInfo(dir, src, resolveLink);
    } catch (AccessControlException e) {
      logAuditEvent(false, "getfileinfo", src);
      throw e;
    } finally {
      readUnlock();
    }
    logAuditEvent(true, "getfileinfo", src);
    return stat;
  }

  /**
   * Returns true if the file is closed
   */
  boolean isFileClosed(final String src) throws IOException {
    checkOperation(OperationCategory.READ);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      return FSDirStatAndListingOp.isFileClosed(dir, src);
    } catch (AccessControlException e) {
      logAuditEvent(false, "isFileClosed", src);
      throw e;
    } finally {
      readUnlock();
    }
  }

  /**
   * Create all the necessary directories
   */
  boolean mkdirs(String src, PermissionStatus permissions,
      boolean createParent) throws IOException {
    HdfsFileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot create directory " + src);
      auditStat = FSDirMkdirOp.mkdirs(this, src, permissions, createParent);
    } catch (AccessControlException e) {
      logAuditEvent(false, "mkdirs", src);
      throw e;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(true, "mkdirs", src, null, auditStat);
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
    readLock();
    boolean success = true;
    try {
      checkOperation(OperationCategory.READ);
      return FSDirStatAndListingOp.getContentSummary(dir, src);
    } catch (AccessControlException ace) {
      success = false;
      throw ace;
    } finally {
      readUnlock();
      logAuditEvent(success, "contentSummary", src);
    }
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
    checkOperation(OperationCategory.WRITE);
    writeLock();
    boolean success = false;
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot set quota on " + src);
      FSDirAttrOp.setQuota(dir, src, nsQuota, ssQuota, type);
      success = true;
    } finally {
      writeUnlock();
      if (success) {
        getEditLog().logSync();
      }
      logAuditEvent(success, "setQuota", src);
    }
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
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);

    FSPermissionChecker pc = getPermissionChecker();
    waitForLoadingFSImage();
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot fsync file " + src);
      src = dir.resolvePath(pc, src, pathComponents);
      final INode inode;
      if (fileId == INodeId.GRANDFATHER_INODE_ID) {
        // Older clients may not have given us an inode ID to work with.
        // In this case, we have to try to resolve the path and hope it
        // hasn't changed or been deleted since the file was opened for write.
        inode = dir.getINode(src);
      } else {
        inode = dir.getInode(fileId);
        if (inode != null) src = inode.getFullPathName();
      }
      final INodeFile pendingFile = checkLease(src, clientName, inode, fileId);
      if (lastBlockLength > 0) {
        pendingFile.getFileUnderConstructionFeature().updateLengthOfLastBlock(
            pendingFile, lastBlockLength);
      }
      persistBlocks(src, pendingFile, false);
    } finally {
      writeUnlock();
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
    BlockInfoContiguous[] blocks = pendingFile.getBlocks();

    int nrCompleteBlocks;
    BlockInfoContiguous curBlock = null;
    for(nrCompleteBlocks = 0; nrCompleteBlocks < nrBlocks; nrCompleteBlocks++) {
      curBlock = blocks[nrCompleteBlocks];
      if(!curBlock.isComplete())
        break;
      assert blockManager.checkMinReplication(curBlock) :
              "A COMPLETE block is not minimally replicated in " + src;
    }

    // If there are no incomplete blocks associated with this file,
    // then reap lease immediately and close the file.
    if(nrCompleteBlocks == nrBlocks) {
      finalizeINodeFileUnderConstruction(src, pendingFile,
          iip.getLatestSnapshotId());
      NameNode.stateChangeLog.warn("BLOCK*"
        + " internalReleaseLease: All existing blocks are COMPLETE,"
        + " lease removed, file closed.");
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
    final BlockInfoContiguous lastBlock = pendingFile.getLastBlock();
    BlockUCState lastBlockState = lastBlock.getBlockUCState();
    BlockInfoContiguous penultimateBlock = pendingFile.getPenultimateBlock();

    // If penultimate block doesn't exist then its minReplication is met
    boolean penultimateBlockMinReplication = penultimateBlock == null ? true :
        blockManager.checkMinReplication(penultimateBlock);

    switch(lastBlockState) {
    case COMPLETE:
      assert false : "Already checked that the last block is incomplete";
      break;
    case COMMITTED:
      // Close file if committed blocks are minimally replicated
      if(penultimateBlockMinReplication &&
          blockManager.checkMinReplication(lastBlock)) {
        finalizeINodeFileUnderConstruction(src, pendingFile,
            iip.getLatestSnapshotId());
        NameNode.stateChangeLog.warn("BLOCK*"
          + " internalReleaseLease: Committed blocks are minimally replicated,"
          + " lease removed, file closed.");
        return true;  // closed!
      }
      // Cannot close file right now, since some blocks 
      // are not yet minimally replicated.
      // This may potentially cause infinite loop in lease recovery
      // if there are no valid replicas on data-nodes.
      String message = "DIR* NameSystem.internalReleaseLease: " +
          "Failed to release lease for file " + src +
          ". Committed blocks are waiting to be minimally replicated." +
          " Try again later.";
      NameNode.stateChangeLog.warn(message);
      throw new AlreadyBeingCreatedException(message);
    case UNDER_CONSTRUCTION:
    case UNDER_RECOVERY:
      final BlockInfoContiguousUnderConstruction uc = (BlockInfoContiguousUnderConstruction)lastBlock;
      // determine if last block was intended to be truncated
      Block recoveryBlock = uc.getTruncateBlock();
      boolean truncateRecovery = recoveryBlock != null;
      boolean copyOnTruncate = truncateRecovery &&
          recoveryBlock.getBlockId() != uc.getBlockId();
      assert !copyOnTruncate ||
          recoveryBlock.getBlockId() < uc.getBlockId() &&
          recoveryBlock.getGenerationStamp() < uc.getGenerationStamp() &&
          recoveryBlock.getNumBytes() > uc.getNumBytes() :
            "wrong recoveryBlock";

      // setup the last block locations from the blockManager if not known
      if (uc.getNumExpectedLocations() == 0) {
        uc.setExpectedLocations(blockManager.getStorages(lastBlock));
      }

      if (uc.getNumExpectedLocations() == 0 && uc.getNumBytes() == 0) {
        // There is no datanode reported to this block.
        // may be client have crashed before writing data to pipeline.
        // This blocks doesn't need any recovery.
        // We can remove this block and close the file.
        pendingFile.removeLastBlock(lastBlock);
        finalizeINodeFileUnderConstruction(src, pendingFile,
            iip.getLatestSnapshotId());
        NameNode.stateChangeLog.warn("BLOCK* internalReleaseLease: "
            + "Removed empty last block and closed file.");
        return true;
      }
      // start recovery of the last block for this file
      long blockRecoveryId = nextGenerationStamp(blockIdManager.isLegacyBlock(uc));
      lease = reassignLease(lease, src, recoveryLeaseHolder, pendingFile);
      if(copyOnTruncate) {
        uc.setGenerationStamp(blockRecoveryId);
      } else if(truncateRecovery) {
        recoveryBlock.setGenerationStamp(blockRecoveryId);
      }
      uc.initializeBlockRecovery(blockRecoveryId);
      leaseManager.renewLease(lease);
      // Cannot close file right now, since the last block requires recovery.
      // This may potentially cause infinite loop in lease recovery
      // if there are no valid replicas on data-nodes.
      NameNode.stateChangeLog.warn(
                "DIR* NameSystem.internalReleaseLease: " +
                "File " + src + " has not been closed." +
               " Lease recovery is in progress. " +
                "RecoveryId = " + blockRecoveryId + " for block " + lastBlock);
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
    return reassignLeaseInternal(lease, src, newHolder, pendingFile);
  }
  
  Lease reassignLeaseInternal(Lease lease, String src, String newHolder,
      INodeFile pendingFile) {
    assert hasWriteLock();
    pendingFile.getFileUnderConstructionFeature().setClientName(newHolder);
    return leaseManager.reassignLease(lease, src, newHolder);
  }

  private void commitOrCompleteLastBlock(final INodeFile fileINode,
      final INodesInPath iip, final Block commitBlock) throws IOException {
    assert hasWriteLock();
    Preconditions.checkArgument(fileINode.isUnderConstruction());
    if (!blockManager.commitOrCompleteLastBlock(fileINode, commitBlock)) {
      return;
    }

    // Adjust disk space consumption if required
    final long diff = fileINode.getPreferredBlockSize() - commitBlock.getNumBytes();    
    if (diff > 0) {
      try {
        dir.updateSpaceConsumed(iip, 0, -diff, fileINode.getFileReplication());
      } catch (IOException e) {
        LOG.warn("Unexpected exception while updating disk space.", e);
      }
    }
  }

  private void finalizeINodeFileUnderConstruction(String src,
      INodeFile pendingFile, int latestSnapshot) throws IOException {
    assert hasWriteLock();

    FileUnderConstructionFeature uc = pendingFile.getFileUnderConstructionFeature();
    if (uc == null) {
      throw new IOException("Cannot finalize file " + src
          + " because it is not under construction");
    }
    leaseManager.removeLease(uc.getClientName(), src);
    
    pendingFile.recordModification(latestSnapshot);

    // The file is no longer pending.
    // Create permanent INode, update blocks. No need to replace the inode here
    // since we just remove the uc feature from pendingFile
    pendingFile.toCompleteFile(now());

    waitForLoadingFSImage();
    // close file and persist block allocations for this file
    closeFile(src, pendingFile);

    blockManager.checkReplication(pendingFile);
  }

  @VisibleForTesting
  BlockInfoContiguous getStoredBlock(Block block) {
    return blockManager.getStoredBlock(block);
  }
  
  @Override
  public boolean isInSnapshot(BlockInfoContiguousUnderConstruction blockUC) {
    assert hasReadLock();
    final BlockCollection bc = blockUC.getBlockCollection();
    if (bc == null || !(bc instanceof INodeFile)
        || !bc.isUnderConstruction()) {
      return false;
    }

    String fullName = bc.getName();
    try {
      if (fullName != null && fullName.startsWith(Path.SEPARATOR)
          && dir.getINode(fullName) == bc) {
        // If file exists in normal path then no need to look in snapshot
        return false;
      }
    } catch (UnresolvedLinkException e) {
      LOG.error("Error while resolving the link : " + fullName, e);
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
    waitForLoadingFSImage();
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      // If a DN tries to commit to the standby, the recovery will
      // fail, and the next retry will succeed on the new NN.
  
      checkNameNodeSafeMode(
          "Cannot commitBlockSynchronization while in safe mode");
      final BlockInfoContiguous storedBlock = getStoredBlock(
          ExtendedBlock.getLocalBlock(oldBlock));
      if (storedBlock == null) {
        if (deleteblock) {
          // This may be a retry attempt so ignore the failure
          // to locate the block.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Block (=" + oldBlock + ") not found");
          }
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
      BlockCollection blockCollection = storedBlock.getBlockCollection();
      if (blockCollection == null) {
        throw new IOException("The blockCollection of " + storedBlock
            + " is null, likely because the file owning this block was"
            + " deleted and the block removal is delayed");
      }
      INodeFile iFile = ((INode)blockCollection).asFile();
      src = iFile.getFullPathName();
      if (isFileDeleted(iFile)) {
        throw new FileNotFoundException("File not found: "
            + src + ", likely due to delayed block removal");
      }
      if ((!iFile.isUnderConstruction() || storedBlock.isComplete()) &&
          iFile.getLastBlock().isComplete()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unexpected block (=" + oldBlock
                    + ") since the file (=" + iFile.getLocalName()
                    + ") is not under construction");
        }
        return;
      }

      BlockInfoContiguousUnderConstruction truncatedBlock =
          (BlockInfoContiguousUnderConstruction) iFile.getLastBlock();
      long recoveryId = truncatedBlock.getBlockRecoveryId();
      boolean copyTruncate =
          truncatedBlock.getBlockId() != storedBlock.getBlockId();
      if(recoveryId != newgenerationstamp) {
        throw new IOException("The recovery id " + newgenerationstamp
                              + " does not match current recovery id "
                              + recoveryId + " for block " + oldBlock);
      }

      if (deleteblock) {
        Block blockToDel = ExtendedBlock.getLocalBlock(oldBlock);
        boolean remove = iFile.removeLastBlock(blockToDel);
        if (remove) {
          blockManager.removeBlock(storedBlock);
        }
      }
      else {
        // update last block
        if(!copyTruncate) {
          storedBlock.setGenerationStamp(newgenerationstamp);
          storedBlock.setNumBytes(newlength);
        }

        // find the DatanodeDescriptor objects
        ArrayList<DatanodeDescriptor> trimmedTargets =
            new ArrayList<DatanodeDescriptor>(newtargets.length);
        ArrayList<String> trimmedStorages =
            new ArrayList<String>(newtargets.length);
        if (newtargets.length > 0) {
          for (int i = 0; i < newtargets.length; ++i) {
            // try to get targetNode
            DatanodeDescriptor targetNode =
                blockManager.getDatanodeManager().getDatanode(newtargets[i]);
            if (targetNode != null) {
              trimmedTargets.add(targetNode);
              trimmedStorages.add(newtargetstorages[i]);
            } else if (LOG.isDebugEnabled()) {
              LOG.debug("DatanodeDescriptor (=" + newtargets[i] + ") not found");
            }
          }
        }
        if ((closeFile) && !trimmedTargets.isEmpty()) {
          // the file is getting closed. Insert block locations into blockManager.
          // Otherwise fsck will report these blocks as MISSING, especially if the
          // blocksReceived from Datanodes take a long time to arrive.
          for (int i = 0; i < trimmedTargets.size(); i++) {
            DatanodeStorageInfo storageInfo =
                trimmedTargets.get(i).getStorageInfo(trimmedStorages.get(i));
            if (storageInfo != null) {
              if(copyTruncate) {
                storageInfo.addBlock(truncatedBlock);
              } else {
                storageInfo.addBlock(storedBlock);
              }
            }
          }
        }

        // add pipeline locations into the INodeUnderConstruction
        DatanodeStorageInfo[] trimmedStorageInfos =
            blockManager.getDatanodeManager().getDatanodeStorageInfos(
                trimmedTargets.toArray(new DatanodeID[trimmedTargets.size()]),
                trimmedStorages.toArray(new String[trimmedStorages.size()]),
                "src=%s, oldBlock=%s, newgenerationstamp=%d, newlength=%d",
                src, oldBlock, newgenerationstamp, newlength);

        if(copyTruncate) {
          iFile.setLastBlock(truncatedBlock, trimmedStorageInfos);
        } else {
          iFile.setLastBlock(storedBlock, trimmedStorageInfos);
          if (closeFile) {
            blockManager.markBlockReplicasAsCorrupt(storedBlock,
                oldGenerationStamp, oldNumBytes, trimmedStorageInfos);
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
        persistBlocks(src, iFile, false);
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (closeFile) {
      LOG.info("commitBlockSynchronization(oldBlock=" + oldBlock
          + ", file=" + src
          + ", newgenerationstamp=" + newgenerationstamp
          + ", newlength=" + newlength
          + ", newtargets=" + Arrays.asList(newtargets) + ") successful");
    } else {
      LOG.info("commitBlockSynchronization(" + oldBlock + ") successful");
    }
  }

  /**
   * @param pendingFile open file that needs to be closed
   * @param storedBlock last block
   * @return Path of the file that was closed.
   * @throws IOException on error
   */
  @VisibleForTesting
  void closeFileCommitBlocks(String src, INodeFile pendingFile,
      BlockInfoContiguous storedBlock) throws IOException {
    final INodesInPath iip = INodesInPath.fromINode(pendingFile);

    // commit the last block and complete it if it has minimum replicas
    commitOrCompleteLastBlock(pendingFile, iip, storedBlock);

    //remove lease, close file
    finalizeINodeFileUnderConstruction(src, pendingFile,
        Snapshot.findLatestSnapshot(pendingFile, Snapshot.CURRENT_STATE_ID));
  }

  /**
   * Renew the lease(s) held by the given client
   */
  void renewLease(String holder) throws IOException {
    checkOperation(OperationCategory.WRITE);
    readLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot renew lease for " + holder);
      leaseManager.renewLease(holder);
    } finally {
      readUnlock();
    }
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
    DirectoryListing dl = null;
    readLock();
    try {
      checkOperation(NameNode.OperationCategory.READ);
      dl = FSDirStatAndListingOp.getListingInt(dir, src, startAfter,
          needLocation);
    } catch (AccessControlException e) {
      logAuditEvent(false, "listStatus", src);
      throw e;
    } finally {
      readUnlock();
    }
    logAuditEvent(true, "listStatus", src);
    return dl;
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
      getBlockManager().getDatanodeManager().registerDatanode(nodeReg);
      checkSafeMode();
    } finally {
      writeUnlock();
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
      VolumeFailureSummary volumeFailureSummary) throws IOException {
    readLock();
    try {
      //get datanode commands
      final int maxTransfer = blockManager.getMaxReplicationStreams()
          - xmitsInProgress;
      DatanodeCommand[] cmds = blockManager.getDatanodeManager().handleHeartbeat(
          nodeReg, reports, blockPoolId, cacheCapacity, cacheUsed,
          xceiverCount, maxTransfer, failedVolumes, volumeFailureSummary);
      
      //create ha status
      final NNHAStatusHeartbeat haState = new NNHAStatusHeartbeat(
          haContext.getState().getServiceState(),
          getFSImage().getLastAppliedOrWrittenTxId());

      return new HeartbeatResponse(cmds, haState, rollingUpgradeInfo);
    } finally {
      readUnlock();
    }
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
    Preconditions.checkState(nnResourceChecker != null,
        "nnResourceChecker not initialized");
    hasResourcesAvailable = nnResourceChecker.hasAvailableDiskSpace();
  }

  /**
   * Persist the block list for the inode.
   * @param path
   * @param file
   * @param logRetryCache
   */
  private void persistBlocks(String path, INodeFile file,
                             boolean logRetryCache) {
    assert hasWriteLock();
    Preconditions.checkArgument(file.isUnderConstruction());
    getEditLog().logUpdateBlocks(path, file, logRetryCache);
    NameNode.stateChangeLog.debug("persistBlocks: {} with {} blocks is" +
        " peristed to the file system", path, file.getBlocks().length);
  }

  /**
   * Close file.
   * @param path
   * @param file
   */
  private void closeFile(String path, INodeFile file) {
    assert hasWriteLock();
    waitForLoadingFSImage();
    // file is closed
    getEditLog().logCloseFile(path, file);
    NameNode.stateChangeLog.debug("closeFile: {} with {} blocks is persisted" +
        " to the file system", path, file.getBlocks().length);
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
          FSEditLog editLog = getFSImage().getEditLog();
          long numEdits =
              editLog.getLastWrittenTxId() - editLog.getCurSegmentTxId();
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
        final Iterator<Block> it = blockManager.getCorruptReplicaBlockIterator();

        while (it.hasNext()) {
          Block b = it.next();
          BlockInfoContiguous blockInfo = blockManager.getStoredBlock(b);
          if (blockInfo.getBlockCollection().getStoragePolicyID()
              == lpPolicy.getId()) {
            filesToDelete.add(blockInfo.getBlockCollection());
          }
        }

        for (BlockCollection bc : filesToDelete) {
          LOG.warn("Removing lazyPersist file " + bc.getName() + " with no replicas.");
          BlocksMapUpdateInfo toRemoveBlocks =
              FSDirDeleteOp.deleteInternal(
                  FSNamesystem.this, bc.getName(),
                  INodesInPath.fromINode((INodeFile) bc), false);
          changed |= toRemoveBlocks != null;
          if (toRemoveBlocks != null) {
            removeBlocks(toRemoveBlocks); // Incremental deletion of blocks
          }
        }
      } finally {
        writeUnlock();
      }
      if (changed) {
        getEditLog().logSync();
      }
    }

    @Override
    public void run() {
      while (fsRunning && shouldRun) {
        try {
          clearCorruptLazyPersistFiles();
        } catch (Exception e) {
          FSNamesystem.LOG.error(
              "Ignoring exception in LazyPersistFileScrubber:", e);
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

  private void checkBlock(ExtendedBlock block) throws IOException {
    if (block != null && !this.blockPoolId.equals(block.getBlockPoolId())) {
      throw new IOException("Unexpected BlockPoolId " + block.getBlockPoolId()
          + " - expected " + blockPoolId);
    }
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
  
  @Metric({"ExpiredHeartbeats", "Number of expired heartbeats"})
  public int getExpiredHeartbeats() {
    return datanodeStatistics.getExpiredHeartbeats();
  }
  
  @Metric({"TransactionsSinceLastCheckpoint",
      "Number of transactions since last checkpoint"})
  public long getTransactionsSinceLastCheckpoint() {
    return getEditLog().getLastWrittenTxId() -
        getFSImage().getStorage().getMostRecentCheckpointTxId();
  }
  
  @Metric({"TransactionsSinceLastLogRoll",
      "Number of transactions since last edit log roll"})
  public long getTransactionsSinceLastLogRoll() {
    if (isInStandbyState() || !getEditLog().isSegmentOpen()) {
      return 0;
    } else {
      return getEditLog().getLastWrittenTxId() -
        getEditLog().getCurSegmentTxId() + 1;
    }
  }
  
  @Metric({"LastWrittenTransactionId", "Transaction ID written to the edit log"})
  public long getLastWrittenTransactionId() {
    return getEditLog().getLastWrittenTxId();
  }
  
  @Metric({"LastCheckpointTime",
      "Time in milliseconds since the epoch of the last checkpoint"})
  public long getLastCheckpointTime() {
    return getFSImage().getStorage().getMostRecentCheckpointTime();
  }

  /** @see ClientProtocol#getStats() */
  long[] getStats() {
    final long[] stats = datanodeStatistics.getStats();
    stats[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX] = getUnderReplicatedBlocks();
    stats[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX] = getCorruptReplicaBlocks();
    stats[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX] = getMissingBlocksCount();
    stats[ClientProtocol.GET_STATS_MISSING_REPL_ONE_BLOCKS_IDX] =
        getMissingReplOneBlocksCount();
    return stats;
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

  int getNumberOfDatanodes(DatanodeReportType type) {
    readLock();
    try {
      return getBlockManager().getDatanodeManager().getDatanodeListForReport(
          type).size(); 
    } finally {
      readUnlock();
    }
  }

  DatanodeInfo[] datanodeReport(final DatanodeReportType type
      ) throws AccessControlException, StandbyException {
    checkSuperuserPrivilege();
    checkOperation(OperationCategory.UNCHECKED);
    readLock();
    try {
      checkOperation(OperationCategory.UNCHECKED);
      final DatanodeManager dm = getBlockManager().getDatanodeManager();      
      final List<DatanodeDescriptor> results = dm.getDatanodeListForReport(type);

      DatanodeInfo[] arr = new DatanodeInfo[results.size()];
      for (int i=0; i<arr.length; i++) {
        arr[i] = new DatanodeInfo(results.get(i));
      }
      return arr;
    } finally {
      readUnlock();
    }
  }

  DatanodeStorageReport[] getDatanodeStorageReport(final DatanodeReportType type
      ) throws AccessControlException, StandbyException {
    checkSuperuserPrivilege();
    checkOperation(OperationCategory.UNCHECKED);
    readLock();
    try {
      checkOperation(OperationCategory.UNCHECKED);
      final DatanodeManager dm = getBlockManager().getDatanodeManager();      
      final List<DatanodeDescriptor> datanodes = dm.getDatanodeListForReport(type);

      DatanodeStorageReport[] reports = new DatanodeStorageReport[datanodes.size()];
      for (int i = 0; i < reports.length; i++) {
        final DatanodeDescriptor d = datanodes.get(i);
        reports[i] = new DatanodeStorageReport(new DatanodeInfo(d),
            d.getStorageReports());
      }
      return reports;
    } finally {
      readUnlock();
    }
  }

  /**
   * Save namespace image.
   * This will save current namespace into fsimage file and empty edits file.
   * Requires superuser privilege and safe mode.
   * 
   * @throws AccessControlException if superuser privilege is violated.
   * @throws IOException if 
   */
  void saveNamespace() throws AccessControlException, IOException {
    checkOperation(OperationCategory.UNCHECKED);
    checkSuperuserPrivilege();

    cpLock();  // Block if a checkpointing is in progress on standby.
    readLock();
    try {
      checkOperation(OperationCategory.UNCHECKED);

      if (!isInSafeMode()) {
        throw new IOException("Safe mode should be turned ON "
            + "in order to create namespace image.");
      }
      getFSImage().saveNamespace(this);
    } finally {
      readUnlock();
      cpUnlock();
    }
    LOG.info("New namespace image has been created");
  }
  
  /**
   * Enables/Disables/Checks restoring failed storage replicas if the storage becomes available again.
   * Requires superuser privilege.
   * 
   * @throws AccessControlException if superuser privilege is violated.
   */
  boolean restoreFailedStorage(String arg) throws AccessControlException,
      StandbyException {
    checkSuperuserPrivilege();
    checkOperation(OperationCategory.UNCHECKED);
    cpLock();  // Block if a checkpointing is in progress on standby.
    writeLock();
    try {
      checkOperation(OperationCategory.UNCHECKED);
      
      // if it is disabled - enable it and vice versa.
      if(arg.equals("check"))
        return getFSImage().getStorage().getRestoreFailedStorage();
      
      boolean val = arg.equals("true");  // false if not
      getFSImage().getStorage().setRestoreFailedStorage(val);
      
      return val;
    } finally {
      writeUnlock();
      cpUnlock();
    }
  }

  Date getStartTime() {
    return new Date(startTime); 
  }
    
  void finalizeUpgrade() throws IOException {
    checkSuperuserPrivilege();
    checkOperation(OperationCategory.UNCHECKED);
    cpLock();  // Block if a checkpointing is in progress on standby.
    writeLock();
    try {
      checkOperation(OperationCategory.UNCHECKED);
      getFSImage().finalizeUpgrade(this.isHaEnabled() && inActiveState());
    } finally {
      writeUnlock();
      cpUnlock();
    }
  }

  void refreshNodes() throws IOException {
    checkOperation(OperationCategory.UNCHECKED);
    checkSuperuserPrivilege();
    getBlockManager().getDatanodeManager().refreshNodes(new HdfsConfiguration());
  }

  void setBalancerBandwidth(long bandwidth) throws IOException {
    checkOperation(OperationCategory.UNCHECKED);
    checkSuperuserPrivilege();
    getBlockManager().getDatanodeManager().setBalancerBandwidth(bandwidth);
  }

  /**
   * Persist the new block (the last block of the given file).
   * @param path
   * @param file
   */
  private void persistNewBlock(String path, INodeFile file) {
    Preconditions.checkArgument(file.isUnderConstruction());
    getEditLog().logAddBlock(path, file);
    NameNode.stateChangeLog.debug("persistNewBlock: {} with new block {}," +
        " current total block count is {}", path,
        file.getLastBlock().toString(), file.getBlocks().length);
  }

  /**
   * SafeModeInfo contains information related to the safe mode.
   * <p>
   * An instance of {@link SafeModeInfo} is created when the name node
   * enters safe mode.
   * <p>
   * During name node startup {@link SafeModeInfo} counts the number of
   * <em>safe blocks</em>, those that have at least the minimal number of
   * replicas, and calculates the ratio of safe blocks to the total number
   * of blocks in the system, which is the size of blocks in
   * {@link FSNamesystem#blockManager}. When the ratio reaches the
   * {@link #threshold} it starts the SafeModeMonitor daemon in order
   * to monitor whether the safe mode {@link #extension} is passed.
   * Then it leaves safe mode and destroys itself.
   * <p>
   * If safe mode is turned on manually then the number of safe blocks is
   * not tracked because the name node is not intended to leave safe mode
   * automatically in the case.
   *
   * @see ClientProtocol#setSafeMode(HdfsConstants.SafeModeAction, boolean)
   */
  public class SafeModeInfo {
    // configuration fields
    /** Safe mode threshold condition %.*/
    private final double threshold;
    /** Safe mode minimum number of datanodes alive */
    private final int datanodeThreshold;
    /**
     * Safe mode extension after the threshold.
     * Make it volatile so that getSafeModeTip can read the latest value
     * without taking a lock.
     */
    private volatile int extension;
    /** Min replication required by safe mode. */
    private final int safeReplication;
    /** threshold for populating needed replication queues */
    private final double replQueueThreshold;
    // internal fields
    /** Time when threshold was reached.
     * <br> -1 safe mode is off
     * <br> 0 safe mode is on, and threshold is not reached yet
     * <br> >0 safe mode is on, but we are in extension period 
     */
    private long reached = -1;  
    private long reachedTimestamp = -1;
    /** Total number of blocks. */
    int blockTotal; 
    /** Number of safe blocks. */
    int blockSafe;
    /** Number of blocks needed to satisfy safe mode threshold condition */
    private int blockThreshold;
    /** Number of blocks needed before populating replication queues */
    private int blockReplQueueThreshold;
    /** time of the last status printout */
    private long lastStatusReport = 0;
    /**
     * Was safemode entered automatically because available resources were low.
     * Make it volatile so that getSafeModeTip can read the latest value
     * without taking a lock.
     */
    private volatile boolean resourcesLow = false;
    /** Should safemode adjust its block totals as blocks come in */
    private boolean shouldIncrementallyTrackBlocks = false;
    /** counter for tracking startup progress of reported blocks */
    private Counter awaitingReportedBlocksCounter;
    
    /**
     * Creates SafeModeInfo when the name node enters
     * automatic safe mode at startup.
     *  
     * @param conf configuration
     */
    private SafeModeInfo(Configuration conf) {
      this.threshold = conf.getFloat(DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
          DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
      if(threshold > 1.0) {
        LOG.warn("The threshold value should't be greater than 1, threshold: " + threshold);
      }
      this.datanodeThreshold = conf.getInt(
        DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY,
        DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT);
      this.extension = conf.getInt(DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, 0);
      this.safeReplication = conf.getInt(DFS_NAMENODE_REPLICATION_MIN_KEY, 
                                         DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
      
      LOG.info(DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY + " = " + threshold);
      LOG.info(DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY + " = " + datanodeThreshold);
      LOG.info(DFS_NAMENODE_SAFEMODE_EXTENSION_KEY + "     = " + extension);

      // default to safe mode threshold (i.e., don't populate queues before leaving safe mode)
      this.replQueueThreshold = 
        conf.getFloat(DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY,
                      (float) threshold);
      this.blockTotal = 0; 
      this.blockSafe = 0;
    }

    /**
     * In the HA case, the StandbyNode can be in safemode while the namespace
     * is modified by the edit log tailer. In this case, the number of total
     * blocks changes as edits are processed (eg blocks are added and deleted).
     * However, we don't want to do the incremental tracking during the
     * startup-time loading process -- only once the initial total has been
     * set after the image has been loaded.
     */
    private boolean shouldIncrementallyTrackBlocks() {
      return shouldIncrementallyTrackBlocks;
    }

    /**
     * Creates SafeModeInfo when safe mode is entered manually, or because
     * available resources are low.
     *
     * The {@link #threshold} is set to 1.5 so that it could never be reached.
     * {@link #blockTotal} is set to -1 to indicate that safe mode is manual.
     * 
     * @see SafeModeInfo
     */
    private SafeModeInfo(boolean resourcesLow) {
      this.threshold = 1.5f;  // this threshold can never be reached
      this.datanodeThreshold = Integer.MAX_VALUE;
      this.extension = Integer.MAX_VALUE;
      this.safeReplication = Short.MAX_VALUE + 1; // more than maxReplication
      this.replQueueThreshold = 1.5f; // can never be reached
      this.blockTotal = -1;
      this.blockSafe = -1;
      this.resourcesLow = resourcesLow;
      enter();
      reportStatus("STATE* Safe mode is ON.", true);
    }
      
    /**
     * Check if safe mode is on.
     * @return true if in safe mode
     */
    private synchronized boolean isOn() {
      doConsistencyCheck();
      return this.reached >= 0;
    }
      
    /**
     * Enter safe mode.
     */
    private void enter() {
      this.reached = 0;
      this.reachedTimestamp = 0;
    }
      
    /**
     * Leave safe mode.
     * <p>
     * Check for invalid, under- & over-replicated blocks in the end of startup.
     */
    private synchronized void leave() {
      // if not done yet, initialize replication queues.
      // In the standby, do not populate repl queues
      if (!isPopulatingReplQueues() && shouldPopulateReplQueues()) {
        initializeReplQueues();
      }
      long timeInSafemode = now() - startTime;
      NameNode.stateChangeLog.info("STATE* Leaving safe mode after " 
                                    + timeInSafemode/1000 + " secs");
      NameNode.getNameNodeMetrics().setSafeModeTime((int) timeInSafemode);

      //Log the following only once (when transitioning from ON -> OFF)
      if (reached >= 0) {
        NameNode.stateChangeLog.info("STATE* Safe mode is OFF"); 
      }
      reached = -1;
      reachedTimestamp = -1;
      safeMode = null;
      final NetworkTopology nt = blockManager.getDatanodeManager().getNetworkTopology();
      NameNode.stateChangeLog.info("STATE* Network topology has "
          + nt.getNumOfRacks() + " racks and "
          + nt.getNumOfLeaves() + " datanodes");
      NameNode.stateChangeLog.info("STATE* UnderReplicatedBlocks has "
          + blockManager.numOfUnderReplicatedBlocks() + " blocks");

      startSecretManagerIfNecessary();

      // If startup has not yet completed, end safemode phase.
      StartupProgress prog = NameNode.getStartupProgress();
      if (prog.getStatus(Phase.SAFEMODE) != Status.COMPLETE) {
        prog.endStep(Phase.SAFEMODE, STEP_AWAITING_REPORTED_BLOCKS);
        prog.endPhase(Phase.SAFEMODE);
      }
    }

    /**
     * Check whether we have reached the threshold for 
     * initializing replication queues.
     */
    private synchronized boolean canInitializeReplQueues() {
      return shouldPopulateReplQueues()
          && blockSafe >= blockReplQueueThreshold;
    }
      
    /** 
     * Safe mode can be turned off iff 
     * the threshold is reached and 
     * the extension time have passed.
     * @return true if can leave or false otherwise.
     */
    private synchronized boolean canLeave() {
      if (reached == 0) {
        return false;
      }

      if (monotonicNow() - reached < extension) {
        reportStatus("STATE* Safe mode ON, in safe mode extension.", false);
        return false;
      }

      if (needEnter()) {
        reportStatus("STATE* Safe mode ON, thresholds not met.", false);
        return false;
      }

      return true;
    }
      
    /** 
     * There is no need to enter safe mode 
     * if DFS is empty or {@link #threshold} == 0
     */
    private boolean needEnter() {
      return (threshold != 0 && blockSafe < blockThreshold) ||
        (datanodeThreshold != 0 && getNumLiveDataNodes() < datanodeThreshold) ||
        (!nameNodeHasResourcesAvailable());
    }
      
    /**
     * Check and trigger safe mode if needed. 
     */
    private void checkMode() {
      // Have to have write-lock since leaving safemode initializes
      // repl queues, which requires write lock
      assert hasWriteLock();
      if (inTransitionToActive()) {
        return;
      }
      // if smmthread is already running, the block threshold must have been 
      // reached before, there is no need to enter the safe mode again
      if (smmthread == null && needEnter()) {
        enter();
        // check if we are ready to initialize replication queues
        if (canInitializeReplQueues() && !isPopulatingReplQueues()
            && !haEnabled) {
          initializeReplQueues();
        }
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // the threshold is reached or was reached before
      if (!isOn() ||                           // safe mode is off
          extension <= 0 || threshold <= 0) {  // don't need to wait
        this.leave(); // leave safe mode
        return;
      }
      if (reached > 0) {  // threshold has already been reached before
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // start monitor
      reached = monotonicNow();
      reachedTimestamp = now();
      if (smmthread == null) {
        smmthread = new Daemon(new SafeModeMonitor());
        smmthread.start();
        reportStatus("STATE* Safe mode extension entered.", true);
      }

      // check if we are ready to initialize replication queues
      if (canInitializeReplQueues() && !isPopulatingReplQueues() && !haEnabled) {
        initializeReplQueues();
      }
    }
      
    /**
     * Set total number of blocks.
     */
    private synchronized void setBlockTotal(int total) {
      this.blockTotal = total;
      this.blockThreshold = (int) (blockTotal * threshold);
      this.blockReplQueueThreshold = 
        (int) (blockTotal * replQueueThreshold);
      if (haEnabled) {
        // After we initialize the block count, any further namespace
        // modifications done while in safe mode need to keep track
        // of the number of total blocks in the system.
        this.shouldIncrementallyTrackBlocks = true;
      }
      if(blockSafe < 0)
        this.blockSafe = 0;
      checkMode();
    }
      
    /**
     * Increment number of safe blocks if current block has 
     * reached minimal replication.
     * @param replication current replication 
     */
    private synchronized void incrementSafeBlockCount(short replication) {
      if (replication == safeReplication) {
        this.blockSafe++;

        // Report startup progress only if we haven't completed startup yet.
        StartupProgress prog = NameNode.getStartupProgress();
        if (prog.getStatus(Phase.SAFEMODE) != Status.COMPLETE) {
          if (this.awaitingReportedBlocksCounter == null) {
            this.awaitingReportedBlocksCounter = prog.getCounter(Phase.SAFEMODE,
              STEP_AWAITING_REPORTED_BLOCKS);
          }
          this.awaitingReportedBlocksCounter.increment();
        }

        checkMode();
      }
    }
      
    /**
     * Decrement number of safe blocks if current block has 
     * fallen below minimal replication.
     * @param replication current replication 
     */
    private synchronized void decrementSafeBlockCount(short replication) {
      if (replication == safeReplication-1) {
        this.blockSafe--;
        //blockSafe is set to -1 in manual / low resources safemode
        assert blockSafe >= 0 || isManual() || areResourcesLow();
        checkMode();
      }
    }

    /**
     * Check if safe mode was entered manually
     */
    private boolean isManual() {
      return extension == Integer.MAX_VALUE;
    }

    /**
     * Set manual safe mode.
     */
    private synchronized void setManual() {
      extension = Integer.MAX_VALUE;
    }

    /**
     * Check if safe mode was entered due to resources being low.
     */
    private boolean areResourcesLow() {
      return resourcesLow;
    }

    /**
     * Set that resources are low for this instance of safe mode.
     */
    private void setResourcesLow() {
      resourcesLow = true;
    }

    /**
     * A tip on how safe mode is to be turned off: manually or automatically.
     */
    String getTurnOffTip() {
      if(!isOn()) {
        return "Safe mode is OFF.";
      }

      //Manual OR low-resource safemode. (Admin intervention required)
      String adminMsg = "It was turned on manually. ";
      if (areResourcesLow()) {
        adminMsg = "Resources are low on NN. Please add or free up more "
          + "resources then turn off safe mode manually. NOTE:  If you turn off"
          + " safe mode before adding resources, "
          + "the NN will immediately return to safe mode. ";
      }
      if (isManual() || areResourcesLow()) {
        return adminMsg
          + "Use \"hdfs dfsadmin -safemode leave\" to turn safe mode off.";
      }

      boolean thresholdsMet = true;
      int numLive = getNumLiveDataNodes();
      String msg = "";
      if (blockSafe < blockThreshold) {
        msg += String.format(
          "The reported blocks %d needs additional %d"
          + " blocks to reach the threshold %.4f of total blocks %d.%n",
          blockSafe, (blockThreshold - blockSafe) + 1, threshold, blockTotal);
        thresholdsMet = false;
      } else {
        msg += String.format("The reported blocks %d has reached the threshold"
            + " %.4f of total blocks %d. ", blockSafe, threshold, blockTotal);
      }
      if (numLive < datanodeThreshold) {
        msg += String.format(
          "The number of live datanodes %d needs an additional %d live "
          + "datanodes to reach the minimum number %d.%n",
          numLive, (datanodeThreshold - numLive), datanodeThreshold);
        thresholdsMet = false;
      } else {
        msg += String.format("The number of live datanodes %d has reached "
            + "the minimum number %d. ",
            numLive, datanodeThreshold);
      }
      msg += (reached > 0) ? "In safe mode extension. " : "";
      msg += "Safe mode will be turned off automatically ";

      if (!thresholdsMet) {
        msg += "once the thresholds have been reached.";
      } else if (reached + extension - monotonicNow() > 0) {
        msg += ("in " + (reached + extension - monotonicNow()) / 1000 + " seconds.");
      } else {
        msg += "soon.";
      }

      return msg;
    }

    /**
     * Print status every 20 seconds.
     */
    private void reportStatus(String msg, boolean rightNow) {
      long curTime = now();
      if(!rightNow && (curTime - lastStatusReport < 20 * 1000))
        return;
      NameNode.stateChangeLog.info(msg + " \n" + getTurnOffTip());
      lastStatusReport = curTime;
    }

    @Override
    public String toString() {
      String resText = "Current safe blocks = " 
        + blockSafe 
        + ". Target blocks = " + blockThreshold + " for threshold = %" + threshold
        + ". Minimal replication = " + safeReplication + ".";
      if (reached > 0) 
        resText += " Threshold was reached " + new Date(reachedTimestamp) + ".";
      return resText;
    }
      
    /**
     * Checks consistency of the class state.
     * This is costly so only runs if asserts are enabled.
     */
    private void doConsistencyCheck() {
      boolean assertsOn = false;
      assert assertsOn = true; // set to true if asserts are on
      if (!assertsOn) return;
      
      if (blockTotal == -1 && blockSafe == -1) {
        return; // manual safe mode
      }
      int activeBlocks = blockManager.getActiveBlockCount();
      if ((blockTotal != activeBlocks) &&
          !(blockSafe >= 0 && blockSafe <= blockTotal)) {
        throw new AssertionError(
            " SafeMode: Inconsistent filesystem state: "
        + "SafeMode data: blockTotal=" + blockTotal
        + " blockSafe=" + blockSafe + "; "
        + "BlockManager data: active="  + activeBlocks);
      }
    }

    private synchronized void adjustBlockTotals(int deltaSafe, int deltaTotal) {
      if (!shouldIncrementallyTrackBlocks) {
        return;
      }
      assert haEnabled;
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adjusting block totals from " +
            blockSafe + "/" + blockTotal + " to " +
            (blockSafe + deltaSafe) + "/" + (blockTotal + deltaTotal));
      }
      assert blockSafe + deltaSafe >= 0 : "Can't reduce blockSafe " +
        blockSafe + " by " + deltaSafe + ": would be negative";
      assert blockTotal + deltaTotal >= 0 : "Can't reduce blockTotal " +
        blockTotal + " by " + deltaTotal + ": would be negative";
      
      blockSafe += deltaSafe;
      setBlockTotal(blockTotal + deltaTotal);
    }
  }
    
  /**
   * Periodically check whether it is time to leave safe mode.
   * This thread starts when the threshold level is reached.
   *
   */
  class SafeModeMonitor implements Runnable {
    /** interval in msec for checking safe mode: {@value} */
    private static final long recheckInterval = 1000;
      
    /**
     */
    @Override
    public void run() {
      while (fsRunning) {
        writeLock();
        try {
          if (safeMode == null) { // Not in safe mode.
            break;
          }
          if (safeMode.canLeave()) {
            // Leave safe mode.
            safeMode.leave();
            smmthread = null;
            break;
          }
        } finally {
          writeUnlock();
        }

        try {
          Thread.sleep(recheckInterval);
        } catch (InterruptedException ie) {
          // Ignored
        }
      }
      if (!fsRunning) {
        LOG.info("NameNode is being shutdown, exit SafeModeMonitor thread");
      }
    }
  }
    
  boolean setSafeMode(SafeModeAction action) throws IOException {
    if (action != SafeModeAction.SAFEMODE_GET) {
      checkSuperuserPrivilege();
      switch(action) {
      case SAFEMODE_LEAVE: // leave safe mode
        leaveSafeMode();
        break;
      case SAFEMODE_ENTER: // enter safe mode
        enterSafeMode(false);
        break;
      default:
        LOG.error("Unexpected safe mode action");
      }
    }
    return isInSafeMode();
  }

  @Override
  public void checkSafeMode() {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode != null) {
      safeMode.checkMode();
    }
  }

  @Override
  public boolean isInSafeMode() {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null)
      return false;
    return safeMode.isOn();
  }

  @Override
  public boolean isInStartupSafeMode() {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null)
      return false;
    // If the NN is in safemode, and not due to manual / low resources, we
    // assume it must be because of startup. If the NN had low resources during
    // startup, we assume it came out of startup safemode and it is now in low
    // resources safemode
    return !safeMode.isManual() && !safeMode.areResourcesLow()
      && safeMode.isOn();
  }

  /**
   * Check if replication queues are to be populated
   * @return true when node is HAState.Active and not in the very first safemode
   */
  @Override
  public boolean isPopulatingReplQueues() {
    if (!shouldPopulateReplQueues()) {
      return false;
    }
    return initializedReplQueues;
  }

  private boolean shouldPopulateReplQueues() {
    if(haContext == null || haContext.getState() == null)
      return false;
    return haContext.getState().shouldPopulateReplQueues();
  }

  @Override
  public void incrementSafeBlockCount(int replication) {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null)
      return;
    safeMode.incrementSafeBlockCount((short)replication);
  }

  @Override
  public void decrementSafeBlockCount(Block b) {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) // mostly true
      return;
    BlockInfoContiguous storedBlock = getStoredBlock(b);
    if (storedBlock.isComplete()) {
      safeMode.decrementSafeBlockCount((short)blockManager.countNodes(b).liveReplicas());
    }
  }
  
  /**
   * Adjust the total number of blocks safe and expected during safe mode.
   * If safe mode is not currently on, this is a no-op.
   * @param deltaSafe the change in number of safe blocks
   * @param deltaTotal the change i nnumber of total blocks expected
   */
  @Override
  public void adjustSafeModeBlockTotals(int deltaSafe, int deltaTotal) {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null)
      return;
    safeMode.adjustBlockTotals(deltaSafe, deltaTotal);
  }

  /**
   * Set the total number of blocks in the system. 
   */
  public void setBlockTotal() {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null)
      return;
    safeMode.setBlockTotal((int)getCompleteBlocksTotal());
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
   * Get the total number of COMPLETE blocks in the system.
   * For safe mode only complete blocks are counted.
   */
  private long getCompleteBlocksTotal() {
    // Calculate number of blocks under construction
    long numUCBlocks = 0;
    readLock();
    numUCBlocks = leaseManager.getNumUnderConstructionBlocks();
    try {
      return getBlocksTotal() - numUCBlocks;
    } finally {
      readUnlock();
    }
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
      if (!isInSafeMode()) {
        safeMode = new SafeModeInfo(resourcesLow);
        return;
      }
      if (resourcesLow) {
        safeMode.setResourcesLow();
      } else {
        safeMode.setManual();
      }
      if (isEditlogOpenForWrite) {
        getEditLog().logSyncAll();
      }
      NameNode.stateChangeLog.info("STATE* Safe mode is ON"
          + safeMode.getTurnOffTip());
    } finally {
      writeUnlock();
    }
  }

  /**
   * Leave safe mode.
   */
  void leaveSafeMode() {
    writeLock();
    try {
      if (!isInSafeMode()) {
        NameNode.stateChangeLog.info("STATE* Safe mode is already OFF"); 
        return;
      }
      safeMode.leave();
    } finally {
      writeUnlock();
    }
  }
    
  String getSafeModeTip() {
    // There is no need to take readLock.
    // Don't use isInSafeMode as this.safeMode might be set to null.
    // after isInSafeMode returns.
    boolean inSafeMode;
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) {
      inSafeMode = false;
    } else {
      inSafeMode = safeMode.isOn();
    }

    if (!inSafeMode) {
      return "";
    } else {
      return safeMode.getTurnOffTip();
    }
  }

  CheckpointSignature rollEditLog() throws IOException {
    checkSuperuserPrivilege();
    checkOperation(OperationCategory.JOURNAL);
    writeLock();
    try {
      checkOperation(OperationCategory.JOURNAL);
      checkNameNodeSafeMode("Log not rolled");
      if (Server.isRpcInvocation()) {
        LOG.info("Roll Edit Log from " + Server.getRemoteAddress());
      }
      return getFSImage().rollEditLog();
    } finally {
      writeUnlock();
    }
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
          activeNamenode);
      getEditLog().logSync();
      return cmd;
    } finally {
      writeUnlock();
    }
  }

  public void processIncrementalBlockReport(final DatanodeID nodeID,
      final StorageReceivedDeletedBlocks srdb)
      throws IOException {
    writeLock();
    try {
      blockManager.processIncrementalBlockReport(nodeID, srdb);
    } finally {
      writeUnlock();
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
      readUnlock();
    }
  }

  PermissionStatus createFsOwnerPermissions(FsPermission permission) {
    return new PermissionStatus(fsOwner.getShortUserName(), supergroup, permission);
  }

  private void checkUnreadableBySuperuser(FSPermissionChecker pc,
      INode inode, int snapshotId)
      throws IOException {
    if (pc.isSuperUser()) {
      for (XAttr xattr : FSDirXAttrOp.getXAttrs(dir, inode, snapshotId)) {
        if (XAttrHelper.getPrefixName(xattr).
            equals(SECURITY_XATTR_UNREADABLE_BY_SUPERUSER)) {
          throw new AccessControlException("Access is denied for " +
              pc.getUser() + " since the superuser is not allowed to " +
              "perform this operation.");
        }
      }
    }
  }

  @Override
  public void checkSuperuserPrivilege()
      throws AccessControlException {
    if (isPermissionEnabled) {
      FSPermissionChecker pc = getPermissionChecker();
      pc.checkSuperuserPrivilege();
    }
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

  /**
   * Get the total number of objects in the system. 
   */
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

  @Override // FSNamesystemMBean
  @Metric
  public long getPendingReplicationBlocks() {
    return blockManager.getPendingReplicationBlocksCount();
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getUnderReplicatedBlocks() {
    return blockManager.getUnderReplicatedBlocksCount();
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

  @Override
  public long getBlockDeletionStartTime() {
    return startTime + blockManager.getStartupDelayBlockDeletionInMs();
  }

  @Metric
  public long getExcessBlocks() {
    return blockManager.getExcessBlocksCount();
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

  @Override // FSNamesystemMBean
  public String getFSState() {
    return isInSafeMode() ? "safeMode" : "Operational";
  }
  
  private ObjectName mbeanName;
  private ObjectName mxbeanName;

  /**
   * Register the FSNamesystem MBean using the name
   *        "hadoop:service=NameNode,name=FSNamesystemState"
   */
  private void registerMBean() {
    // We can only implement one MXBean interface, so we keep the old one.
    try {
      StandardMBean bean = new StandardMBean(this, FSNamesystemMBean.class);
      mbeanName = MBeans.register("NameNode", "FSNamesystemState", bean);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad MBean setup", e);
    }

    LOG.info("Registered FSNamesystemState MBean");
  }

  /**
   * shutdown FSNamesystem
   */
  void shutdown() {
    if (snapshotManager != null) {
      snapshotManager.shutdown();
    }
    if (mbeanName != null) {
      MBeans.unregister(mbeanName);
      mbeanName = null;
    }
    if (mxbeanName != null) {
      MBeans.unregister(mxbeanName);
      mxbeanName = null;
    }
    if (dir != null) {
      dir.shutdown();
    }
    if (blockManager != null) {
      blockManager.shutdown();
    }
  }

  @Override // FSNamesystemMBean
  public int getNumLiveDataNodes() {
    return getBlockManager().getDatanodeManager().getNumLiveDataNodes();
  }

  @Override // FSNamesystemMBean
  public int getNumDeadDataNodes() {
    return getBlockManager().getDatanodeManager().getNumDeadDataNodes();
  }
  
  @Override // FSNamesystemMBean
  public int getNumDecomLiveDataNodes() {
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    getBlockManager().getDatanodeManager().fetchDatanodes(live, null, true);
    int liveDecommissioned = 0;
    for (DatanodeDescriptor node : live) {
      liveDecommissioned += node.isDecommissioned() ? 1 : 0;
    }
    return liveDecommissioned;
  }

  @Override // FSNamesystemMBean
  public int getNumDecomDeadDataNodes() {
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    getBlockManager().getDatanodeManager().fetchDatanodes(null, dead, true);
    int deadDecommissioned = 0;
    for (DatanodeDescriptor node : dead) {
      deadDecommissioned += node.isDecommissioned() ? 1 : 0;
    }
    return deadDecommissioned;
  }

  @Override // FSNamesystemMBean
  public int getVolumeFailuresTotal() {
    List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    getBlockManager().getDatanodeManager().fetchDatanodes(live, null, true);
    int volumeFailuresTotal = 0;
    for (DatanodeDescriptor node: live) {
      volumeFailuresTotal += node.getVolumeFailures();
    }
    return volumeFailuresTotal;
  }

  @Override // FSNamesystemMBean
  public long getEstimatedCapacityLostTotal() {
    List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    getBlockManager().getDatanodeManager().fetchDatanodes(live, null, true);
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
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(topMap);
    } catch (IOException e) {
      LOG.warn("Failed to fetch TopUser metrics", e);
    }
    return null;
  }

  /**
   * Increments, logs and then returns the stamp
   */
  long nextGenerationStamp(boolean legacyBlock)
      throws IOException, SafeModeException {
    assert hasWriteLock();
    checkNameNodeSafeMode("Cannot get next generation stamp");

    long gs = blockIdManager.nextGenerationStamp(legacyBlock);
    if (legacyBlock) {
      getEditLog().logGenerationStampV1(gs);
    } else {
      getEditLog().logGenerationStampV2(gs);
    }

    // NB: callers sync the log
    return gs;
  }

  /**
   * Increments, logs and then returns the block ID
   */
  private long nextBlockId() throws IOException {
    assert hasWriteLock();
    checkNameNodeSafeMode("Cannot get next block ID");
    final long blockId = blockIdManager.nextBlockId();
    getEditLog().logAllocateBlockId(blockId);
    // NB: callers sync the log
    return blockId;
  }

  private boolean isFileDeleted(INodeFile file) {
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
    BlockInfoContiguous storedBlock = getStoredBlock(ExtendedBlock.getLocalBlock(block));
    if (storedBlock == null || 
        storedBlock.getBlockUCState() != BlockUCState.UNDER_CONSTRUCTION) {
        throw new IOException(block + 
            " does not exist or is not under Construction" + storedBlock);
    }
    
    // check file inode
    final INodeFile file = ((INode)storedBlock.getBlockCollection()).asFile();
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
      writeUnlock();
    }
  }

  /**
   * Get a new generation stamp together with an access token for 
   * a block under construction
   * 
   * This method is called for recovering a failed pipeline or setting up
   * a pipeline to append to a block.
   * 
   * @param block a block
   * @param clientName the name of a client
   * @return a located block with a new generation stamp and an access token
   * @throws IOException if any error occurs
   */
  LocatedBlock updateBlockForPipeline(ExtendedBlock block, 
      String clientName) throws IOException {
    LocatedBlock locatedBlock;
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      // check vadility of parameters
      checkUCBlock(block, clientName);
  
      // get a new generation stamp and an access token
      block.setGenerationStamp(nextGenerationStamp(blockIdManager.isLegacyBlock(block.getLocalBlock())));
      locatedBlock = new LocatedBlock(block, new DatanodeInfo[0]);
      blockManager.setBlockToken(locatedBlock, AccessMode.WRITE);
    } finally {
      writeUnlock();
    }
    // Ensure we record the new generation stamp
    getEditLog().logSync();
    return locatedBlock;
  }
  
  /**
   * Update a pipeline for a block under construction
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
    LOG.info("updatePipeline(" + oldBlock.getLocalBlock()
             + ", newGS=" + newBlock.getGenerationStamp()
             + ", newLength=" + newBlock.getNumBytes()
             + ", newNodes=" + Arrays.asList(newNodes)
             + ", client=" + clientName
             + ")");
    waitForLoadingFSImage();
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Pipeline not updated");
      assert newBlock.getBlockId()==oldBlock.getBlockId() : newBlock + " and "
        + oldBlock + " has different block identifier";
      updatePipelineInternal(clientName, oldBlock, newBlock, newNodes,
          newStorageIDs, logRetryCache);
    } finally {
      writeUnlock();
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
    final BlockInfoContiguousUnderConstruction blockinfo
        = (BlockInfoContiguousUnderConstruction)pendingFile.getLastBlock();

    // check new GS & length: this is not expected
    if (newBlock.getGenerationStamp() <= blockinfo.getGenerationStamp() ||
        newBlock.getNumBytes() < blockinfo.getNumBytes()) {
      String msg = "Update " + oldBlock + " (len = " + 
        blockinfo.getNumBytes() + ") to an older state: " + newBlock + 
        " (len = " + newBlock.getNumBytes() +")";
      LOG.warn(msg);
      throw new IOException(msg);
    }

    // Update old block with the new generation stamp and new length
    blockinfo.setNumBytes(newBlock.getNumBytes());
    blockinfo.setGenerationStampAndVerifyReplicas(newBlock.getGenerationStamp());

    // find the DatanodeDescriptor objects
    final DatanodeStorageInfo[] storages = blockManager.getDatanodeManager()
        .getDatanodeStorageInfos(newNodes, newStorageIDs,
            "src=%s, oldBlock=%s, newBlock=%s, clientName=%s",
            src, oldBlock, newBlock, clientName);
    blockinfo.setExpectedLocations(storages);

    persistBlocks(src, pendingFile, logRetryCache);
  }

  // rename was successful. If any part of the renamed subtree had
  // files that were being written to, update with new filename.
  void unprotectedChangeLease(String src, String dst) {
    assert hasWriteLock();
    leaseManager.changeLease(src, dst);
  }

  /**
   * Serializes leases.
   */
  void saveFilesUnderConstruction(DataOutputStream out,
      Map<Long, INodeFile> snapshotUCMap) throws IOException {
    // This is run by an inferior thread of saveNamespace, which holds a read
    // lock on our behalf. If we took the read lock here, we could block
    // for fairness if a writer is waiting on the lock.
    synchronized (leaseManager) {
      Map<String, INodeFile> nodes = leaseManager.getINodesUnderConstruction();
      for (Map.Entry<String, INodeFile> entry : nodes.entrySet()) {
        // TODO: for HDFS-5428, because of rename operations, some
        // under-construction files that are
        // in the current fs directory can also be captured in the
        // snapshotUCMap. We should remove them from the snapshotUCMap.
        snapshotUCMap.remove(entry.getValue().getId());
      }

      out.writeInt(nodes.size() + snapshotUCMap.size()); // write the size
      for (Map.Entry<String, INodeFile> entry : nodes.entrySet()) {
        FSImageSerialization.writeINodeUnderConstruction(
            out, entry.getValue(), entry.getKey());
      }
      for (Map.Entry<Long, INodeFile> entry : snapshotUCMap.entrySet()) {
        // for those snapshot INodeFileUC, we use "/.reserved/.inodes/<inodeid>"
        // as their paths
        StringBuilder b = new StringBuilder();
        b.append(FSDirectory.DOT_RESERVED_PATH_PREFIX)
            .append(Path.SEPARATOR).append(FSDirectory.DOT_INODES_STRING)
            .append(Path.SEPARATOR).append(entry.getValue().getId());
        FSImageSerialization.writeINodeUnderConstruction(
            out, entry.getValue(), b.toString());
      }
    }
  }

  /**
   * @return all the under-construction files in the lease map
   */
  Map<String, INodeFile> getFilesUnderConstruction() {
    synchronized (leaseManager) {
      return leaseManager.getINodesUnderConstruction();
    }
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
      if(getFSImage().getStorage().getNamespaceID() 
         != bnReg.getNamespaceID())
        throw new IOException("Incompatible namespaceIDs: "
            + " Namenode namespaceID = "
            + getFSImage().getStorage().getNamespaceID() + "; "
            + bnReg.getRole() +
            " node namespaceID = " + bnReg.getNamespaceID());
      if (bnReg.getRole() == NamenodeRole.BACKUP) {
        getFSImage().getEditLog().registerBackupNode(
            bnReg, nnReg);
      }
    } finally {
      writeUnlock();
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
      if(getFSImage().getStorage().getNamespaceID()
         != registration.getNamespaceID())
        throw new IOException("Incompatible namespaceIDs: "
            + " Namenode namespaceID = "
            + getFSImage().getStorage().getNamespaceID() + "; "
            + registration.getRole() +
            " node namespaceID = " + registration.getNamespaceID());
      getEditLog().releaseBackupStream(registration);
    } finally {
      writeUnlock();
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
    checkSuperuserPrivilege();
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("there are no corrupt file blocks.");
      }
      return corruptFiles;
    }

    readLock();
    try {
      checkOperation(OperationCategory.READ);
      if (!isPopulatingReplQueues()) {
        throw new IOException("Cannot run listCorruptFileBlocks because " +
                              "replication queues have not been initialized.");
      }
      // print a limited # of corrupt files per call

      final Iterator<Block> blkIterator = blockManager.getCorruptReplicaBlockIterator();

      int skip = getIntCookie(cookieTab[0]);
      for (int i = 0; i < skip && blkIterator.hasNext(); i++) {
        blkIterator.next();
      }

      while (blkIterator.hasNext()) {
        Block blk = blkIterator.next();
        final INode inode = (INode)blockManager.getBlockCollection(blk);
        skip++;
        if (inode != null && blockManager.countNodes(blk).liveReplicas() == 0) {
          String src = FSDirectory.getFullPathName(inode);
          if (src.startsWith(path)){
            corruptFiles.add(new CorruptFileBlockInfo(src, blk));
            count++;
            if (count >= DEFAULT_MAX_CORRUPT_FILEBLOCKS_RETURNED)
              break;
          }
        }
      }
      cookieTab[0] = String.valueOf(skip);
      if (LOG.isDebugEnabled()) {
        LOG.debug("list corrupt file blocks returned: " + count);
      }
      return corruptFiles;
    } finally {
      readUnlock();
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
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
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
    long expiryTime;
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      checkNameNodeSafeMode("Cannot renew delegation token");
      if (!isAllowedDelegationTokenOp()) {
        throw new IOException(
            "Delegation Token can be renewed only with kerberos or web authentication");
      }
      String renewer = getRemoteUser().getShortUserName();
      expiryTime = dtSecretManager.renewToken(token, renewer);
      DelegationTokenIdentifier id = new DelegationTokenIdentifier();
      ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
      DataInputStream in = new DataInputStream(buf);
      id.readFields(in);
      getEditLog().logRenewDelegationToken(id, expiryTime);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    return expiryTime;
  }

  /**
   * 
   * @param token token to cancel
   * @throws IOException on error
   */
  void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);

      checkNameNodeSafeMode("Cannot cancel delegation token");
      String canceller = getRemoteUser().getUserName();
      DelegationTokenIdentifier id = dtSecretManager
        .cancelToken(token, canceller);
      getEditLog().logCancelDelegationToken(id);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
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
      List<SecretManagerSection.PersistToken> tokens) throws IOException {
    dtSecretManager.loadSecretManagerState(new SecretManagerState(s, keys, tokens));
  }

  /**
   * Log the updateMasterKey operation to edit logs
   * 
   * @param key new delegation key.
   */
  public void logUpdateMasterKey(DelegationKey key) {
    
    assert !isInSafeMode() :
      "this should never be called while in safemode, since we stop " +
      "the DT manager before entering safemode!";
    // No need to hold FSN lock since we don't access any internal
    // structures, and this is stopped before the FSN shuts itself
    // down, etc.
    getEditLog().logUpdateMasterKey(key);
    getEditLog().logSync();
  }
  
  /**
   * Log the cancellation of expired tokens to edit logs
   * 
   * @param id token identifier to cancel
   */
  public void logExpireDelegationToken(DelegationTokenIdentifier id) {
    assert !isInSafeMode() :
      "this should never be called while in safemode, since we stop " +
      "the DT manager before entering safemode!";
    // No need to hold FSN lock since we don't access any internal
    // structures, and this is stopped before the FSN shuts itself
    // down, etc.
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
   * Returns authentication method used to establish the connection
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
    return Server.isRpcInvocation() || NamenodeWebHdfsMethods.isWebHdfsInvocation();
  }

  private static InetAddress getRemoteIp() {
    InetAddress ip = Server.getRemoteIp();
    if (ip != null) {
      return ip;
    }
    return NamenodeWebHdfsMethods.getRemoteIp();
  }
  
  // optimize ugi lookup for RPC operations to avoid a trip through
  // UGI.getCurrentUser which is synch'ed
  private static UserGroupInformation getRemoteUser() throws IOException {
    return NameNode.getRemoteUser();
  }
  
  /**
   * Log fsck event in the audit log 
   */
  void logFsckEvent(String src, InetAddress remoteAddress) throws IOException {
    if (isAuditEnabled()) {
      logAuditEvent(true, getRemoteUser(),
                    remoteAddress,
                    "fsck", src, null, null);
    }
  }
  /**
   * Register NameNodeMXBean
   */
  private void registerMXBean() {
    mxbeanName = MBeans.register("NameNode", "NameNodeInfo", this);
  }

  /**
   * Class representing Namenode information for JMX interfaces
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
  @Metric
  public long getTotalFiles() {
    return getFilesTotal();
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
   * key and value is a map of live node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getLiveNodes() {
    final Map<String, Map<String,Object>> info = 
      new HashMap<String, Map<String,Object>>();
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    blockManager.getDatanodeManager().fetchDatanodes(live, null, true);
    for (DatanodeDescriptor node : live) {
      ImmutableMap.Builder<String, Object> innerinfo =
          ImmutableMap.<String,Object>builder();
      innerinfo
          .put("infoAddr", node.getInfoAddr())
          .put("infoSecureAddr", node.getInfoSecureAddr())
          .put("xferaddr", node.getXferAddr())
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
          .put("volfails", node.getVolumeFailures());
      VolumeFailureSummary volumeFailureSummary = node.getVolumeFailureSummary();
      if (volumeFailureSummary != null) {
        innerinfo
            .put("failedStorageLocations",
                volumeFailureSummary.getFailedStorageLocations())
            .put("lastVolumeFailureDate",
                volumeFailureSummary.getLastVolumeFailureDate())
            .put("estimatedCapacityLostTotal",
                volumeFailureSummary.getEstimatedCapacityLostTotal());
      }
      info.put(node.getHostName() + ":" + node.getXferPort(), innerinfo.build());
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of dead node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getDeadNodes() {
    final Map<String, Map<String, Object>> info = 
      new HashMap<String, Map<String, Object>>();
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    blockManager.getDatanodeManager().fetchDatanodes(null, dead, true);
    for (DatanodeDescriptor node : dead) {
      Map<String, Object> innerinfo = ImmutableMap.<String, Object>builder()
          .put("lastContact", getLastContact(node))
          .put("decommissioned", node.isDecommissioned())
          .put("xferaddr", node.getXferAddr())
          .build();
      info.put(node.getHostName() + ":" + node.getXferPort(), innerinfo);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of decommissioning node attribute keys to its
   * values
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
          .put("underReplicatedBlocks",
              node.decommissioningStatus.getUnderReplicatedBlocks())
          .put("decommissionOnlyReplicas",
              node.decommissioningStatus.getDecommissionOnlyReplicas())
          .put("underReplicateInOpenFiles",
              node.decommissioningStatus.getUnderReplicatedInOpenFiles())
          .build();
      info.put(node.getHostName() + ":" + node.getXferPort(), innerinfo);
    }
    return JSON.toString(info);
  }

  private long getLastContact(DatanodeDescriptor alivenode) {
    return (monotonicNow() - alivenode.getLastUpdateMonotonic())/1000;
  }

  private long getDfsUsed(DatanodeDescriptor alivenode) {
    return alivenode.getDfsUsed();
  }

  @Override  // NameNodeMXBean
  public String getClusterId() {
    return getFSImage().getStorage().getClusterID();
  }
  
  @Override  // NameNodeMXBean
  public String getBlockPoolId() {
    return blockPoolId;
  }
  
  @Override  // NameNodeMXBean
  public String getNameDirStatuses() {
    Map<String, Map<File, StorageDirType>> statusMap =
      new HashMap<String, Map<File, StorageDirType>>();
    
    Map<File, StorageDirType> activeDirs = new HashMap<File, StorageDirType>();
    for (Iterator<StorageDirectory> it
        = getFSImage().getStorage().dirIterator(); it.hasNext();) {
      StorageDirectory st = it.next();
      activeDirs.put(st.getRoot(), st.getStorageDirType());
    }
    statusMap.put("active", activeDirs);
    
    List<Storage.StorageDirectory> removedStorageDirs
        = getFSImage().getStorage().getRemovedStorageDirs();
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
      boolean openForWrite = log.isOpenForWrite();
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
  
  @Override  // NameNodeMXBean
  public String getNNStarted() {
    return getStartTime().toString();
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

  public BlockIdManager getBlockIdManager() {
    return blockIdManager;
  }

  /** @return the FSDirectory. */
  public FSDirectory getFSDirectory() {
    return dir;
  }
  /** Set the FSDirectory. */
  @VisibleForTesting
  public void setFSDirectory(FSDirectory dir) {
    this.dir = dir;
  }
  /** @return the cache manager. */
  public CacheManager getCacheManager() {
    return cacheManager;
  }

  @Override  // NameNodeMXBean
  public String getCorruptFiles() {
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
    } catch (IOException e) {
      LOG.warn("Get corrupt file blocks returned error: " + e.getMessage());
    }
    return JSON.toString(list);
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
  
  @Override
  public boolean isGenStampInFuture(Block block) {
    return blockIdManager.isGenStampInFuture(block);
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
  public SafeModeInfo getSafeModeInfoForTests() {
    return safeMode;
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
    boolean success = false;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot allow snapshot for " + path);
      checkSuperuserPrivilege();
      FSDirSnapshotOp.allowSnapshot(dir, snapshotManager, path);
      success = true;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(success, "allowSnapshot", path, null, null);
  }
  
  /** Disallow snapshot on a directory. */
  void disallowSnapshot(String path) throws IOException {
    checkOperation(OperationCategory.WRITE);
    boolean success = false;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot disallow snapshot for " + path);
      checkSuperuserPrivilege();
      FSDirSnapshotOp.disallowSnapshot(dir, snapshotManager, path);
      success = true;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(success, "disallowSnapshot", path, null, null);
  }
  
  /**
   * Create a snapshot
   * @param snapshotRoot The directory path where the snapshot is taken
   * @param snapshotName The name of the snapshot
   */
  String createSnapshot(String snapshotRoot, String snapshotName,
                        boolean logRetryCache) throws IOException {
    String snapshotPath = null;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot create snapshot for " + snapshotRoot);
      snapshotPath = FSDirSnapshotOp.createSnapshot(dir,
          snapshotManager, snapshotRoot, snapshotName, logRetryCache);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(snapshotPath != null, "createSnapshot", snapshotRoot,
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
    boolean success = false;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot rename snapshot for " + path);
      FSDirSnapshotOp.renameSnapshot(dir, snapshotManager, path,
          snapshotOldName, snapshotNewName, logRetryCache);
      success = true;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    String oldSnapshotRoot = Snapshot.getSnapshotPath(path, snapshotOldName);
    String newSnapshotRoot = Snapshot.getSnapshotPath(path, snapshotNewName);
    logAuditEvent(success, "renameSnapshot", oldSnapshotRoot,
        newSnapshotRoot, null);
  }
  
  /**
   * Get the list of snapshottable directories that are owned 
   * by the current user. Return all the snapshottable directories if the 
   * current user is a super user.
   * @return The list of all the current snapshottable directories
   * @throws IOException
   */
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    SnapshottableDirectoryStatus[] status = null;
    checkOperation(OperationCategory.READ);
    boolean success = false;
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      status = FSDirSnapshotOp.getSnapshottableDirListing(dir, snapshotManager);
      success = true;
    } finally {
      readUnlock();
    }
    logAuditEvent(success, "listSnapshottableDirectory", null, null, null);
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
    SnapshotDiffReport diffs = null;
    checkOperation(OperationCategory.READ);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      diffs = FSDirSnapshotOp.getSnapshotDiffReport(dir, snapshotManager,
          path, fromSnapshot, toSnapshot);
    } finally {
      readUnlock();
    }

    logAuditEvent(diffs != null, "computeSnapshotDiff", null, null, null);
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
    boolean success = false;
    writeLock();
    BlocksMapUpdateInfo blocksToBeDeleted = null;
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot delete snapshot for " + snapshotRoot);

      blocksToBeDeleted = FSDirSnapshotOp.deleteSnapshot(dir, snapshotManager,
          snapshotRoot, snapshotName, logRetryCache);
      success = true;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();

    // Breaking the pattern as removing blocks have to happen outside of the
    // global lock
    if (blocksToBeDeleted != null) {
      removeBlocks(blocksToBeDeleted);
    }

    String rootPath = Snapshot.getSnapshotPath(snapshotRoot, snapshotName);
    logAuditEvent(success, "deleteSnapshot", rootPath, null, null);
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
    checkSuperuserPrivilege();
    checkOperation(OperationCategory.READ);
    readLock();
    try {
      if (!isRollingUpgrade()) {
        return null;
      }
      Preconditions.checkNotNull(rollingUpgradeInfo);
      boolean hasRollbackImage = this.getFSImage().hasRollbackFSImage();
      rollingUpgradeInfo.setCreatedRollbackImages(hasRollbackImage);
      return rollingUpgradeInfo;
    } finally {
      readUnlock();
    }
  }

  RollingUpgradeInfo startRollingUpgrade() throws IOException {
    checkSuperuserPrivilege();
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
        getFSImage().rollEditLog();
      }
    } finally {
      writeUnlock();
    }

    getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(true, "startRollingUpgrade", null, null, null);
    }
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
    rollingUpgradeInfo = new RollingUpgradeInfo(blockPoolId,
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
      readUnlock();
    }
    return new RollingUpgradeInfo.Bean(upgradeInfo);
  }

  /** Is rolling upgrade in progress? */
  public boolean isRollingUpgrade() {
    return rollingUpgradeInfo != null && !rollingUpgradeInfo.isFinalized();
  }

  void checkRollingUpgrade(String action) throws RollingUpgradeException {
    if (isRollingUpgrade()) {
      throw new RollingUpgradeException("Failed to " + action
          + " since a rolling upgrade is already in progress."
          + " Existing rolling upgrade info:\n" + rollingUpgradeInfo);
    }
  }

  RollingUpgradeInfo finalizeRollingUpgrade() throws IOException {
    checkSuperuserPrivilege();
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
        getFSImage().rollEditLog();
      }
      getFSImage().updateStorageVersion();
      getFSImage().renameCheckpoint(NameNodeFile.IMAGE_ROLLBACK,
          NameNodeFile.IMAGE);
    } finally {
      writeUnlock();
    }

    if (!haEnabled) {
      // Sync not needed for ha since the edit was rolled after logging.
      getEditLog().logSync();
    }

    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(true, "finalizeRollingUpgrade", null, null, null);
    }
    return rollingUpgradeInfo;
  }

  void finalizeRollingUpgradeInternal(long finalizeTime) {
    // Set the finalize time
    rollingUpgradeInfo.finalize(finalizeTime);
  }

  long addCacheDirective(CacheDirectiveInfo directive,
                         EnumSet<CacheFlag> flags, boolean logRetryCache)
      throws IOException {
    CacheDirectiveInfo effectiveDirective = null;
    if (!flags.contains(CacheFlag.FORCE)) {
      cacheManager.waitForRescanIfNeeded();
    }
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot add cache directive", safeMode);
      }
      effectiveDirective = FSNDNCacheOp.addCacheDirective(this, cacheManager,
          directive, flags, logRetryCache);
    } finally {
      writeUnlock();
      boolean success = effectiveDirective != null;
      if (success) {
        getEditLog().logSync();
      }

      String effectiveDirectiveStr = effectiveDirective != null ?
          effectiveDirective.toString() : null;
      logAuditEvent(success, "addCacheDirective", effectiveDirectiveStr,
          null, null);
    }
    return effectiveDirective != null ? effectiveDirective.getId() : 0;
  }

  void modifyCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags, boolean logRetryCache) throws IOException {
    boolean success = false;
    if (!flags.contains(CacheFlag.FORCE)) {
      cacheManager.waitForRescanIfNeeded();
    }
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot add cache directive", safeMode);
      }
      FSNDNCacheOp.modifyCacheDirective(this, cacheManager, directive, flags,
          logRetryCache);
      success = true;
    } finally {
      writeUnlock();
      if (success) {
        getEditLog().logSync();
      }
      String idStr = "{id: " + directive.getId().toString() + "}";
      logAuditEvent(success, "modifyCacheDirective", idStr,
          directive.toString(), null);
    }
  }

  void removeCacheDirective(long id, boolean logRetryCache) throws IOException {
    boolean success = false;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot remove cache directives", safeMode);
      }
      FSNDNCacheOp.removeCacheDirective(this, cacheManager, id, logRetryCache);
      success = true;
    } finally {
      writeUnlock();
      String idStr = "{id: " + Long.toString(id) + "}";
      logAuditEvent(success, "removeCacheDirective", idStr, null,
          null);
    }
    getEditLog().logSync();
  }

  BatchedListEntries<CacheDirectiveEntry> listCacheDirectives(
      long startId, CacheDirectiveInfo filter) throws IOException {
    checkOperation(OperationCategory.READ);
    BatchedListEntries<CacheDirectiveEntry> results;
    cacheManager.waitForRescanIfNeeded();
    readLock();
    boolean success = false;
    try {
      checkOperation(OperationCategory.READ);
      results = FSNDNCacheOp.listCacheDirectives(this, cacheManager, startId,
          filter);
      success = true;
    } finally {
      readUnlock();
      logAuditEvent(success, "listCacheDirectives", filter.toString(), null,
          null);
    }
    return results;
  }

  void addCachePool(CachePoolInfo req, boolean logRetryCache)
      throws IOException {
    writeLock();
    boolean success = false;
    String poolInfoStr = null;
    try {
      checkOperation(OperationCategory.WRITE);
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot add cache pool " + req.getPoolName(), safeMode);
      }
      CachePoolInfo info = FSNDNCacheOp.addCachePool(this, cacheManager, req,
          logRetryCache);
      poolInfoStr = info.toString();
      success = true;
    } finally {
      writeUnlock();
      logAuditEvent(success, "addCachePool", poolInfoStr, null, null);
    }
    
    getEditLog().logSync();
  }

  void modifyCachePool(CachePoolInfo req, boolean logRetryCache)
      throws IOException {
    writeLock();
    boolean success = false;
    try {
      checkOperation(OperationCategory.WRITE);
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot modify cache pool " + req.getPoolName(), safeMode);
      }
      FSNDNCacheOp.modifyCachePool(this, cacheManager, req, logRetryCache);
      success = true;
    } finally {
      writeUnlock();
      String poolNameStr = "{poolName: " +
          (req == null ? null : req.getPoolName()) + "}";
      logAuditEvent(success, "modifyCachePool", poolNameStr,
                    req == null ? null : req.toString(), null);
    }

    getEditLog().logSync();
  }

  void removeCachePool(String cachePoolName, boolean logRetryCache)
      throws IOException {
    writeLock();
    boolean success = false;
    try {
      checkOperation(OperationCategory.WRITE);
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot remove cache pool " + cachePoolName, safeMode);
      }
      FSNDNCacheOp.removeCachePool(this, cacheManager, cachePoolName,
          logRetryCache);
      success = true;
    } finally {
      writeUnlock();
      String poolNameStr = "{poolName: " + cachePoolName + "}";
      logAuditEvent(success, "removeCachePool", poolNameStr, null, null);
    }
    
    getEditLog().logSync();
  }

  BatchedListEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    BatchedListEntries<CachePoolEntry> results;
    checkOperation(OperationCategory.READ);
    boolean success = false;
    cacheManager.waitForRescanIfNeeded();
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      results = FSNDNCacheOp.listCachePools(this, cacheManager, prevKey);
      success = true;
    } finally {
      readUnlock();
      logAuditEvent(success, "listCachePools", null, null, null);
    }
    return results;
  }

  void modifyAclEntries(final String src, List<AclEntry> aclSpec)
      throws IOException {
    HdfsFileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot modify ACL entries on " + src);
      auditStat = FSDirAclOp.modifyAclEntries(dir, src, aclSpec);
    } catch (AccessControlException e) {
      logAuditEvent(false, "modifyAclEntries", src);
      throw e;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(true, "modifyAclEntries", src, null, auditStat);
  }

  void removeAclEntries(final String src, List<AclEntry> aclSpec)
      throws IOException {
    checkOperation(OperationCategory.WRITE);
    HdfsFileStatus auditStat = null;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot remove ACL entries on " + src);
      auditStat = FSDirAclOp.removeAclEntries(dir, src, aclSpec);
    } catch (AccessControlException e) {
      logAuditEvent(false, "removeAclEntries", src);
      throw e;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(true, "removeAclEntries", src, null, auditStat);
  }

  void removeDefaultAcl(final String src) throws IOException {
    HdfsFileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot remove default ACL entries on " + src);
      auditStat = FSDirAclOp.removeDefaultAcl(dir, src);
    } catch (AccessControlException e) {
      logAuditEvent(false, "removeDefaultAcl", src);
      throw e;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(true, "removeDefaultAcl", src, null, auditStat);
  }

  void removeAcl(final String src) throws IOException {
    HdfsFileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot remove ACL on " + src);
      auditStat = FSDirAclOp.removeAcl(dir, src);
    } catch (AccessControlException e) {
      logAuditEvent(false, "removeAcl", src);
      throw e;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(true, "removeAcl", src, null, auditStat);
  }

  void setAcl(final String src, List<AclEntry> aclSpec) throws IOException {
    HdfsFileStatus auditStat = null;
    checkOperation(OperationCategory.WRITE);
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot set ACL on " + src);
      auditStat = FSDirAclOp.setAcl(dir, src, aclSpec);
    } catch (AccessControlException e) {
      logAuditEvent(false, "setAcl", src);
      throw e;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(true, "setAcl", src, null, auditStat);
  }

  AclStatus getAclStatus(String src) throws IOException {
    checkOperation(OperationCategory.READ);
    boolean success = false;
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      final AclStatus ret = FSDirAclOp.getAclStatus(dir, src);
      success = true;
      return ret;
    } finally {
      readUnlock();
      logAuditEvent(success, "getAclStatus", src);
    }
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
                            boolean logRetryCache)
    throws IOException, UnresolvedLinkException,
      SafeModeException, AccessControlException {
    try {
      if (provider == null) {
        throw new IOException(
            "Can't create an encryption zone for " + src +
            " since no key provider is available.");
      }
      if (keyName == null || keyName.isEmpty()) {
        throw new IOException("Must specify a key name when creating an " +
            "encryption zone");
      }
      KeyProvider.Metadata metadata = provider.getMetadata(keyName);
      if (metadata == null) {
        /*
         * It would be nice if we threw something more specific than
         * IOException when the key is not found, but the KeyProvider API
         * doesn't provide for that. If that API is ever changed to throw
         * something more specific (e.g. UnknownKeyException) then we can
         * update this to match it, or better yet, just rethrow the
         * KeyProvider's exception.
         */
        throw new IOException("Key " + keyName + " doesn't exist.");
      }
      // If the provider supports pool for EDEKs, this will fill in the pool
      generateEncryptedDataEncryptionKey(keyName);
      createEncryptionZoneInt(src, metadata.getCipher(),
          keyName, logRetryCache);
    } catch (AccessControlException e) {
      logAuditEvent(false, "createEncryptionZone", src);
      throw e;
    }
  }

  private void createEncryptionZoneInt(final String srcArg, String cipher,
      String keyName, final boolean logRetryCache) throws IOException {
    String src = srcArg;
    HdfsFileStatus resultingStat = null;
    checkSuperuserPrivilege();
    final byte[][] pathComponents =
      FSDirectory.getPathComponentsForReservedPath(src);
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      checkSuperuserPrivilege();
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot create encryption zone on " + src);
      src = dir.resolvePath(pc, src, pathComponents);

      final CipherSuite suite = CipherSuite.convert(cipher);
      // For now this is hardcoded, as we only support one method.
      final CryptoProtocolVersion version =
          CryptoProtocolVersion.ENCRYPTION_ZONES;
      final XAttr ezXAttr = dir.createEncryptionZone(src, suite,
          version, keyName);
      List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
      xAttrs.add(ezXAttr);
      getEditLog().logSetXAttrs(src, xAttrs, logRetryCache);
      final INodesInPath iip = dir.getINodesInPath4Write(src, false);
      resultingStat = dir.getAuditFileInfo(iip);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(true, "createEncryptionZone", srcArg, null, resultingStat);
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
    String src = srcArg;
    HdfsFileStatus resultingStat = null;
    final byte[][] pathComponents =
        FSDirectory.getPathComponentsForReservedPath(src);
    boolean success = false;
    final FSPermissionChecker pc = getPermissionChecker();
    checkOperation(OperationCategory.READ);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      src = dir.resolvePath(pc, src, pathComponents);
      final INodesInPath iip = dir.getINodesInPath(src, true);
      if (isPermissionEnabled) {
        dir.checkPathAccess(pc, iip, FsAction.READ);
      }
      final EncryptionZone ret = dir.getEZForPath(iip);
      resultingStat = dir.getAuditFileInfo(iip);
      success = true;
      return ret;
    } finally {
      readUnlock();
      logAuditEvent(success, "getEZForPath", srcArg, null, resultingStat);
    }
  }

  BatchedListEntries<EncryptionZone> listEncryptionZones(long prevId)
      throws IOException {
    boolean success = false;
    checkSuperuserPrivilege();
    checkOperation(OperationCategory.READ);
    readLock();
    try {
      checkSuperuserPrivilege();
      checkOperation(OperationCategory.READ);
      final BatchedListEntries<EncryptionZone> ret =
          dir.listEncryptionZones(prevId);
      success = true;
      return ret;
    } finally {
      readUnlock();
      logAuditEvent(success, "listEncryptionZones", null);
    }
  }

  void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag,
                boolean logRetryCache)
      throws IOException {
    HdfsFileStatus auditStat = null;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot set XAttr on " + src);
      auditStat = FSDirXAttrOp.setXAttr(dir, src, xAttr, flag, logRetryCache);
    } catch (AccessControlException e) {
      logAuditEvent(false, "setXAttr", src);
      throw e;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(true, "setXAttr", src, null, auditStat);
  }

  List<XAttr> getXAttrs(final String src, List<XAttr> xAttrs)
      throws IOException {
    checkOperation(OperationCategory.READ);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      return FSDirXAttrOp.getXAttrs(dir, src, xAttrs);
    } catch (AccessControlException e) {
      logAuditEvent(false, "getXAttrs", src);
      throw e;
    } finally {
      readUnlock();
    }
  }

  List<XAttr> listXAttrs(String src) throws IOException {
    checkOperation(OperationCategory.READ);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      return FSDirXAttrOp.listXAttrs(dir, src);
    } catch (AccessControlException e) {
      logAuditEvent(false, "listXAttrs", src);
      throw e;
    } finally {
      readUnlock();
    }
  }

  void removeXAttr(String src, XAttr xAttr, boolean logRetryCache)
      throws IOException {
    HdfsFileStatus auditStat = null;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot remove XAttr entry on " + src);
      auditStat = FSDirXAttrOp.removeXAttr(dir, src, xAttr, logRetryCache);
    } catch (AccessControlException e) {
      logAuditEvent(false, "removeXAttr", src);
      throw e;
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    logAuditEvent(true, "removeXAttr", src, null, auditStat);
  }

  void checkAccess(String src, FsAction mode) throws IOException {
    checkOperation(OperationCategory.READ);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      src = FSDirectory.resolvePath(src, pathComponents, dir);
      final INodesInPath iip = dir.getINodesInPath(src, true);
      INode inode = iip.getLastINode();
      if (inode == null) {
        throw new FileNotFoundException("Path not found");
      }
      if (isPermissionEnabled) {
        FSPermissionChecker pc = getPermissionChecker();
        dir.checkPathAccess(pc, iip, mode);
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, "checkAccess", src);
      throw e;
    } finally {
      readUnlock();
    }
  }

  /**
   * Default AuditLogger implementation; used when no access logger is
   * defined in the config file. It can also be explicitly listed in the
   * config file.
   */
  private static class DefaultAuditLogger extends HdfsAuditLogger {

    private boolean logTokenTrackingId;

    @Override
    public void initialize(Configuration conf) {
      logTokenTrackingId = conf.getBoolean(
          DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY,
          DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT);
    }

    @Override
    public void logAuditEvent(boolean succeeded, String userName,
        InetAddress addr, String cmd, String src, String dst,
        FileStatus status, UserGroupInformation ugi,
        DelegationTokenSecretManager dtSecretManager) {
      if (auditLog.isInfoEnabled()) {
        final StringBuilder sb = auditBuffer.get();
        sb.setLength(0);
        sb.append("allowed=").append(succeeded).append("\t");
        sb.append("ugi=").append(userName).append("\t");
        sb.append("ip=").append(addr).append("\t");
        sb.append("cmd=").append(cmd).append("\t");
        sb.append("src=").append(src).append("\t");
        sb.append("dst=").append(dst).append("\t");
        if (null == status) {
          sb.append("perm=null");
        } else {
          sb.append("perm=");
          sb.append(status.getOwner()).append(":");
          sb.append(status.getGroup()).append(":");
          sb.append(status.getPermission());
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
        sb.append("\t").append("proto=");
        sb.append(NamenodeWebHdfsMethods.isWebHdfsInvocation() ? "webhdfs" : "rpc");
        logAuditMessage(sb.toString());
      }
    }

    public void logAuditMessage(String message) {
      auditLog.info(message);
    }
  }

  private static void enableAsyncAuditLog() {
    if (!(auditLog instanceof Log4JLogger)) {
      LOG.warn("Log4j is required to enable async auditlog");
      return;
    }
    Logger logger = ((Log4JLogger)auditLog).getLogger();
    @SuppressWarnings("unchecked")
    List<Appender> appenders = Collections.list(logger.getAllAppenders());
    // failsafe against trying to async it more than once
    if (!appenders.isEmpty() && !(appenders.get(0) instanceof AsyncAppender)) {
      AsyncAppender asyncAppender = new AsyncAppender();
      // change logger to have an async appender containing all the
      // previously configured appenders
      for (Appender appender : appenders) {
        logger.removeAppender(appender);
        asyncAppender.addAppender(appender);
      }
      logger.addAppender(asyncAppender);        
    }
  }

}

