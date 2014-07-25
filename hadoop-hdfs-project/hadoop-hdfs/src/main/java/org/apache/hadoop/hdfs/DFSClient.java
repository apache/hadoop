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

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_WRITES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHE_READAHEAD;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_RETRY_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CONTEXT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CONTEXT_DEFAULT;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.SocketFactory;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveIterator;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolIterator;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.LossyRetryInvocationHandler;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DataChecksum.Type;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;

/********************************************************
 * DFSClient can connect to a Hadoop Filesystem and 
 * perform basic file tasks.  It uses the ClientProtocol
 * to communicate with a NameNode daemon, and connects 
 * directly to DataNodes to read/write block data.
 *
 * Hadoop DFS users should obtain an instance of 
 * DistributedFileSystem, which uses DFSClient to handle
 * filesystem tasks.
 *
 ********************************************************/
@InterfaceAudience.Private
public class DFSClient implements java.io.Closeable, RemotePeerFactory,
    DataEncryptionKeyFactory {
  public static final Log LOG = LogFactory.getLog(DFSClient.class);
  public static final long SERVER_DEFAULTS_VALIDITY_PERIOD = 60 * 60 * 1000L; // 1 hour
  static final int TCP_WINDOW_SIZE = 128 * 1024; // 128 KB

  private final Configuration conf;
  private final Conf dfsClientConf;
  final ClientProtocol namenode;
  /* The service used for delegation tokens */
  private Text dtService;

  final UserGroupInformation ugi;
  volatile boolean clientRunning = true;
  volatile long lastLeaseRenewal;
  private volatile FsServerDefaults serverDefaults;
  private volatile long serverDefaultsLastUpdate;
  final String clientName;
  final SocketFactory socketFactory;
  final ReplaceDatanodeOnFailure dtpReplaceDatanodeOnFailure;
  final FileSystem.Statistics stats;
  private final String authority;
  private final Random r = new Random();
  private SocketAddress[] localInterfaceAddrs;
  private DataEncryptionKey encryptionKey;
  final SaslDataTransferClient saslClient;
  private final CachingStrategy defaultReadCachingStrategy;
  private final CachingStrategy defaultWriteCachingStrategy;
  private final ClientContext clientContext;
  private volatile long hedgedReadThresholdMillis;
  private static final DFSHedgedReadMetrics HEDGED_READ_METRIC =
      new DFSHedgedReadMetrics();
  private static ThreadPoolExecutor HEDGED_READ_THREAD_POOL;
  
  /**
   * DFSClient configuration 
   */
  public static class Conf {
    final int hdfsTimeout;    // timeout value for a DFS operation.

    final int maxFailoverAttempts;
    final int maxRetryAttempts;
    final int failoverSleepBaseMillis;
    final int failoverSleepMaxMillis;
    final int maxBlockAcquireFailures;
    final int confTime;
    final int ioBufferSize;
    final ChecksumOpt defaultChecksumOpt;
    final int writePacketSize;
    final int socketTimeout;
    final int socketCacheCapacity;
    final long socketCacheExpiry;
    final long excludedNodesCacheExpiry;
    /** Wait time window (in msec) if BlockMissingException is caught */
    final int timeWindow;
    final int nCachedConnRetry;
    final int nBlockWriteRetry;
    final int nBlockWriteLocateFollowingRetry;
    final long defaultBlockSize;
    final long prefetchSize;
    final short defaultReplication;
    final String taskId;
    final FsPermission uMask;
    final boolean connectToDnViaHostname;
    final boolean getHdfsBlocksMetadataEnabled;
    final int getFileBlockStorageLocationsNumThreads;
    final int getFileBlockStorageLocationsTimeoutMs;
    final int retryTimesForGetLastBlockLength;
    final int retryIntervalForGetLastBlockLength;
    final long datanodeRestartTimeout;
    final long dfsclientSlowIoWarningThresholdMs;

    final boolean useLegacyBlockReader;
    final boolean useLegacyBlockReaderLocal;
    final String domainSocketPath;
    final boolean skipShortCircuitChecksums;
    final int shortCircuitBufferSize;
    final boolean shortCircuitLocalReads;
    final boolean domainSocketDataTraffic;
    final int shortCircuitStreamsCacheSize;
    final long shortCircuitStreamsCacheExpiryMs; 
    final int shortCircuitSharedMemoryWatcherInterruptCheckMs;
    
    final boolean shortCircuitMmapEnabled;
    final int shortCircuitMmapCacheSize;
    final long shortCircuitMmapCacheExpiryMs;
    final long shortCircuitMmapCacheRetryTimeout;
    final long shortCircuitCacheStaleThresholdMs;

    public Conf(Configuration conf) {
      // The hdfsTimeout is currently the same as the ipc timeout 
      hdfsTimeout = Client.getTimeout(conf);
      maxFailoverAttempts = conf.getInt(
          DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
          DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);
      maxRetryAttempts = conf.getInt(
          DFS_CLIENT_RETRY_MAX_ATTEMPTS_KEY,
          DFS_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT);
      failoverSleepBaseMillis = conf.getInt(
          DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY,
          DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT);
      failoverSleepMaxMillis = conf.getInt(
          DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY,
          DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT);

      maxBlockAcquireFailures = conf.getInt(
          DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY,
          DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT);
      confTime = conf.getInt(DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
          HdfsServerConstants.WRITE_TIMEOUT);
      ioBufferSize = conf.getInt(
          CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
          CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
      defaultChecksumOpt = getChecksumOptFromConf(conf);
      socketTimeout = conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY,
          HdfsServerConstants.READ_TIMEOUT);
      /** dfs.write.packet.size is an internal config variable */
      writePacketSize = conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY,
          DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
      defaultBlockSize = conf.getLongBytes(DFS_BLOCK_SIZE_KEY,
          DFS_BLOCK_SIZE_DEFAULT);
      defaultReplication = (short) conf.getInt(
          DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT);
      taskId = conf.get("mapreduce.task.attempt.id", "NONMAPREDUCE");
      socketCacheCapacity = conf.getInt(DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY,
          DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT);
      socketCacheExpiry = conf.getLong(DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY,
          DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT);
      excludedNodesCacheExpiry = conf.getLong(
          DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL,
          DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT);
      prefetchSize = conf.getLong(DFS_CLIENT_READ_PREFETCH_SIZE_KEY,
          10 * defaultBlockSize);
      timeWindow = conf.getInt(DFS_CLIENT_RETRY_WINDOW_BASE, 3000);
      nCachedConnRetry = conf.getInt(DFS_CLIENT_CACHED_CONN_RETRY_KEY,
          DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT);
      nBlockWriteRetry = conf.getInt(DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY,
          DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT);
      nBlockWriteLocateFollowingRetry = conf.getInt(
          DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY,
          DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT);
      uMask = FsPermission.getUMask(conf);
      connectToDnViaHostname = conf.getBoolean(DFS_CLIENT_USE_DN_HOSTNAME,
          DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
      getHdfsBlocksMetadataEnabled = conf.getBoolean(
          DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED, 
          DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT);
      getFileBlockStorageLocationsNumThreads = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_NUM_THREADS,
          DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_NUM_THREADS_DEFAULT);
      getFileBlockStorageLocationsTimeoutMs = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS,
          DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS_DEFAULT);
      retryTimesForGetLastBlockLength = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_RETRY_TIMES_GET_LAST_BLOCK_LENGTH,
          DFSConfigKeys.DFS_CLIENT_RETRY_TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT);
      retryIntervalForGetLastBlockLength = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_RETRY_INTERVAL_GET_LAST_BLOCK_LENGTH,
        DFSConfigKeys.DFS_CLIENT_RETRY_INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT);

      useLegacyBlockReader = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER,
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER_DEFAULT);
      useLegacyBlockReaderLocal = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT);
      shortCircuitLocalReads = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT);
      domainSocketDataTraffic = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC,
          DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT);
      domainSocketPath = conf.getTrimmed(
          DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
          DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT);

      if (BlockReaderLocal.LOG.isDebugEnabled()) {
        BlockReaderLocal.LOG.debug(
            DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL
            + " = " + useLegacyBlockReaderLocal);
        BlockReaderLocal.LOG.debug(
            DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY
            + " = " + shortCircuitLocalReads);
        BlockReaderLocal.LOG.debug(
            DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC
            + " = " + domainSocketDataTraffic);
        BlockReaderLocal.LOG.debug(
            DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY
            + " = " + domainSocketPath);
      }

      skipShortCircuitChecksums = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_DEFAULT);
      shortCircuitBufferSize = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_DEFAULT);
      shortCircuitStreamsCacheSize = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_DEFAULT);
      shortCircuitStreamsCacheExpiryMs = conf.getLong(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_DEFAULT);
      shortCircuitMmapEnabled = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_MMAP_ENABLED,
          DFSConfigKeys.DFS_CLIENT_MMAP_ENABLED_DEFAULT);
      shortCircuitMmapCacheSize = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_SIZE,
          DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_SIZE_DEFAULT);
      shortCircuitMmapCacheExpiryMs = conf.getLong(
          DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS,
          DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS_DEFAULT);
      shortCircuitMmapCacheRetryTimeout = conf.getLong(
          DFSConfigKeys.DFS_CLIENT_MMAP_RETRY_TIMEOUT_MS,
          DFSConfigKeys.DFS_CLIENT_MMAP_RETRY_TIMEOUT_MS_DEFAULT);
      shortCircuitCacheStaleThresholdMs = conf.getLong(
          DFSConfigKeys.DFS_CLIENT_SHORT_CIRCUIT_REPLICA_STALE_THRESHOLD_MS,
          DFSConfigKeys.DFS_CLIENT_SHORT_CIRCUIT_REPLICA_STALE_THRESHOLD_MS_DEFAULT);
      shortCircuitSharedMemoryWatcherInterruptCheckMs = conf.getInt(
          DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS,
          DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT);

      datanodeRestartTimeout = conf.getLong(
          DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY,
          DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT) * 1000;
      dfsclientSlowIoWarningThresholdMs = conf.getLong(
          DFSConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY,
          DFSConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT);
    }

    public boolean isUseLegacyBlockReaderLocal() {
      return useLegacyBlockReaderLocal;
    }

    public String getDomainSocketPath() {
      return domainSocketPath;
    }

    public boolean isShortCircuitLocalReads() {
      return shortCircuitLocalReads;
    }

    public boolean isDomainSocketDataTraffic() {
      return domainSocketDataTraffic;
    }

    private DataChecksum.Type getChecksumType(Configuration conf) {
      final String checksum = conf.get(
          DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY,
          DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT);
      try {
        return DataChecksum.Type.valueOf(checksum);
      } catch(IllegalArgumentException iae) {
        LOG.warn("Bad checksum type: " + checksum + ". Using default "
            + DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT);
        return DataChecksum.Type.valueOf(
            DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT); 
      }
    }

    // Construct a checksum option from conf
    private ChecksumOpt getChecksumOptFromConf(Configuration conf) {
      DataChecksum.Type type = getChecksumType(conf);
      int bytesPerChecksum = conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY,
          DFS_BYTES_PER_CHECKSUM_DEFAULT);
      return new ChecksumOpt(type, bytesPerChecksum);
    }

    // create a DataChecksum with the default option.
    private DataChecksum createChecksum() throws IOException {
      return createChecksum(null);
    }

    private DataChecksum createChecksum(ChecksumOpt userOpt) 
        throws IOException {
      // Fill in any missing field with the default.
      ChecksumOpt myOpt = ChecksumOpt.processChecksumOpt(
          defaultChecksumOpt, userOpt);
      DataChecksum dataChecksum = DataChecksum.newDataChecksum(
          myOpt.getChecksumType(),
          myOpt.getBytesPerChecksum());
      if (dataChecksum == null) {
        throw new IOException("Invalid checksum type specified: "
            + myOpt.getChecksumType().name());
      }
      return dataChecksum;
    }
  }
 
  public Conf getConf() {
    return dfsClientConf;
  }

  Configuration getConfiguration() {
    return conf;
  }

  /**
   * A map from file names to {@link DFSOutputStream} objects
   * that are currently being written by this client.
   * Note that a file can only be written by a single client.
   */
  private final Map<Long, DFSOutputStream> filesBeingWritten
      = new HashMap<Long, DFSOutputStream>();

  /**
   * Same as this(NameNode.getAddress(conf), conf);
   * @see #DFSClient(InetSocketAddress, Configuration)
   * @deprecated Deprecated at 0.21
   */
  @Deprecated
  public DFSClient(Configuration conf) throws IOException {
    this(NameNode.getAddress(conf), conf);
  }
  
  public DFSClient(InetSocketAddress address, Configuration conf) throws IOException {
    this(NameNode.getUri(address), conf);
  }

  /**
   * Same as this(nameNodeUri, conf, null);
   * @see #DFSClient(URI, Configuration, FileSystem.Statistics)
   */
  public DFSClient(URI nameNodeUri, Configuration conf
      ) throws IOException {
    this(nameNodeUri, conf, null);
  }

  /**
   * Same as this(nameNodeUri, null, conf, stats);
   * @see #DFSClient(URI, ClientProtocol, Configuration, FileSystem.Statistics) 
   */
  public DFSClient(URI nameNodeUri, Configuration conf,
                   FileSystem.Statistics stats)
    throws IOException {
    this(nameNodeUri, null, conf, stats);
  }
  
  /** 
   * Create a new DFSClient connected to the given nameNodeUri or rpcNamenode.
   * If HA is enabled and a positive value is set for 
   * {@link DFSConfigKeys#DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY} in the
   * configuration, the DFSClient will use {@link LossyRetryInvocationHandler}
   * as its RetryInvocationHandler. Otherwise one of nameNodeUri or rpcNamenode 
   * must be null.
   */
  @VisibleForTesting
  public DFSClient(URI nameNodeUri, ClientProtocol rpcNamenode,
      Configuration conf, FileSystem.Statistics stats)
    throws IOException {
    // Copy only the required DFSClient configuration
    this.dfsClientConf = new Conf(conf);
    if (this.dfsClientConf.useLegacyBlockReaderLocal) {
      LOG.debug("Using legacy short-circuit local reads.");
    }
    this.conf = conf;
    this.stats = stats;
    this.socketFactory = NetUtils.getSocketFactory(conf, ClientProtocol.class);
    this.dtpReplaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);

    this.ugi = UserGroupInformation.getCurrentUser();
    
    this.authority = nameNodeUri == null? "null": nameNodeUri.getAuthority();
    this.clientName = "DFSClient_" + dfsClientConf.taskId + "_" + 
        DFSUtil.getRandom().nextInt()  + "_" + Thread.currentThread().getId();
    
    int numResponseToDrop = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY,
        DFSConfigKeys.DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_DEFAULT);
    NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo = null;
    if (numResponseToDrop > 0) {
      // This case is used for testing.
      LOG.warn(DFSConfigKeys.DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY
          + " is set to " + numResponseToDrop
          + ", this hacked client will proactively drop responses");
      proxyInfo = NameNodeProxies.createProxyWithLossyRetryHandler(conf,
          nameNodeUri, ClientProtocol.class, numResponseToDrop);
    }
    
    if (proxyInfo != null) {
      this.dtService = proxyInfo.getDelegationTokenService();
      this.namenode = proxyInfo.getProxy();
    } else if (rpcNamenode != null) {
      // This case is used for testing.
      Preconditions.checkArgument(nameNodeUri == null);
      this.namenode = rpcNamenode;
      dtService = null;
    } else {
      Preconditions.checkArgument(nameNodeUri != null,
          "null URI");
      proxyInfo = NameNodeProxies.createProxy(conf, nameNodeUri,
          ClientProtocol.class);
      this.dtService = proxyInfo.getDelegationTokenService();
      this.namenode = proxyInfo.getProxy();
    }

    String localInterfaces[] =
      conf.getTrimmedStrings(DFSConfigKeys.DFS_CLIENT_LOCAL_INTERFACES);
    localInterfaceAddrs = getLocalInterfaceAddrs(localInterfaces);
    if (LOG.isDebugEnabled() && 0 != localInterfaces.length) {
      LOG.debug("Using local interfaces [" +
      Joiner.on(',').join(localInterfaces)+ "] with addresses [" +
      Joiner.on(',').join(localInterfaceAddrs) + "]");
    }
    
    Boolean readDropBehind = (conf.get(DFS_CLIENT_CACHE_DROP_BEHIND_READS) == null) ?
        null : conf.getBoolean(DFS_CLIENT_CACHE_DROP_BEHIND_READS, false);
    Long readahead = (conf.get(DFS_CLIENT_CACHE_READAHEAD) == null) ?
        null : conf.getLong(DFS_CLIENT_CACHE_READAHEAD, 0);
    Boolean writeDropBehind = (conf.get(DFS_CLIENT_CACHE_DROP_BEHIND_WRITES) == null) ?
        null : conf.getBoolean(DFS_CLIENT_CACHE_DROP_BEHIND_WRITES, false);
    this.defaultReadCachingStrategy =
        new CachingStrategy(readDropBehind, readahead);
    this.defaultWriteCachingStrategy =
        new CachingStrategy(writeDropBehind, readahead);
    this.clientContext = ClientContext.get(
        conf.get(DFS_CLIENT_CONTEXT, DFS_CLIENT_CONTEXT_DEFAULT),
        dfsClientConf);
    this.hedgedReadThresholdMillis = conf.getLong(
        DFSConfigKeys.DFS_DFSCLIENT_HEDGED_READ_THRESHOLD_MILLIS,
        DFSConfigKeys.DEFAULT_DFSCLIENT_HEDGED_READ_THRESHOLD_MILLIS);
    int numThreads = conf.getInt(
        DFSConfigKeys.DFS_DFSCLIENT_HEDGED_READ_THREADPOOL_SIZE,
        DFSConfigKeys.DEFAULT_DFSCLIENT_HEDGED_READ_THREADPOOL_SIZE);
    if (numThreads > 0) {
      this.initThreadsNumForHedgedReads(numThreads);
    }
    this.saslClient = new SaslDataTransferClient(
      DataTransferSaslUtil.getSaslPropertiesResolver(conf),
      TrustedChannelResolver.getInstance(conf),
      conf.getBoolean(
        IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT));
  }
  
  /**
   * Return the socket addresses to use with each configured
   * local interface. Local interfaces may be specified by IP
   * address, IP address range using CIDR notation, interface
   * name (e.g. eth0) or sub-interface name (e.g. eth0:0).
   * The socket addresses consist of the IPs for the interfaces
   * and the ephemeral port (port 0). If an IP, IP range, or
   * interface name matches an interface with sub-interfaces
   * only the IP of the interface is used. Sub-interfaces can
   * be used by specifying them explicitly (by IP or name).
   * 
   * @return SocketAddresses for the configured local interfaces,
   *    or an empty array if none are configured
   * @throws UnknownHostException if a given interface name is invalid
   */
  private static SocketAddress[] getLocalInterfaceAddrs(
      String interfaceNames[]) throws UnknownHostException {
    List<SocketAddress> localAddrs = new ArrayList<SocketAddress>();
    for (String interfaceName : interfaceNames) {
      if (InetAddresses.isInetAddress(interfaceName)) {
        localAddrs.add(new InetSocketAddress(interfaceName, 0));
      } else if (NetUtils.isValidSubnet(interfaceName)) {
        for (InetAddress addr : NetUtils.getIPs(interfaceName, false)) {
          localAddrs.add(new InetSocketAddress(addr, 0));
        }
      } else {
        for (String ip : DNS.getIPs(interfaceName, false)) {
          localAddrs.add(new InetSocketAddress(ip, 0));
        }
      }
    }
    return localAddrs.toArray(new SocketAddress[localAddrs.size()]);
  }

  /**
   * Select one of the configured local interfaces at random. We use a random
   * interface because other policies like round-robin are less effective
   * given that we cache connections to datanodes.
   *
   * @return one of the local interface addresses at random, or null if no
   *    local interfaces are configured
   */
  SocketAddress getRandomLocalInterfaceAddr() {
    if (localInterfaceAddrs.length == 0) {
      return null;
    }
    final int idx = r.nextInt(localInterfaceAddrs.length);
    final SocketAddress addr = localInterfaceAddrs[idx];
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using local interface " + addr);
    }
    return addr;
  }

  /**
   * Return the number of times the client should go back to the namenode
   * to retrieve block locations when reading.
   */
  int getMaxBlockAcquireFailures() {
    return dfsClientConf.maxBlockAcquireFailures;
  }

  /**
   * Return the timeout that clients should use when writing to datanodes.
   * @param numNodes the number of nodes in the pipeline.
   */
  int getDatanodeWriteTimeout(int numNodes) {
    return (dfsClientConf.confTime > 0) ?
      (dfsClientConf.confTime + HdfsServerConstants.WRITE_TIMEOUT_EXTENSION * numNodes) : 0;
  }

  int getDatanodeReadTimeout(int numNodes) {
    return dfsClientConf.socketTimeout > 0 ?
        (HdfsServerConstants.READ_TIMEOUT_EXTENSION * numNodes +
            dfsClientConf.socketTimeout) : 0;
  }
  
  int getHdfsTimeout() {
    return dfsClientConf.hdfsTimeout;
  }
  
  @VisibleForTesting
  public String getClientName() {
    return clientName;
  }

  void checkOpen() throws IOException {
    if (!clientRunning) {
      IOException result = new IOException("Filesystem closed");
      throw result;
    }
  }

  /** Return the lease renewer instance. The renewer thread won't start
   *  until the first output stream is created. The same instance will
   *  be returned until all output streams are closed.
   */
  public LeaseRenewer getLeaseRenewer() throws IOException {
      return LeaseRenewer.getInstance(authority, ugi, this);
  }

  /** Get a lease and start automatic renewal */
  private void beginFileLease(final long inodeId, final DFSOutputStream out)
      throws IOException {
    getLeaseRenewer().put(inodeId, out, this);
  }

  /** Stop renewal of lease for the file. */
  void endFileLease(final long inodeId) throws IOException {
    getLeaseRenewer().closeFile(inodeId, this);
  }
    

  /** Put a file. Only called from LeaseRenewer, where proper locking is
   *  enforced to consistently update its local dfsclients array and 
   *  client's filesBeingWritten map.
   */
  void putFileBeingWritten(final long inodeId, final DFSOutputStream out) {
    synchronized(filesBeingWritten) {
      filesBeingWritten.put(inodeId, out);
      // update the last lease renewal time only when there was no
      // writes. once there is one write stream open, the lease renewer
      // thread keeps it updated well with in anyone's expiration time.
      if (lastLeaseRenewal == 0) {
        updateLastLeaseRenewal();
      }
    }
  }

  /** Remove a file. Only called from LeaseRenewer. */
  void removeFileBeingWritten(final long inodeId) {
    synchronized(filesBeingWritten) {
      filesBeingWritten.remove(inodeId);
      if (filesBeingWritten.isEmpty()) {
        lastLeaseRenewal = 0;
      }
    }
  }

  /** Is file-being-written map empty? */
  boolean isFilesBeingWrittenEmpty() {
    synchronized(filesBeingWritten) {
      return filesBeingWritten.isEmpty();
    }
  }
  
  /** @return true if the client is running */
  boolean isClientRunning() {
    return clientRunning;
  }

  long getLastLeaseRenewal() {
    return lastLeaseRenewal;
  }

  void updateLastLeaseRenewal() {
    synchronized(filesBeingWritten) {
      if (filesBeingWritten.isEmpty()) {
        return;
      }
      lastLeaseRenewal = Time.now();
    }
  }

  /**
   * Renew leases.
   * @return true if lease was renewed. May return false if this
   * client has been closed or has no files open.
   **/
  boolean renewLease() throws IOException {
    if (clientRunning && !isFilesBeingWrittenEmpty()) {
      try {
        namenode.renewLease(clientName);
        updateLastLeaseRenewal();
        return true;
      } catch (IOException e) {
        // Abort if the lease has already expired. 
        final long elapsed = Time.now() - getLastLeaseRenewal();
        if (elapsed > HdfsConstants.LEASE_HARDLIMIT_PERIOD) {
          LOG.warn("Failed to renew lease for " + clientName + " for "
              + (elapsed/1000) + " seconds (>= hard-limit ="
              + (HdfsConstants.LEASE_HARDLIMIT_PERIOD/1000) + " seconds.) "
              + "Closing all files being written ...", e);
          closeAllFilesBeingWritten(true);
        } else {
          // Let the lease renewer handle it and retry.
          throw e;
        }
      }
    }
    return false;
  }
  
  /**
   * Close connections the Namenode.
   */
  void closeConnectionToNamenode() {
    RPC.stopProxy(namenode);
  }
  
  /** Abort and release resources held.  Ignore all errors. */
  void abort() {
    clientRunning = false;
    closeAllFilesBeingWritten(true);
    try {
      // remove reference to this client and stop the renewer,
      // if there is no more clients under the renewer.
      getLeaseRenewer().closeClient(this);
    } catch (IOException ioe) {
       LOG.info("Exception occurred while aborting the client " + ioe);
    }
    closeConnectionToNamenode();
  }

  /** Close/abort all files being written. */
  private void closeAllFilesBeingWritten(final boolean abort) {
    for(;;) {
      final long inodeId;
      final DFSOutputStream out;
      synchronized(filesBeingWritten) {
        if (filesBeingWritten.isEmpty()) {
          return;
        }
        inodeId = filesBeingWritten.keySet().iterator().next();
        out = filesBeingWritten.remove(inodeId);
      }
      if (out != null) {
        try {
          if (abort) {
            out.abort();
          } else {
            out.close();
          }
        } catch(IOException ie) {
          LOG.error("Failed to " + (abort? "abort": "close") +
                  " inode " + inodeId, ie);
        }
      }
    }
  }

  /**
   * Close the file system, abandoning all of the leases and files being
   * created and close connections to the namenode.
   */
  @Override
  public synchronized void close() throws IOException {
    if(clientRunning) {
      closeAllFilesBeingWritten(false);
      clientRunning = false;
      getLeaseRenewer().closeClient(this);
      // close connections to the namenode
      closeConnectionToNamenode();
    }
  }

  /**
   * Close all open streams, abandoning all of the leases and files being
   * created.
   * @param abort whether streams should be gracefully closed
   */
  public void closeOutputStreams(boolean abort) {
    if (clientRunning) {
      closeAllFilesBeingWritten(abort);
    }
  }

  /**
   * Get the default block size for this cluster
   * @return the default block size in bytes
   */
  public long getDefaultBlockSize() {
    return dfsClientConf.defaultBlockSize;
  }
    
  /**
   * @see ClientProtocol#getPreferredBlockSize(String)
   */
  public long getBlockSize(String f) throws IOException {
    try {
      return namenode.getPreferredBlockSize(f);
    } catch (IOException ie) {
      LOG.warn("Problem getting block size", ie);
      throw ie;
    }
  }

  /**
   * Get server default values for a number of configuration params.
   * @see ClientProtocol#getServerDefaults()
   */
  public FsServerDefaults getServerDefaults() throws IOException {
    long now = Time.now();
    if (now - serverDefaultsLastUpdate > SERVER_DEFAULTS_VALIDITY_PERIOD) {
      serverDefaults = namenode.getServerDefaults();
      serverDefaultsLastUpdate = now;
    }
    return serverDefaults;
  }
  
  /**
   * Get a canonical token service name for this client's tokens.  Null should
   * be returned if the client is not using tokens.
   * @return the token service for the client
   */
  @InterfaceAudience.LimitedPrivate( { "HDFS" }) 
  public String getCanonicalServiceName() {
    return (dtService != null) ? dtService.toString() : null;
  }
  
  /**
   * @see ClientProtocol#getDelegationToken(Text)
   */
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    assert dtService != null;
    Token<DelegationTokenIdentifier> token =
      namenode.getDelegationToken(renewer);

    if (token != null) {
      token.setService(this.dtService);
      LOG.info("Created " + DelegationTokenIdentifier.stringifyToken(token));
    } else {
      LOG.info("Cannot get delegation token from " + renewer);
    }
    return token;

  }

  /**
   * Renew a delegation token
   * @param token the token to renew
   * @return the new expiration time
   * @throws InvalidToken
   * @throws IOException
   * @deprecated Use Token.renew instead.
   */
  @Deprecated
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    LOG.info("Renewing " + DelegationTokenIdentifier.stringifyToken(token));
    try {
      return token.renew(conf);
    } catch (InterruptedException ie) {                                       
      throw new RuntimeException("caught interrupted", ie);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(InvalidToken.class,
                                     AccessControlException.class);
    }
  }
  
  private static final Map<String, Boolean> localAddrMap = Collections
      .synchronizedMap(new HashMap<String, Boolean>());
  
  public static boolean isLocalAddress(InetSocketAddress targetAddr) {
    InetAddress addr = targetAddr.getAddress();
    Boolean cached = localAddrMap.get(addr.getHostAddress());
    if (cached != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Address " + targetAddr +
                  (cached ? " is local" : " is not local"));
      }
      return cached;
    }
    
    boolean local = NetUtils.isLocalAddress(addr);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Address " + targetAddr +
                (local ? " is local" : " is not local"));
    }
    localAddrMap.put(addr.getHostAddress(), local);
    return local;
  }
  
  /**
   * Cancel a delegation token
   * @param token the token to cancel
   * @throws InvalidToken
   * @throws IOException
   * @deprecated Use Token.cancel instead.
   */
  @Deprecated
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    LOG.info("Cancelling " + DelegationTokenIdentifier.stringifyToken(token));
    try {
      token.cancel(conf);
     } catch (InterruptedException ie) {                                       
      throw new RuntimeException("caught interrupted", ie);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(InvalidToken.class,
                                     AccessControlException.class);
    }
  }
  
  @InterfaceAudience.Private
  public static class Renewer extends TokenRenewer {
    
    static {
      //Ensure that HDFS Configuration files are loaded before trying to use
      // the renewer.
      HdfsConfiguration.init();
    }
    
    @Override
    public boolean handleKind(Text kind) {
      return DelegationTokenIdentifier.HDFS_DELEGATION_KIND.equals(kind);
    }

    @SuppressWarnings("unchecked")
    @Override
    public long renew(Token<?> token, Configuration conf) throws IOException {
      Token<DelegationTokenIdentifier> delToken = 
        (Token<DelegationTokenIdentifier>) token;
      ClientProtocol nn = getNNProxy(delToken, conf);
      try {
        return nn.renewDelegationToken(delToken);
      } catch (RemoteException re) {
        throw re.unwrapRemoteException(InvalidToken.class, 
                                       AccessControlException.class);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void cancel(Token<?> token, Configuration conf) throws IOException {
      Token<DelegationTokenIdentifier> delToken = 
          (Token<DelegationTokenIdentifier>) token;
      LOG.info("Cancelling " + 
               DelegationTokenIdentifier.stringifyToken(delToken));
      ClientProtocol nn = getNNProxy(delToken, conf);
      try {
        nn.cancelDelegationToken(delToken);
      } catch (RemoteException re) {
        throw re.unwrapRemoteException(InvalidToken.class,
            AccessControlException.class);
      }
    }
    
    private static ClientProtocol getNNProxy(
        Token<DelegationTokenIdentifier> token, Configuration conf)
        throws IOException {
      URI uri = HAUtil.getServiceUriFromToken(HdfsConstants.HDFS_URI_SCHEME,
              token);
      if (HAUtil.isTokenForLogicalUri(token) &&
          !HAUtil.isLogicalUri(conf, uri)) {
        // If the token is for a logical nameservice, but the configuration
        // we have disagrees about that, we can't actually renew it.
        // This can be the case in MR, for example, if the RM doesn't
        // have all of the HA clusters configured in its configuration.
        throw new IOException("Unable to map logical nameservice URI '" +
            uri + "' to a NameNode. Local configuration does not have " +
            "a failover proxy provider configured.");
      }
      
      NameNodeProxies.ProxyAndInfo<ClientProtocol> info =
        NameNodeProxies.createProxy(conf, uri, ClientProtocol.class);
      assert info.getDelegationTokenService().equals(token.getService()) :
        "Returned service '" + info.getDelegationTokenService().toString() +
        "' doesn't match expected service '" +
        token.getService().toString() + "'";
        
      return info.getProxy();
    }

    @Override
    public boolean isManaged(Token<?> token) throws IOException {
      return true;
    }
    
  }

  /**
   * Report corrupt blocks that were discovered by the client.
   * @see ClientProtocol#reportBadBlocks(LocatedBlock[])
   */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    namenode.reportBadBlocks(blocks);
  }
  
  public short getDefaultReplication() {
    return dfsClientConf.defaultReplication;
  }
  
  public LocatedBlocks getLocatedBlocks(String src, long start)
      throws IOException {
    return getLocatedBlocks(src, start, dfsClientConf.prefetchSize);
  }

  /*
   * This is just a wrapper around callGetBlockLocations, but non-static so that
   * we can stub it out for tests.
   */
  @VisibleForTesting
  public LocatedBlocks getLocatedBlocks(String src, long start, long length)
      throws IOException {
    return callGetBlockLocations(namenode, src, start, length);
  }

  /**
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  static LocatedBlocks callGetBlockLocations(ClientProtocol namenode,
      String src, long start, long length) 
      throws IOException {
    try {
      return namenode.getBlockLocations(src, start, length);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Recover a file's lease
   * @param src a file's path
   * @return true if the file is already closed
   * @throws IOException
   */
  boolean recoverLease(String src) throws IOException {
    checkOpen();

    try {
      return namenode.recoverLease(src, clientName);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(FileNotFoundException.class,
                                     AccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Get block location info about file
   * 
   * getBlockLocations() returns a list of hostnames that store 
   * data for a specific file region.  It returns a set of hostnames
   * for every block within the indicated region.
   *
   * This function is very useful when writing code that considers
   * data-placement when performing operations.  For example, the
   * MapReduce system tries to schedule tasks on the same machines
   * as the data-block the task processes. 
   */
  public BlockLocation[] getBlockLocations(String src, long start, 
    long length) throws IOException, UnresolvedLinkException {
    LocatedBlocks blocks = getLocatedBlocks(src, start, length);
    BlockLocation[] locations =  DFSUtil.locatedBlocks2Locations(blocks);
    HdfsBlockLocation[] hdfsLocations = new HdfsBlockLocation[locations.length];
    for (int i = 0; i < locations.length; i++) {
      hdfsLocations[i] = new HdfsBlockLocation(locations[i], blocks.get(i));
    }
    return hdfsLocations;
  }
  
  /**
   * Get block location information about a list of {@link HdfsBlockLocation}.
   * Used by {@link DistributedFileSystem#getFileBlockStorageLocations(List)} to
   * get {@link BlockStorageLocation}s for blocks returned by
   * {@link DistributedFileSystem#getFileBlockLocations(org.apache.hadoop.fs.FileStatus, long, long)}
   * .
   * 
   * This is done by making a round of RPCs to the associated datanodes, asking
   * the volume of each block replica. The returned array of
   * {@link BlockStorageLocation} expose this information as a
   * {@link VolumeId}.
   * 
   * @param blockLocations
   *          target blocks on which to query volume location information
   * @return volumeBlockLocations original block array augmented with additional
   *         volume location information for each replica.
   */
  public BlockStorageLocation[] getBlockStorageLocations(
      List<BlockLocation> blockLocations) throws IOException,
      UnsupportedOperationException, InvalidBlockTokenException {
    if (!getConf().getHdfsBlocksMetadataEnabled) {
      throw new UnsupportedOperationException("Datanode-side support for " +
          "getVolumeBlockLocations() must also be enabled in the client " +
          "configuration.");
    }
    // Downcast blockLocations and fetch out required LocatedBlock(s)
    List<LocatedBlock> blocks = new ArrayList<LocatedBlock>();
    for (BlockLocation loc : blockLocations) {
      if (!(loc instanceof HdfsBlockLocation)) {
        throw new ClassCastException("DFSClient#getVolumeBlockLocations " +
            "expected to be passed HdfsBlockLocations");
      }
      HdfsBlockLocation hdfsLoc = (HdfsBlockLocation) loc;
      blocks.add(hdfsLoc.getLocatedBlock());
    }
    
    // Re-group the LocatedBlocks to be grouped by datanodes, with the values
    // a list of the LocatedBlocks on the datanode.
    Map<DatanodeInfo, List<LocatedBlock>> datanodeBlocks = 
        new LinkedHashMap<DatanodeInfo, List<LocatedBlock>>();
    for (LocatedBlock b : blocks) {
      for (DatanodeInfo info : b.getLocations()) {
        if (!datanodeBlocks.containsKey(info)) {
          datanodeBlocks.put(info, new ArrayList<LocatedBlock>());
        }
        List<LocatedBlock> l = datanodeBlocks.get(info);
        l.add(b);
      }
    }
        
    // Make RPCs to the datanodes to get volume locations for its replicas
    Map<DatanodeInfo, HdfsBlocksMetadata> metadatas = BlockStorageLocationUtil
        .queryDatanodesForHdfsBlocksMetadata(conf, datanodeBlocks,
            getConf().getFileBlockStorageLocationsNumThreads,
            getConf().getFileBlockStorageLocationsTimeoutMs,
            getConf().connectToDnViaHostname);
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("metadata returned: "
          + Joiner.on("\n").withKeyValueSeparator("=").join(metadatas));
    }
    
    // Regroup the returned VolumeId metadata to again be grouped by
    // LocatedBlock rather than by datanode
    Map<LocatedBlock, List<VolumeId>> blockVolumeIds = BlockStorageLocationUtil
        .associateVolumeIdsWithBlocks(blocks, metadatas);
    
    // Combine original BlockLocations with new VolumeId information
    BlockStorageLocation[] volumeBlockLocations = BlockStorageLocationUtil
        .convertToVolumeBlockLocations(blocks, blockVolumeIds);

    return volumeBlockLocations;
  }
  
  public DFSInputStream open(String src) 
      throws IOException, UnresolvedLinkException {
    return open(src, dfsClientConf.ioBufferSize, true, null);
  }

  /**
   * Create an input stream that obtains a nodelist from the
   * namenode, and then reads from all the right places.  Creates
   * inner subclass of InputStream that does the right out-of-band
   * work.
   * @deprecated Use {@link #open(String, int, boolean)} instead.
   */
  @Deprecated
  public DFSInputStream open(String src, int buffersize, boolean verifyChecksum,
                             FileSystem.Statistics stats)
      throws IOException, UnresolvedLinkException {
    return open(src, buffersize, verifyChecksum);
  }
  

  /**
   * Create an input stream that obtains a nodelist from the
   * namenode, and then reads from all the right places.  Creates
   * inner subclass of InputStream that does the right out-of-band
   * work.
   */
  public DFSInputStream open(String src, int buffersize, boolean verifyChecksum)
      throws IOException, UnresolvedLinkException {
    checkOpen();
    //    Get block info from namenode
    return new DFSInputStream(this, src, buffersize, verifyChecksum);
  }

  /**
   * Get the namenode associated with this DFSClient object
   * @return the namenode associated with this DFSClient object
   */
  public ClientProtocol getNamenode() {
    return namenode;
  }
  
  /**
   * Call {@link #create(String, boolean, short, long, Progressable)} with
   * default <code>replication</code> and <code>blockSize<code> and null <code>
   * progress</code>.
   */
  public OutputStream create(String src, boolean overwrite) 
      throws IOException {
    return create(src, overwrite, dfsClientConf.defaultReplication,
        dfsClientConf.defaultBlockSize, null);
  }
    
  /**
   * Call {@link #create(String, boolean, short, long, Progressable)} with
   * default <code>replication</code> and <code>blockSize<code>.
   */
  public OutputStream create(String src, 
                             boolean overwrite,
                             Progressable progress) throws IOException {
    return create(src, overwrite, dfsClientConf.defaultReplication,
        dfsClientConf.defaultBlockSize, progress);
  }
    
  /**
   * Call {@link #create(String, boolean, short, long, Progressable)} with
   * null <code>progress</code>.
   */
  public OutputStream create(String src, 
                             boolean overwrite, 
                             short replication,
                             long blockSize) throws IOException {
    return create(src, overwrite, replication, blockSize, null);
  }

  /**
   * Call {@link #create(String, boolean, short, long, Progressable, int)}
   * with default bufferSize.
   */
  public OutputStream create(String src, boolean overwrite, short replication,
      long blockSize, Progressable progress) throws IOException {
    return create(src, overwrite, replication, blockSize, progress,
        dfsClientConf.ioBufferSize);
  }

  /**
   * Call {@link #create(String, FsPermission, EnumSet, short, long, 
   * Progressable, int, ChecksumOpt)} with default <code>permission</code>
   * {@link FsPermission#getFileDefault()}.
   * 
   * @param src File name
   * @param overwrite overwrite an existing file if true
   * @param replication replication factor for the file
   * @param blockSize maximum block size
   * @param progress interface for reporting client progress
   * @param buffersize underlying buffersize
   * 
   * @return output stream
   */
  public OutputStream create(String src,
                             boolean overwrite,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize)
      throws IOException {
    return create(src, FsPermission.getFileDefault(),
        overwrite ? EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
            : EnumSet.of(CreateFlag.CREATE), replication, blockSize, progress,
        buffersize, null);
  }

  /**
   * Call {@link #create(String, FsPermission, EnumSet, boolean, short, 
   * long, Progressable, int, ChecksumOpt)} with <code>createParent</code>
   *  set to true.
   */
  public DFSOutputStream create(String src, 
                             FsPermission permission,
                             EnumSet<CreateFlag> flag, 
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize,
                             ChecksumOpt checksumOpt)
      throws IOException {
    return create(src, permission, flag, true,
        replication, blockSize, progress, buffersize, checksumOpt, null);
  }

  /**
   * Create a new dfs file with the specified block replication 
   * with write-progress reporting and return an output stream for writing
   * into the file.  
   * 
   * @param src File name
   * @param permission The permission of the directory being created.
   *          If null, use default permission {@link FsPermission#getFileDefault()}
   * @param flag indicates create a new file or create/overwrite an
   *          existing file or append to an existing file
   * @param createParent create missing parent directory if true
   * @param replication block replication
   * @param blockSize maximum block size
   * @param progress interface for reporting client progress
   * @param buffersize underlying buffer size 
   * @param checksumOpt checksum options
   * 
   * @return output stream
   * 
   * @see ClientProtocol#create(String, FsPermission, String, EnumSetWritable,
   * boolean, short, long) for detailed description of exceptions thrown
   */
  public DFSOutputStream create(String src, 
                             FsPermission permission,
                             EnumSet<CreateFlag> flag, 
                             boolean createParent,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize,
                             ChecksumOpt checksumOpt) throws IOException {
    return create(src, permission, flag, createParent, replication, blockSize, 
        progress, buffersize, checksumOpt, null);
  }

  /**
   * Same as {@link #create(String, FsPermission, EnumSet, boolean, short, long,
   * Progressable, int, ChecksumOpt)} with the addition of favoredNodes that is
   * a hint to where the namenode should place the file blocks.
   * The favored nodes hint is not persisted in HDFS. Hence it may be honored
   * at the creation time only. HDFS could move the blocks during balancing or
   * replication, to move the blocks from favored nodes. A value of null means
   * no favored nodes for this create
   */
  public DFSOutputStream create(String src, 
                             FsPermission permission,
                             EnumSet<CreateFlag> flag, 
                             boolean createParent,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize,
                             ChecksumOpt checksumOpt,
                             InetSocketAddress[] favoredNodes) throws IOException {
    checkOpen();
    if (permission == null) {
      permission = FsPermission.getFileDefault();
    }
    FsPermission masked = permission.applyUMask(dfsClientConf.uMask);
    if(LOG.isDebugEnabled()) {
      LOG.debug(src + ": masked=" + masked);
    }
    String[] favoredNodeStrs = null;
    if (favoredNodes != null) {
      favoredNodeStrs = new String[favoredNodes.length];
      for (int i = 0; i < favoredNodes.length; i++) {
        favoredNodeStrs[i] = 
            favoredNodes[i].getHostName() + ":" 
                         + favoredNodes[i].getPort();
      }
    }
    final DFSOutputStream result = DFSOutputStream.newStreamForCreate(this,
        src, masked, flag, createParent, replication, blockSize, progress,
        buffersize, dfsClientConf.createChecksum(checksumOpt), favoredNodeStrs);
    beginFileLease(result.getFileId(), result);
    return result;
  }
  
  /**
   * Append to an existing file if {@link CreateFlag#APPEND} is present
   */
  private DFSOutputStream primitiveAppend(String src, EnumSet<CreateFlag> flag,
      int buffersize, Progressable progress) throws IOException {
    if (flag.contains(CreateFlag.APPEND)) {
      HdfsFileStatus stat = getFileInfo(src);
      if (stat == null) { // No file to append to
        // New file needs to be created if create option is present
        if (!flag.contains(CreateFlag.CREATE)) {
          throw new FileNotFoundException("failed to append to non-existent file "
              + src + " on client " + clientName);
        }
        return null;
      }
      return callAppend(stat, src, buffersize, progress);
    }
    return null;
  }
  
  /**
   * Same as {{@link #create(String, FsPermission, EnumSet, short, long,
   *  Progressable, int, ChecksumOpt)} except that the permission
   *  is absolute (ie has already been masked with umask.
   */
  public DFSOutputStream primitiveCreate(String src, 
                             FsPermission absPermission,
                             EnumSet<CreateFlag> flag,
                             boolean createParent,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize,
                             ChecksumOpt checksumOpt)
      throws IOException, UnresolvedLinkException {
    checkOpen();
    CreateFlag.validate(flag);
    DFSOutputStream result = primitiveAppend(src, flag, buffersize, progress);
    if (result == null) {
      DataChecksum checksum = dfsClientConf.createChecksum(checksumOpt);
      result = DFSOutputStream.newStreamForCreate(this, src, absPermission,
          flag, createParent, replication, blockSize, progress, buffersize,
          checksum);
    }
    beginFileLease(result.getFileId(), result);
    return result;
  }
  
  /**
   * Creates a symbolic link.
   * 
   * @see ClientProtocol#createSymlink(String, String,FsPermission, boolean) 
   */
  public void createSymlink(String target, String link, boolean createParent)
      throws IOException {
    try {
      FsPermission dirPerm = 
          FsPermission.getDefault().applyUMask(dfsClientConf.uMask); 
      namenode.createSymlink(target, link, dirPerm, createParent);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileAlreadyExistsException.class, 
                                     FileNotFoundException.class,
                                     ParentNotDirectoryException.class,
                                     NSQuotaExceededException.class, 
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    }
  }

  /**
   * Resolve the *first* symlink, if any, in the path.
   * 
   * @see ClientProtocol#getLinkTarget(String)
   */
  public String getLinkTarget(String path) throws IOException { 
    checkOpen();
    try {
      return namenode.getLinkTarget(path);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class);
    }
  }

  /** Method to get stream returned by append call */
  private DFSOutputStream callAppend(HdfsFileStatus stat, String src,
      int buffersize, Progressable progress) throws IOException {
    LocatedBlock lastBlock = null;
    try {
      lastBlock = namenode.append(src, clientName);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     SafeModeException.class,
                                     DSQuotaExceededException.class,
                                     UnsupportedOperationException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    }
    return DFSOutputStream.newStreamForAppend(this, src, buffersize, progress,
        lastBlock, stat, dfsClientConf.createChecksum());
  }
  
  /**
   * Append to an existing HDFS file.  
   * 
   * @param src file name
   * @param buffersize buffer size
   * @param progress for reporting write-progress; null is acceptable.
   * @param statistics file system statistics; null is acceptable.
   * @return an output stream for writing into the file
   * 
   * @see ClientProtocol#append(String, String) 
   */
  public HdfsDataOutputStream append(final String src, final int buffersize,
      final Progressable progress, final FileSystem.Statistics statistics
      ) throws IOException {
    final DFSOutputStream out = append(src, buffersize, progress);
    return new HdfsDataOutputStream(out, statistics, out.getInitialLen());
  }

  private DFSOutputStream append(String src, int buffersize, Progressable progress) 
      throws IOException {
    checkOpen();
    HdfsFileStatus stat = getFileInfo(src);
    if (stat == null) { // No file found
      throw new FileNotFoundException("failed to append to non-existent file "
          + src + " on client " + clientName);
    }
    final DFSOutputStream result = callAppend(stat, src, buffersize, progress);
    beginFileLease(result.getFileId(), result);
    return result;
  }

  /**
   * Set replication for an existing file.
   * @param src file name
   * @param replication replication to set the file to
   * 
   * @see ClientProtocol#setReplication(String, short)
   */
  public boolean setReplication(String src, short replication)
      throws IOException {
    try {
      return namenode.setReplication(src, replication);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     SafeModeException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    }
  }

  /**
   * Rename file or directory.
   * @see ClientProtocol#rename(String, String)
   * @deprecated Use {@link #rename(String, String, Options.Rename...)} instead.
   */
  @Deprecated
  public boolean rename(String src, String dst) throws IOException {
    checkOpen();
    try {
      return namenode.rename(src, dst);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    }
  }

  /**
   * Move blocks from src to trg and delete src
   * See {@link ClientProtocol#concat(String, String [])}. 
   */
  public void concat(String trg, String [] srcs) throws IOException {
    checkOpen();
    try {
      namenode.concat(trg, srcs);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    }
  }
  /**
   * Rename file or directory.
   * @see ClientProtocol#rename2(String, String, Options.Rename...)
   */
  public void rename(String src, String dst, Options.Rename... options)
      throws IOException {
    checkOpen();
    try {
      namenode.rename2(src, dst, options);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     DSQuotaExceededException.class,
                                     FileAlreadyExistsException.class,
                                     FileNotFoundException.class,
                                     ParentNotDirectoryException.class,
                                     SafeModeException.class,
                                     NSQuotaExceededException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    }
  }
  /**
   * Delete file or directory.
   * See {@link ClientProtocol#delete(String, boolean)}. 
   */
  @Deprecated
  public boolean delete(String src) throws IOException {
    checkOpen();
    return namenode.delete(src, true);
  }

  /**
   * delete file or directory.
   * delete contents of the directory if non empty and recursive 
   * set to true
   *
   * @see ClientProtocol#delete(String, boolean)
   */
  public boolean delete(String src, boolean recursive) throws IOException {
    checkOpen();
    try {
      return namenode.delete(src, recursive);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     SafeModeException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    }
  }
  
  /** Implemented using getFileInfo(src)
   */
  public boolean exists(String src) throws IOException {
    checkOpen();
    return getFileInfo(src) != null;
  }

  /**
   * Get a partial listing of the indicated directory
   * No block locations need to be fetched
   */
  public DirectoryListing listPaths(String src,  byte[] startAfter)
    throws IOException {
    return listPaths(src, startAfter, false);
  }
  
  /**
   * Get a partial listing of the indicated directory
   *
   * Recommend to use HdfsFileStatus.EMPTY_NAME as startAfter
   * if the application wants to fetch a listing starting from
   * the first entry in the directory
   *
   * @see ClientProtocol#getListing(String, byte[], boolean)
   */
  public DirectoryListing listPaths(String src,  byte[] startAfter,
      boolean needLocation) 
    throws IOException {
    checkOpen();
    try {
      return namenode.getListing(src, startAfter, needLocation);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Get the file info for a specific file or directory.
   * @param src The string representation of the path to the file
   * @return object containing information regarding the file
   *         or null if file not found
   *         
   * @see ClientProtocol#getFileInfo(String) for description of exceptions
   */
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    checkOpen();
    try {
      return namenode.getFileInfo(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }
  
  /**
   * Close status of a file
   * @return true if file is already closed
   */
  public boolean isFileClosed(String src) throws IOException{
    checkOpen();
    try {
      return namenode.isFileClosed(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }
  
  /**
   * Get the file info for a specific file or directory. If src
   * refers to a symlink then the FileStatus of the link is returned.
   * @param src path to a file or directory.
   * 
   * For description of exceptions thrown 
   * @see ClientProtocol#getFileLinkInfo(String)
   */
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
    checkOpen();
    try {
      return namenode.getFileLinkInfo(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     UnresolvedPathException.class);
     }
   }
  
  @InterfaceAudience.Private
  public void clearDataEncryptionKey() {
    LOG.debug("Clearing encryption key");
    synchronized (this) {
      encryptionKey = null;
    }
  }
  
  /**
   * @return true if data sent between this client and DNs should be encrypted,
   *         false otherwise.
   * @throws IOException in the event of error communicating with the NN
   */
  boolean shouldEncryptData() throws IOException {
    FsServerDefaults d = getServerDefaults();
    return d == null ? false : d.getEncryptDataTransfer();
  }
  
  @Override
  public DataEncryptionKey newDataEncryptionKey() throws IOException {
    if (shouldEncryptData()) {
      synchronized (this) {
        if (encryptionKey == null ||
            encryptionKey.expiryDate < Time.now()) {
          LOG.debug("Getting new encryption token from NN");
          encryptionKey = namenode.getDataEncryptionKey();
        }
        return encryptionKey;
      }
    } else {
      return null;
    }
  }

  /**
   * Get the checksum of the whole file of a range of the file. Note that the
   * range always starts from the beginning of the file.
   * @param src The file path
   * @param length the length of the range, i.e., the range is [0, length]
   * @return The checksum 
   * @see DistributedFileSystem#getFileChecksum(Path)
   */
  public MD5MD5CRC32FileChecksum getFileChecksum(String src, long length)
      throws IOException {
    checkOpen();
    Preconditions.checkArgument(length >= 0);
    //get block locations for the file range
    LocatedBlocks blockLocations = callGetBlockLocations(namenode, src, 0,
        length);
    if (null == blockLocations) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    List<LocatedBlock> locatedblocks = blockLocations.getLocatedBlocks();
    final DataOutputBuffer md5out = new DataOutputBuffer();
    int bytesPerCRC = -1;
    DataChecksum.Type crcType = DataChecksum.Type.DEFAULT;
    long crcPerBlock = 0;
    boolean refetchBlocks = false;
    int lastRetriedIndex = -1;

    // get block checksum for each block
    long remaining = length;
    for(int i = 0; i < locatedblocks.size() && remaining > 0; i++) {
      if (refetchBlocks) {  // refetch to get fresh tokens
        blockLocations = callGetBlockLocations(namenode, src, 0, length);
        if (null == blockLocations) {
          throw new FileNotFoundException("File does not exist: " + src);
        }
        locatedblocks = blockLocations.getLocatedBlocks();
        refetchBlocks = false;
      }
      LocatedBlock lb = locatedblocks.get(i);
      final ExtendedBlock block = lb.getBlock();
      if (remaining < block.getNumBytes()) {
        block.setNumBytes(remaining);
      }
      remaining -= block.getNumBytes();
      final DatanodeInfo[] datanodes = lb.getLocations();
      
      //try each datanode location of the block
      final int timeout = 3000 * datanodes.length + dfsClientConf.socketTimeout;
      boolean done = false;
      for(int j = 0; !done && j < datanodes.length; j++) {
        DataOutputStream out = null;
        DataInputStream in = null;
        
        try {
          //connect to a datanode
          IOStreamPair pair = connectToDN(datanodes[j], timeout, lb);
          out = new DataOutputStream(new BufferedOutputStream(pair.out,
              HdfsConstants.SMALL_BUFFER_SIZE));
          in = new DataInputStream(pair.in);

          if (LOG.isDebugEnabled()) {
            LOG.debug("write to " + datanodes[j] + ": "
                + Op.BLOCK_CHECKSUM + ", block=" + block);
          }
          // get block MD5
          new Sender(out).blockChecksum(block, lb.getBlockToken());

          final BlockOpResponseProto reply =
            BlockOpResponseProto.parseFrom(PBHelper.vintPrefixed(in));

          if (reply.getStatus() != Status.SUCCESS) {
            if (reply.getStatus() == Status.ERROR_ACCESS_TOKEN) {
              throw new InvalidBlockTokenException();
            } else {
              throw new IOException("Bad response " + reply + " for block "
                  + block + " from datanode " + datanodes[j]);
            }
          }
          
          OpBlockChecksumResponseProto checksumData =
            reply.getChecksumResponse();

          //read byte-per-checksum
          final int bpc = checksumData.getBytesPerCrc();
          if (i == 0) { //first block
            bytesPerCRC = bpc;
          }
          else if (bpc != bytesPerCRC) {
            throw new IOException("Byte-per-checksum not matched: bpc=" + bpc
                + " but bytesPerCRC=" + bytesPerCRC);
          }
          
          //read crc-per-block
          final long cpb = checksumData.getCrcPerBlock();
          if (locatedblocks.size() > 1 && i == 0) {
            crcPerBlock = cpb;
          }

          //read md5
          final MD5Hash md5 = new MD5Hash(
              checksumData.getMd5().toByteArray());
          md5.write(md5out);
          
          // read crc-type
          final DataChecksum.Type ct;
          if (checksumData.hasCrcType()) {
            ct = PBHelper.convert(checksumData
                .getCrcType());
          } else {
            LOG.debug("Retrieving checksum from an earlier-version DataNode: " +
                      "inferring checksum by reading first byte");
            ct = inferChecksumTypeByReading(lb, datanodes[j]);
          }

          if (i == 0) { // first block
            crcType = ct;
          } else if (crcType != DataChecksum.Type.MIXED
              && crcType != ct) {
            // if crc types are mixed in a file
            crcType = DataChecksum.Type.MIXED;
          }

          done = true;

          if (LOG.isDebugEnabled()) {
            if (i == 0) {
              LOG.debug("set bytesPerCRC=" + bytesPerCRC
                  + ", crcPerBlock=" + crcPerBlock);
            }
            LOG.debug("got reply from " + datanodes[j] + ": md5=" + md5);
          }
        } catch (InvalidBlockTokenException ibte) {
          if (i > lastRetriedIndex) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got access token error in response to OP_BLOCK_CHECKSUM "
                  + "for file " + src + " for block " + block
                  + " from datanode " + datanodes[j]
                  + ". Will retry the block once.");
            }
            lastRetriedIndex = i;
            done = true; // actually it's not done; but we'll retry
            i--; // repeat at i-th block
            refetchBlocks = true;
            break;
          }
        } catch (IOException ie) {
          LOG.warn("src=" + src + ", datanodes["+j+"]=" + datanodes[j], ie);
        } finally {
          IOUtils.closeStream(in);
          IOUtils.closeStream(out);
        }
      }

      if (!done) {
        throw new IOException("Fail to get block MD5 for " + block);
      }
    }

    //compute file MD5
    final MD5Hash fileMD5 = MD5Hash.digest(md5out.getData()); 
    switch (crcType) {
      case CRC32:
        return new MD5MD5CRC32GzipFileChecksum(bytesPerCRC,
            crcPerBlock, fileMD5);
      case CRC32C:
        return new MD5MD5CRC32CastagnoliFileChecksum(bytesPerCRC,
            crcPerBlock, fileMD5);
      default:
        // If there is no block allocated for the file,
        // return one with the magic entry that matches what previous
        // hdfs versions return.
        if (locatedblocks.size() == 0) {
          return new MD5MD5CRC32GzipFileChecksum(0, 0, fileMD5);
        }

        // we should never get here since the validity was checked
        // when getCrcType() was called above.
        return null;
    }
  }

  /**
   * Connect to the given datanode's datantrasfer port, and return
   * the resulting IOStreamPair. This includes encryption wrapping, etc.
   */
  private IOStreamPair connectToDN(DatanodeInfo dn, int timeout,
      LocatedBlock lb) throws IOException {
    boolean success = false;
    Socket sock = null;
    try {
      sock = socketFactory.createSocket();
      String dnAddr = dn.getXferAddr(getConf().connectToDnViaHostname);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connecting to datanode " + dnAddr);
      }
      NetUtils.connect(sock, NetUtils.createSocketAddr(dnAddr), timeout);
      sock.setSoTimeout(timeout);
  
      OutputStream unbufOut = NetUtils.getOutputStream(sock);
      InputStream unbufIn = NetUtils.getInputStream(sock);
      IOStreamPair ret = saslClient.newSocketSend(sock, unbufOut, unbufIn, this,
        lb.getBlockToken(), dn);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeSocket(sock);
      }
    }
  }
  
  /**
   * Infer the checksum type for a replica by sending an OP_READ_BLOCK
   * for the first byte of that replica. This is used for compatibility
   * with older HDFS versions which did not include the checksum type in
   * OpBlockChecksumResponseProto.
   *
   * @param lb the located block
   * @param dn the connected datanode
   * @return the inferred checksum type
   * @throws IOException if an error occurs
   */
  private Type inferChecksumTypeByReading(LocatedBlock lb, DatanodeInfo dn)
      throws IOException {
    IOStreamPair pair = connectToDN(dn, dfsClientConf.socketTimeout, lb);

    try {
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(pair.out,
          HdfsConstants.SMALL_BUFFER_SIZE));
      DataInputStream in = new DataInputStream(pair.in);
  
      new Sender(out).readBlock(lb.getBlock(), lb.getBlockToken(), clientName,
          0, 1, true, CachingStrategy.newDefaultStrategy());
      final BlockOpResponseProto reply =
          BlockOpResponseProto.parseFrom(PBHelper.vintPrefixed(in));
      
      if (reply.getStatus() != Status.SUCCESS) {
        if (reply.getStatus() == Status.ERROR_ACCESS_TOKEN) {
          throw new InvalidBlockTokenException();
        } else {
          throw new IOException("Bad response " + reply + " trying to read "
              + lb.getBlock() + " from datanode " + dn);
        }
      }
      
      return PBHelper.convert(reply.getReadOpChecksumInfo().getChecksum().getType());
    } finally {
      IOUtils.cleanup(null, pair.in, pair.out);
    }
  }

  /**
   * Set permissions to a file or directory.
   * @param src path name.
   * @param permission permission to set to
   * 
   * @see ClientProtocol#setPermission(String, FsPermission)
   */
  public void setPermission(String src, FsPermission permission)
      throws IOException {
    checkOpen();
    try {
      namenode.setPermission(src, permission);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     SafeModeException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    }
  }

  /**
   * Set file or directory owner.
   * @param src path name.
   * @param username user id.
   * @param groupname user group.
   * 
   * @see ClientProtocol#setOwner(String, String, String)
   */
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    checkOpen();
    try {
      namenode.setOwner(src, username, groupname);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     SafeModeException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);                                   
    }
  }

  /**
   * @see ClientProtocol#getStats()
   */
  public FsStatus getDiskStatus() throws IOException {
    long rawNums[] = namenode.getStats();
    return new FsStatus(rawNums[0], rawNums[1], rawNums[2]);
  }

  /**
   * Returns count of blocks with no good replicas left. Normally should be 
   * zero.
   * @throws IOException
   */ 
  public long getMissingBlocksCount() throws IOException {
    return namenode.getStats()[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX];
  }
  
  /**
   * Returns count of blocks with one of more replica missing.
   * @throws IOException
   */ 
  public long getUnderReplicatedBlocksCount() throws IOException {
    return namenode.getStats()[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX];
  }
  
  /**
   * Returns count of blocks with at least one replica marked corrupt. 
   * @throws IOException
   */ 
  public long getCorruptBlocksCount() throws IOException {
    return namenode.getStats()[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX];
  }
  
  /**
   * @return a list in which each entry describes a corrupt file/block
   * @throws IOException
   */
  public CorruptFileBlocks listCorruptFileBlocks(String path,
                                                 String cookie)
    throws IOException {
    return namenode.listCorruptFileBlocks(path, cookie);
  }

  public DatanodeInfo[] datanodeReport(DatanodeReportType type)
  throws IOException {
    return namenode.getDatanodeReport(type);
  }
    
  /**
   * Enter, leave or get safe mode.
   * 
   * @see ClientProtocol#setSafeMode(HdfsConstants.SafeModeAction,boolean)
   */
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return setSafeMode(action, false);
  }
  
  /**
   * Enter, leave or get safe mode.
   * 
   * @param action
   *          One of SafeModeAction.GET, SafeModeAction.ENTER and
   *          SafeModeActiob.LEAVE
   * @param isChecked
   *          If true, then check only active namenode's safemode status, else
   *          check first namenode's status.
   * @see ClientProtocol#setSafeMode(HdfsConstants.SafeModeAction, boolean)
   */
  public boolean setSafeMode(SafeModeAction action, boolean isChecked) throws IOException{
    return namenode.setSafeMode(action, isChecked);    
  }
 
  /**
   * Create one snapshot.
   * 
   * @param snapshotRoot The directory where the snapshot is to be taken
   * @param snapshotName Name of the snapshot
   * @return the snapshot path.
   * @see ClientProtocol#createSnapshot(String, String)
   */
  public String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    checkOpen();
    try {
      return namenode.createSnapshot(snapshotRoot, snapshotName);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }
  
  /**
   * Delete a snapshot of a snapshottable directory.
   * 
   * @param snapshotRoot The snapshottable directory that the 
   *                    to-be-deleted snapshot belongs to
   * @param snapshotName The name of the to-be-deleted snapshot
   * @throws IOException
   * @see ClientProtocol#deleteSnapshot(String, String)
   */
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    try {
      namenode.deleteSnapshot(snapshotRoot, snapshotName);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }
  
  /**
   * Rename a snapshot.
   * @param snapshotDir The directory path where the snapshot was taken
   * @param snapshotOldName Old name of the snapshot
   * @param snapshotNewName New name of the snapshot
   * @throws IOException
   * @see ClientProtocol#renameSnapshot(String, String, String)
   */
  public void renameSnapshot(String snapshotDir, String snapshotOldName,
      String snapshotNewName) throws IOException {
    checkOpen();
    try {
      namenode.renameSnapshot(snapshotDir, snapshotOldName, snapshotNewName);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }
  
  /**
   * Get all the current snapshottable directories.
   * @return All the current snapshottable directories
   * @throws IOException
   * @see ClientProtocol#getSnapshottableDirListing()
   */
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    checkOpen();
    try {
      return namenode.getSnapshottableDirListing();
    } catch(RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }

  /**
   * Allow snapshot on a directory.
   * 
   * @see ClientProtocol#allowSnapshot(String snapshotRoot)
   */
  public void allowSnapshot(String snapshotRoot) throws IOException {
    checkOpen();
    try {
      namenode.allowSnapshot(snapshotRoot);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }
  
  /**
   * Disallow snapshot on a directory.
   * 
   * @see ClientProtocol#disallowSnapshot(String snapshotRoot)
   */
  public void disallowSnapshot(String snapshotRoot) throws IOException {
    checkOpen();
    try {
      namenode.disallowSnapshot(snapshotRoot);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }
  
  /**
   * Get the difference between two snapshots, or between a snapshot and the
   * current tree of a directory.
   * @see ClientProtocol#getSnapshotDiffReport(String, String, String)
   */
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotDir,
      String fromSnapshot, String toSnapshot) throws IOException {
    checkOpen();
    try {
      return namenode.getSnapshotDiffReport(snapshotDir,
          fromSnapshot, toSnapshot);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }

  public long addCacheDirective(
      CacheDirectiveInfo info, EnumSet<CacheFlag> flags) throws IOException {
    checkOpen();
    try {
      return namenode.addCacheDirective(info, flags);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }
  
  public void modifyCacheDirective(
      CacheDirectiveInfo info, EnumSet<CacheFlag> flags) throws IOException {
    checkOpen();
    try {
      namenode.modifyCacheDirective(info, flags);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }

  public void removeCacheDirective(long id)
      throws IOException {
    checkOpen();
    try {
      namenode.removeCacheDirective(id);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }
  
  public RemoteIterator<CacheDirectiveEntry> listCacheDirectives(
      CacheDirectiveInfo filter) throws IOException {
    return new CacheDirectiveIterator(namenode, filter);
  }

  public void addCachePool(CachePoolInfo info) throws IOException {
    checkOpen();
    try {
      namenode.addCachePool(info);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }

  public void modifyCachePool(CachePoolInfo info) throws IOException {
    checkOpen();
    try {
      namenode.modifyCachePool(info);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }

  public void removeCachePool(String poolName) throws IOException {
    checkOpen();
    try {
      namenode.removeCachePool(poolName);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }

  public RemoteIterator<CachePoolEntry> listCachePools() throws IOException {
    return new CachePoolIterator(namenode);
  }

  /**
   * Save namespace image.
   * 
   * @see ClientProtocol#saveNamespace()
   */
  void saveNamespace() throws AccessControlException, IOException {
    try {
      namenode.saveNamespace();
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class);
    }
  }

  /**
   * Rolls the edit log on the active NameNode.
   * @return the txid of the new log segment 
   *
   * @see ClientProtocol#rollEdits()
   */
  long rollEdits() throws AccessControlException, IOException {
    try {
      return namenode.rollEdits();
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class);
    }
  }

  @VisibleForTesting
  ExtendedBlock getPreviousBlock(long fileId) {
    return filesBeingWritten.get(fileId).getBlock();
  }
  
  /**
   * enable/disable restore failed storage.
   * 
   * @see ClientProtocol#restoreFailedStorage(String arg)
   */
  boolean restoreFailedStorage(String arg)
      throws AccessControlException, IOException{
    return namenode.restoreFailedStorage(arg);
  }

  /**
   * Refresh the hosts and exclude files.  (Rereads them.)
   * See {@link ClientProtocol#refreshNodes()} 
   * for more details.
   * 
   * @see ClientProtocol#refreshNodes()
   */
  public void refreshNodes() throws IOException {
    namenode.refreshNodes();
  }

  /**
   * Dumps DFS data structures into specified file.
   * 
   * @see ClientProtocol#metaSave(String)
   */
  public void metaSave(String pathname) throws IOException {
    namenode.metaSave(pathname);
  }

  /**
   * Requests the namenode to tell all datanodes to use a new, non-persistent
   * bandwidth value for dfs.balance.bandwidthPerSec.
   * See {@link ClientProtocol#setBalancerBandwidth(long)} 
   * for more details.
   * 
   * @see ClientProtocol#setBalancerBandwidth(long)
   */
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    namenode.setBalancerBandwidth(bandwidth);
  }
    
  /**
   * @see ClientProtocol#finalizeUpgrade()
   */
  public void finalizeUpgrade() throws IOException {
    namenode.finalizeUpgrade();
  }

  RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action) throws IOException {
    return namenode.rollingUpgrade(action);
  }

  /**
   */
  @Deprecated
  public boolean mkdirs(String src) throws IOException {
    return mkdirs(src, null, true);
  }

  /**
   * Create a directory (or hierarchy of directories) with the given
   * name and permission.
   *
   * @param src The path of the directory being created
   * @param permission The permission of the directory being created.
   * If permission == null, use {@link FsPermission#getDefault()}.
   * @param createParent create missing parent directory if true
   * 
   * @return True if the operation success.
   * 
   * @see ClientProtocol#mkdirs(String, FsPermission, boolean)
   */
  public boolean mkdirs(String src, FsPermission permission,
      boolean createParent) throws IOException {
    if (permission == null) {
      permission = FsPermission.getDefault();
    }
    FsPermission masked = permission.applyUMask(dfsClientConf.uMask);
    return primitiveMkdir(src, masked, createParent);
  }

  /**
   * Same {{@link #mkdirs(String, FsPermission, boolean)} except
   * that the permissions has already been masked against umask.
   */
  public boolean primitiveMkdir(String src, FsPermission absPermission)
    throws IOException {
    return primitiveMkdir(src, absPermission, true);
  }

  /**
   * Same {{@link #mkdirs(String, FsPermission, boolean)} except
   * that the permissions has already been masked against umask.
   */
  public boolean primitiveMkdir(String src, FsPermission absPermission, 
    boolean createParent)
    throws IOException {
    checkOpen();
    if (absPermission == null) {
      absPermission = 
        FsPermission.getDefault().applyUMask(dfsClientConf.uMask);
    } 

    if(LOG.isDebugEnabled()) {
      LOG.debug(src + ": masked=" + absPermission);
    }
    try {
      return namenode.mkdirs(src, absPermission, createParent);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     InvalidPathException.class,
                                     FileAlreadyExistsException.class,
                                     FileNotFoundException.class,
                                     ParentNotDirectoryException.class,
                                     SafeModeException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    }
  }
  
  /**
   * Get {@link ContentSummary} rooted at the specified directory.
   * @param path The string representation of the path
   * 
   * @see ClientProtocol#getContentSummary(String)
   */
  ContentSummary getContentSummary(String src) throws IOException {
    try {
      return namenode.getContentSummary(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Sets or resets quotas for a directory.
   * @see ClientProtocol#setQuota(String, long, long)
   */
  void setQuota(String src, long namespaceQuota, long diskspaceQuota) 
      throws IOException {
    // sanity check
    if ((namespaceQuota <= 0 && namespaceQuota != HdfsConstants.QUOTA_DONT_SET &&
         namespaceQuota != HdfsConstants.QUOTA_RESET) ||
        (diskspaceQuota <= 0 && diskspaceQuota != HdfsConstants.QUOTA_DONT_SET &&
         diskspaceQuota != HdfsConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Invalid values for quota : " +
                                         namespaceQuota + " and " + 
                                         diskspaceQuota);
                                         
    }
    try {
      namenode.setQuota(src, namespaceQuota, diskspaceQuota);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    }
  }

  /**
   * set the modification and access time of a file
   * 
   * @see ClientProtocol#setTimes(String, long, long)
   */
  public void setTimes(String src, long mtime, long atime) throws IOException {
    checkOpen();
    try {
      namenode.setTimes(src, mtime, atime);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    }
  }

  /**
   * @deprecated use {@link HdfsDataInputStream} instead.
   */
  @Deprecated
  public static class DFSDataInputStream extends HdfsDataInputStream {

    public DFSDataInputStream(DFSInputStream in) throws IOException {
      super(in);
    }
  }

  void reportChecksumFailure(String file, ExtendedBlock blk, DatanodeInfo dn) {
    DatanodeInfo [] dnArr = { dn };
    LocatedBlock [] lblocks = { new LocatedBlock(blk, dnArr) };
    reportChecksumFailure(file, lblocks);
  }
    
  // just reports checksum failure and ignores any exception during the report.
  void reportChecksumFailure(String file, LocatedBlock lblocks[]) {
    try {
      reportBadBlocks(lblocks);
    } catch (IOException ie) {
      LOG.info("Found corruption while reading " + file
          + ". Error repairing corrupt blocks. Bad blocks remain.", ie);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[clientName=" + clientName
        + ", ugi=" + ugi + "]"; 
  }

  public CachingStrategy getDefaultReadCachingStrategy() {
    return defaultReadCachingStrategy;
  }

  public CachingStrategy getDefaultWriteCachingStrategy() {
    return defaultWriteCachingStrategy;
  }

  public ClientContext getClientContext() {
    return clientContext;
  }

  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    checkOpen();
    try {
      namenode.modifyAclEntries(src, aclSpec);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     AclException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     SafeModeException.class,
                                     SnapshotAccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }

  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    checkOpen();
    try {
      namenode.removeAclEntries(src, aclSpec);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     AclException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     SafeModeException.class,
                                     SnapshotAccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }

  public void removeDefaultAcl(String src) throws IOException {
    checkOpen();
    try {
      namenode.removeDefaultAcl(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     AclException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     SafeModeException.class,
                                     SnapshotAccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }

  public void removeAcl(String src) throws IOException {
    checkOpen();
    try {
      namenode.removeAcl(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     AclException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     SafeModeException.class,
                                     SnapshotAccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }

  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    checkOpen();
    try {
      namenode.setAcl(src, aclSpec);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     AclException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     SafeModeException.class,
                                     SnapshotAccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }

  public AclStatus getAclStatus(String src) throws IOException {
    checkOpen();
    try {
      return namenode.getAclStatus(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     AclException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }
  
  public void setXAttr(String src, String name, byte[] value, 
      EnumSet<XAttrSetFlag> flag) throws IOException {
    checkOpen();
    try {
      namenode.setXAttr(src, XAttrHelper.buildXAttr(name, value), flag);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     SafeModeException.class,
                                     SnapshotAccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }
  
  public byte[] getXAttr(String src, String name) throws IOException {
    checkOpen();
    try {
      final List<XAttr> xAttrs = XAttrHelper.buildXAttrAsList(name);
      final List<XAttr> result = namenode.getXAttrs(src, xAttrs);
      return XAttrHelper.getFirstXAttrValue(result);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }
  
  public Map<String, byte[]> getXAttrs(String src) throws IOException {
    checkOpen();
    try {
      return XAttrHelper.buildXAttrMap(namenode.getXAttrs(src, null));
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }
  
  public Map<String, byte[]> getXAttrs(String src, List<String> names) 
      throws IOException {
    checkOpen();
    try {
      return XAttrHelper.buildXAttrMap(namenode.getXAttrs(
          src, XAttrHelper.buildXAttrs(names)));
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }
  
  public List<String> listXAttrs(String src)
          throws IOException {
    checkOpen();
    try {
      final Map<String, byte[]> xattrs =
        XAttrHelper.buildXAttrMap(namenode.listXAttrs(src));
      return Lists.newArrayList(xattrs.keySet());
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  public void removeXAttr(String src, String name) throws IOException {
    checkOpen();
    try {
      namenode.removeXAttr(src, XAttrHelper.buildXAttr(name));
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     SafeModeException.class,
                                     SnapshotAccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }

  @Override // RemotePeerFactory
  public Peer newConnectedPeer(InetSocketAddress addr,
      Token<BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
      throws IOException {
    Peer peer = null;
    boolean success = false;
    Socket sock = null;
    try {
      sock = socketFactory.createSocket();
      NetUtils.connect(sock, addr,
        getRandomLocalInterfaceAddr(),
        dfsClientConf.socketTimeout);
      peer = TcpPeerServer.peerFromSocketAndKey(saslClient, sock, this,
          blockToken, datanodeId);
      success = true;
      return peer;
    } finally {
      if (!success) {
        IOUtils.cleanup(LOG, peer);
        IOUtils.closeSocket(sock);
      }
    }
  }

  /**
   * Create hedged reads thread pool, HEDGED_READ_THREAD_POOL, if
   * it does not already exist.
   * @param num Number of threads for hedged reads thread pool.
   * If zero, skip hedged reads thread pool creation.
   */
  private synchronized void initThreadsNumForHedgedReads(int num) {
    if (num <= 0 || HEDGED_READ_THREAD_POOL != null) return;
    HEDGED_READ_THREAD_POOL = new ThreadPoolExecutor(1, num, 60,
        TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new Daemon.DaemonFactory() {
          private final AtomicInteger threadIndex =
            new AtomicInteger(0); 
          @Override
          public Thread newThread(Runnable r) {
            Thread t = super.newThread(r);
            t.setName("hedgedRead-" +
              threadIndex.getAndIncrement());
            return t;
          }
        },
        new ThreadPoolExecutor.CallerRunsPolicy() {

      @Override
      public void rejectedExecution(Runnable runnable,
          ThreadPoolExecutor e) {
        LOG.info("Execution rejected, Executing in current thread");
        HEDGED_READ_METRIC.incHedgedReadOpsInCurThread();
        // will run in the current thread
        super.rejectedExecution(runnable, e);
      }
    });
    HEDGED_READ_THREAD_POOL.allowCoreThreadTimeOut(true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using hedged reads; pool threads=" + num);
    }
  }

  long getHedgedReadTimeout() {
    return this.hedgedReadThresholdMillis;
  }

  @VisibleForTesting
  void setHedgedReadTimeout(long timeoutMillis) {
    this.hedgedReadThresholdMillis = timeoutMillis;
  }

  ThreadPoolExecutor getHedgedReadsThreadPool() {
    return HEDGED_READ_THREAD_POOL;
  }

  boolean isHedgedReadsEnabled() {
    return (HEDGED_READ_THREAD_POOL != null) &&
      HEDGED_READ_THREAD_POOL.getMaximumPoolSize() > 0;
  }

  DFSHedgedReadMetrics getHedgedReadMetrics() {
    return HEDGED_READ_METRIC;
  }
}
