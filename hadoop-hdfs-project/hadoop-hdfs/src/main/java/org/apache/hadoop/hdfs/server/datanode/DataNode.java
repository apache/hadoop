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
package org.apache.hadoop.hdfs.server.datanode;


import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_GETSPACEUSED_CLASSNAME;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DU_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DU_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_GETSPACEUSED_JITTER_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_GETSPACEUSED_JITTER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_SPLIT_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ALLOW_SAME_DISK_TIERING;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ALLOW_SAME_DISK_TIERING_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_OOB_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_OOB_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PLUGINS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_STARTUP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DISK_BALANCER_ENABLED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DISK_BALANCER_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY;
import static org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage.PIPELINE_SETUP_APPEND_RECOVERY;
import static org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage.PIPELINE_SETUP_CREATE;
import static org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage.PIPELINE_SETUP_STREAMING_RECOVERY;
import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Preconditions.checkNotNull;
import static org.apache.hadoop.util.Time.now;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.fs.WindowsGetSpaceUsed;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ReconfigurationProtocolService;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;
import javax.management.ObjectName;
import javax.net.SocketFactory;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurableBase;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager.LockLevel;
import org.apache.hadoop.hdfs.server.datanode.checker.DatasetVolumeChecker;
import org.apache.hadoop.hdfs.server.datanode.checker.StorageLocationChecker;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.BlockPoolSlice;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.*;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.net.DomainPeerServer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.DatanodeLocalInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeVolumeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.ReconfigurationProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferServer;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ClientDatanodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DNTransferAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InterDatanodeProtocolService;
import org.apache.hadoop.hdfs.protocolPB.ClientDatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientDatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeLifelineProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.InterDatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.InterDatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.InterDatanodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.protocolPB.ReconfigurationProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ReconfigurationProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.BlockPoolTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.MetricsLoggerTask;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.datanode.erasurecode.ErasureCodingWorker;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.AddBlockPoolException;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeDiskMetrics;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodePeerMetrics;
import org.apache.hadoop.hdfs.server.datanode.web.DatanodeHttpServer;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerConstants;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.tracing.Tracer;
import org.eclipse.jetty.util.ajax.JSON;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**********************************************************
 * DataNode is a class (and program) that stores a set of
 * blocks for a DFS deployment.  A single deployment can
 * have one or many DataNodes.  Each DataNode communicates
 * regularly with a single NameNode.  It also communicates
 * with client code and other DataNodes from time to time.
 *
 * DataNodes store a series of named blocks.  The DataNode
 * allows client code to read these blocks, or to write new
 * block data.  The DataNode may also, in response to instructions
 * from its NameNode, delete blocks or copy blocks to/from other
 * DataNodes.
 *
 * The DataNode maintains just one critical table:
 *   block{@literal ->} stream of bytes (of BLOCK_SIZE or less)
 *
 * This info is stored on a local disk.  The DataNode
 * reports the table's contents to the NameNode upon startup
 * and every so often afterwards.
 *
 * DataNodes spend their lives in an endless loop of asking
 * the NameNode for something to do.  A NameNode cannot connect
 * to a DataNode directly; a NameNode simply returns values from
 * functions invoked by a DataNode.
 *
 * DataNodes maintain an open server socket so that client code 
 * or other DataNodes can read/write data.  The host/port for
 * this server is reported to the NameNode, which then sends that
 * information to clients or other DataNodes that might be interested.
 *
 **********************************************************/
@InterfaceAudience.Private
public class DataNode extends ReconfigurableBase
    implements InterDatanodeProtocol, ClientDatanodeProtocol,
        DataNodeMXBean, ReconfigurationProtocol {
  public static final Logger LOG = LoggerFactory.getLogger(DataNode.class);
  
  static{
    HdfsConfiguration.init();
  }

  public static final String DN_CLIENTTRACE_FORMAT =
        "src: %s" +      // src IP
        ", dest: %s" +   // dst IP
        ", volume: %s" + // volume
        ", bytes: %s" +  // byte count
        ", op: %s" +     // operation
        ", cliID: %s" +  // DFSClient id
        ", offset: %s" + // offset
        ", srvID: %s" +  // DatanodeRegistration
        ", blockid: %s" + // block id
        ", duration(ns): %s";  // duration time

  static final Logger CLIENT_TRACE_LOG =
      LoggerFactory.getLogger(DataNode.class.getName() + ".clienttrace");
  
  private static final String USAGE =
      "Usage: hdfs datanode [-regular | -rollback | -rollingupgrade rollback" +
      " ]\n" +
      "    -regular                 : Normal DataNode startup (default).\n" +
      "    -rollback                : Rollback a standard or rolling upgrade.\n" +
      "    -rollingupgrade rollback : Rollback a rolling upgrade operation.\n" +
      "  Refer to HDFS documentation for the difference between standard\n" +
      "  and rolling upgrades.";

  static final int CURRENT_BLOCK_FORMAT_VERSION = 1;
  public static final int MAX_VOLUME_FAILURE_TOLERATED_LIMIT = -1;
  public static final String MAX_VOLUME_FAILURES_TOLERATED_MSG =
      "should be greater than or equal to -1";

  /** A list of property that are reconfigurable at runtime. */
  private static final List<String> RECONFIGURABLE_PROPERTIES =
      Collections.unmodifiableList(
          Arrays.asList(
              DFS_DATANODE_DATA_DIR_KEY,
              DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
              DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
              DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY,
              DFS_BLOCKREPORT_INITIAL_DELAY_KEY,
              DFS_DATANODE_MAX_RECEIVER_THREADS_KEY,
              DFS_CACHEREPORT_INTERVAL_MSEC_KEY,
              DFS_DATANODE_PEER_STATS_ENABLED_KEY,
              DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY,
              DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY,
              DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY,
              DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY,
              DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY,
              DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY,
              DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY,
              DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY,
              FS_DU_INTERVAL_KEY,
              FS_GETSPACEUSED_JITTER_KEY,
              FS_GETSPACEUSED_CLASSNAME,
              DFS_DISK_BALANCER_ENABLED,
              DFS_DISK_BALANCER_PLAN_VALID_INTERVAL,
              DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY,
              DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY,
              DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY,
              DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY));

  public static final String METRICS_LOG_NAME = "DataNodeMetricsLog";

  private static final String DATANODE_HTRACE_PREFIX = "datanode.htrace.";
  private final FileIoProvider fileIoProvider;

  private static final String NETWORK_ERRORS = "networkErrors";

  /**
   * Use {@link NetUtils#createSocketAddr(String)} instead.
   */
  @Deprecated
  public static InetSocketAddress createSocketAddr(String target) {
    return NetUtils.createSocketAddr(target);
  }
  
  volatile boolean shouldRun = true;
  volatile boolean shutdownForUpgrade = false;
  private boolean shutdownInProgress = false;
  private BlockPoolManager blockPoolManager;
  volatile FsDatasetSpi<? extends FsVolumeSpi> data = null;
  private String clusterId = null;

  final AtomicInteger xmitsInProgress = new AtomicInteger();
  Daemon dataXceiverServer = null;
  DataXceiverServer xserver = null;
  Daemon localDataXceiverServer = null;
  ShortCircuitRegistry shortCircuitRegistry = null;
  ThreadGroup threadGroup = null;
  private DNConf dnConf;
  private volatile boolean heartbeatsDisabledForTests = false;
  private volatile boolean ibrDisabledForTests = false;
  private volatile boolean cacheReportsDisabledForTests = false;
  private DataStorage storage = null;

  private DatanodeHttpServer httpServer = null;
  private int infoPort;
  private int infoSecurePort;

  DataNodeMetrics metrics;
  @Nullable
  private volatile DataNodePeerMetrics peerMetrics;
  private volatile DataNodeDiskMetrics diskMetrics;
  private InetSocketAddress streamingAddr;

  private LoadingCache<String, Map<String, Long>> datanodeNetworkCounts;

  private String hostName;
  private DatanodeID id;
  
  final private String fileDescriptorPassingDisabledReason;
  boolean isBlockTokenEnabled;
  BlockPoolTokenSecretManager blockPoolTokenSecretManager;
  private boolean hasAnyBlockPoolRegistered = false;
  
  private  BlockScanner blockScanner;
  private DirectoryScanner directoryScanner = null;
  
  /** Activated plug-ins. */
  private List<ServicePlugin> plugins;
  
  // For InterDataNodeProtocol
  public RPC.Server ipcServer;

  private JvmPauseMonitor pauseMonitor;

  private SecureResources secureResources = null;
  // dataDirs must be accessed while holding the DataNode lock.
  private List<StorageLocation> dataDirs;
  private final String confVersion;
  private final long maxNumberOfBlocksToLog;
  private final boolean pipelineSupportECN;
  private final boolean pipelineSupportSlownode;

  private final List<String> usersWithLocalPathAccess;
  private final boolean connectToDnViaHostname;
  ReadaheadPool readaheadPool;
  SaslDataTransferClient saslClient;
  SaslDataTransferServer saslServer;
  private ObjectName dataNodeInfoBeanName;
  private ReentrantReadWriteLock dataNodeInfoBeanLock;
  // Test verification only
  private volatile long lastDiskErrorCheck;
  private String supergroup;
  private boolean isPermissionEnabled;
  private String dnUserName = null;
  private BlockRecoveryWorker blockRecoveryWorker;
  private ErasureCodingWorker ecWorker;
  private final Tracer tracer;
  private static final int NUM_CORES = Runtime.getRuntime()
      .availableProcessors();
  private final double congestionRatio;
  private DiskBalancer diskBalancer;
  private DataSetLockManager dataSetLockManager;

  private final ExecutorService xferService;

  @Nullable
  private final StorageLocationChecker storageLocationChecker;

  private final DatasetVolumeChecker volumeChecker;

  private final SocketFactory socketFactory;

  private static Tracer createTracer(Configuration conf) {
    return new Tracer.Builder("DataNode").
        conf(TraceUtils.wrapHadoopConf(DATANODE_HTRACE_PREFIX, conf)).
        build();
  }

  private long[] oobTimeouts; /** timeout value of each OOB type */

  private ScheduledThreadPoolExecutor metricsLoggerTimer;

  private long startTime = 0;

  private DataTransferThrottler ecReconstuctReadThrottler;
  private DataTransferThrottler ecReconstuctWriteThrottler;

  /**
   * Creates a dummy DataNode for testing purpose.
   */
  @VisibleForTesting
  @InterfaceAudience.LimitedPrivate("HDFS")
  DataNode(final Configuration conf) throws DiskErrorException {
    super(conf);
    this.tracer = createTracer(conf);
    this.fileIoProvider = new FileIoProvider(conf, this);
    this.fileDescriptorPassingDisabledReason = null;
    this.maxNumberOfBlocksToLog = 0;
    this.confVersion = null;
    this.usersWithLocalPathAccess = null;
    this.connectToDnViaHostname = false;
    this.blockScanner = new BlockScanner(this, this.getConf());
    this.pipelineSupportECN = false;
    this.pipelineSupportSlownode = false;
    this.socketFactory = NetUtils.getDefaultSocketFactory(conf);
    this.dnConf = new DNConf(this);
    this.dataSetLockManager = new DataSetLockManager(conf);
    initOOBTimeout();
    storageLocationChecker = null;
    volumeChecker = new DatasetVolumeChecker(conf, new Timer());
    this.xferService =
        HadoopExecutors.newCachedThreadPool(new Daemon.DaemonFactory());
    double congestionRationTmp = conf.getDouble(DFSConfigKeys.DFS_PIPELINE_CONGESTION_RATIO,
        DFSConfigKeys.DFS_PIPELINE_CONGESTION_RATIO_DEFAULT);
    this.congestionRatio = congestionRationTmp > 0 ?
        congestionRationTmp : DFSConfigKeys.DFS_PIPELINE_CONGESTION_RATIO_DEFAULT;
  }

  /**
   * Create the DataNode given a configuration, an array of dataDirs,
   * and a namenode proxy.
   */
  DataNode(final Configuration conf,
           final List<StorageLocation> dataDirs,
           final StorageLocationChecker storageLocationChecker,
           final SecureResources resources) throws IOException {
    super(conf);
    this.tracer = createTracer(conf);
    this.fileIoProvider = new FileIoProvider(conf, this);
    this.dataSetLockManager = new DataSetLockManager(conf);
    this.blockScanner = new BlockScanner(this);
    this.lastDiskErrorCheck = 0;
    this.maxNumberOfBlocksToLog = conf.getLong(DFS_MAX_NUM_BLOCKS_TO_LOG_KEY,
        DFS_MAX_NUM_BLOCKS_TO_LOG_DEFAULT);

    this.usersWithLocalPathAccess = Arrays.asList(
        conf.getTrimmedStrings(DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY));
    this.connectToDnViaHostname = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    this.supergroup = conf.get(DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
        DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
    this.isPermissionEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY,
        DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT);
    this.pipelineSupportECN = conf.getBoolean(
        DFSConfigKeys.DFS_PIPELINE_ECN_ENABLED,
        DFSConfigKeys.DFS_PIPELINE_ECN_ENABLED_DEFAULT);
    this.pipelineSupportSlownode = conf.getBoolean(
        DFSConfigKeys.DFS_PIPELINE_SLOWNODE_ENABLED,
        DFSConfigKeys.DFS_PIPELINE_SLOWNODE_ENABLED_DEFAULT);

    confVersion = "core-" +
        conf.get("hadoop.common.configuration.version", "UNSPECIFIED") +
        ",hdfs-" +
        conf.get("hadoop.hdfs.configuration.version", "UNSPECIFIED");

    this.volumeChecker = new DatasetVolumeChecker(conf, new Timer());
    this.xferService =
        HadoopExecutors.newCachedThreadPool(new Daemon.DaemonFactory());

    // Determine whether we should try to pass file descriptors to clients.
    if (conf.getBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY,
              HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT)) {
      String reason = DomainSocket.getLoadingFailureReason();
      if (reason != null) {
        LOG.warn("File descriptor passing is disabled because {}", reason);
        this.fileDescriptorPassingDisabledReason = reason;
      } else {
        LOG.info("File descriptor passing is enabled.");
        this.fileDescriptorPassingDisabledReason = null;
      }
    } else {
      this.fileDescriptorPassingDisabledReason =
          "File descriptor passing was not configured.";
      LOG.debug(this.fileDescriptorPassingDisabledReason);
    }

    this.socketFactory = NetUtils.getDefaultSocketFactory(conf);

    try {
      hostName = getHostName(conf);
      LOG.info("Configured hostname is {}", hostName);
      startDataNode(dataDirs, resources);
    } catch (IOException ie) {
      shutdown();
      throw ie;
    }
    final int dncCacheMaxSize =
        conf.getInt(DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_KEY,
            DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_DEFAULT) ;
    datanodeNetworkCounts =
        CacheBuilder.newBuilder()
            .maximumSize(dncCacheMaxSize)
            .build(new CacheLoader<String, Map<String, Long>>() {
              @Override
              public Map<String, Long> load(String key) {
                final Map<String, Long> ret = new ConcurrentHashMap<>();
                ret.put(NETWORK_ERRORS, 0L);
                return ret;
              }
            });

    initOOBTimeout();
    this.storageLocationChecker = storageLocationChecker;
    long ecReconstuctReadBandwidth = conf.getLongBytes(
        DFSConfigKeys.DFS_DATANODE_EC_RECONSTRUCT_READ_BANDWIDTHPERSEC_KEY,
        DFSConfigKeys.DFS_DATANODE_EC_RECONSTRUCT_READ_BANDWIDTHPERSEC_DEFAULT);
    long ecReconstuctWriteBandwidth = conf.getLongBytes(
        DFSConfigKeys.DFS_DATANODE_EC_RECONSTRUCT_WRITE_BANDWIDTHPERSEC_KEY,
        DFSConfigKeys.DFS_DATANODE_EC_RECONSTRUCT_WRITE_BANDWIDTHPERSEC_DEFAULT);
    this.ecReconstuctReadThrottler = ecReconstuctReadBandwidth > 0 ?
        new DataTransferThrottler(100, ecReconstuctReadBandwidth) : null;
    this.ecReconstuctWriteThrottler = ecReconstuctWriteBandwidth > 0 ?
        new DataTransferThrottler(100, ecReconstuctWriteBandwidth) : null;
    double congestionRationTmp = conf.getDouble(DFSConfigKeys.DFS_PIPELINE_CONGESTION_RATIO,
        DFSConfigKeys.DFS_PIPELINE_CONGESTION_RATIO_DEFAULT);
    this.congestionRatio = congestionRationTmp > 0 ?
        congestionRationTmp : DFSConfigKeys.DFS_PIPELINE_CONGESTION_RATIO_DEFAULT;
  }

  @Override  // ReconfigurableBase
  protected Configuration getNewConf() {
    return new HdfsConfiguration();
  }

  /**
   * {@inheritDoc }.
   */
  @Override
  public String reconfigurePropertyImpl(String property, String newVal)
      throws ReconfigurationException {
    switch (property) {
    case DFS_DATANODE_DATA_DIR_KEY: {
      IOException rootException = null;
      try {
        LOG.info("Reconfiguring {} to {}", property, newVal);
        this.refreshVolumes(newVal);
        return getConf().get(DFS_DATANODE_DATA_DIR_KEY);
      } catch (IOException e) {
        rootException = e;
      } finally {
        // Send a full block report to let NN acknowledge the volume changes.
        try {
          triggerBlockReport(
              new BlockReportOptions.Factory().setIncremental(false).build());
        } catch (IOException e) {
          LOG.warn("Exception while sending the block report after refreshing"
              + " volumes {} to {}", property, newVal, e);
          if (rootException == null) {
            rootException = e;
          }
        } finally {
          if (rootException != null) {
            throw new ReconfigurationException(property, newVal,
                getConf().get(property), rootException);
          }
        }
      }
      break;
    }
    case DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY: {
      ReconfigurationException rootException = null;
      try {
        LOG.info("Reconfiguring {} to {}", property, newVal);
        int movers;
        if (newVal == null) {
          // set to default
          movers = DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT;
        } else {
          movers = Integer.parseInt(newVal);
          if (movers <= 0) {
            rootException = new ReconfigurationException(
                property,
                newVal,
                getConf().get(property),
                new IllegalArgumentException(
                    "balancer max concurrent movers must be larger than 0"));
          }
        }
        boolean success = xserver.updateBalancerMaxConcurrentMovers(movers);
        if (!success) {
          rootException = new ReconfigurationException(
              property,
              newVal,
              getConf().get(property),
              new IllegalArgumentException(
                  "Could not modify concurrent moves thread count"));
        }
        return Integer.toString(movers);
      } catch (NumberFormatException nfe) {
        rootException = new ReconfigurationException(
            property, newVal, getConf().get(property), nfe);
      } finally {
        if (rootException != null) {
          LOG.warn(String.format(
              "Exception in updating balancer max concurrent movers %s to %s",
              property, newVal), rootException);
          throw rootException;
        }
      }
      break;
    }
    case DFS_BLOCKREPORT_INTERVAL_MSEC_KEY:
    case DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY:
    case DFS_BLOCKREPORT_INITIAL_DELAY_KEY:
      return reconfBlockReportParameters(property, newVal);
    case DFS_DATANODE_MAX_RECEIVER_THREADS_KEY:
    case DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY:
    case DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY:
    case DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY:
      return reconfDataXceiverParameters(property, newVal);
    case DFS_CACHEREPORT_INTERVAL_MSEC_KEY:
      return reconfCacheReportParameters(property, newVal);
    case DFS_DATANODE_PEER_STATS_ENABLED_KEY:
    case DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY:
    case DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY:
    case DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY:
      return reconfSlowPeerParameters(property, newVal);
    case DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY:
    case DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY:
    case DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY:
    case DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY:
    case DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY:
      return reconfSlowDiskParameters(property, newVal);
    case FS_DU_INTERVAL_KEY:
    case FS_GETSPACEUSED_JITTER_KEY:
    case FS_GETSPACEUSED_CLASSNAME:
      return reconfDfsUsageParameters(property, newVal);
    case DFS_DISK_BALANCER_ENABLED:
    case DFS_DISK_BALANCER_PLAN_VALID_INTERVAL:
      return reconfDiskBalancerParameters(property, newVal);
    case DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY:
      return reconfSlowIoWarningThresholdParameters(property, newVal);
    default:
      break;
    }
    throw new ReconfigurationException(
        property, newVal, getConf().get(property));
  }

  private String reconfDataXceiverParameters(String property, String newVal)
      throws ReconfigurationException {
    String result = null;
    try {
      LOG.info("Reconfiguring {} to {}", property, newVal);
      if (property.equals(DFS_DATANODE_MAX_RECEIVER_THREADS_KEY)) {
        Preconditions.checkNotNull(getXferServer(), "DataXceiverServer has not been initialized.");
        int threads = (newVal == null ? DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT :
            Integer.parseInt(newVal));
        result = Integer.toString(threads);
        getXferServer().setMaxXceiverCount(threads);
      } else if (property.equals(DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY)) {
        Preconditions.checkNotNull(getXferServer(), "DataXceiverServer has not been initialized.");
        long bandwidthPerSec = (newVal == null ?
            DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_DEFAULT : Long.parseLong(newVal));
        DataTransferThrottler transferThrottler = null;
        if (bandwidthPerSec > 0) {
          transferThrottler = new DataTransferThrottler(bandwidthPerSec);
        } else {
          bandwidthPerSec = 0;
        }
        result = Long.toString(bandwidthPerSec);
        getXferServer().setTransferThrottler(transferThrottler);
      } else if (property.equals(DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY)) {
        Preconditions.checkNotNull(getXferServer(), "DataXceiverServer has not been initialized.");
        long bandwidthPerSec = (newVal == null ? DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_DEFAULT :
            Long.parseLong(newVal));
        DataTransferThrottler writeThrottler = null;
        if (bandwidthPerSec > 0) {
          writeThrottler = new DataTransferThrottler(bandwidthPerSec);
        } else {
          bandwidthPerSec = 0;
        }
        result = Long.toString(bandwidthPerSec);
        getXferServer().setWriteThrottler(writeThrottler);
      } else if (property.equals(DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY)) {
        Preconditions.checkNotNull(getXferServer(), "DataXceiverServer has not been initialized.");
        long bandwidthPerSec = (newVal == null ? DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_DEFAULT :
            Long.parseLong(newVal));
        DataTransferThrottler readThrottler = null;
        if (bandwidthPerSec > 0) {
          readThrottler = new DataTransferThrottler(bandwidthPerSec);
        } else {
          bandwidthPerSec = 0;
        }
        result = Long.toString(bandwidthPerSec);
        getXferServer().setReadThrottler(readThrottler);
      }
      LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
      return result;
    } catch (IllegalArgumentException e) {
      throw new ReconfigurationException(property, newVal, getConf().get(property), e);
    }
  }

  private String reconfCacheReportParameters(String property, String newVal)
      throws ReconfigurationException {
    String result;
    try {
      LOG.info("Reconfiguring {} to {}", property, newVal);
      Preconditions.checkNotNull(dnConf, "DNConf has not been initialized.");
      long reportInterval = (newVal == null ? DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT :
          Long.parseLong(newVal));
      result = Long.toString(reportInterval);
      dnConf.setCacheReportInterval(reportInterval);
      LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
      return result;
    } catch (IllegalArgumentException e) {
      throw new ReconfigurationException(property, newVal, getConf().get(property), e);
    }
  }

  private String reconfBlockReportParameters(String property, String newVal)
      throws ReconfigurationException {
    String result = null;
    try {
      LOG.info("Reconfiguring {} to {}", property, newVal);
      if (property.equals(DFS_BLOCKREPORT_INTERVAL_MSEC_KEY)) {
        Preconditions.checkNotNull(dnConf, "DNConf has not been initialized.");
        long intervalMs = newVal == null ? DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT :
            Long.parseLong(newVal);
        result = Long.toString(intervalMs);
        dnConf.setBlockReportInterval(intervalMs);
        for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
          if (bpos != null) {
            for (BPServiceActor actor : bpos.getBPServiceActors()) {
              actor.getScheduler().setBlockReportIntervalMs(intervalMs);
            }
          }
        }
      } else if (property.equals(DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY)) {
        Preconditions.checkNotNull(dnConf, "DNConf has not been initialized.");
        long threshold = newVal == null ? DFS_BLOCKREPORT_SPLIT_THRESHOLD_DEFAULT :
            Long.parseLong(newVal);
        result = Long.toString(threshold);
        dnConf.setBlockReportSplitThreshold(threshold);
      } else if (property.equals(DFS_BLOCKREPORT_INITIAL_DELAY_KEY)) {
        Preconditions.checkNotNull(dnConf, "DNConf has not been initialized.");
        int initialDelay = newVal == null ? DFS_BLOCKREPORT_INITIAL_DELAY_DEFAULT :
            Integer.parseInt(newVal);
        result = Integer.toString(initialDelay);
        dnConf.setInitBRDelayMs(result);
      }
      LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
      return result;
    } catch (IllegalArgumentException e) {
      throw new ReconfigurationException(property, newVal, getConf().get(property), e);
    }
  }

  private String reconfSlowPeerParameters(String property, String newVal)
      throws ReconfigurationException {
    String result = null;
    try {
      LOG.info("Reconfiguring {} to {}", property, newVal);
      if (property.equals(DFS_DATANODE_PEER_STATS_ENABLED_KEY)) {
        Preconditions.checkNotNull(dnConf, "DNConf has not been initialized.");
        if (newVal != null && !newVal.equalsIgnoreCase("true")
            && !newVal.equalsIgnoreCase("false")) {
          throw new IllegalArgumentException("Not a valid Boolean value for " + property +
              " in reconfSlowPeerParameters");
        }
        boolean enable = (newVal == null ? DFS_DATANODE_PEER_STATS_ENABLED_DEFAULT :
            Boolean.parseBoolean(newVal));
        result = Boolean.toString(enable);
        dnConf.setPeerStatsEnabled(enable);
        if (enable) {
          // Create if it doesn't exist, overwrite if it does.
          peerMetrics = DataNodePeerMetrics.create(getDisplayName(), getConf());
        }
      } else if (property.equals(DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY)) {
        Preconditions.checkNotNull(peerMetrics, "DataNode peer stats may be disabled.");
        long minNodes = (newVal == null ? DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_DEFAULT :
            Long.parseLong(newVal));
        result = Long.toString(minNodes);
        peerMetrics.setMinOutlierDetectionNodes(minNodes);
      } else if (property.equals(DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY)) {
        Preconditions.checkNotNull(peerMetrics, "DataNode peer stats may be disabled.");
        long threshold = (newVal == null ? DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_DEFAULT :
            Long.parseLong(newVal));
        result = Long.toString(threshold);
        peerMetrics.setLowThresholdMs(threshold);
      } else if (property.equals(DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY)) {
        Preconditions.checkNotNull(peerMetrics, "DataNode peer stats may be disabled.");
        long minSamples = (newVal == null ?
            DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_DEFAULT :
            Long.parseLong(newVal));
        result = Long.toString(minSamples);
        peerMetrics.setMinOutlierDetectionSamples(minSamples);
      }
      LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
      return result;
    } catch (IllegalArgumentException e) {
      throw new ReconfigurationException(property, newVal, getConf().get(property), e);
    }
  }

  private String reconfSlowDiskParameters(String property, String newVal)
      throws ReconfigurationException {
    String result = null;
    try {
      LOG.info("Reconfiguring {} to {}", property, newVal);
      if (property.equals(DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY)) {
        checkNotNull(dnConf, "DNConf has not been initialized.");
        String reportInterval = (newVal == null ? DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_DEFAULT :
            newVal);
        result = reportInterval;
        dnConf.setOutliersReportIntervalMs(reportInterval);
        for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
          if (bpos != null) {
            for (BPServiceActor actor : bpos.getBPServiceActors()) {
              actor.getScheduler().setOutliersReportIntervalMs(
                  dnConf.outliersReportIntervalMs);
            }
          }
        }
      } else if (property.equals(DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY)) {
        checkNotNull(dnConf, "DNConf has not been initialized.");
        int samplingPercentage = (newVal == null ?
            DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_DEFAULT :
            Integer.parseInt(newVal));
        result = Integer.toString(samplingPercentage);
        dnConf.setFileIoProfilingSamplingPercentage(samplingPercentage);
        if (fileIoProvider != null) {
          fileIoProvider.getProfilingEventHook().setSampleRangeMax(samplingPercentage);
        }
        if (samplingPercentage > 0 && diskMetrics == null) {
          diskMetrics = new DataNodeDiskMetrics(this,
              dnConf.outliersReportIntervalMs, getConf());
        } else if (samplingPercentage <= 0 && diskMetrics != null) {
          diskMetrics.shutdownAndWait();
        }
      } else if (property.equals(DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY)) {
        checkNotNull(diskMetrics, "DataNode disk stats may be disabled.");
        long minDisks = (newVal == null ? DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_DEFAULT :
            Long.parseLong(newVal));
        result = Long.toString(minDisks);
        diskMetrics.setMinOutlierDetectionDisks(minDisks);
      } else if (property.equals(DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY)) {
        checkNotNull(diskMetrics, "DataNode disk stats may be disabled.");
        long threshold = (newVal == null ? DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_DEFAULT :
            Long.parseLong(newVal));
        result = Long.toString(threshold);
        diskMetrics.setLowThresholdMs(threshold);
      } else if (property.equals(DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY)) {
        checkNotNull(diskMetrics, "DataNode disk stats may be disabled.");
        int maxSlowDisksToExclude = (newVal == null ?
            DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_DEFAULT : Integer.parseInt(newVal));
        result = Integer.toString(maxSlowDisksToExclude);
        diskMetrics.setMaxSlowDisksToExclude(maxSlowDisksToExclude);
      }
      LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
      return result;
    } catch (IllegalArgumentException e) {
      throw new ReconfigurationException(property, newVal, getConf().get(property), e);
    }
  }

  private String reconfDfsUsageParameters(String property, String newVal)
      throws ReconfigurationException {
    String result = null;
    try {
      LOG.info("Reconfiguring {} to {}", property, newVal);
      if (data == null) {
        LOG.debug("FsDatasetSpi has not been initialized.");
        throw new IOException("FsDatasetSpi has not been initialized");
      }
      if (property.equals(FS_DU_INTERVAL_KEY)) {
        long interval = (newVal == null ? FS_DU_INTERVAL_DEFAULT :
            Long.parseLong(newVal));
        result = Long.toString(interval);
        List<FsVolumeImpl> volumeList = data.getVolumeList();
        for (FsVolumeImpl fsVolume : volumeList) {
          Map<String, BlockPoolSlice> blockPoolSlices = fsVolume.getBlockPoolSlices();
          for (BlockPoolSlice value : blockPoolSlices.values()) {
            value.updateDfsUsageConfig(interval, null, null);
          }
        }
      } else if (property.equals(FS_GETSPACEUSED_JITTER_KEY)) {
        long jitter = (newVal == null ? FS_GETSPACEUSED_JITTER_DEFAULT :
            Long.parseLong(newVal));
        result = Long.toString(jitter);
        List<FsVolumeImpl> volumeList = data.getVolumeList();
        for (FsVolumeImpl fsVolume : volumeList) {
          Map<String, BlockPoolSlice> blockPoolSlices = fsVolume.getBlockPoolSlices();
          for (BlockPoolSlice value : blockPoolSlices.values()) {
            value.updateDfsUsageConfig(null, jitter, null);
          }
        }
      } else if (property.equals(FS_GETSPACEUSED_CLASSNAME)) {
        Class<? extends GetSpaceUsed> klass;
        if (newVal == null) {
          if (Shell.WINDOWS) {
            klass = DU.class;
          } else {
            klass = WindowsGetSpaceUsed.class;
          }
        } else {
          klass = Class.forName(newVal).asSubclass(GetSpaceUsed.class);
        }
        result = klass.getName();
        List<FsVolumeImpl> volumeList = data.getVolumeList();
        for (FsVolumeImpl fsVolume : volumeList) {
          Map<String, BlockPoolSlice> blockPoolSlices = fsVolume.getBlockPoolSlices();
          for (BlockPoolSlice value : blockPoolSlices.values()) {
            value.updateDfsUsageConfig(null, null, klass);
          }
        }
      }
      LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
      return result;
    } catch (IllegalArgumentException | IOException | ClassNotFoundException e) {
      throw new ReconfigurationException(property, newVal, getConf().get(property), e);
    }
  }

  private String reconfDiskBalancerParameters(String property, String newVal)
      throws ReconfigurationException {
    String result = null;
    try {
      LOG.info("Reconfiguring {} to {}", property, newVal);
      if (property.equals(DFS_DISK_BALANCER_ENABLED)) {
        if (newVal != null && !newVal.equalsIgnoreCase("true")
            && !newVal.equalsIgnoreCase("false")) {
          throw new IllegalArgumentException("Not a valid Boolean value for " + property);
        }
        boolean enable = (newVal == null ? DFS_DISK_BALANCER_ENABLED_DEFAULT :
            Boolean.parseBoolean(newVal));
        getDiskBalancer().setDiskBalancerEnabled(enable);
        result = Boolean.toString(enable);
      } else if (property.equals(DFS_DISK_BALANCER_PLAN_VALID_INTERVAL)) {
        if (newVal == null) {
          // set to default
          long defaultInterval = getConf().getTimeDuration(
              DFS_DISK_BALANCER_PLAN_VALID_INTERVAL,
              DFS_DISK_BALANCER_PLAN_VALID_INTERVAL_DEFAULT,
              TimeUnit.MILLISECONDS);
          getDiskBalancer().setPlanValidityInterval(defaultInterval);
          result = DFS_DISK_BALANCER_PLAN_VALID_INTERVAL_DEFAULT;
        } else {
          long newInterval = getConf()
              .getTimeDurationHelper(DFS_DISK_BALANCER_PLAN_VALID_INTERVAL,
                  newVal, TimeUnit.MILLISECONDS);
          getDiskBalancer().setPlanValidityInterval(newInterval);
          result = newVal;
        }
      }
      LOG.info("RECONFIGURE* changed {} to {}", property, result);
      return result;
    } catch (IllegalArgumentException | IOException e) {
      throw new ReconfigurationException(property, newVal, getConf().get(property), e);
    }
  }

  private String reconfSlowIoWarningThresholdParameters(String property, String newVal)
      throws ReconfigurationException {
    String result;
    try {
      LOG.info("Reconfiguring {} to {}", property, newVal);
      Preconditions.checkNotNull(dnConf, "DNConf has not been initialized.");
      long slowIoWarningThreshold = (newVal == null ?
          DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT :
          Long.parseLong(newVal));
      result = Long.toString(slowIoWarningThreshold);
      dnConf.setDatanodeSlowIoWarningThresholdMs(slowIoWarningThreshold);
      LOG.info("RECONFIGURE* changed {} to {}", property, newVal);
      return result;
    } catch (IllegalArgumentException e) {
      throw new ReconfigurationException(property, newVal, getConf().get(property), e);
    }
  }

  /**
   * Get a list of the keys of the re-configurable properties in configuration.
   */
  @Override // Reconfigurable
  public Collection<String> getReconfigurableProperties() {
    return RECONFIGURABLE_PROPERTIES;
  }

  /**
   * The ECN bit for the DataNode. The DataNode should return:
   * <ul>
   *   <li>ECN.DISABLED when ECN is disabled.</li>
   *   <li>ECN.SUPPORTED when ECN is enabled but the DN still has capacity.</li>
   *   <li>ECN.CONGESTED when ECN is enabled and the DN is congested.</li>
   * </ul>
   */
  public PipelineAck.ECN getECN() {
    if (!pipelineSupportECN) {
      return PipelineAck.ECN.DISABLED;
    }
    double load = ManagementFactory.getOperatingSystemMXBean()
        .getSystemLoadAverage();
    return load > NUM_CORES * congestionRatio ? PipelineAck.ECN.CONGESTED :
        PipelineAck.ECN.SUPPORTED;
  }

  /**
   * The SLOW bit for the DataNode of the specific BlockPool.
   * The DataNode should return:
   * <ul>
   *   <li>SLOW.DISABLED when SLOW is disabled
   *   <li>SLOW.NORMAL when SLOW is enabled and DN is not slownode.</li>
   *   <li>SLOW.SLOW when SLOW is enabled and DN is slownode.</li>
   * </ul>
   */
  public PipelineAck.SLOW getSLOWByBlockPoolId(String bpId) {
    if (!pipelineSupportSlownode) {
      return PipelineAck.SLOW.DISABLED;
    }
    return isSlownodeByBlockPoolId(bpId) ? PipelineAck.SLOW.SLOW :
        PipelineAck.SLOW.NORMAL;
  }

  public FileIoProvider getFileIoProvider() {
    return fileIoProvider;
  }

  /**
   * Contains the StorageLocations for changed data volumes.
   */
  @VisibleForTesting
  static class ChangedVolumes {
    /** The storage locations of the newly added volumes. */
    List<StorageLocation> newLocations = Lists.newArrayList();
    /** The storage locations of the volumes that are removed. */
    List<StorageLocation> deactivateLocations = Lists.newArrayList();
    /** The unchanged locations that existed in the old configuration. */
    List<StorageLocation> unchangedLocations = Lists.newArrayList();
  }

  /**
   * Parse the new DFS_DATANODE_DATA_DIR value in the configuration to detect
   * changed volumes.
   * @param newVolumes a comma separated string that specifies the data volumes.
   * @return changed volumes.
   * @throws IOException if none of the directories are specified in the
   * configuration, or the storage type of a directory is changed.
   */
  @VisibleForTesting
  ChangedVolumes parseChangedVolumes(String newVolumes) throws IOException {
    Configuration conf = new Configuration();
    conf.set(DFS_DATANODE_DATA_DIR_KEY, newVolumes);
    List<StorageLocation> newStorageLocations = getStorageLocations(conf);

    if (newStorageLocations.isEmpty()) {
      throw new IOException("No directory is specified.");
    }

    // Use the existing storage locations from the current conf
    // to detect new storage additions or removals.
    Map<String, StorageLocation> existingStorageLocations = new HashMap<>();
    for (StorageLocation loc : getStorageLocations(getConf())) {
      existingStorageLocations.put(loc.getNormalizedUri().toString(), loc);
    }

    ChangedVolumes results = new ChangedVolumes();
    results.newLocations.addAll(newStorageLocations);

    for (Iterator<Storage.StorageDirectory> it = storage.dirIterator();
         it.hasNext(); ) {
      Storage.StorageDirectory dir = it.next();
      boolean found = false;
      for (Iterator<StorageLocation> newLocationItr =
           results.newLocations.iterator(); newLocationItr.hasNext();) {
        StorageLocation newLocation = newLocationItr.next();
        if (newLocation.matchesStorageDirectory(dir)) {
          StorageLocation oldLocation = existingStorageLocations.get(
              newLocation.getNormalizedUri().toString());
          if (oldLocation != null &&
              oldLocation.getStorageType() != newLocation.getStorageType()) {
            throw new IOException("Changing storage type is not allowed.");
          }
          // Update the unchanged locations as this location
          // from the new conf is really not a new one.
          newLocationItr.remove();
          results.unchangedLocations.add(newLocation);
          found = true;
          break;
        }
      }

      // New conf doesn't have the storage location which available in
      // the current storage locations. Add to the deactivateLocations list.
      if (!found) {
        LOG.info("Deactivation request received for active volume: {}",
            dir.getRoot());
        results.deactivateLocations.add(
            StorageLocation.parse(dir.getRoot().toString()));
      }
    }

    // Use the failed storage locations from the current conf
    // to detect removals in the new conf.
    if (getFSDataset().getNumFailedVolumes() > 0) {
      for (String failedStorageLocation : getFSDataset()
          .getVolumeFailureSummary().getFailedStorageLocations()) {
        boolean found = false;
        for (Iterator<StorageLocation> newLocationItr =
             results.newLocations.iterator(); newLocationItr.hasNext();) {
          StorageLocation newLocation = newLocationItr.next();
          if (newLocation.toString().equals(
              failedStorageLocation)) {
            // The failed storage is being re-added. DataNode#refreshVolumes()
            // will take care of re-assessing it.
            found = true;
            break;
          }
        }

        // New conf doesn't have this failed storage location.
        // Add to the deactivate locations list.
        if (!found) {
          LOG.info("Deactivation request received for failed volume: {}",
              failedStorageLocation);
          results.deactivateLocations.add(StorageLocation.parse(
              failedStorageLocation));
        }
      }
    }

    validateVolumesWithSameDiskTiering(results);

    return results;
  }

  /**
   * Check conflict with same disk tiering feature
   * and throws exception.
   *
   * TODO: We can add feature to
   *   allow refreshing volume with capacity ratio,
   *   and solve the case of replacing volume on same mount.
   */
  private void validateVolumesWithSameDiskTiering(ChangedVolumes
      changedVolumes) throws IOException {
    if (dnConf.getConf().getBoolean(DFS_DATANODE_ALLOW_SAME_DISK_TIERING,
        DFS_DATANODE_ALLOW_SAME_DISK_TIERING_DEFAULT)
        && data.getMountVolumeMap() != null) {
      // Check if mount already exist.
      for (StorageLocation location : changedVolumes.newLocations) {
        if (StorageType.allowSameDiskTiering(location.getStorageType())) {
          File dir = new File(location.getUri());
          // Get the first parent dir that exists to check disk mount point.
          while (!dir.exists()) {
            dir = dir.getParentFile();
            if (dir == null) {
              throw new IOException("Invalid path: "
                  + location + ": directory does not exist");
            }
          }
          DF df = new DF(dir, dnConf.getConf());
          String mount = df.getMount();
          if (data.getMountVolumeMap().hasMount(mount)) {
            String errMsg = "Disk mount " + mount
                + " already has volume, when trying to add "
                + location + ". Please try removing mounts first"
                + " or restart datanode.";
            LOG.error(errMsg);
            throw new IOException(errMsg);
          }
        }
      }
    }
  }

  /**
   * Attempts to reload data volumes with new configuration.
   * @param newVolumes a comma separated string that specifies the data volumes.
   * @throws IOException on error. If an IOException is thrown, some new volumes
   * may have been successfully added and removed.
   */
  private void refreshVolumes(String newVolumes) throws IOException {
    // Add volumes for each Namespace
    final List<NamespaceInfo> nsInfos = Lists.newArrayList();
    for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
      nsInfos.add(bpos.getNamespaceInfo());
    }
    synchronized(this) {
      Configuration conf = getConf();
      conf.set(DFS_DATANODE_DATA_DIR_KEY, newVolumes);
      ExecutorService service = null;
      int numOldDataDirs = dataDirs.size();
      ChangedVolumes changedVolumes = parseChangedVolumes(newVolumes);
      StringBuilder errorMessageBuilder = new StringBuilder();
      List<String> effectiveVolumes = Lists.newArrayList();
      for (StorageLocation sl : changedVolumes.unchangedLocations) {
        effectiveVolumes.add(sl.toString());
      }

      try {
        if (numOldDataDirs + getFSDataset().getNumFailedVolumes()
            + changedVolumes.newLocations.size()
            - changedVolumes.deactivateLocations.size() <= 0) {
          throw new IOException("Attempt to remove all volumes.");
        }
        if (!changedVolumes.newLocations.isEmpty()) {
          LOG.info("Adding new volumes: {}",
              Joiner.on(",").join(changedVolumes.newLocations));

          service = Executors
              .newFixedThreadPool(changedVolumes.newLocations.size());
          List<Future<IOException>> exceptions = Lists.newArrayList();

          checkStorageState("refreshVolumes");
          for (final StorageLocation location : changedVolumes.newLocations) {
            exceptions.add(service.submit(new Callable<IOException>() {
              @Override
              public IOException call() {
                try {
                  data.addVolume(location, nsInfos);
                } catch (IOException e) {
                  return e;
                }
                return null;
              }
            }));
          }

          for (int i = 0; i < changedVolumes.newLocations.size(); i++) {
            StorageLocation volume = changedVolumes.newLocations.get(i);
            Future<IOException> ioExceptionFuture = exceptions.get(i);
            try {
              IOException ioe = ioExceptionFuture.get();
              if (ioe != null) {
                errorMessageBuilder.append(
                    String.format("FAILED TO ADD: %s: %s%n",
                        volume, ioe.getMessage()));
                LOG.error("Failed to add volume: {}", volume, ioe);
              } else {
                effectiveVolumes.add(volume.toString());
                LOG.info("Successfully added volume: {}", volume);
              }
            } catch (Exception e) {
              errorMessageBuilder.append(
                  String.format("FAILED to ADD: %s: %s%n", volume,
                      e.toString()));
              LOG.error("Failed to add volume: {}", volume, e);
            }
          }
        }

        try {
          removeVolumes(changedVolumes.deactivateLocations);
        } catch (IOException e) {
          errorMessageBuilder.append(e.getMessage());
          LOG.error("Failed to remove volume", e);
        }

        if (errorMessageBuilder.length() > 0) {
          throw new IOException(errorMessageBuilder.toString());
        }
      } finally {
        if (service != null) {
          service.shutdown();
        }
        conf.set(DFS_DATANODE_DATA_DIR_KEY,
            Joiner.on(",").join(effectiveVolumes));
        dataDirs = getStorageLocations(conf);
      }
    }
  }

  /**
   * Remove volumes from DataNode.
   * See {@link #removeVolumes(Collection, boolean)} for details.
   *
   * @param locations the StorageLocations of the volumes to be removed.
   * @throws IOException
   */
  private void removeVolumes(final Collection<StorageLocation> locations)
    throws IOException {
    if (locations.isEmpty()) {
      return;
    }
    removeVolumes(locations, true);
  }

  /**
   * Remove volumes from DataNode.
   *
   * It does three things:
   * <li>
   *   <ul>Remove volumes and block info from FsDataset.</ul>
   *   <ul>Remove volumes from DataStorage.</ul>
   *   <ul>Reset configuration DATA_DIR and {@link #dataDirs} to represent
   *   active volumes.</ul>
   * </li>
   * @param storageLocations the absolute path of volumes.
   * @param clearFailure if true, clears the failure information related to the
   *                     volumes.
   * @throws IOException
   */
  private synchronized void removeVolumes(
      final Collection<StorageLocation> storageLocations, boolean clearFailure)
      throws IOException {
    if (storageLocations.isEmpty()) {
      return;
    }

    LOG.info(String.format("Deactivating volumes (clear failure=%b): %s",
        clearFailure, Joiner.on(",").join(storageLocations)));

    IOException ioe = null;
    checkStorageState("removeVolumes");
    // Remove volumes and block infos from FsDataset.
    data.removeVolumes(storageLocations, clearFailure);

    // Remove volumes from DataStorage.
    try {
      storage.removeVolumes(storageLocations);
    } catch (IOException e) {
      ioe = e;
    }

    // Set configuration and dataDirs to reflect volume changes.
    for (Iterator<StorageLocation> it = dataDirs.iterator(); it.hasNext(); ) {
      StorageLocation loc = it.next();
      if (storageLocations.contains(loc)) {
        it.remove();
      }
    }
    getConf().set(DFS_DATANODE_DATA_DIR_KEY, Joiner.on(",").join(dataDirs));

    if (ioe != null) {
      throw ioe;
    }
  }

  private void setClusterId(final String nsCid, final String bpid
      ) throws IOException {
    dataNodeInfoBeanLock.writeLock().lock();
    try {
      if(clusterId != null && !clusterId.equals(nsCid)) {
        throw new IOException ("Cluster IDs not matched: dn cid=" + clusterId
                + " but ns cid="+ nsCid + "; bpid=" + bpid);
      }
      // else
      clusterId = nsCid;
    } finally {
      dataNodeInfoBeanLock.writeLock().unlock();
    }
  }

  /**
   * Returns the hostname for this datanode. If the hostname is not
   * explicitly configured in the given config, then it is determined
   * via the DNS class.
   *
   * @param config configuration
   * @return the hostname (NB: may not be a FQDN)
   * @throws UnknownHostException if the dfs.datanode.dns.interface
   *    option is used and the hostname can not be determined
   */
  private static String getHostName(Configuration config)
      throws UnknownHostException {
    String name = config.get(DFS_DATANODE_HOST_NAME_KEY);
    if (name == null) {
      String dnsInterface = config.get(
          CommonConfigurationKeys.HADOOP_SECURITY_DNS_INTERFACE_KEY);
      String nameServer = config.get(
          CommonConfigurationKeys.HADOOP_SECURITY_DNS_NAMESERVER_KEY);
      boolean fallbackToHosts = false;

      if (dnsInterface == null) {
        // Try the legacy configuration keys.
        dnsInterface = config.get(DFS_DATANODE_DNS_INTERFACE_KEY);
        nameServer = config.get(DFS_DATANODE_DNS_NAMESERVER_KEY);
      } else {
        // If HADOOP_SECURITY_DNS_* is set then also attempt hosts file
        // resolution if DNS fails. We will not use hosts file resolution
        // by default to avoid breaking existing clusters.
        fallbackToHosts = true;
      }

      name = DNS.getDefaultHost(dnsInterface, nameServer, fallbackToHosts);
    }
    return name;
  }

  /**
   * @see DFSUtil#getHttpPolicy(org.apache.hadoop.conf.Configuration)
   * for information related to the different configuration options and
   * Http Policy is decided.
   */
  private void startInfoServer()
    throws IOException {
    // SecureDataNodeStarter will bind the privileged port to the channel if
    // the DN is started by JSVC, pass it along.
    ServerSocketChannel httpServerChannel = secureResources != null ?
        secureResources.getHttpServerChannel() : null;

    httpServer = new DatanodeHttpServer(getConf(), this, httpServerChannel);
    httpServer.start();
    if (httpServer.getHttpAddress() != null) {
      infoPort = httpServer.getHttpAddress().getPort();
    }
    if (httpServer.getHttpsAddress() != null) {
      infoSecurePort = httpServer.getHttpsAddress().getPort();
    }
  }

  private void startPlugins(Configuration conf) {
    try {
      plugins = conf.getInstances(DFS_DATANODE_PLUGINS_KEY,
          ServicePlugin.class);
    } catch (RuntimeException e) {
      String pluginsValue = conf.get(DFS_DATANODE_PLUGINS_KEY);
      LOG.error("Unable to load DataNode plugins. " +
              "Specified list of plugins: {}",
          pluginsValue, e);
      throw e;
    }
    for (ServicePlugin p: plugins) {
      try {
        p.start(this);
        LOG.info("Started plug-in {}", p);
      } catch (Throwable t) {
        LOG.warn("ServicePlugin {} could not be started", p, t);
      }
    }
  }

  private void initIpcServer() throws IOException {
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        getConf().getTrimmed(DFS_DATANODE_IPC_ADDRESS_KEY));
    
    // Add all the RPC protocols that the Datanode implements    
    RPC.setProtocolEngine(getConf(), ClientDatanodeProtocolPB.class,
        ProtobufRpcEngine2.class);
    ClientDatanodeProtocolServerSideTranslatorPB clientDatanodeProtocolXlator = 
          new ClientDatanodeProtocolServerSideTranslatorPB(this);
    BlockingService service = ClientDatanodeProtocolService
        .newReflectiveBlockingService(clientDatanodeProtocolXlator);
    ipcServer = new RPC.Builder(getConf())
        .setProtocol(ClientDatanodeProtocolPB.class)
        .setInstance(service)
        .setBindAddress(ipcAddr.getHostName())
        .setPort(ipcAddr.getPort())
        .setNumHandlers(
            getConf().getInt(DFS_DATANODE_HANDLER_COUNT_KEY,
                DFS_DATANODE_HANDLER_COUNT_DEFAULT)).setVerbose(false)
        .setSecretManager(blockPoolTokenSecretManager).build();

    ReconfigurationProtocolServerSideTranslatorPB reconfigurationProtocolXlator
        = new ReconfigurationProtocolServerSideTranslatorPB(this);
    service = ReconfigurationProtocolService
        .newReflectiveBlockingService(reconfigurationProtocolXlator);
    DFSUtil.addInternalPBProtocol(getConf(), ReconfigurationProtocolPB.class, service,
        ipcServer);

    InterDatanodeProtocolServerSideTranslatorPB interDatanodeProtocolXlator = 
        new InterDatanodeProtocolServerSideTranslatorPB(this);
    service = InterDatanodeProtocolService
        .newReflectiveBlockingService(interDatanodeProtocolXlator);
    DFSUtil.addInternalPBProtocol(getConf(), InterDatanodeProtocolPB.class, service,
        ipcServer);

    LOG.info("Opened IPC server at {}", ipcServer.getListenerAddress());

    // set service-level authorization security policy
    if (getConf().getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      ipcServer.refreshServiceAcl(getConf(), new HDFSPolicyProvider());
    }
  }

  /** Check whether the current user is in the superuser group. */
  private void checkSuperuserPrivilege() throws IOException, AccessControlException {
    if (!isPermissionEnabled) {
      return;
    }
    // Try to get the ugi in the RPC call.
    UserGroupInformation callerUgi = Server.getRemoteUser();
    if (callerUgi == null) {
      // This is not from RPC.
      callerUgi = UserGroupInformation.getCurrentUser();
    }

    // Is this by the DN user itself?
    assert dnUserName != null;
    if (callerUgi.getUserName().equals(dnUserName)) {
      return;
    }

    // Is the user a member of the super group?
    if (callerUgi.getGroupsSet().contains(supergroup)) {
      return;
    }
    // Not a superuser.
    throw new AccessControlException();
  }

  private void shutdownPeriodicScanners() {
    shutdownDirectoryScanner();
    blockScanner.removeAllVolumeScanners();
  }

  /**
   * See {@link DirectoryScanner}
   */
  private synchronized void initDirectoryScanner(Configuration conf) {
    if (directoryScanner != null) {
      return;
    }
    String reason = null;
    if (conf.getTimeDuration(DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY,
        DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT, TimeUnit.SECONDS) < 0) {
      reason = "verification is turned off by configuration";
    } else if ("SimulatedFSDataset".equals(data.getClass().getSimpleName())) {
      reason = "verifcation is not supported by SimulatedFSDataset";
    } 
    if (reason == null) {
      directoryScanner = new DirectoryScanner(data, conf);
      directoryScanner.start();
    } else {
      LOG.warn("Periodic Directory Tree Verification scan " +
              "is disabled because {}",
          reason);
    }
  }
  
  private synchronized void shutdownDirectoryScanner() {
    if (directoryScanner != null) {
      directoryScanner.shutdown();
    }
  }

  /**
   * Initilizes {@link DiskBalancer}.
   * @param  data - FSDataSet
   * @param conf - Config
   */
  private void initDiskBalancer(FsDatasetSpi data,
                                             Configuration conf) {
    if (this.diskBalancer != null) {
      return;
    }

    DiskBalancer.BlockMover mover = new DiskBalancer.DiskBalancerMover(data,
        conf);
    this.diskBalancer = new DiskBalancer(getDatanodeUuid(), conf, mover);
  }

  /**
   * Shutdown disk balancer.
   */
  private void shutdownDiskBalancer() {
    if (this.diskBalancer != null) {
      this.diskBalancer.shutdown();
      this.diskBalancer = null;
    }
  }

  private void initDataXceiver() throws IOException {
    // find free port or use privileged port provided
    TcpPeerServer tcpPeerServer;
    if (secureResources != null) {
      tcpPeerServer = new TcpPeerServer(secureResources);
    } else {
      int backlogLength = getConf().getInt(
          CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
          CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);
      tcpPeerServer = new TcpPeerServer(dnConf.socketWriteTimeout,
          DataNode.getStreamingAddr(getConf()), backlogLength);
    }
    if (dnConf.getTransferSocketRecvBufferSize() > 0) {
      tcpPeerServer.setReceiveBufferSize(
          dnConf.getTransferSocketRecvBufferSize());
    }
    streamingAddr = tcpPeerServer.getStreamingAddr();
    LOG.info("Opened streaming server at {}", streamingAddr);
    this.threadGroup = new ThreadGroup("dataXceiverServer");
    xserver = new DataXceiverServer(tcpPeerServer, getConf(), this);
    this.dataXceiverServer = new Daemon(threadGroup, xserver);
    this.threadGroup.setDaemon(true); // auto destroy when empty

    if (getConf().getBoolean(
        HdfsClientConfigKeys.Read.ShortCircuit.KEY,
        HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT) ||
        getConf().getBoolean(
            HdfsClientConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC,
            HdfsClientConfigKeys
              .DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT)) {
      DomainPeerServer domainPeerServer =
                getDomainPeerServer(getConf(), streamingAddr.getPort());
      if (domainPeerServer != null) {
        this.localDataXceiverServer = new Daemon(threadGroup,
            new DataXceiverServer(domainPeerServer, getConf(), this));
        LOG.info("Listening on UNIX domain socket: {}",
            domainPeerServer.getBindPath());
      }
    }
    this.shortCircuitRegistry = new ShortCircuitRegistry(getConf());
  }

  private static DomainPeerServer getDomainPeerServer(Configuration conf,
      int port) throws IOException {
    String domainSocketPath =
        conf.getTrimmed(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
            DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT);
    if (domainSocketPath.isEmpty()) {
      if (conf.getBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY,
            HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT) &&
         (!conf.getBoolean(HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
          HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT))) {
        LOG.warn("Although short-circuit local reads are configured, " +
            "they are disabled because you didn't configure {}",
            DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY);
      }
      return null;
    }
    if (DomainSocket.getLoadingFailureReason() != null) {
      throw new RuntimeException("Although a UNIX domain socket " +
          "path is configured as " + domainSocketPath + ", we cannot " +
          "start a localDataXceiverServer because " +
          DomainSocket.getLoadingFailureReason());
    }
    DomainPeerServer domainPeerServer =
      new DomainPeerServer(domainSocketPath, port);
    int recvBufferSize = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
        DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);
    if (recvBufferSize > 0) {
      domainPeerServer.setReceiveBufferSize(recvBufferSize);
    }
    return domainPeerServer;
  }
  
  // calls specific to BP
  public void notifyNamenodeReceivedBlock(ExtendedBlock block, String delHint,
      String storageUuid, boolean isOnTransientStorage) {
    BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
    if(bpos != null) {
      bpos.notifyNamenodeReceivedBlock(block, delHint, storageUuid,
          isOnTransientStorage);
    } else {
      LOG.error("Cannot find BPOfferService for reporting block received " +
              "for bpid={}", block.getBlockPoolId());
    }
  }
  
  // calls specific to BP
  protected void notifyNamenodeReceivingBlock(
      ExtendedBlock block, String storageUuid) {
    BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
    if(bpos != null) {
      bpos.notifyNamenodeReceivingBlock(block, storageUuid);
    } else {
      LOG.error("Cannot find BPOfferService for reporting block receiving " +
          "for bpid={}", block.getBlockPoolId());
    }
  }
  
  /** Notify the corresponding namenode to delete the block. */
  public void notifyNamenodeDeletedBlock(ExtendedBlock block, String storageUuid) {
    BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
    if (bpos != null) {
      bpos.notifyNamenodeDeletedBlock(block, storageUuid);
    } else {
      LOG.error("Cannot find BPOfferService for reporting block deleted for bpid="
          + block.getBlockPoolId());
    }
  }
  
  /**
   * Report a bad block which is hosted on the local DN.
   */
  public void reportBadBlocks(ExtendedBlock block) throws IOException{
    FsVolumeSpi volume = getFSDataset().getVolume(block);
    if (volume == null) {
      LOG.warn("Cannot find FsVolumeSpi to report bad block: {}", block);
      return;
    }
    reportBadBlocks(block, volume);
  }

  /**
   * Report a bad block which is hosted on the local DN.
   *
   * @param block the bad block which is hosted on the local DN
   * @param volume the volume that block is stored in and the volume
   *        must not be null
   * @throws IOException
   */
  public void reportBadBlocks(ExtendedBlock block, FsVolumeSpi volume)
      throws IOException {
    BPOfferService bpos = getBPOSForBlock(block);
    bpos.reportBadBlocks(
        block, volume.getStorageID(), volume.getStorageType());
  }

  /**
   * Report a bad block on another DN (eg if we received a corrupt replica
   * from a remote host).
   * @param srcDataNode the DN hosting the bad block
   * @param block the block itself
   */
  public void reportRemoteBadBlock(DatanodeInfo srcDataNode, ExtendedBlock block)
      throws IOException {
    BPOfferService bpos = getBPOSForBlock(block);
    bpos.reportRemoteBadBlock(srcDataNode, block);
  }

  public void reportCorruptedBlocks(
      DFSUtilClient.CorruptedBlocks corruptedBlocks) throws IOException {
    Map<ExtendedBlock, Set<DatanodeInfo>> corruptionMap =
        corruptedBlocks.getCorruptionMap();
    if (corruptionMap != null) {
      for (Map.Entry<ExtendedBlock, Set<DatanodeInfo>> entry :
          corruptionMap.entrySet()) {
        for (DatanodeInfo dnInfo : entry.getValue()) {
          reportRemoteBadBlock(dnInfo, entry.getKey());
        }
      }
    }
  }

  /**
   * Return the BPOfferService instance corresponding to the given block.
   * @return the BPOS
   * @throws IOException if no such BPOS can be found
   */
  private BPOfferService getBPOSForBlock(ExtendedBlock block)
      throws IOException {
    Preconditions.checkNotNull(block);
    BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
    if (bpos == null) {
      throw new IOException("cannot locate OfferService thread for bp="+
          block.getBlockPoolId());
    }
    return bpos;
  }

  // used only for testing
  @VisibleForTesting
  public void setHeartbeatsDisabledForTests(
      boolean heartbeatsDisabledForTests) {
    this.heartbeatsDisabledForTests = heartbeatsDisabledForTests;
  }

  @VisibleForTesting
  boolean areHeartbeatsDisabledForTests() {
    return this.heartbeatsDisabledForTests;
  }

  @VisibleForTesting
  void setIBRDisabledForTest(boolean disabled) {
    this.ibrDisabledForTests = disabled;
  }

  @VisibleForTesting
  boolean areIBRDisabledForTests() {
    return this.ibrDisabledForTests;
  }

  void setCacheReportsDisabledForTest(boolean disabled) {
    this.cacheReportsDisabledForTests = disabled;
  }

  @VisibleForTesting
  boolean areCacheReportsDisabledForTests() {
    return this.cacheReportsDisabledForTests;
  }

  /**
   * This method starts the data node with the specified conf.
   * 
   * If conf's DFS_DATANODE_FSDATASET_FACTORY_KEY property is set
   * then a simulated storage based data node is created.
   * 
   * @param dataDirectories - only for a non-simulated storage data node
   * @throws IOException
   */
  void startDataNode(List<StorageLocation> dataDirectories,
                     SecureResources resources
                     ) throws IOException {

    // settings global for all BPs in the Data Node
    this.secureResources = resources;
    synchronized (this) {
      this.dataDirs = dataDirectories;
    }
    this.dnConf = new DNConf(this);
    checkSecureConfig(dnConf, getConf(), resources);

    if (dnConf.maxLockedMemory > 0) {
      if (!NativeIO.POSIX.getCacheManipulator().verifyCanMlock()) {
        throw new RuntimeException(String.format(
            "Cannot start datanode because the configured max locked memory" +
            " size (%s) is greater than zero and native code is not available.",
            DFS_DATANODE_MAX_LOCKED_MEMORY_KEY));
      }
      if (Path.WINDOWS) {
        NativeIO.Windows.extendWorkingSetSize(dnConf.maxLockedMemory);
      } else {
        long ulimit = NativeIO.POSIX.getCacheManipulator().getMemlockLimit();
        if (dnConf.maxLockedMemory > ulimit) {
          throw new RuntimeException(String.format(
            "Cannot start datanode because the configured max locked memory" +
            " size (%s) of %d bytes is more than the datanode's available" +
            " RLIMIT_MEMLOCK ulimit of %d bytes.",
            DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
            dnConf.maxLockedMemory,
            ulimit));
        }
      }
    }
    LOG.info("Starting DataNode with maxLockedMemory = {}",
        dnConf.maxLockedMemory);

    int volFailuresTolerated = dnConf.getVolFailuresTolerated();
    int volsConfigured = dnConf.getVolsConfigured();
    if (volFailuresTolerated < MAX_VOLUME_FAILURE_TOLERATED_LIMIT
        || volFailuresTolerated >= volsConfigured) {
      throw new HadoopIllegalArgumentException("Invalid value configured for "
          + "dfs.datanode.failed.volumes.tolerated - " + volFailuresTolerated
          + ". Value configured is either less than -1 or >= "
          + "to the number of configured volumes (" + volsConfigured + ").");
    }

    storage = new DataStorage();
    
    // global DN settings
    registerMXBean();
    initDataXceiver();
    startInfoServer();
    pauseMonitor = new JvmPauseMonitor();
    pauseMonitor.init(getConf());
    pauseMonitor.start();
  
    // BlockPoolTokenSecretManager is required to create ipc server.
    this.blockPoolTokenSecretManager = new BlockPoolTokenSecretManager();

    // Login is done by now. Set the DN user name.
    dnUserName = UserGroupInformation.getCurrentUser().getUserName();
    LOG.info("dnUserName = {}", dnUserName);
    LOG.info("supergroup = {}", supergroup);
    initIpcServer();

    metrics = DataNodeMetrics.create(getConf(), getDisplayName());
    peerMetrics = dnConf.peerStatsEnabled ?
        DataNodePeerMetrics.create(getDisplayName(), getConf()) : null;
    metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);

    ecWorker = new ErasureCodingWorker(getConf(), this);
    blockRecoveryWorker = new BlockRecoveryWorker(this);

    blockPoolManager = new BlockPoolManager(this);
    blockPoolManager.refreshNamenodes(getConf());

    // Create the ReadaheadPool from the DataNode context so we can
    // exit without having to explicitly shutdown its thread pool.
    readaheadPool = ReadaheadPool.getInstance();
    saslClient = new SaslDataTransferClient(dnConf.getConf(),
        dnConf.saslPropsResolver, dnConf.trustedChannelResolver);
    saslServer = new SaslDataTransferServer(dnConf, blockPoolTokenSecretManager);
    startMetricsLogger();

    if (dnConf.diskStatsEnabled) {
      diskMetrics = new DataNodeDiskMetrics(this,
          dnConf.outliersReportIntervalMs, getConf());
    }
  }

  /**
   * Checks if the DataNode has a secure configuration if security is enabled.
   * There are 2 possible configurations that are considered secure:
   * 1. The server has bound to privileged ports for RPC and HTTP via
   *   SecureDataNodeStarter.
   * 2. The configuration enables SASL on DataTransferProtocol and HTTPS (no
   *   plain HTTP) for the HTTP server.  The SASL handshake guarantees
   *   authentication of the RPC server before a client transmits a secret, such
   *   as a block access token.  Similarly, SSL guarantees authentication of the
   *   HTTP server before a client transmits a secret, such as a delegation
   *   token.
   * It is not possible to run with both privileged ports and SASL on
   * DataTransferProtocol.  For backwards-compatibility, the connection logic
   * must check if the target port is a privileged port, and if so, skip the
   * SASL handshake.
   *
   * @param dnConf DNConf to check
   * @param conf Configuration to check
   * @param resources SecuredResources obtained for DataNode
   * @throws RuntimeException if security enabled, but configuration is insecure
   */
  private static void checkSecureConfig(DNConf dnConf, Configuration conf,
      SecureResources resources) throws RuntimeException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }

    // Abort out of inconsistent state if Kerberos is enabled
    // but block access tokens are not enabled.
    boolean isEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY,
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT);
    if (!isEnabled) {
      String errMessage = "Security is enabled but block access tokens " +
          "(via " + DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY + ") " +
          "aren't enabled. This may cause issues " +
          "when clients attempt to connect to a DataNode. Aborting DataNode";
      throw new RuntimeException(errMessage);
    }

    if (dnConf.getIgnoreSecurePortsForTesting()) {
      return;
    }

    if (resources != null) {
      final boolean httpSecured = resources.isHttpPortPrivileged()
          || DFSUtil.getHttpPolicy(conf) == HttpConfig.Policy.HTTPS_ONLY;
      final boolean rpcSecured = resources.isRpcPortPrivileged()
          || resources.isSaslEnabled();

      // Allow secure DataNode to startup if:
      // 1. Http is secure.
      // 2. Rpc is secure
      if (rpcSecured && httpSecured) {
        return;
      }
    } else {
      // Handle cases when SecureDataNodeStarter#getSecureResources is not
      // invoked
      SaslPropertiesResolver saslPropsResolver = dnConf.getSaslPropsResolver();
      if (saslPropsResolver != null &&
          DFSUtil.getHttpPolicy(conf) == HttpConfig.Policy.HTTPS_ONLY) {
        return;
      }
    }

    throw new RuntimeException("Cannot start secure DataNode due to incorrect "
        + "config. See https://cwiki.apache.org/confluence/display/HADOOP/"
        + "Secure+DataNode for details.");
  }
  
  public static String generateUuid() {
    return UUID.randomUUID().toString();
  }

  public SaslDataTransferClient getSaslClient() {
    return saslClient;
  }

  /**
   * Verify that the DatanodeUuid has been initialized. If this is a new
   * datanode then we generate a new Datanode Uuid and persist it to disk.
   *
   * @throws IOException
   */
  synchronized void checkDatanodeUuid() throws IOException {
    if (storage.getDatanodeUuid() == null) {
      storage.setDatanodeUuid(generateUuid());
      storage.writeAll();
      LOG.info("Generated and persisted new Datanode UUID {}",
          storage.getDatanodeUuid());
    }
  }

  /**
   * Create a DatanodeRegistration for a specific block pool.
   * @param nsInfo the namespace info from the first part of the NN handshake
   */
  DatanodeRegistration createBPRegistration(NamespaceInfo nsInfo) {
    StorageInfo storageInfo = storage.getBPStorage(nsInfo.getBlockPoolID());
    if (storageInfo == null) {
      // it's null in the case of SimulatedDataSet
      storageInfo = new StorageInfo(
          DataNodeLayoutVersion.getCurrentLayoutVersion(),
          nsInfo.getNamespaceID(), nsInfo.clusterID, nsInfo.getCTime(),
          NodeType.DATA_NODE);
    }

    DatanodeID dnId = new DatanodeID(
        streamingAddr.getAddress().getHostAddress(), hostName, 
        storage.getDatanodeUuid(), getXferPort(), getInfoPort(),
            infoSecurePort, getIpcPort());
    return new DatanodeRegistration(dnId, storageInfo, 
        new ExportedBlockKeys(), VersionInfo.getVersion());
  }

  /**
   * Check that the registration returned from a NameNode is consistent
   * with the information in the storage. If the storage is fresh/unformatted,
   * sets the storage ID based on this registration.
   * Also updates the block pool's state in the secret manager.
   */
  synchronized void bpRegistrationSucceeded(DatanodeRegistration bpRegistration,
      String blockPoolId) throws IOException {
    id = bpRegistration;

    if(!storage.getDatanodeUuid().equals(bpRegistration.getDatanodeUuid())) {
      throw new IOException("Inconsistent Datanode IDs. Name-node returned "
          + bpRegistration.getDatanodeUuid()
          + ". Expecting " + storage.getDatanodeUuid());
    }
    
    registerBlockPoolWithSecretManager(bpRegistration, blockPoolId);
  }
  
  /**
   * After the block pool has contacted the NN, registers that block pool
   * with the secret manager, updating it with the secrets provided by the NN.
   * @throws IOException on error
   */
  private synchronized void registerBlockPoolWithSecretManager(
      DatanodeRegistration bpRegistration, String blockPoolId) throws IOException {
    ExportedBlockKeys keys = bpRegistration.getExportedKeys();
    if (!hasAnyBlockPoolRegistered) {
      hasAnyBlockPoolRegistered = true;
      isBlockTokenEnabled = keys.isBlockTokenEnabled();
    } else {
      if (isBlockTokenEnabled != keys.isBlockTokenEnabled()) {
        throw new RuntimeException("Inconsistent configuration of block access"
            + " tokens. Either all block pools must be configured to use block"
            + " tokens, or none may be.");
      }
    }
    if (!isBlockTokenEnabled) return;
    
    if (!blockPoolTokenSecretManager.isBlockPoolRegistered(blockPoolId)) {
      long blockKeyUpdateInterval = keys.getKeyUpdateInterval();
      long blockTokenLifetime = keys.getTokenLifetime();
      LOG.info("Block token params received from NN: " +
          "for block pool {} keyUpdateInterval={} min(s), " +
          "tokenLifetime={} min(s)",
          blockPoolId, blockKeyUpdateInterval / (60 * 1000),
          blockTokenLifetime / (60 * 1000));
      final boolean enableProtobuf = getConf().getBoolean(
          DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_PROTOBUF_ENABLE,
          DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_PROTOBUF_ENABLE_DEFAULT);
      final BlockTokenSecretManager secretMgr = 
          new BlockTokenSecretManager(0, blockTokenLifetime, blockPoolId,
              dnConf.encryptionAlgorithm, enableProtobuf);
      blockPoolTokenSecretManager.addBlockPool(blockPoolId, secretMgr);
    }
  }

  /**
   * Remove the given block pool from the block scanner, dataset, and storage.
   */
  void shutdownBlockPool(BPOfferService bpos) {
    blockPoolManager.remove(bpos);
    if (bpos.hasBlockPoolId()) {
      // Possible that this is shutting down before successfully
      // registering anywhere. If that's the case, we wouldn't have
      // a block pool id
      String bpId = bpos.getBlockPoolId();

      if (blockScanner.hasAnyRegisteredScanner()) {
        blockScanner.disableBlockPoolId(bpId);
      }

      if (data != null) {
        data.shutdownBlockPool(bpId);
      }

      if (storage != null) {
        storage.removeBlockPoolStorage(bpId);
      }
    }

  }

  /**
   * One of the Block Pools has successfully connected to its NN.
   * This initializes the local storage for that block pool,
   * checks consistency of the NN's cluster ID, etc.
   * 
   * If this is the first block pool to register, this also initializes
   * the datanode-scoped storage.
   * 
   * @param bpos Block pool offer service
   * @throws IOException if the NN is inconsistent with the local storage.
   */
  void initBlockPool(BPOfferService bpos) throws IOException {
    NamespaceInfo nsInfo = bpos.getNamespaceInfo();
    if (nsInfo == null) {
      throw new IOException("NamespaceInfo not found: Block pool " + bpos
          + " should have retrieved namespace info before initBlockPool.");
    }
    
    setClusterId(nsInfo.clusterID, nsInfo.getBlockPoolID());

    // Register the new block pool with the BP manager.
    blockPoolManager.addBlockPool(bpos);
    
    // In the case that this is the first block pool to connect, initialize
    // the dataset, block scanners, etc.
    initStorage(nsInfo);

    try {
      data.addBlockPool(nsInfo.getBlockPoolID(), getConf());
    } catch (AddBlockPoolException e) {
      handleAddBlockPoolError(e);
    }
    // HDFS-14993: check disk after add the block pool info.
    checkDiskError();

    blockScanner.enableBlockPoolId(bpos.getBlockPoolId());
    initDirectoryScanner(getConf());
    initDiskBalancer(data, getConf());
  }

  /**
   * Handles an AddBlockPoolException object thrown from
   * {@link org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeList#
   * addBlockPool}. Will ensure that all volumes that encounted a
   * AddBlockPoolException are removed from the DataNode and marked as failed
   * volumes in the same way as a runtime volume failure.
   *
   * @param e this exception is a container for all IOException objects caught
   *          in FsVolumeList#addBlockPool.
   */
  private void handleAddBlockPoolError(AddBlockPoolException e)
      throws IOException {
    Map<FsVolumeSpi, IOException> unhealthyDataDirs =
        e.getFailingVolumes();
    if (unhealthyDataDirs != null && !unhealthyDataDirs.isEmpty()) {
      handleVolumeFailures(unhealthyDataDirs.keySet());
    } else {
      LOG.debug("HandleAddBlockPoolError called with empty exception list");
    }
  }

  List<BPOfferService> getAllBpOs() {
    return blockPoolManager.getAllNamenodeThreads();
  }

  BPOfferService getBPOfferService(String bpid){
    return blockPoolManager.get(bpid);
  }
  
  public int getBpOsCount() {
    return blockPoolManager.getAllNamenodeThreads().size();
  }
  
  /**
   * Initializes the {@link #data}. The initialization is done only once, when
   * handshake with the the first namenode is completed.
   */
  private void initStorage(final NamespaceInfo nsInfo) throws IOException {
    final FsDatasetSpi.Factory<? extends FsDatasetSpi<?>> factory
        = FsDatasetSpi.Factory.getFactory(getConf());
    
    if (!factory.isSimulated()) {
      final StartupOption startOpt = getStartupOption(getConf());
      if (startOpt == null) {
        throw new IOException("Startup option not set.");
      }
      final String bpid = nsInfo.getBlockPoolID();
      //read storage info, lock data dirs and transition fs state if necessary
      synchronized (this) {
        storage.recoverTransitionRead(this, nsInfo, dataDirs, startOpt);
      }
      final StorageInfo bpStorage = storage.getBPStorage(bpid);
      LOG.info("Setting up storage: nsid={};bpid={};lv={};" +
              "nsInfo={};dnuuid={}",
          bpStorage.getNamespaceID(), bpid, storage.getLayoutVersion(),
          nsInfo, storage.getDatanodeUuid());
    }

    // If this is a newly formatted DataNode then assign a new DatanodeUuid.
    checkDatanodeUuid();

    synchronized(this)  {
      if (data == null) {
        data = factory.newInstance(this, storage, getConf());
      }
    }
  }

  /**
   * Determine the http server's effective addr
   */
  public static InetSocketAddress getInfoAddr(Configuration conf) {
    return NetUtils.createSocketAddr(conf.getTrimmed(DFS_DATANODE_HTTP_ADDRESS_KEY,
        DFS_DATANODE_HTTP_ADDRESS_DEFAULT));
  }
  
  private void registerMXBean() {
    dataNodeInfoBeanName = MBeans.register("DataNode", "DataNodeInfo", this);
  }
  
  @VisibleForTesting
  public DataXceiverServer getXferServer() {
    return xserver;  
  }
  
  @VisibleForTesting
  public int getXferPort() {
    return streamingAddr.getPort();
  }

  @VisibleForTesting
  public SaslDataTransferServer getSaslServer() {
    return saslServer;
  }

  /**
   * @return name useful for logging or display
   */
  public String getDisplayName() {
    return hostName + ":" + getXferPort();
  }

  /**
   * NB: The datanode can perform data transfer on the streaming
   * address however clients are given the IPC IP address for data
   * transfer, and that may be a different address.
   * 
   * @return socket address for data transfer
   */
  public InetSocketAddress getXferAddress() {
    return streamingAddr;
  }

  /**
   * @return the datanode's IPC port
   */
  public int getIpcPort() {
    return ipcServer.getListenerAddress().getPort();
  }
  
  /**
   * get BP registration by blockPool id
   * @return BP registration object
   * @throws IOException on error
   */
  @VisibleForTesting
  public DatanodeRegistration getDNRegistrationForBP(String bpid) 
  throws IOException {
    DataNodeFaultInjector.get().noRegistration();
    BPOfferService bpos = blockPoolManager.get(bpid);
    if(bpos==null || bpos.bpRegistration==null) {
      throw new IOException("cannot find BPOfferService for bpid="+bpid);
    }
    return bpos.bpRegistration;
  }
  
  /**
   * Creates either NIO or regular depending on socketWriteTimeout.
   */
  public Socket newSocket() throws IOException {
    return socketFactory.createSocket();
  }

  /**
   * Connect to the NN. This is separated out for easier testing.
   */
  DatanodeProtocolClientSideTranslatorPB connectToNN(
      InetSocketAddress nnAddr) throws IOException {
    return new DatanodeProtocolClientSideTranslatorPB(nnAddr, getConf());
  }

  /**
   * Connect to the NN for the lifeline protocol. This is separated out for
   * easier testing.
   *
   * @param lifelineNnAddr address of lifeline RPC server
   * @return lifeline RPC proxy
   */
  DatanodeLifelineProtocolClientSideTranslatorPB connectToLifelineNN(
      InetSocketAddress lifelineNnAddr) throws IOException {
    return new DatanodeLifelineProtocolClientSideTranslatorPB(lifelineNnAddr,
        getConf());
  }

  public static InterDatanodeProtocol createInterDataNodeProtocolProxy(
      DatanodeID datanodeid, final Configuration conf, final int socketTimeout,
      final boolean connectToDnViaHostname) throws IOException {
    final String dnAddr = datanodeid.getIpcAddr(connectToDnViaHostname);
    final InetSocketAddress addr = NetUtils.createSocketAddr(dnAddr);
    LOG.debug("Connecting to datanode {} addr={}",
        dnAddr, addr);
    final UserGroupInformation loginUgi = UserGroupInformation.getLoginUser();
    try {
      return loginUgi
          .doAs(new PrivilegedExceptionAction<InterDatanodeProtocol>() {
            @Override
            public InterDatanodeProtocol run() throws IOException {
              return new InterDatanodeProtocolTranslatorPB(addr, loginUgi,
                  conf, NetUtils.getDefaultSocketFactory(conf), socketTimeout);
            }
          });
    } catch (InterruptedException ie) {
      throw new IOException(ie.getMessage());
    }
  }

  public DataNodeMetrics getMetrics() {
    return metrics;
  }

  public DataNodeDiskMetrics getDiskMetrics() {
    return diskMetrics;
  }
  
  public DataNodePeerMetrics getPeerMetrics() {
    return peerMetrics;
  }

  /** Ensure the authentication method is kerberos */
  private void checkKerberosAuthMethod(String msg) throws IOException {
    // User invoking the call must be same as the datanode user
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    if (UserGroupInformation.getCurrentUser().getAuthenticationMethod() != 
        AuthenticationMethod.KERBEROS) {
      throw new AccessControlException("Error in " + msg
          + "Only kerberos based authentication is allowed.");
    }
  }
  
  private void checkBlockLocalPathAccess() throws IOException {
    checkKerberosAuthMethod("getBlockLocalPathInfo()");
    String currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
    if (!usersWithLocalPathAccess.contains(currentUser)) {
      throw new AccessControlException(
          "Can't continue with getBlockLocalPathInfo() "
              + "authorization. The user " + currentUser
              + " is not configured in "
              + DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY);
    }
  }

  public long getMaxNumberOfBlocksToLog() {
    return maxNumberOfBlocksToLog;
  }

  @Override
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block,
      Token<BlockTokenIdentifier> token) throws IOException {
    checkBlockLocalPathAccess();
    checkBlockToken(block, token, BlockTokenIdentifier.AccessMode.READ);
    checkStorageState("getBlockLocalPathInfo");
    BlockLocalPathInfo info = data.getBlockLocalPathInfo(block);
    if (info != null) {
      LOG.trace("getBlockLocalPathInfo successful " +
          "block={} blockfile {} metafile {}",
          block, info.getBlockPath(), info.getMetaPath());
    } else {
      LOG.trace("getBlockLocalPathInfo for block={} " +
          "returning null", block);
    }

    metrics.incrBlocksGetLocalPathInfo();
    return info;
  }

  @InterfaceAudience.LimitedPrivate("HDFS")
  static public class ShortCircuitFdsUnsupportedException extends IOException {
    private static final long serialVersionUID = 1L;
    public ShortCircuitFdsUnsupportedException(String msg) {
      super(msg);
    }
  }

  @InterfaceAudience.LimitedPrivate("HDFS")
  static public class ShortCircuitFdsVersionException extends IOException {
    private static final long serialVersionUID = 1L;
    public ShortCircuitFdsVersionException(String msg) {
      super(msg);
    }
  }

  FileInputStream[] requestShortCircuitFdsForRead(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> token, int maxVersion) 
          throws ShortCircuitFdsUnsupportedException,
            ShortCircuitFdsVersionException, IOException {
    if (fileDescriptorPassingDisabledReason != null) {
      throw new ShortCircuitFdsUnsupportedException(
          fileDescriptorPassingDisabledReason);
    }
    int blkVersion = CURRENT_BLOCK_FORMAT_VERSION;
    if (maxVersion < blkVersion) {
      throw new ShortCircuitFdsVersionException("Your client is too old " +
        "to read this block!  Its format version is " + 
        blkVersion + ", but the highest format version you can read is " +
        maxVersion);
    }
    metrics.incrBlocksGetLocalPathInfo();
    FileInputStream fis[] = new FileInputStream[2];
    
    try {
      checkStorageState("requestShortCircuitFdsForRead");
      fis[0] = (FileInputStream)data.getBlockInputStream(blk, 0);
      fis[1] = DatanodeUtil.getMetaDataInputStream(blk, data);
    } catch (ClassCastException e) {
      LOG.debug("requestShortCircuitFdsForRead failed", e);
      throw new ShortCircuitFdsUnsupportedException("This DataNode's " +
          "FsDatasetSpi does not support short-circuit local reads");
    }
    return fis;
  }

  private void checkBlockToken(ExtendedBlock block,
      Token<BlockTokenIdentifier> token, AccessMode accessMode)
      throws IOException {
    if (isBlockTokenEnabled) {
      BlockTokenIdentifier id = new BlockTokenIdentifier();
      ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
      DataInputStream in = new DataInputStream(buf);
      id.readFields(in);
      LOG.debug("BlockTokenIdentifier id: {}", id);
      blockPoolTokenSecretManager.checkAccess(id, null, block, accessMode,
          null, null);
    }
  }

  /**
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   * This method can only be called by the offerService thread.
   * Otherwise, deadlock might occur.
   */
  public void shutdown() {
    stopMetricsLogger();
    if (plugins != null) {
      for (ServicePlugin p : plugins) {
        try {
          p.stop();
          LOG.info("Stopped plug-in {}", p);
        } catch (Throwable t) {
          LOG.warn("ServicePlugin {} could not be stopped", p, t);
        }
      }
    }

    List<BPOfferService> bposArray = (this.blockPoolManager == null)
        ? new ArrayList<BPOfferService>()
        : this.blockPoolManager.getAllNamenodeThreads();
    // If shutdown is not for restart, set shouldRun to false early. 
    if (!shutdownForUpgrade) {
      shouldRun = false;
    }

    // When shutting down for restart, DataXceiverServer is interrupted
    // in order to avoid any further acceptance of requests, but the peers
    // for block writes are not closed until the clients are notified.
    if (dataXceiverServer != null) {
      try {
        xserver.sendOOBToPeers();
        ((DataXceiverServer) this.dataXceiverServer.getRunnable()).kill();
        this.dataXceiverServer.interrupt();
      } catch (Exception e) {
        // Ignore, since the out of band messaging is advisory.
        LOG.trace("Exception interrupting DataXceiverServer", e);
      }
    }

    // Record the time of initial notification
    long timeNotified = Time.monotonicNow();

    if (localDataXceiverServer != null) {
      ((DataXceiverServer) this.localDataXceiverServer.getRunnable()).kill();
      this.localDataXceiverServer.interrupt();
    }

    // Terminate directory scanner and block scanner
    shutdownPeriodicScanners();
    shutdownDiskBalancer();

    // Stop the web server
    if (httpServer != null) {
      try {
        httpServer.close();
      } catch (Exception e) {
        LOG.warn("Exception shutting down DataNode HttpServer", e);
      }
    }

    volumeChecker.shutdownAndWait(1, TimeUnit.SECONDS);

    if (storageLocationChecker != null) {
      storageLocationChecker.shutdownAndWait(1, TimeUnit.SECONDS);
    }

    if (pauseMonitor != null) {
      pauseMonitor.stop();
    }

    // shouldRun is set to false here to prevent certain threads from exiting
    // before the restart prep is done.
    this.shouldRun = false;
    
    // wait reconfiguration thread, if any, to exit
    shutdownReconfigurationTask();

    LOG.info("Waiting up to 30 seconds for transfer threads to complete");
    HadoopExecutors.shutdown(this.xferService, LOG, 15L, TimeUnit.SECONDS);

    // wait for all data receiver threads to exit
    if (this.threadGroup != null) {
      int sleepMs = 2;
      while (true) {
        // When shutting down for restart, wait 1 second before forcing
        // termination of receiver threads.
        if (!this.shutdownForUpgrade ||
            (this.shutdownForUpgrade && (Time.monotonicNow() - timeNotified
                > 1000))) {
          this.threadGroup.interrupt();
          break;
        }
        LOG.info("Waiting for threadgroup to exit, active threads is {}",
                 this.threadGroup.activeCount());
        if (this.threadGroup.activeCount() == 0) {
          break;
        }
        try {
          Thread.sleep(sleepMs);
        } catch (InterruptedException e) {}
        sleepMs = sleepMs * 3 / 2; // exponential backoff
        if (sleepMs > 200) {
          sleepMs = 200;
        }
      }
      this.threadGroup = null;
    }
    if (this.dataXceiverServer != null) {
      // wait for dataXceiverServer to terminate
      try {
        this.dataXceiverServer.join();
      } catch (InterruptedException ie) {
      }
    }
    if (this.localDataXceiverServer != null) {
      // wait for localDataXceiverServer to terminate
      try {
        this.localDataXceiverServer.join();
      } catch (InterruptedException ie) {
      }
    }
    if (metrics != null) {
      metrics.setDataNodeActiveXceiversCount(0);
      metrics.setDataNodeReadActiveXceiversCount(0);
      metrics.setDataNodeWriteActiveXceiversCount(0);
      metrics.setDataNodePacketResponderCount(0);
      metrics.setDataNodeBlockRecoveryWorkerCount(0);
    }

   // IPC server needs to be shutdown late in the process, otherwise
   // shutdown command response won't get sent.
   if (ipcServer != null) {
      ipcServer.stop();
    }

    if (ecWorker != null) {
      ecWorker.shutDown();
    }

    if(blockPoolManager != null) {
      try {
        this.blockPoolManager.shutDownAll(bposArray);
      } catch (InterruptedException ie) {
        LOG.warn("Received exception in BlockPoolManager#shutDownAll", ie);
      }
    }
    
    if (storage != null) {
      try {
        this.storage.unlockAll();
      } catch (IOException ie) {
        LOG.warn("Exception when unlocking storage", ie);
      }
    }
    if (data != null) {
      data.shutdown();
    }
    if (metrics != null) {
      metrics.shutdown();
    }
    if (dnConf.diskStatsEnabled && diskMetrics != null) {
      diskMetrics.shutdownAndWait();
    }
    if (dataNodeInfoBeanName != null) {
      MBeans.unregister(dataNodeInfoBeanName);
      dataNodeInfoBeanName = null;
    }
    if (shortCircuitRegistry != null) shortCircuitRegistry.shutdown();
    LOG.info("Shutdown complete.");
    synchronized(this) {
      // it is already false, but setting it again to avoid a findbug warning.
      this.shouldRun = false;
      // Notify the main thread.
      notifyAll();
    }
    tracer.close();
    dataSetLockManager.lockLeakCheck();
  }

  /**
   * Check if there is a disk failure asynchronously
   * and if so, handle the error.
   */
  public void checkDiskErrorAsync(FsVolumeSpi volume) {
    volumeChecker.checkVolume(
        volume, (healthyVolumes, failedVolumes) -> {
          if (failedVolumes.size() > 0) {
            LOG.warn("checkDiskErrorAsync callback got {} failed volumes: {}",
                failedVolumes.size(), failedVolumes);
          } else {
            LOG.debug("checkDiskErrorAsync: no volume failures detected");
          }
          lastDiskErrorCheck = Time.monotonicNow();
          handleVolumeFailures(failedVolumes);
        });
  }

  private void handleDiskError(String failedVolumes, int failedNumber) {
    final boolean hasEnoughResources = data.hasEnoughResource();
    LOG.warn("DataNode.handleDiskError on: " +
        "[{}] Keep Running: {}", failedVolumes, hasEnoughResources);
    
    // If we have enough active valid volumes then we do not want to 
    // shutdown the DN completely.
    int dpError = hasEnoughResources ? DatanodeProtocol.DISK_ERROR  
                                     : DatanodeProtocol.FATAL_DISK_ERROR;  
    metrics.incrVolumeFailures(failedNumber);

    //inform NameNodes
    for(BPOfferService bpos: blockPoolManager.getAllNamenodeThreads()) {
      bpos.trySendErrorReport(dpError, failedVolumes);
    }
    
    if(hasEnoughResources) {
      scheduleAllBlockReport(0);
      return; // do not shutdown
    }
    
    LOG.warn("DataNode is shutting down due to failed volumes: [{}]",
        failedVolumes);
    shouldRun = false;
  }
    
  /** Number of concurrent xceivers per node. */
  @Override // DataNodeMXBean
  public int getXceiverCount() {
    if (metrics == null) {
      return 0;
    }
    return metrics.getDataNodeActiveXceiverCount();
  }

  @Override // DataNodeMXBean
  public int getActiveTransferThreadCount() {
    if (metrics == null) {
      return 0;
    }
    return metrics.getDataNodeActiveXceiverCount()
        + metrics.getDataNodePacketResponderCount()
        + metrics.getDataNodeBlockRecoveryWorkerCount();
  }

  @Override // DataNodeMXBean
  public Map<String, Map<String, Long>> getDatanodeNetworkCounts() {
    return datanodeNetworkCounts.asMap();
  }

  void incrDatanodeNetworkErrors(String host) {
    metrics.incrDatanodeNetworkErrors();

    try {
      datanodeNetworkCounts.get(host).compute(NETWORK_ERRORS,
          (key, errors) -> errors == null ? 1L : errors + 1L);
    } catch (ExecutionException e) {
      LOG.warn("Failed to increment network error counts for host: {}", host);
    }
  }

  @Override //DataNodeMXBean
  public int getXmitsInProgress() {
    return xmitsInProgress.get();
  }
  
  /**
   * Increments the xmitsInProgress count. xmitsInProgress count represents the
   * number of data replication/reconstruction tasks running currently.
   */
  public void incrementXmitsInProgress() {
    xmitsInProgress.getAndIncrement();
  }

  /**
   * Increments the xmitInProgress count by given value.
   *
   * @param delta the amount of xmitsInProgress to increase.
   * @see #incrementXmitsInProgress()
   */
  public void incrementXmitsInProcess(int delta) {
    Preconditions.checkArgument(delta >= 0);
    xmitsInProgress.getAndAdd(delta);
  }

  /**
   * Decrements the xmitsInProgress count
   */
  public void decrementXmitsInProgress() {
    xmitsInProgress.getAndDecrement();
  }

  /**
   * Decrements the xmitsInProgress count by given value.
   *
   * @see #decrementXmitsInProgress()
   */
  public void decrementXmitsInProgress(int delta) {
    Preconditions.checkArgument(delta >= 0);
    xmitsInProgress.getAndAdd(0 - delta);
  }

  private void reportBadBlock(final BPOfferService bpos,
      final ExtendedBlock block, final String msg) {
    FsVolumeSpi volume = getFSDataset().getVolume(block);
    if (volume == null) {
      LOG.warn("Cannot find FsVolumeSpi to report bad block: {}", block);
      return;
    }
    bpos.reportBadBlocks(
        block, volume.getStorageID(), volume.getStorageType());
    LOG.warn(msg);
  }

  @VisibleForTesting
  void transferBlock(ExtendedBlock block, DatanodeInfo[] xferTargets,
      StorageType[] xferTargetStorageTypes, String[] xferTargetStorageIDs)
      throws IOException {
    BPOfferService bpos = getBPOSForBlock(block);
    DatanodeRegistration bpReg = getDNRegistrationForBP(block.getBlockPoolId());

    boolean replicaNotExist = false;
    boolean replicaStateNotFinalized = false;
    boolean blockFileNotExist = false;
    boolean lengthTooShort = false;

    try {
      data.checkBlock(block, block.getNumBytes(), ReplicaState.FINALIZED);
    } catch (ReplicaNotFoundException e) {
      replicaNotExist = true;
    } catch (UnexpectedReplicaStateException e) {
      replicaStateNotFinalized = true;
    } catch (FileNotFoundException e) {
      blockFileNotExist = true;
    } catch (EOFException e) {
      lengthTooShort = true;
    } catch (IOException e) {
      // The IOException indicates not being able to access block file,
      // treat it the same here as blockFileNotExist, to trigger 
      // reporting it as a bad block
      blockFileNotExist = true;      
    }

    if (replicaNotExist || replicaStateNotFinalized) {
      String errStr = "Can't send invalid block " + block;
      LOG.info(errStr);
      bpos.trySendErrorReport(DatanodeProtocol.INVALID_BLOCK, errStr);
      return;
    }
    if (blockFileNotExist) {
      // Report back to NN bad block caused by non-existent block file.
      reportBadBlock(bpos, block, "Can't replicate block " + block
          + " because the block file doesn't exist, or is not accessible");
      return;
    }
    if (lengthTooShort) {
      // Check if NN recorded length matches on-disk length 
      // Shorter on-disk len indicates corruption so report NN the corrupt block
      reportBadBlock(bpos, block, "Can't replicate block " + block
          + " because on-disk length " + data.getLength(block) 
          + " is shorter than NameNode recorded length " + block.getNumBytes());
      return;
    }
    
    int numTargets = xferTargets.length;
    if (numTargets > 0) {
      final String xferTargetsString =
          StringUtils.join(" ", Arrays.asList(xferTargets));
      LOG.info("{} Starting thread to transfer {} to {}", bpReg, block,
          xferTargetsString);

      final DataTransfer dataTransferTask = new DataTransfer(xferTargets,
          xferTargetStorageTypes, xferTargetStorageIDs, block,
          BlockConstructionStage.PIPELINE_SETUP_CREATE, "");

      this.xferService.execute(dataTransferTask);
    }
  }

  void transferBlocks(String poolId, Block blocks[],
      DatanodeInfo[][] xferTargets, StorageType[][] xferTargetStorageTypes,
      String[][] xferTargetStorageIDs) {
    for (int i = 0; i < blocks.length; i++) {
      try {
        transferBlock(new ExtendedBlock(poolId, blocks[i]), xferTargets[i],
            xferTargetStorageTypes[i], xferTargetStorageIDs[i]);
      } catch (IOException ie) {
        LOG.warn("Failed to transfer block {}", blocks[i], ie);
      }
    }
  }

  /* ********************************************************************
  Protocol when a client reads data from Datanode (Cur Ver: 9):
  
  Client's Request :
  =================
   
     Processed in DataXceiver:
     +----------------------------------------------+
     | Common Header   | 1 byte OP == OP_READ_BLOCK |
     +----------------------------------------------+
     
     Processed in readBlock() :
     +-------------------------------------------------------------------------+
     | 8 byte Block ID | 8 byte genstamp | 8 byte start offset | 8 byte length |
     +-------------------------------------------------------------------------+
     |   vInt length   |  <DFSClient id> |
     +-----------------------------------+
     
     Client sends optional response only at the end of receiving data.
       
  DataNode Response :
  ===================
   
    In readBlock() :
    If there is an error while initializing BlockSender :
       +---------------------------+
       | 2 byte OP_STATUS_ERROR    | and connection will be closed.
       +---------------------------+
    Otherwise
       +---------------------------+
       | 2 byte OP_STATUS_SUCCESS  |
       +---------------------------+
       
    Actual data, sent by BlockSender.sendBlock() :
    
      ChecksumHeader :
      +--------------------------------------------------+
      | 1 byte CHECKSUM_TYPE | 4 byte BYTES_PER_CHECKSUM |
      +--------------------------------------------------+
      Followed by actual data in the form of PACKETS: 
      +------------------------------------+
      | Sequence of data PACKETs ....      |
      +------------------------------------+
    
    A "PACKET" is defined further below.
    
    The client reads data until it receives a packet with 
    "LastPacketInBlock" set to true or with a zero length. It then replies
    to DataNode with one of the status codes:
    - CHECKSUM_OK:    All the chunk checksums have been verified
    - SUCCESS:        Data received; checksums not verified
    - ERROR_CHECKSUM: (Currently not used) Detected invalid checksums

      +---------------+
      | 2 byte Status |
      +---------------+
    
    The DataNode expects all well behaved clients to send the 2 byte
    status code. And if the the client doesn't, the DN will close the
    connection. So the status code is optional in the sense that it
    does not affect the correctness of the data. (And the client can
    always reconnect.)
    
    PACKET : Contains a packet header, checksum and data. Amount of data
    ======== carried is set by BUFFER_SIZE.
    
      +-----------------------------------------------------+
      | 4 byte packet length (excluding packet header)      |
      +-----------------------------------------------------+
      | 8 byte offset in the block | 8 byte sequence number |
      +-----------------------------------------------------+
      | 1 byte isLastPacketInBlock                          |
      +-----------------------------------------------------+
      | 4 byte Length of actual data                        |
      +-----------------------------------------------------+
      | x byte checksum data. x is defined below            |
      +-----------------------------------------------------+
      | actual data ......                                  |
      +-----------------------------------------------------+
      
      x = (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM *
          CHECKSUM_SIZE
          
      CHECKSUM_SIZE depends on CHECKSUM_TYPE (usually, 4 for CRC32)
      
      The above packet format is used while writing data to DFS also.
      Not all the fields might be used while reading.
    
   ************************************************************************ */

  /**
   * Used for transferring a block of data.  This class
   * sends a piece of data to another DataNode.
   */
  private class DataTransfer implements Runnable {
    final DatanodeInfo[] targets;
    final StorageType[] targetStorageTypes;
    final private String[] targetStorageIds;
    final ExtendedBlock b;
    final BlockConstructionStage stage;
    final private DatanodeRegistration bpReg;
    final String clientname;
    final CachingStrategy cachingStrategy;

    /** Throttle to block replication when data transfers or writes. */
    private DataTransferThrottler throttler;

    /**
     * Connect to the first item in the target list.  Pass along the 
     * entire target list, the block, and the data.
     */
    DataTransfer(DatanodeInfo targets[], StorageType[] targetStorageTypes,
        String[] targetStorageIds, ExtendedBlock b,
        BlockConstructionStage stage, final String clientname) {
      DataTransferProtocol.LOG.debug("{}: {} (numBytes={}), stage={}, " +
              "clientname={}, targets={}, target storage types={}, " +
              "target storage IDs={}", getClass().getSimpleName(), b,
          b.getNumBytes(), stage, clientname, Arrays.asList(targets),
          targetStorageTypes == null ? "[]" :
              Arrays.asList(targetStorageTypes),
          targetStorageIds == null ? "[]" : Arrays.asList(targetStorageIds));
      this.targets = targets;
      this.targetStorageTypes = targetStorageTypes;
      this.targetStorageIds = targetStorageIds;
      this.b = b;
      this.stage = stage;
      BPOfferService bpos = blockPoolManager.get(b.getBlockPoolId());
      bpReg = bpos.bpRegistration;
      this.clientname = clientname;
      this.cachingStrategy =
          new CachingStrategy(true, getDnConf().readaheadLength);
      if (isTransfer(stage, clientname)) {
        this.throttler = xserver.getTransferThrottler();
      } else if(isWrite(stage)) {
        this.throttler = xserver.getWriteThrottler();
      }
    }

    /**
     * Do the deed, write the bytes
     */
    @Override
    public void run() {
      incrementXmitsInProgress();
      Socket sock = null;
      DataOutputStream out = null;
      DataInputStream in = null;
      BlockSender blockSender = null;
      final boolean isClient = clientname.length() > 0;
      
      try {
        final String dnAddr = targets[0].getXferAddr(connectToDnViaHostname);
        InetSocketAddress curTarget = NetUtils.createSocketAddr(dnAddr);
        LOG.debug("Connecting to datanode {}", dnAddr);
        sock = newSocket();
        NetUtils.connect(sock, curTarget, dnConf.socketTimeout);
        sock.setTcpNoDelay(dnConf.getDataTransferServerTcpNoDelay());
        sock.setSoTimeout(targets.length * dnConf.socketTimeout);

        //
        // Header info
        //
        Token<BlockTokenIdentifier> accessToken = getBlockAccessToken(b,
            EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE),
            targetStorageTypes, targetStorageIds);

        long writeTimeout = dnConf.socketWriteTimeout + 
                            HdfsConstants.WRITE_TIMEOUT_EXTENSION * (targets.length-1);
        OutputStream unbufOut = NetUtils.getOutputStream(sock, writeTimeout);
        InputStream unbufIn = NetUtils.getInputStream(sock);
        DataEncryptionKeyFactory keyFactory =
          getDataEncryptionKeyFactoryForBlock(b);
        IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut,
          unbufIn, keyFactory, accessToken, bpReg);
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            DFSUtilClient.getSmallBufferSize(getConf())));
        in = new DataInputStream(unbufIn);
        blockSender = new BlockSender(b, 0, b.getNumBytes(), 
            false, false, true, DataNode.this, null, cachingStrategy);
        DatanodeInfo srcNode = new DatanodeInfoBuilder().setNodeID(bpReg)
            .build();

        String storageId = targetStorageIds.length > 0 ?
            targetStorageIds[0] : null;
        new Sender(out).writeBlock(b, targetStorageTypes[0], accessToken,
            clientname, targets, targetStorageTypes, srcNode,
            stage, 0, 0, 0, 0, blockSender.getChecksum(), cachingStrategy,
            false, false, null, storageId,
            targetStorageIds);

        // send data & checksum
        blockSender.sendBlock(out, unbufOut, throttler);

        // no response necessary
        LOG.info("{}, at {}: Transmitted {} (numBytes={}) to {}",
            getClass().getSimpleName(), DataNode.this.getDisplayName(),
            b, b.getNumBytes(), curTarget);

        // read ack
        if (isClient) {
          DNTransferAckProto closeAck = DNTransferAckProto.parseFrom(
              PBHelperClient.vintPrefixed(in));
          LOG.debug("{}: close-ack={}", getClass().getSimpleName(), closeAck);
          if (closeAck.getStatus() != Status.SUCCESS) {
            if (closeAck.getStatus() == Status.ERROR_ACCESS_TOKEN) {
              throw new InvalidBlockTokenException(
                  "Got access token error for connect ack, targets="
                   + Arrays.asList(targets));
            } else {
              throw new IOException("Bad connect ack, targets="
                  + Arrays.asList(targets) + " status=" + closeAck.getStatus());
            }
          }
        } else {
          metrics.incrBlocksReplicated();
        }
      } catch (IOException ie) {
        handleBadBlock(b, ie, false);
        LOG.warn("{}:Failed to transfer {} to {} got",
            bpReg, b, targets[0], ie);
      } catch (Throwable t) {
        LOG.error("Failed to transfer block {}", b, t);
      } finally {
        decrementXmitsInProgress();
        IOUtils.closeStream(blockSender);
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);
      }
    }

    @Override
    public String toString() {
      return "DataTransfer " + b + " to " + Arrays.asList(targets);
    }
  }

  /***
   * Use BlockTokenSecretManager to generate block token for current user.
   */
  public Token<BlockTokenIdentifier> getBlockAccessToken(ExtendedBlock b,
      EnumSet<AccessMode> mode,
      StorageType[] storageTypes, String[] storageIds) throws IOException {
    Token<BlockTokenIdentifier> accessToken = 
        BlockTokenSecretManager.DUMMY_TOKEN;
    if (isBlockTokenEnabled) {
      accessToken = blockPoolTokenSecretManager.generateToken(b, mode,
          storageTypes, storageIds);
    }
    return accessToken;
  }

  /**
   * Returns a new DataEncryptionKeyFactory that generates a key from the
   * BlockPoolTokenSecretManager, using the block pool ID of the given block.
   *
   * @param block for which the factory needs to create a key
   * @return DataEncryptionKeyFactory for block's block pool ID
   */
  public DataEncryptionKeyFactory getDataEncryptionKeyFactoryForBlock(
      final ExtendedBlock block) {
    return new DataEncryptionKeyFactory() {
      @Override
      public DataEncryptionKey newDataEncryptionKey() {
        return dnConf.encryptDataTransfer ?
          blockPoolTokenSecretManager.generateDataEncryptionKey(
            block.getBlockPoolId()) : null;
      }
    };
  }

  /**
   * After a block becomes finalized, a datanode increases metric counter,
   * notifies namenode, and adds it to the block scanner
   * @param block block to close
   * @param delHint hint on which excess block to delete
   * @param storageUuid UUID of the storage where block is stored
   */
  void closeBlock(ExtendedBlock block, String delHint, String storageUuid,
      boolean isTransientStorage) {
    metrics.incrBlocksWritten();
    notifyNamenodeReceivedBlock(block, delHint, storageUuid,
        isTransientStorage);
  }

  /** Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  public void runDatanodeDaemon() throws IOException {

    // Verify that blockPoolManager has been started.
    if (!isDatanodeUp()) {
      throw new IOException("Failed to instantiate DataNode.");
    }

    // start dataXceiveServer
    dataXceiverServer.start();
    if (localDataXceiverServer != null) {
      localDataXceiverServer.start();
    }
    ipcServer.setTracer(tracer);
    ipcServer.start();
    startTime = now();
    startPlugins(getConf());
  }

  /**
   * A data node is considered to be up if one of the bp services is up
   */
  public boolean isDatanodeUp() {
    for (BPOfferService bp : blockPoolManager.getAllNamenodeThreads()) {
      if (bp.isAlive()) {
        return true;
      }
    }
    return false;
  }

  /** Instantiate a single datanode object. This must be run by invoking
   *  {@link DataNode#runDatanodeDaemon()} subsequently. 
   */
  public static DataNode instantiateDataNode(String args[],
                                      Configuration conf) throws IOException {
    return instantiateDataNode(args, conf, null);
  }
  
  /** Instantiate a single datanode object, along with its secure resources. 
   * This must be run by invoking{@link DataNode#runDatanodeDaemon()} 
   * subsequently. 
   */
  public static DataNode instantiateDataNode(String args [], Configuration conf,
      SecureResources resources) throws IOException {
    if (conf == null)
      conf = new HdfsConfiguration();
    
    if (args != null) {
      // parse generic hadoop options
      GenericOptionsParser hParser = new GenericOptionsParser(conf, args);
      args = hParser.getRemainingArgs();
    }
    
    if (!parseArguments(args, conf)) {
      printUsage(System.err);
      return null;
    }
    Collection<StorageLocation> dataLocations = getStorageLocations(conf);
    UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, DFS_DATANODE_KEYTAB_FILE_KEY,
        DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, getHostName(conf));
    return makeInstance(dataLocations, conf, resources);
  }

  /**
   * Get the effective file system where the path is located.
   * DF is a packaged cross-platform class, it can get volumes
   * information from current system.
   * @param path - absolute or fully qualified path
   * @param conf - the Configuration
   * @return the effective filesystem of the path
   */
  private static String getEffectiveFileSystem(
      String path, Configuration conf) {
    try {
      DF df = new DF(new File(path), conf);
      return df.getFilesystem();
    } catch (IOException ioe) {
      LOG.error("Failed to get filesystem for dir {}", path, ioe);
    }
    return null;
  }

  /**
   * Sometimes we mount different disks for different storage types
   * as the storage location. It's important to check the volume is
   * mounted rightly before initializing storage locations.
   * @param conf - Configuration
   * @param location - Storage location
   * @return false if the filesystem of location is configured and mismatch
   * with effective filesystem.
   */
  private static boolean checkFileSystemWithConfigured(
      Configuration conf, StorageLocation location) {
    String configFs = StorageType.getConf(
        conf, location.getStorageType(), "filesystem");
    if (configFs != null && !configFs.isEmpty()) {
      String effectiveFs = getEffectiveFileSystem(
          location.getUri().getPath(), conf);
      if (effectiveFs == null || !effectiveFs.equals(configFs)) {
        LOG.error("Filesystem mismatch for storage location {}. " +
            "Configured is {}, effective is {}.",
            location.getUri(), configFs, effectiveFs);
        return false;
      }
    }
    return true;
  }

  public static List<StorageLocation> getStorageLocations(Configuration conf) {
    Collection<String> rawLocations =
        conf.getTrimmedStringCollection(DFS_DATANODE_DATA_DIR_KEY);
    List<StorageLocation> locations =
        new ArrayList<StorageLocation>(rawLocations.size());

    for(String locationString : rawLocations) {
      final StorageLocation location;
      try {
        location = StorageLocation.parse(locationString);
      } catch (IOException | SecurityException ioe) {
        LOG.error("Failed to initialize storage directory {}." +
            "Exception details: {}", locationString, ioe.toString());
        // Ignore the exception.
        continue;
      }
      if(checkFileSystemWithConfigured(conf, location)) {
        locations.add(location);
      }
    }

    return locations;
  }

  /** Instantiate &amp; Start a single datanode daemon and wait for it to
   * finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  @VisibleForTesting
  public static DataNode createDataNode(String args[],
                                 Configuration conf) throws IOException {
    return createDataNode(args, conf, null);
  }
  
  /** Instantiate &amp; Start a single datanode daemon and wait for it to
   * finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  @VisibleForTesting
  @InterfaceAudience.Private
  public static DataNode createDataNode(String args[], Configuration conf,
      SecureResources resources) throws IOException {
    DataNode dn = instantiateDataNode(args, conf, resources);
    if (dn != null) {
      dn.runDatanodeDaemon();
    }
    return dn;
  }

  void join() {
    while (shouldRun) {
      try {
        blockPoolManager.joinAll();
        if (blockPoolManager.getAllNamenodeThreads().size() == 0) {
          shouldRun = false;
        }
        // Terminate if shutdown is complete or 2 seconds after all BPs
        // are shutdown.
        synchronized(this) {
          wait(2000);
        }
      } catch (InterruptedException ex) {
        LOG.warn("Received exception in Datanode#join: {}", ex.toString());
      }
    }
  }

  /**
   * Make an instance of DataNode after ensuring that at least one of the
   * given data directories (and their parent directories, if necessary)
   * can be created.
   * @param dataDirs List of directories, where the new DataNode instance should
   * keep its files.
   * @param conf Configuration instance to use.
   * @param resources Secure resources needed to run under Kerberos
   * @return DataNode instance for given list of data dirs and conf, or null if
   * no directory from this directory list can be created.
   * @throws IOException
   */
  static DataNode makeInstance(Collection<StorageLocation> dataDirs,
      Configuration conf, SecureResources resources) throws IOException {
    List<StorageLocation> locations;
    StorageLocationChecker storageLocationChecker =
        new StorageLocationChecker(conf, new Timer());
    try {
      locations = storageLocationChecker.check(conf, dataDirs);
    } catch (InterruptedException ie) {
      throw new IOException("Failed to instantiate DataNode", ie);
    }
    DefaultMetricsSystem.initialize("DataNode");

    assert locations.size() > 0 : "number of data directories should be > 0";
    return new DataNode(conf, locations, storageLocationChecker, resources);
  }

  @Override
  public String toString() {
    return "DataNode{data=" + data + ", localName='" + getDisplayName()
        + "', datanodeUuid='" + storage.getDatanodeUuid() + "', xmitsInProgress="
        + xmitsInProgress.get() + "}";
  }

  private static void printUsage(PrintStream out) {
    out.println(USAGE + "\n");
  }

  /**
   * Parse and verify command line arguments and set configuration parameters.
   *
   * @return false if passed argements are incorrect
   */
  @VisibleForTesting
  static boolean parseArguments(String args[], Configuration conf) {
    StartupOption startOpt = StartupOption.REGULAR;
    int i = 0;

    if (args != null && args.length != 0) {
      String cmd = args[i++];
      if ("-r".equalsIgnoreCase(cmd) || "--rack".equalsIgnoreCase(cmd)) {
        LOG.error("-r, --rack arguments are not supported anymore. RackID " +
            "resolution is handled by the NameNode.");
        return false;
      } else if (StartupOption.ROLLBACK.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if (StartupOption.REGULAR.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else {
        return false;
      }
    }

    setStartupOption(conf, startOpt);
    return (args == null || i == args.length);    // Fail if more than one cmd specified!
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set(DFS_DATANODE_STARTUP_KEY, opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    String value = conf.get(DFS_DATANODE_STARTUP_KEY,
                            StartupOption.REGULAR.toString());
    return StartupOption.getEnum(value);
  }

  /**
   * This methods  arranges for the data node to send 
   * the block report at the next heartbeat.
   */
  public void scheduleAllBlockReport(long delay) {
    for(BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
      bpos.scheduleBlockReport(delay);
    }
  }

  /**
   * Examples are adding and deleting blocks directly.
   * The most common usage will be when the data node's storage is simulated.
   * 
   * @return the fsdataset that stores the blocks
   */
  @VisibleForTesting
  public FsDatasetSpi<?> getFSDataset() {
    return data;
  }

  @VisibleForTesting
  /** @return the block scanner. */
  public BlockScanner getBlockScanner() {
    return blockScanner;
  }

  @VisibleForTesting
  DirectoryScanner getDirectoryScanner() {
    return directoryScanner;
  }

  @VisibleForTesting
  public BlockPoolTokenSecretManager getBlockPoolTokenSecretManager() {
    return blockPoolTokenSecretManager;
  }

  public static void secureMain(String args[], SecureResources resources) {
    int errorCode = 0;
    try {
      StringUtils.startupShutdownMessage(DataNode.class, args, LOG);
      DataNode datanode = createDataNode(args, null, resources);
      if (datanode != null) {
        datanode.join();
      } else {
        errorCode = 1;
      }
    } catch (Throwable e) {
      LOG.error("Exception in secureMain", e);
      terminate(1, e);
    } finally {
      // We need to terminate the process here because either shutdown was called
      // or some disk related conditions like volumes tolerated or volumes required
      // condition was not met. Also, In secure mode, control will go to Jsvc
      // and Datanode process hangs if it does not exit.
      LOG.warn("Exiting Datanode");
      terminate(errorCode);
    }
  }
  
  public static void main(String args[]) {
    if (DFSUtil.parseHelpArgument(args, DataNode.USAGE, System.out, true)) {
      System.exit(0);
    }

    secureMain(args, null);
  }

  // InterDataNodeProtocol implementation
  @Override // InterDatanodeProtocol
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
      throws IOException {
    checkStorageState("initReplicaRecovery");
    return data.initReplicaRecovery(rBlock);
  }

  /**
   * Update replica with the new generation stamp and length.  
   */
  @Override // InterDatanodeProtocol
  public String updateReplicaUnderRecovery(final ExtendedBlock oldBlock,
      final long recoveryId, final long newBlockId, final long newLength)
      throws IOException {
    checkStorageState("updateReplicaUnderRecovery");
    final Replica r = data.updateReplicaUnderRecovery(oldBlock,
        recoveryId, newBlockId, newLength);
    // Notify the namenode of the updated block info. This is important
    // for HA, since otherwise the standby node may lose track of the
    // block locations until the next block report.
    ExtendedBlock newBlock = new ExtendedBlock(oldBlock);
    newBlock.setGenerationStamp(recoveryId);
    newBlock.setBlockId(newBlockId);
    newBlock.setNumBytes(newLength);
    final String storageID = r.getStorageUuid();
    notifyNamenodeReceivedBlock(newBlock, null, storageID,
        r.isOnTransientStorage());
    return storageID;
  }

  @Override // ClientDataNodeProtocol
  public long getReplicaVisibleLength(final ExtendedBlock block) throws IOException {
    checkReadAccess(block);
    return data.getReplicaVisibleLength(block);
  }

  private void checkReadAccess(final ExtendedBlock block) throws IOException {
    // Make sure this node has registered for the block pool.
    try {
      getDNRegistrationForBP(block.getBlockPoolId());
    } catch (IOException e) {
      // if it has not registered with the NN, throw an exception back.
      throw new org.apache.hadoop.ipc.RetriableException(
          "Datanode not registered. Try again later.");
    }

    if (isBlockTokenEnabled) {
      Set<TokenIdentifier> tokenIds = UserGroupInformation.getCurrentUser()
          .getTokenIdentifiers();
      if (tokenIds.size() != 1) {
        throw new IOException("Can't continue since none or more than one "
            + "BlockTokenIdentifier is found.");
      }
      for (TokenIdentifier tokenId : tokenIds) {
        BlockTokenIdentifier id = (BlockTokenIdentifier) tokenId;
        LOG.debug("BlockTokenIdentifier: {}", id);
        blockPoolTokenSecretManager.checkAccess(id, null, block,
            BlockTokenIdentifier.AccessMode.READ, null, null);
      }
    }
  }

  /**
   * Transfer a replica to the datanode targets.
   * @param b the block to transfer.
   *          The corresponding replica must be an RBW or a Finalized.
   *          Its GS and numBytes will be set to
   *          the stored GS and the visible length. 
   * @param targets targets to transfer the block to
   * @param client client name
   */
  void transferReplicaForPipelineRecovery(final ExtendedBlock b,
      final DatanodeInfo[] targets, final StorageType[] targetStorageTypes,
      final String[] targetStorageIds, final String client)
      throws IOException {
    final long storedGS;
    final long visible;
    final BlockConstructionStage stage;

    //get replica information
    try (AutoCloseableLock lock = dataSetLockManager.readLock(
        LockLevel.BLOCK_POOl, b.getBlockPoolId())) {
      Block storedBlock = data.getStoredBlock(b.getBlockPoolId(),
          b.getBlockId());
      if (null == storedBlock) {
        throw new IOException(b + " not found in datanode.");
      }
      storedGS = storedBlock.getGenerationStamp();
      if (storedGS < b.getGenerationStamp()) {
        throw new IOException(storedGS
            + " = storedGS < b.getGenerationStamp(), b=" + b);
      }
      // Update the genstamp with storedGS
      b.setGenerationStamp(storedGS);
      if (data.isValidRbw(b)) {
        stage = BlockConstructionStage.TRANSFER_RBW;
        LOG.debug("Replica is being written!");
      } else if (data.isValidBlock(b)) {
        stage = BlockConstructionStage.TRANSFER_FINALIZED;
        LOG.debug("Replica is finalized!");
      } else {
        final String r = data.getReplicaString(b.getBlockPoolId(), b.getBlockId());
        throw new IOException(b + " is neither a RBW nor a Finalized, r=" + r);
      }
      visible = data.getReplicaVisibleLength(b);
    }
    //set visible length
    b.setNumBytes(visible);

    if (targets.length > 0) {
      if (LOG.isDebugEnabled()) {
        final String xferTargetsString =
            StringUtils.join(" ", Arrays.asList(targets));
        LOG.debug("Transferring a replica to {}", xferTargetsString);
      }

      final DataTransfer dataTransferTask = new DataTransfer(targets,
          targetStorageTypes, targetStorageIds, b, stage, client);

      @SuppressWarnings("unchecked")
      Future<Void> f = (Future<Void>) this.xferService.submit(dataTransferTask);
      try {
        f.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException("Pipeline recovery for " + b + " is interrupted.",
            e);
      }
    }
  }

  /**
   * Finalize a pending upgrade in response to DNA_FINALIZE.
   * @param blockPoolId the block pool to finalize
   */
  void finalizeUpgradeForPool(String blockPoolId) throws IOException {
    storage.finalizeUpgrade(blockPoolId);
  }

  static InetSocketAddress getStreamingAddr(Configuration conf) {
    return NetUtils.createSocketAddr(
        conf.getTrimmed(DFS_DATANODE_ADDRESS_KEY, DFS_DATANODE_ADDRESS_DEFAULT));
  }

  @Override // DataNodeMXBean
  public String getSoftwareVersion() {
    return VersionInfo.getVersion();
  }

  @Override // DataNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion() + ", r" + VersionInfo.getRevision();
  }
  
  @Override // DataNodeMXBean
  public String getRpcPort(){
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        this.getConf().get(DFS_DATANODE_IPC_ADDRESS_KEY));
    return Integer.toString(ipcAddr.getPort());
  }

  @Override // DataNodeMXBean
  public String getDataPort(){
    InetSocketAddress dataAddr = NetUtils.createSocketAddr(
        this.getConf().get(DFS_DATANODE_ADDRESS_KEY));
    return Integer.toString(dataAddr.getPort());
  }

  @Override // DataNodeMXBean
  public String getHttpPort(){
    return String.valueOf(infoPort);
  }

  @Override // DataNodeMXBean
  public long getDNStartedTimeInMillis() {
    return this.startTime;
  }

  public String getRevision() {
    return VersionInfo.getRevision();
  }

  /**
   * @return the datanode's http port
   */
  public int getInfoPort() {
    return infoPort;
  }

  /**
   * @return the datanode's https port
   */
  public int getInfoSecurePort() {
    return infoSecurePort;
  }

  /**
   * Returned information is a JSON representation of a map with 
   * name node host name as the key and block pool Id as the value.
   * Note that, if there are multiple NNs in an NA nameservice,
   * a given block pool may be represented twice.
   */
  @Override // DataNodeMXBean
  public String getNamenodeAddresses() {
    final Map<String, String> info = new HashMap<String, String>();
    for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
      if (bpos != null) {
        for (BPServiceActor actor : bpos.getBPServiceActors()) {
          info.put(actor.getNNSocketAddress().getHostName(),
              bpos.getBlockPoolId());
        }
      }
    }
    return JSON.toString(info);
  }

 /**
   * Return hostname of the datanode.
   */
  @Override // DataNodeMXBean
  public String getDatanodeHostname() {
    return this.hostName;
  }

  /**
   * Returned information is a JSON representation of an array,
   * each element of the array is a map contains the information
   * about a block pool service actor.
   */
  @Override // DataNodeMXBean
  public String getBPServiceActorInfo() {
    return JSON.toString(getBPServiceActorInfoMap());
  }

  @VisibleForTesting
  public List<Map<String, String>> getBPServiceActorInfoMap() {
    final List<Map<String, String>> infoArray = new ArrayList<>();
    for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
      if (bpos != null) {
        for (BPServiceActor actor : bpos.getBPServiceActors()) {
          infoArray.add(actor.getActorInfoMap());
        }
      }
    }
    return infoArray;
  }

  /**
   * Returned information is a JSON representation of a map with 
   * volume name as the key and value is a map of volume attribute 
   * keys to its values
   */
  @Override // DataNodeMXBean
  public String getVolumeInfo() {
    if (data == null) {
      LOG.debug("Storage not yet initialized.");
      return "";
    }
    return JSON.toString(data.getVolumeInfoMap());
  }
  
  @Override // DataNodeMXBean
  public String getClusterId() {
    dataNodeInfoBeanLock.readLock().lock();
    try {
      return clusterId;
    } finally {
      dataNodeInfoBeanLock.readLock().unlock();
    }
  }

  @Override // DataNodeMXBean
  public String getDiskBalancerStatus() {
    try {
      return getDiskBalancer().queryWorkStatus().toJsonString();
    } catch (IOException ex) {
      LOG.debug("Reading diskbalancer Status failed.", ex);
      return "";
    }
  }

  @Override
  public boolean isSecurityEnabled() {
    return UserGroupInformation.isSecurityEnabled();
  }

  public void refreshNamenodes(Configuration conf) throws IOException {
    blockPoolManager.refreshNamenodes(conf);
  }

  @Override // ClientDatanodeProtocol
  public void refreshNamenodes() throws IOException {
    checkSuperuserPrivilege();
    setConf(new Configuration());
    refreshNamenodes(getConf());
  }
  
  @Override // ClientDatanodeProtocol
  public void deleteBlockPool(String blockPoolId, boolean force)
      throws IOException {
    checkSuperuserPrivilege();
    LOG.info("deleteBlockPool command received for block pool {}, " +
        "force={}", blockPoolId, force);
    if (blockPoolManager.get(blockPoolId) != null) {
      LOG.warn("The block pool {} is still running, cannot be deleted.",
          blockPoolId);
      throw new IOException(
          "The block pool is still running. First do a refreshNamenodes to " +
          "shutdown the block pool service");
    }
    checkStorageState("deleteBlockPool");
    data.deleteBlockPool(blockPoolId, force);
  }

  /**
   * Check if storage has been initialized.
   * @param methodName caller name
   * @throws IOException throw IOException if not yet initialized.
   */
  private void checkStorageState(String methodName) throws IOException {
    if (data == null) {
      String message = "Storage not yet initialized for " + methodName;
      LOG.debug(message);
      throw new IOException(message);
    }
  }

  @Override // ClientDatanodeProtocol
  public synchronized void shutdownDatanode(boolean forUpgrade) throws IOException {
    checkSuperuserPrivilege();
    LOG.info("shutdownDatanode command received (upgrade={}). " +
        "Shutting down Datanode...", forUpgrade);

    // Shutdown can be called only once.
    if (shutdownInProgress) {
      throw new IOException("Shutdown already in progress.");
    }
    shutdownInProgress = true;
    shutdownForUpgrade = forUpgrade;

    // Asynchronously start the shutdown process so that the rpc response can be
    // sent back.
    Thread shutdownThread = new Thread("Async datanode shutdown thread") {
      @Override public void run() {
        if (!shutdownForUpgrade) {
          // Delay the shutdown a bit if not doing for restart.
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) { }
        }
        shutdown();
      }
    };

    shutdownThread.setDaemon(true);
    shutdownThread.start();
  }

  @Override //ClientDatanodeProtocol
  public void evictWriters() throws IOException {
    checkSuperuserPrivilege();
    LOG.info("Evicting all writers.");
    xserver.stopWriters();
  }

  @Override //ClientDatanodeProtocol
  public DatanodeLocalInfo getDatanodeInfo() {
    long uptime = ManagementFactory.getRuntimeMXBean().getUptime()/1000;
    return new DatanodeLocalInfo(VersionInfo.getVersion(),
        confVersion, uptime);
  }

  @Override // ClientDatanodeProtocol & ReconfigurationProtocol
  public void startReconfiguration() throws IOException {
    checkSuperuserPrivilege();
    startReconfigurationTask();
  }

  @Override // ClientDatanodeProtocol & ReconfigurationProtocol
  public ReconfigurationTaskStatus getReconfigurationStatus() throws IOException {
    checkSuperuserPrivilege();
    return getReconfigurationTaskStatus();
  }

  @Override // ClientDatanodeProtocol & ReconfigurationProtocol
  public List<String> listReconfigurableProperties()
      throws IOException {
    checkSuperuserPrivilege();
    return RECONFIGURABLE_PROPERTIES;
  }

  @Override // ClientDatanodeProtocol
  public void triggerBlockReport(BlockReportOptions options)
      throws IOException {
    checkSuperuserPrivilege();
    InetSocketAddress namenodeAddr = options.getNamenodeAddr();
    boolean shouldTriggerToAllNn = (namenodeAddr == null);
    for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
      if (bpos != null) {
        for (BPServiceActor actor : bpos.getBPServiceActors()) {
          if (shouldTriggerToAllNn || namenodeAddr.equals(actor.nnAddr)) {
            actor.triggerBlockReport(options);
          }
        }
      }
    }
  }

  /**
   * @param addr rpc address of the namenode
   * @return true if the datanode is connected to a NameNode at the
   * given address
   */
  public boolean isConnectedToNN(InetSocketAddress addr) {
    for (BPOfferService bpos : getAllBpOs()) {
      for (BPServiceActor bpsa : bpos.getBPServiceActors()) {
        if (addr.equals(bpsa.getNNSocketAddress())) {
          return bpsa.isAlive();
        }
      }
    }
    return false;
  }
  
  /**
   * @param bpid block pool Id
   * @return true - if BPOfferService thread is alive
   */
  public boolean isBPServiceAlive(String bpid) {
    BPOfferService bp = blockPoolManager.get(bpid);
    return bp != null ? bp.isAlive() : false;
  }

  boolean isRestarting() {
    return shutdownForUpgrade;
  }

  /**
   * A datanode is considered to be fully started if all the BP threads are
   * alive and all the block pools are initialized.
   * 
   * @return true - if the data node is fully started
   */
  public boolean isDatanodeFullyStarted() {
    return isDatanodeFullyStarted(false);
  }

  /**
   * A datanode is considered to be fully started if all the BP threads are
   * alive and all the block pools are initialized. If checkConnectionToActiveNamenode is true,
   * the datanode is considered to be fully started if it is also heartbeating to
   * active namenode in addition to the above-mentioned conditions.
   *
   * @param checkConnectionToActiveNamenode if true, performs additional check of whether datanode
   * is heartbeating to active namenode.
   * @return true if the datanode is fully started and also conditionally connected to active
   * namenode, false otherwise.
   */
  public boolean isDatanodeFullyStarted(boolean checkConnectionToActiveNamenode) {
    if (checkConnectionToActiveNamenode) {
      for (BPOfferService bp : blockPoolManager.getAllNamenodeThreads()) {
        if (!bp.isInitialized() || !bp.isAlive() || bp.getActiveNN() == null) {
          return false;
        }
      }
      return true;
    }
    for (BPOfferService bp : blockPoolManager.getAllNamenodeThreads()) {
      if (!bp.isInitialized() || !bp.isAlive()) {
        return false;
      }
    }
    return true;
  }

  @VisibleForTesting
  public DatanodeID getDatanodeId() {
    return id;
  }
  
  @VisibleForTesting
  public void clearAllBlockSecretKeys() {
    blockPoolTokenSecretManager.clearAllKeysForTesting();
  }

  @Override // ClientDatanodeProtocol
  public long getBalancerBandwidth() {
    DataXceiverServer dxcs =
                       (DataXceiverServer) this.dataXceiverServer.getRunnable();
    return dxcs.balanceThrottler.getBandwidth();
  }
  
  public DNConf getDnConf() {
    return dnConf;
  }

  public String getDatanodeUuid() {
    return storage == null ? null : storage.getDatanodeUuid();
  }

  boolean shouldRun() {
    return shouldRun;
  }

  @VisibleForTesting
  DataStorage getStorage() {
    return storage;
  }

  public ShortCircuitRegistry getShortCircuitRegistry() {
    return shortCircuitRegistry;
  }

  public DataTransferThrottler getEcReconstuctReadThrottler() {
    return ecReconstuctReadThrottler;
  }

  public DataTransferThrottler getEcReconstuctWriteThrottler() {
    return ecReconstuctWriteThrottler;
  }

  /**
   * Check the disk error synchronously.
   */
  @VisibleForTesting
  public void checkDiskError() throws IOException {
    Set<FsVolumeSpi> unhealthyVolumes;
    try {
      unhealthyVolumes = volumeChecker.checkAllVolumes(data);
      lastDiskErrorCheck = Time.monotonicNow();
    } catch (InterruptedException e) {
      LOG.error("Interrupted while running disk check", e);
      throw new IOException("Interrupted while running disk check", e);
    }

    if (unhealthyVolumes.size() > 0) {
      LOG.warn("checkDiskError got {} failed volumes - {}",
          unhealthyVolumes.size(), unhealthyVolumes);
      handleVolumeFailures(unhealthyVolumes);
    } else {
      LOG.debug("checkDiskError encountered no failures");
    }
  }

  @VisibleForTesting
  public void handleVolumeFailures(Set<FsVolumeSpi> unhealthyVolumes) {
    if (unhealthyVolumes.isEmpty()) {
      LOG.debug("handleVolumeFailures done with empty " +
          "unhealthyVolumes");
      return;
    }

    data.handleVolumeFailures(unhealthyVolumes);
    int failedNumber = unhealthyVolumes.size();
    Set<StorageLocation> unhealthyLocations = new HashSet<>(failedNumber);

    StringBuilder sb = new StringBuilder("DataNode failed volumes:");
    for (FsVolumeSpi vol : unhealthyVolumes) {
      unhealthyLocations.add(vol.getStorageLocation());
      sb.append(vol.getStorageLocation()).append(";");
    }

    try {
      // Remove all unhealthy volumes from DataNode.
      removeVolumes(unhealthyLocations, false);
    } catch (IOException e) {
      LOG.warn("Error occurred when removing unhealthy storage dirs", e);
    }
    LOG.debug("{}", sb);
    // send blockreport regarding volume failure
    handleDiskError(sb.toString(), failedNumber);
  }

  /**
   * A bad block need to be handled, either to add to blockScanner suspect queue
   * or report to NameNode directly.
   *
   * If the method is called by scanner, then the block must be a bad block, we
   * report it to NameNode directly. Otherwise if we judge it as a bad block
   * according to exception type, then we try to add the bad block to
   * blockScanner suspect queue if blockScanner is enabled, or report to
   * NameNode directly otherwise.
   *
   * @param block The suspicious block
   * @param e The exception encountered when accessing the block
   * @param fromScanner Is it from blockScanner. The blockScanner will call this
   *          method only when it's sure that the block is corrupt.
   */
  void handleBadBlock(ExtendedBlock block, IOException e, boolean fromScanner) {

    boolean isBadBlock = fromScanner || (e instanceof DiskFileCorruptException
        || e instanceof CorruptMetaHeaderException);

    if (!isBadBlock) {
      return;
    }
    if (!fromScanner && blockScanner.isEnabled()) {
      FsVolumeSpi volume = data.getVolume(block);
      if (volume == null) {
        LOG.warn("Cannot find FsVolumeSpi to handle bad block: {}", block);
        return;
      }
      blockScanner.markSuspectBlock(volume.getStorageID(), block);
    } else {
      try {
        reportBadBlocks(block);
      } catch (IOException ie) {
        LOG.warn("report bad block {} failed", block, ie);
      }
    }
  }

  @VisibleForTesting
  public long getLastDiskErrorCheck() {
    return lastDiskErrorCheck;
  }

  public BlockRecoveryWorker getBlockRecoveryWorker(){
    return blockRecoveryWorker;
  }

  public ErasureCodingWorker getErasureCodingWorker(){
    return ecWorker;
  }

  IOStreamPair connectToDN(DatanodeInfo datanodeID, int timeout,
                           ExtendedBlock block,
                           Token<BlockTokenIdentifier> blockToken)
      throws IOException {

    return DFSUtilClient.connectToDN(datanodeID, timeout, getConf(),
        saslClient, NetUtils.getDefaultSocketFactory(getConf()), false,
        getDataEncryptionKeyFactoryForBlock(block), blockToken);
  }

  /**
   * Get timeout value of each OOB type from configuration
   */
  private void initOOBTimeout() {
    final int oobStart = Status.OOB_RESTART_VALUE; // the first OOB type
    final int oobEnd = Status.OOB_RESERVED3_VALUE; // the last OOB type
    final int numOobTypes = oobEnd - oobStart + 1;
    oobTimeouts = new long[numOobTypes];

    final String[] ele = getConf().get(DFS_DATANODE_OOB_TIMEOUT_KEY,
        DFS_DATANODE_OOB_TIMEOUT_DEFAULT).split(",");
    for (int i = 0; i < numOobTypes; i++) {
      oobTimeouts[i] = (i < ele.length) ? Long.parseLong(ele[i]) : 0;
    }
  }

  /**
   * Get the timeout to be used for transmitting the OOB type
   * @return the timeout in milliseconds
   */
  public long getOOBTimeout(Status status)
      throws IOException {
    if (status.getNumber() < Status.OOB_RESTART_VALUE ||
        status.getNumber() > Status.OOB_RESERVED3_VALUE) {
      // Not an OOB.
      throw new IOException("Not an OOB status: " + status);
    }

    return oobTimeouts[status.getNumber() - Status.OOB_RESTART_VALUE];
  }

  /**
   * Start a timer to periodically write DataNode metrics to the log file. This
   * behavior can be disabled by configuration.
   *
   */
  protected void startMetricsLogger() {
    long metricsLoggerPeriodSec = getConf().getInt(
        DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY,
        DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT);

    if (metricsLoggerPeriodSec <= 0) {
      return;
    }

    // Schedule the periodic logging.
    metricsLoggerTimer = new ScheduledThreadPoolExecutor(1);
    metricsLoggerTimer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    metricsLoggerTimer.scheduleWithFixedDelay(new MetricsLoggerTask(METRICS_LOG_NAME,
        "DataNode", (short) 0), metricsLoggerPeriodSec, metricsLoggerPeriodSec,
        TimeUnit.SECONDS);
  }

  protected void stopMetricsLogger() {
    if (metricsLoggerTimer != null) {
      metricsLoggerTimer.shutdown();
      metricsLoggerTimer = null;
    }
  }

  @VisibleForTesting
  ScheduledThreadPoolExecutor getMetricsLoggerTimer() {
    return metricsLoggerTimer;
  }

  public Tracer getTracer() {
    return tracer;
  }

  /**
   * Allows submission of a disk balancer Job.
   * @param planID  - Hash value of the plan.
   * @param planVersion - Plan version, reserved for future use. We have only
   *                    version 1 now.
   * @param planFile - Plan file name
   * @param planData - Actual plan data in json format
   * @throws IOException
   */
  @Override
  public void submitDiskBalancerPlan(String planID, long planVersion,
      String planFile, String planData, boolean skipDateCheck)
      throws IOException {
    checkSuperuserPrivilege();
    if (getStartupOption(getConf()) != StartupOption.REGULAR) {
      throw new DiskBalancerException(
          "Datanode is in special state, e.g. Upgrade/Rollback etc."
              + " Disk balancing not permitted.",
          DiskBalancerException.Result.DATANODE_STATUS_NOT_REGULAR);
    }

    getDiskBalancer().submitPlan(planID, planVersion, planFile, planData,
            skipDateCheck);
  }

  /**
   * Cancels a running plan.
   * @param planID - Hash string that identifies a plan
   */
  @Override
  public void cancelDiskBalancePlan(String planID) throws
      IOException {
    checkSuperuserPrivilege();
    getDiskBalancer().cancelPlan(planID);
  }

  /**
   * Returns the status of current or last executed work plan.
   * @return DiskBalancerWorkStatus.
   * @throws IOException
   */
  @Override
  public DiskBalancerWorkStatus queryDiskBalancerPlan() throws IOException {
    checkSuperuserPrivilege();
    return getDiskBalancer().queryWorkStatus();
  }

  /**
   * Gets a runtime configuration value from  diskbalancer instance. For
   * example : DiskBalancer bandwidth.
   *
   * @param key - String that represents the run time key value.
   * @return value of the key as a string.
   * @throws IOException - Throws if there is no such key
   */
  @Override
  public String getDiskBalancerSetting(String key) throws IOException {
    checkSuperuserPrivilege();
    Preconditions.checkNotNull(key);
    switch (key) {
    case DiskBalancerConstants.DISKBALANCER_VOLUME_NAME:
      return getDiskBalancer().getVolumeNames();
    case DiskBalancerConstants.DISKBALANCER_BANDWIDTH :
      return Long.toString(getDiskBalancer().getBandwidth());
    default:
      LOG.error("Disk Balancer - Unknown key in get balancer setting. Key: {}",
          key);
      throw new DiskBalancerException("Unknown key",
          DiskBalancerException.Result.UNKNOWN_KEY);
    }
  }

  @VisibleForTesting
  void setBlockScanner(BlockScanner blockScanner) {
    this.blockScanner = blockScanner;
  }

  @Override // DataNodeMXBean
  public String getSendPacketDownstreamAvgInfo() {
    return dnConf.peerStatsEnabled && peerMetrics != null ?
        peerMetrics.dumpSendPacketDownstreamAvgInfoAsJson() : null;
  }

  @Override // DataNodeMXBean
  public String getSlowDisks() {
    if (!dnConf.diskStatsEnabled || diskMetrics == null) {
      //Disk Stats not enabled
      return null;
    }
    Set<String> slowDisks = diskMetrics.getDiskOutliersStats().keySet();
    return JSON.toString(slowDisks);
  }


  @Override
  public List<DatanodeVolumeInfo> getVolumeReport() throws IOException {
    checkSuperuserPrivilege();
    checkStorageState("getVolumeReport");
    Map<String, Object> volumeInfoMap = data.getVolumeInfoMap();
    if (volumeInfoMap == null) {
      LOG.warn("DataNode volume info not available.");
      return new ArrayList<>(0);
    }
    List<DatanodeVolumeInfo> volumeInfoList = new ArrayList<>();
    for (Entry<String, Object> volume : volumeInfoMap.entrySet()) {
      @SuppressWarnings("unchecked")
      Map<String, Object> volumeInfo = (Map<String, Object>) volume.getValue();
      DatanodeVolumeInfo dnStorageInfo = new DatanodeVolumeInfo(
          volume.getKey(), (Long) volumeInfo.get("usedSpace"),
          (Long) volumeInfo.get("freeSpace"),
          (Long) volumeInfo.get("reservedSpace"),
          (Long) volumeInfo.get("reservedSpaceForReplicas"),
          (Long) volumeInfo.get("numBlocks"),
          (StorageType) volumeInfo.get("storageType"));
      volumeInfoList.add(dnStorageInfo);
    }
    return volumeInfoList;
  }

  @VisibleForTesting
  public DiskBalancer getDiskBalancer() throws IOException {
    if (this.diskBalancer == null) {
      throw new IOException("DiskBalancer is not initialized");
    }
    return this.diskBalancer;
  }

  /**
   * Construct DataTransfer in {@link DataNode#transferBlock}, the
   * BlockConstructionStage is PIPELINE_SETUP_CREATE and clientName is "".
   */
  private static boolean isTransfer(BlockConstructionStage stage,
      String clientName) {
    if (stage == PIPELINE_SETUP_CREATE && clientName.isEmpty()) {
      return true;
    }
    return false;
  }

  /**
   * Construct DataTransfer in
   * {@link DataNode#transferReplicaForPipelineRecovery}.
   *
   * When recover pipeline, BlockConstructionStage is
   * PIPELINE_SETUP_APPEND_RECOVERY,
   * PIPELINE_SETUP_STREAMING_RECOVERY,PIPELINE_CLOSE_RECOVERY. If
   * BlockConstructionStage is PIPELINE_CLOSE_RECOVERY, don't need transfer
   * replica. So BlockConstructionStage is PIPELINE_SETUP_APPEND_RECOVERY,
   * PIPELINE_SETUP_STREAMING_RECOVERY.
   */
  private static boolean isWrite(BlockConstructionStage stage) {
    return (stage == PIPELINE_SETUP_STREAMING_RECOVERY
        || stage == PIPELINE_SETUP_APPEND_RECOVERY);
  }

  public DataSetLockManager getDataSetLockManager() {
    return dataSetLockManager;
  }

  boolean isSlownodeByBlockPoolId(String bpId) {
    return blockPoolManager.isSlownodeByBlockPoolId(bpId);
  }

  boolean isSlownode() {
    return blockPoolManager.isSlownode();
  }

  @VisibleForTesting
  public BlockPoolManager getBlockPoolManager() {
    return blockPoolManager;
  }
}
