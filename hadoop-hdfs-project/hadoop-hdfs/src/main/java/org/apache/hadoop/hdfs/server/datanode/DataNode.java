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


import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PLUGINS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_STARTUP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;

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
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurableBase;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtil.ConfiguredNNAddress;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.net.DomainPeerServer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeLocalInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
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
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.InterDatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.InterDatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.InterDatanodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockPoolTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.datanode.web.DatanodeHttpServer;
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
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
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
import org.apache.hadoop.tracing.TraceAdminPB.TraceAdminService;
import org.apache.hadoop.tracing.TraceAdminProtocolPB;
import org.apache.hadoop.tracing.TraceAdminProtocolServerSideTranslatorPB;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.tracing.SpanReceiverHost;
import org.apache.hadoop.tracing.SpanReceiverInfo;
import org.apache.hadoop.tracing.TraceAdminProtocol;
import org.mortbay.util.ajax.JSON;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.protobuf.BlockingService;

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
 *   block-> stream of bytes (of BLOCK_SIZE or less)
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
        TraceAdminProtocol, DataNodeMXBean {
  public static final Log LOG = LogFactory.getLog(DataNode.class);
  
  static{
    HdfsConfiguration.init();
  }

  public static final String DN_CLIENTTRACE_FORMAT =
        "src: %s" +      // src IP
        ", dest: %s" +   // dst IP
        ", bytes: %s" +  // byte count
        ", op: %s" +     // operation
        ", cliID: %s" +  // DFSClient id
        ", offset: %s" + // offset
        ", srvID: %s" +  // DatanodeRegistration
        ", blockid: %s" + // block id
        ", duration: %s";  // duration time
        
  static final Log ClientTraceLog =
    LogFactory.getLog(DataNode.class.getName() + ".clienttrace");
  
  private static final String USAGE =
      "Usage: java DataNode [-regular | -rollback]\n" +
      "    -regular                 : Normal DataNode startup (default).\n" +
      "    -rollback                : Rollback a standard or rolling upgrade.\n" +
      "  Refer to HDFS documentation for the difference between standard\n" +
      "  and rolling upgrades.";

  static final int CURRENT_BLOCK_FORMAT_VERSION = 1;

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

  public final static String EMPTY_DEL_HINT = "";
  final AtomicInteger xmitsInProgress = new AtomicInteger();
  Daemon dataXceiverServer = null;
  DataXceiverServer xserver = null;
  Daemon localDataXceiverServer = null;
  ShortCircuitRegistry shortCircuitRegistry = null;
  ThreadGroup threadGroup = null;
  private DNConf dnConf;
  private volatile boolean heartbeatsDisabledForTests = false;
  private DataStorage storage = null;

  private DatanodeHttpServer httpServer = null;
  private int infoPort;
  private int infoSecurePort;

  DataNodeMetrics metrics;
  private InetSocketAddress streamingAddr;
  
  // See the note below in incrDatanodeNetworkErrors re: concurrency.
  private LoadingCache<String, Map<String, Long>> datanodeNetworkCounts;

  private String hostName;
  private DatanodeID id;
  
  final private String fileDescriptorPassingDisabledReason;
  boolean isBlockTokenEnabled;
  BlockPoolTokenSecretManager blockPoolTokenSecretManager;
  private boolean hasAnyBlockPoolRegistered = false;
  
  private final BlockScanner blockScanner;
  private DirectoryScanner directoryScanner = null;
  
  /** Activated plug-ins. */
  private List<ServicePlugin> plugins;
  
  // For InterDataNodeProtocol
  public RPC.Server ipcServer;

  private JvmPauseMonitor pauseMonitor;

  private SecureResources secureResources = null;
  // dataDirs must be accessed while holding the DataNode lock.
  private List<StorageLocation> dataDirs;
  private Configuration conf;
  private final String confVersion;
  private final long maxNumberOfBlocksToLog;
  private final boolean pipelineSupportECN;

  private final List<String> usersWithLocalPathAccess;
  private final boolean connectToDnViaHostname;
  ReadaheadPool readaheadPool;
  SaslDataTransferClient saslClient;
  SaslDataTransferServer saslServer;
  private final boolean getHdfsBlockLocationsEnabled;
  private ObjectName dataNodeInfoBeanName;
  private Thread checkDiskErrorThread = null;
  protected final int checkDiskErrorInterval = 5*1000;
  private boolean checkDiskErrorFlag = false;
  private Object checkDiskErrorMutex = new Object();
  private long lastDiskErrorCheck;
  private String supergroup;
  private boolean isPermissionEnabled;
  private String dnUserName = null;

  private SpanReceiverHost spanReceiverHost;

  /**
   * Creates a dummy DataNode for testing purpose.
   */
  @VisibleForTesting
  @InterfaceAudience.LimitedPrivate("HDFS")
  DataNode(final Configuration conf) {
    super(conf);
    this.blockScanner = new BlockScanner(this, conf);
    this.fileDescriptorPassingDisabledReason = null;
    this.maxNumberOfBlocksToLog = 0;
    this.confVersion = null;
    this.usersWithLocalPathAccess = null;
    this.connectToDnViaHostname = false;
    this.getHdfsBlockLocationsEnabled = false;
    this.pipelineSupportECN = false;
  }

  /**
   * Create the DataNode given a configuration, an array of dataDirs,
   * and a namenode proxy
   */
  DataNode(final Configuration conf,
           final List<StorageLocation> dataDirs,
           final SecureResources resources) throws IOException {
    super(conf);
    this.blockScanner = new BlockScanner(this, conf);
    this.lastDiskErrorCheck = 0;
    this.maxNumberOfBlocksToLog = conf.getLong(DFS_MAX_NUM_BLOCKS_TO_LOG_KEY,
        DFS_MAX_NUM_BLOCKS_TO_LOG_DEFAULT);

    this.usersWithLocalPathAccess = Arrays.asList(
        conf.getTrimmedStrings(DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY));
    this.connectToDnViaHostname = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    this.getHdfsBlockLocationsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED, 
        DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT);
    this.supergroup = conf.get(DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
        DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
    this.isPermissionEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY,
        DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT);
    this.pipelineSupportECN = conf.getBoolean(
        DFSConfigKeys.DFS_PIPELINE_ECN_ENABLED,
        DFSConfigKeys.DFS_PIPELINE_ECN_ENABLED_DEFAULT);

    confVersion = "core-" +
        conf.get("hadoop.common.configuration.version", "UNSPECIFIED") +
        ",hdfs-" +
        conf.get("hadoop.hdfs.configuration.version", "UNSPECIFIED");

    // Determine whether we should try to pass file descriptors to clients.
    if (conf.getBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY,
              DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT)) {
      String reason = DomainSocket.getLoadingFailureReason();
      if (reason != null) {
        LOG.warn("File descriptor passing is disabled because " + reason);
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

    try {
      hostName = getHostName(conf);
      LOG.info("Configured hostname is " + hostName);
      startDataNode(conf, dataDirs, resources);
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
              public Map<String, Long> load(String key) throws Exception {
                final Map<String, Long> ret = new HashMap<String, Long>();
                ret.put("networkErrors", 0L);
                return ret;
              }
            });
  }

  @Override
  public void reconfigurePropertyImpl(String property, String newVal)
      throws ReconfigurationException {
    if (property.equals(DFS_DATANODE_DATA_DIR_KEY)) {
      try {
        LOG.info("Reconfiguring " + property + " to " + newVal);
        this.refreshVolumes(newVal);
      } catch (IOException e) {
        throw new ReconfigurationException(property, newVal,
            getConf().get(property), e);
      }
    } else {
      throw new ReconfigurationException(
          property, newVal, getConf().get(property));
    }
  }

  /**
   * Get a list of the keys of the re-configurable properties in configuration.
   */
  @Override
  public Collection<String> getReconfigurableProperties() {
    List<String> reconfigurable =
        Collections.unmodifiableList(Arrays.asList(DFS_DATANODE_DATA_DIR_KEY));
    return reconfigurable;
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
    return pipelineSupportECN ? PipelineAck.ECN.SUPPORTED : PipelineAck.ECN
      .DISABLED;
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
   * configuration.
   */
  @VisibleForTesting
  ChangedVolumes parseChangedVolumes(String newVolumes) throws IOException {
    Configuration conf = new Configuration();
    conf.set(DFS_DATANODE_DATA_DIR_KEY, newVolumes);
    List<StorageLocation> locations = getStorageLocations(conf);

    if (locations.isEmpty()) {
      throw new IOException("No directory is specified.");
    }

    ChangedVolumes results = new ChangedVolumes();
    results.newLocations.addAll(locations);

    for (Iterator<Storage.StorageDirectory> it = storage.dirIterator();
         it.hasNext(); ) {
      Storage.StorageDirectory dir = it.next();
      boolean found = false;
      for (Iterator<StorageLocation> sl = results.newLocations.iterator();
           sl.hasNext(); ) {
        StorageLocation location = sl.next();
        if (location.getFile().getCanonicalPath().equals(
            dir.getRoot().getCanonicalPath())) {
          sl.remove();
          results.unchangedLocations.add(location);
          found = true;
          break;
        }
      }

      if (!found) {
        results.deactivateLocations.add(
            StorageLocation.parse(dir.getRoot().toString()));
      }
    }

    return results;
  }

  /**
   * Attempts to reload data volumes with new configuration.
   * @param newVolumes a comma separated string that specifies the data volumes.
   * @throws IOException on error. If an IOException is thrown, some new volumes
   * may have been successfully added and removed.
   */
  private synchronized void refreshVolumes(String newVolumes) throws IOException {
    Configuration conf = getConf();
    conf.set(DFS_DATANODE_DATA_DIR_KEY, newVolumes);

    int numOldDataDirs = dataDirs.size();
    ChangedVolumes changedVolumes = parseChangedVolumes(newVolumes);
    StringBuilder errorMessageBuilder = new StringBuilder();
    List<String> effectiveVolumes = Lists.newArrayList();
    for (StorageLocation sl : changedVolumes.unchangedLocations) {
      effectiveVolumes.add(sl.toString());
    }

    try {
      if (numOldDataDirs + changedVolumes.newLocations.size() -
          changedVolumes.deactivateLocations.size() <= 0) {
        throw new IOException("Attempt to remove all volumes.");
      }
      if (!changedVolumes.newLocations.isEmpty()) {
        LOG.info("Adding new volumes: " +
            Joiner.on(",").join(changedVolumes.newLocations));

        // Add volumes for each Namespace
        final List<NamespaceInfo> nsInfos = Lists.newArrayList();
        for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
          nsInfos.add(bpos.getNamespaceInfo());
        }
        ExecutorService service = Executors.newFixedThreadPool(
            changedVolumes.newLocations.size());
        List<Future<IOException>> exceptions = Lists.newArrayList();
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
              LOG.error("Failed to add volume: " + volume, ioe);
            } else {
              effectiveVolumes.add(volume.toString());
              LOG.info("Successfully added volume: " + volume);
            }
          } catch (Exception e) {
            errorMessageBuilder.append(
                String.format("FAILED to ADD: %s: %s%n", volume,
                              e.toString()));
            LOG.error("Failed to add volume: " + volume, e);
          }
        }
      }

      try {
        removeVolumes(changedVolumes.deactivateLocations);
      } catch (IOException e) {
        errorMessageBuilder.append(e.getMessage());
        LOG.error("Failed to remove volume: " + e.getMessage(), e);
      }

      if (errorMessageBuilder.length() > 0) {
        throw new IOException(errorMessageBuilder.toString());
      }
    } finally {
      conf.set(DFS_DATANODE_DATA_DIR_KEY,
          Joiner.on(",").join(effectiveVolumes));
      dataDirs = getStorageLocations(conf);

      // Send a full block report to let NN acknowledge the volume changes.
      triggerBlockReport(new BlockReportOptions.Factory()
          .setIncremental(false).build());
    }
  }

  /**
   * Remove volumes from DataNode.
   * See {@link removeVolumes(final Set<File>, boolean)} for details.
   *
   * @param locations the StorageLocations of the volumes to be removed.
   * @throws IOException
   */
  private void removeVolumes(final Collection<StorageLocation> locations)
    throws IOException {
    if (locations.isEmpty()) {
      return;
    }
    Set<File> volumesToRemove = new HashSet<>();
    for (StorageLocation loc : locations) {
      volumesToRemove.add(loc.getFile().getAbsoluteFile());
    }
    removeVolumes(volumesToRemove, true);
  }

  /**
   * Remove volumes from DataNode.
   *
   * It does three things:
   * <li>
   *   <ul>Remove volumes and block info from FsDataset.</ul>
   *   <ul>Remove volumes from DataStorage.</ul>
   *   <ul>Reset configuration DATA_DIR and {@link dataDirs} to represent
   *   active volumes.</ul>
   * </li>
   * @param absoluteVolumePaths the absolute path of volumes.
   * @param clearFailure if true, clears the failure information related to the
   *                     volumes.
   * @throws IOException
   */
  private synchronized void removeVolumes(
      final Set<File> absoluteVolumePaths, boolean clearFailure)
      throws IOException {
    for (File vol : absoluteVolumePaths) {
      Preconditions.checkArgument(vol.isAbsolute());
    }

    if (absoluteVolumePaths.isEmpty()) {
      return;
    }

    LOG.info(String.format("Deactivating volumes (clear failure=%b): %s",
        clearFailure, Joiner.on(",").join(absoluteVolumePaths)));

    IOException ioe = null;
    // Remove volumes and block infos from FsDataset.
    data.removeVolumes(absoluteVolumePaths, clearFailure);

    // Remove volumes from DataStorage.
    try {
      storage.removeVolumes(absoluteVolumePaths);
    } catch (IOException e) {
      ioe = e;
    }

    // Set configuration and dataDirs to reflect volume changes.
    for (Iterator<StorageLocation> it = dataDirs.iterator(); it.hasNext(); ) {
      StorageLocation loc = it.next();
      if (absoluteVolumePaths.contains(loc.getFile().getAbsoluteFile())) {
        it.remove();
      }
    }
    conf.set(DFS_DATANODE_DATA_DIR_KEY, Joiner.on(",").join(dataDirs));

    if (ioe != null) {
      throw ioe;
    }
  }

  private synchronized void setClusterId(final String nsCid, final String bpid
      ) throws IOException {
    if(clusterId != null && !clusterId.equals(nsCid)) {
      throw new IOException ("Cluster IDs not matched: dn cid=" + clusterId 
          + " but ns cid="+ nsCid + "; bpid=" + bpid);
    }
    // else
    clusterId = nsCid;
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
      name = DNS.getDefaultHost(
          config.get(DFS_DATANODE_DNS_INTERFACE_KEY,
                     DFS_DATANODE_DNS_INTERFACE_DEFAULT),
          config.get(DFS_DATANODE_DNS_NAMESERVER_KEY,
                     DFS_DATANODE_DNS_NAMESERVER_DEFAULT));
    }
    return name;
  }

  /**
   * @see DFSUtil#getHttpPolicy(org.apache.hadoop.conf.Configuration)
   * for information related to the different configuration options and
   * Http Policy is decided.
   */
  private void startInfoServer(Configuration conf)
    throws IOException {
    // SecureDataNodeStarter will bind the privileged port to the channel if
    // the DN is started by JSVC, pass it along.
    ServerSocketChannel httpServerChannel = secureResources != null ?
        secureResources.getHttpServerChannel() : null;

    this.httpServer = new DatanodeHttpServer(conf, this, httpServerChannel);
    httpServer.start();
    if (httpServer.getHttpAddress() != null) {
      infoPort = httpServer.getHttpAddress().getPort();
    }
    if (httpServer.getHttpsAddress() != null) {
      infoSecurePort = httpServer.getHttpsAddress().getPort();
    }
  }

  private void startPlugins(Configuration conf) {
    plugins = conf.getInstances(DFS_DATANODE_PLUGINS_KEY, ServicePlugin.class);
    for (ServicePlugin p: plugins) {
      try {
        p.start(this);
        LOG.info("Started plug-in " + p);
      } catch (Throwable t) {
        LOG.warn("ServicePlugin " + p + " could not be started", t);
      }
    }
  }
  

  private void initIpcServer(Configuration conf) throws IOException {
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        conf.getTrimmed(DFS_DATANODE_IPC_ADDRESS_KEY));
    
    // Add all the RPC protocols that the Datanode implements    
    RPC.setProtocolEngine(conf, ClientDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    ClientDatanodeProtocolServerSideTranslatorPB clientDatanodeProtocolXlator = 
          new ClientDatanodeProtocolServerSideTranslatorPB(this);
    BlockingService service = ClientDatanodeProtocolService
        .newReflectiveBlockingService(clientDatanodeProtocolXlator);
    ipcServer = new RPC.Builder(conf)
        .setProtocol(ClientDatanodeProtocolPB.class)
        .setInstance(service)
        .setBindAddress(ipcAddr.getHostName())
        .setPort(ipcAddr.getPort())
        .setNumHandlers(
            conf.getInt(DFS_DATANODE_HANDLER_COUNT_KEY,
                DFS_DATANODE_HANDLER_COUNT_DEFAULT)).setVerbose(false)
        .setSecretManager(blockPoolTokenSecretManager).build();
    
    InterDatanodeProtocolServerSideTranslatorPB interDatanodeProtocolXlator = 
        new InterDatanodeProtocolServerSideTranslatorPB(this);
    service = InterDatanodeProtocolService
        .newReflectiveBlockingService(interDatanodeProtocolXlator);
    DFSUtil.addPBProtocol(conf, InterDatanodeProtocolPB.class, service,
        ipcServer);

    TraceAdminProtocolServerSideTranslatorPB traceAdminXlator =
        new TraceAdminProtocolServerSideTranslatorPB(this);
    BlockingService traceAdminService = TraceAdminService
        .newReflectiveBlockingService(traceAdminXlator);
    DFSUtil.addPBProtocol(conf, TraceAdminProtocolPB.class, traceAdminService,
        ipcServer);

    LOG.info("Opened IPC server at " + ipcServer.getListenerAddress());

    // set service-level authorization security policy
    if (conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      ipcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
    }
  }

  /** Check whether the current user is in the superuser group. */
  private void checkSuperuserPrivilege() throws IOException, AccessControlException {
    if (!isPermissionEnabled) {
      return;
    }
    // Try to get the ugi in the RPC call.
    UserGroupInformation callerUgi = ipcServer.getRemoteUser();
    if (callerUgi == null) {
      // This is not from RPC.
      callerUgi = UserGroupInformation.getCurrentUser();
    }

    // Is this by the DN user itself?
    assert dnUserName != null;
    if (callerUgi.getShortUserName().equals(dnUserName)) {
      return;
    }

    // Is the user a member of the super group?
    List<String> groups = Arrays.asList(callerUgi.getGroupNames());
    if (groups.contains(supergroup)) {
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
    if (conf.getInt(DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 
                    DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT) < 0) {
      reason = "verification is turned off by configuration";
    } else if ("SimulatedFSDataset".equals(data.getClass().getSimpleName())) {
      reason = "verifcation is not supported by SimulatedFSDataset";
    } 
    if (reason == null) {
      directoryScanner = new DirectoryScanner(this, data, conf);
      directoryScanner.start();
    } else {
      LOG.info("Periodic Directory Tree Verification scan is disabled because " +
                   reason);
    }
  }
  
  private synchronized void shutdownDirectoryScanner() {
    if (directoryScanner != null) {
      directoryScanner.shutdown();
    }
  }
  
  private void initDataXceiver(Configuration conf) throws IOException {
    // find free port or use privileged port provided
    TcpPeerServer tcpPeerServer;
    if (secureResources != null) {
      tcpPeerServer = new TcpPeerServer(secureResources);
    } else {
      tcpPeerServer = new TcpPeerServer(dnConf.socketWriteTimeout,
          DataNode.getStreamingAddr(conf));
    }
    tcpPeerServer.setReceiveBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);
    streamingAddr = tcpPeerServer.getStreamingAddr();
    LOG.info("Opened streaming server at " + streamingAddr);
    this.threadGroup = new ThreadGroup("dataXceiverServer");
    xserver = new DataXceiverServer(tcpPeerServer, conf, this);
    this.dataXceiverServer = new Daemon(threadGroup, xserver);
    this.threadGroup.setDaemon(true); // auto destroy when empty

    if (conf.getBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY,
              DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT) ||
        conf.getBoolean(DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC,
              DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT)) {
      DomainPeerServer domainPeerServer =
                getDomainPeerServer(conf, streamingAddr.getPort());
      if (domainPeerServer != null) {
        this.localDataXceiverServer = new Daemon(threadGroup,
            new DataXceiverServer(domainPeerServer, conf, this));
        LOG.info("Listening on UNIX domain socket: " +
            domainPeerServer.getBindPath());
      }
    }
    this.shortCircuitRegistry = new ShortCircuitRegistry(conf);
  }

  static DomainPeerServer getDomainPeerServer(Configuration conf,
      int port) throws IOException {
    String domainSocketPath =
        conf.getTrimmed(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
            DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT);
    if (domainSocketPath.isEmpty()) {
      if (conf.getBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY,
            DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT) &&
         (!conf.getBoolean(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT))) {
        LOG.warn("Although short-circuit local reads are configured, " +
            "they are disabled because you didn't configure " +
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
    domainPeerServer.setReceiveBufferSize(
        HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);
    return domainPeerServer;
  }
  
  // calls specific to BP
  public void notifyNamenodeReceivedBlock(
      ExtendedBlock block, String delHint, String storageUuid) {
    BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
    if(bpos != null) {
      bpos.notifyNamenodeReceivedBlock(block, delHint, storageUuid);
    } else {
      LOG.error("Cannot find BPOfferService for reporting block received for bpid="
          + block.getBlockPoolId());
    }
  }
  
  // calls specific to BP
  protected void notifyNamenodeReceivingBlock(
      ExtendedBlock block, String storageUuid) {
    BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
    if(bpos != null) {
      bpos.notifyNamenodeReceivingBlock(block, storageUuid);
    } else {
      LOG.error("Cannot find BPOfferService for reporting block receiving for bpid="
          + block.getBlockPoolId());
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
    BPOfferService bpos = getBPOSForBlock(block);
    FsVolumeSpi volume = getFSDataset().getVolume(block);
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
  
  /**
   * Try to send an error report to the NNs associated with the given
   * block pool.
   * @param bpid the block pool ID
   * @param errCode error code to send
   * @param errMsg textual message to send
   */
  void trySendErrorReport(String bpid, int errCode, String errMsg) {
    BPOfferService bpos = blockPoolManager.get(bpid);
    if (bpos == null) {
      throw new IllegalArgumentException("Bad block pool: " + bpid);
    }
    bpos.trySendErrorReport(errCode, errMsg);
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
  void setHeartbeatsDisabledForTests(
      boolean heartbeatsDisabledForTests) {
    this.heartbeatsDisabledForTests = heartbeatsDisabledForTests;
  }
  
  boolean areHeartbeatsDisabledForTests() {
    return this.heartbeatsDisabledForTests;
  }

  /**
   * This method starts the data node with the specified conf.
   * 
   * @param conf - the configuration
   *  if conf's CONFIG_PROPERTY_SIMULATED property is set
   *  then a simulated storage based data node is created.
   * 
   * @param dataDirs - only for a non-simulated storage data node
   * @throws IOException
   */
  void startDataNode(Configuration conf, 
                     List<StorageLocation> dataDirs,
                     SecureResources resources
                     ) throws IOException {

    // settings global for all BPs in the Data Node
    this.secureResources = resources;
    synchronized (this) {
      this.dataDirs = dataDirs;
    }
    this.conf = conf;
    this.dnConf = new DNConf(conf);
    checkSecureConfig(dnConf, conf, resources);

    this.spanReceiverHost =
      SpanReceiverHost.get(conf, DFSConfigKeys.DFS_SERVER_HTRACE_PREFIX);

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
    LOG.info("Starting DataNode with maxLockedMemory = " +
        dnConf.maxLockedMemory);

    storage = new DataStorage();
    
    // global DN settings
    registerMXBean();
    initDataXceiver(conf);
    startInfoServer(conf);
    pauseMonitor = new JvmPauseMonitor(conf);
    pauseMonitor.start();
  
    // BlockPoolTokenSecretManager is required to create ipc server.
    this.blockPoolTokenSecretManager = new BlockPoolTokenSecretManager();

    // Login is done by now. Set the DN user name.
    dnUserName = UserGroupInformation.getCurrentUser().getShortUserName();
    LOG.info("dnUserName = " + dnUserName);
    LOG.info("supergroup = " + supergroup);
    initIpcServer(conf);

    metrics = DataNodeMetrics.create(conf, getDisplayName());
    metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);
    
    blockPoolManager = new BlockPoolManager(this);
    blockPoolManager.refreshNamenodes(conf);

    // Create the ReadaheadPool from the DataNode context so we can
    // exit without having to explicitly shutdown its thread pool.
    readaheadPool = ReadaheadPool.getInstance();
    saslClient = new SaslDataTransferClient(dnConf.conf, 
        dnConf.saslPropsResolver, dnConf.trustedChannelResolver);
    saslServer = new SaslDataTransferServer(dnConf, blockPoolTokenSecretManager);
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
    SaslPropertiesResolver saslPropsResolver = dnConf.getSaslPropsResolver();
    if (resources != null && saslPropsResolver == null) {
      return;
    }
    if (dnConf.getIgnoreSecurePortsForTesting()) {
      return;
    }
    if (saslPropsResolver != null &&
        DFSUtil.getHttpPolicy(conf) == HttpConfig.Policy.HTTPS_ONLY &&
        resources == null) {
      return;
    }
    throw new RuntimeException("Cannot start secure DataNode without " +
      "configuring either privileged resources or SASL RPC data transfer " +
      "protection and SSL for HTTP.  Using privileged resources in " +
      "combination with SASL RPC data transfer protection is not supported.");
  }
  
  public static String generateUuid() {
    return UUID.randomUUID().toString();
  }

  /**
   * Verify that the DatanodeUuid has been initialized. If this is a new
   * datanode then we generate a new Datanode Uuid and persist it to disk.
   *
   * @throws IOException
   */
  private synchronized void checkDatanodeUuid() throws IOException {
    if (storage.getDatanodeUuid() == null) {
      storage.setDatanodeUuid(generateUuid());
      storage.writeAll();
      LOG.info("Generated and persisted new Datanode UUID " +
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
          DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION,
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
      LOG.info("Block token params received from NN: for block pool " +
          blockPoolId + " keyUpdateInterval="
          + blockKeyUpdateInterval / (60 * 1000)
          + " min(s), tokenLifetime=" + blockTokenLifetime / (60 * 1000)
          + " min(s)");
      final BlockTokenSecretManager secretMgr = 
          new BlockTokenSecretManager(0, blockTokenLifetime, blockPoolId,
              dnConf.encryptionAlgorithm);
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

      blockScanner.disableBlockPoolId(bpId);

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

    // Exclude failed disks before initializing the block pools to avoid startup
    // failures.
    checkDiskError();

    data.addBlockPool(nsInfo.getBlockPoolID(), conf);
    blockScanner.enableBlockPoolId(bpos.getBlockPoolId());
    initDirectoryScanner(conf);
  }

  BPOfferService[] getAllBpOs() {
    return blockPoolManager.getAllNamenodeThreads();
  }
  
  int getBpOsCount() {
    return blockPoolManager.getAllNamenodeThreads().length;
  }
  
  /**
   * Initializes the {@link #data}. The initialization is done only once, when
   * handshake with the the first namenode is completed.
   */
  private void initStorage(final NamespaceInfo nsInfo) throws IOException {
    final FsDatasetSpi.Factory<? extends FsDatasetSpi<?>> factory
        = FsDatasetSpi.Factory.getFactory(conf);
    
    if (!factory.isSimulated()) {
      final StartupOption startOpt = getStartupOption(conf);
      if (startOpt == null) {
        throw new IOException("Startup option not set.");
      }
      final String bpid = nsInfo.getBlockPoolID();
      //read storage info, lock data dirs and transition fs state if necessary
      synchronized (this) {
        storage.recoverTransitionRead(this, nsInfo, dataDirs, startOpt);
      }
      final StorageInfo bpStorage = storage.getBPStorage(bpid);
      LOG.info("Setting up storage: nsid=" + bpStorage.getNamespaceID()
          + ";bpid=" + bpid + ";lv=" + storage.getLayoutVersion()
          + ";nsInfo=" + nsInfo + ";dnuuid=" + storage.getDatanodeUuid());
    }

    // If this is a newly formatted DataNode then assign a new DatanodeUuid.
    checkDatanodeUuid();

    synchronized(this)  {
      if (data == null) {
        data = factory.newInstance(this, storage, conf);
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
  
  /**
   * @return name useful for logging
   */
  public String getDisplayName() {
    // NB: our DatanodeID may not be set yet
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
  protected Socket newSocket() throws IOException {
    return (dnConf.socketWriteTimeout > 0) ? 
           SocketChannel.open().socket() : new Socket();                                   
  }

  /**
   * Connect to the NN. This is separated out for easier testing.
   */
  DatanodeProtocolClientSideTranslatorPB connectToNN(
      InetSocketAddress nnAddr) throws IOException {
    return new DatanodeProtocolClientSideTranslatorPB(nnAddr, conf);
  }

  public static InterDatanodeProtocol createInterDataNodeProtocolProxy(
      DatanodeID datanodeid, final Configuration conf, final int socketTimeout,
      final boolean connectToDnViaHostname) throws IOException {
    final String dnAddr = datanodeid.getIpcAddr(connectToDnViaHostname);
    final InetSocketAddress addr = NetUtils.createSocketAddr(dnAddr);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to datanode " + dnAddr + " addr=" + addr);
    }
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
              + " is not allowed to call getBlockLocalPathInfo");
    }
  }

  public long getMaxNumberOfBlocksToLog() {
    return maxNumberOfBlocksToLog;
  }

  @Override
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block,
      Token<BlockTokenIdentifier> token) throws IOException {
    checkBlockLocalPathAccess();
    checkBlockToken(block, token, BlockTokenSecretManager.AccessMode.READ);
    Preconditions.checkNotNull(data, "Storage not yet initialized");
    BlockLocalPathInfo info = data.getBlockLocalPathInfo(block);
    if (LOG.isDebugEnabled()) {
      if (info != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("getBlockLocalPathInfo successful block=" + block
              + " blockfile " + info.getBlockPath() + " metafile "
              + info.getMetaPath());
        }
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("getBlockLocalPathInfo for block=" + block
              + " returning null");
        }
      }
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
      fis[0] = (FileInputStream)data.getBlockInputStream(blk, 0);
      fis[1] = DatanodeUtil.getMetaDataInputStream(blk, data);
    } catch (ClassCastException e) {
      LOG.debug("requestShortCircuitFdsForRead failed", e);
      throw new ShortCircuitFdsUnsupportedException("This DataNode's " +
          "FsDatasetSpi does not support short-circuit local reads");
    }
    return fis;
  }

  @Override
  public HdfsBlocksMetadata getHdfsBlocksMetadata(
      String bpId, long[] blockIds,
      List<Token<BlockTokenIdentifier>> tokens) throws IOException, 
      UnsupportedOperationException {
    if (!getHdfsBlockLocationsEnabled) {
      throw new UnsupportedOperationException("Datanode#getHdfsBlocksMetadata "
          + " is not enabled in datanode config");
    }
    if (blockIds.length != tokens.size()) {
      throw new IOException("Differing number of blocks and tokens");
    }
    // Check access for each block
    for (int i = 0; i < blockIds.length; i++) {
      checkBlockToken(new ExtendedBlock(bpId, blockIds[i]),
          tokens.get(i), BlockTokenSecretManager.AccessMode.READ);
    }

    DataNodeFaultInjector.get().getHdfsBlocksMetadata();

    return data.getHdfsBlocksMetadata(bpId, blockIds);
  }
  
  private void checkBlockToken(ExtendedBlock block, Token<BlockTokenIdentifier> token,
      AccessMode accessMode) throws IOException {
    if (isBlockTokenEnabled) {
      BlockTokenIdentifier id = new BlockTokenIdentifier();
      ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
      DataInputStream in = new DataInputStream(buf);
      id.readFields(in);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got: " + id.toString());
      }
      blockPoolTokenSecretManager.checkAccess(id, null, block, accessMode);
    }
  }

  /**
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   * This method can only be called by the offerService thread.
   * Otherwise, deadlock might occur.
   */
  public void shutdown() {
    if (plugins != null) {
      for (ServicePlugin p : plugins) {
        try {
          p.stop();
          LOG.info("Stopped plug-in " + p);
        } catch (Throwable t) {
          LOG.warn("ServicePlugin " + p + " could not be stopped", t);
        }
      }
    }
    
    // We need to make a copy of the original blockPoolManager#offerServices to
    // make sure blockPoolManager#shutDownAll() can still access all the 
    // BPOfferServices, since after setting DataNode#shouldRun to false the 
    // offerServices may be modified.
    BPOfferService[] bposArray = this.blockPoolManager == null ? null
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
      } catch (Throwable e) {
        // Ignore, since the out of band messaging is advisory.
      }
    }

    // Interrupt the checkDiskErrorThread and terminate it.
    if(this.checkDiskErrorThread != null) {
      this.checkDiskErrorThread.interrupt();
    }
    
    // Record the time of initial notification
    long timeNotified = Time.monotonicNow();

    if (localDataXceiverServer != null) {
      ((DataXceiverServer) this.localDataXceiverServer.getRunnable()).kill();
      this.localDataXceiverServer.interrupt();
    }

    // Terminate directory scanner and block scanner
    shutdownPeriodicScanners();

    // Stop the web server
    if (httpServer != null) {
      try {
        httpServer.close();
      } catch (Exception e) {
        LOG.warn("Exception shutting down DataNode HttpServer", e);
      }
    }

    if (pauseMonitor != null) {
      pauseMonitor.stop();
    }

    // shouldRun is set to false here to prevent certain threads from exiting
    // before the restart prep is done.
    this.shouldRun = false;
    
    // wait reconfiguration thread, if any, to exit
    shutdownReconfigurationTask();

    // wait for all data receiver threads to exit
    if (this.threadGroup != null) {
      int sleepMs = 2;
      while (true) {
        // When shutting down for restart, wait 2.5 seconds before forcing
        // termination of receiver threads.
        if (!this.shutdownForUpgrade ||
            (this.shutdownForUpgrade && (Time.monotonicNow() - timeNotified
                > 1000))) {
          this.threadGroup.interrupt();
          break;
        }
        LOG.info("Waiting for threadgroup to exit, active threads is " +
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
   
   // IPC server needs to be shutdown late in the process, otherwise
   // shutdown command response won't get sent.
   if (ipcServer != null) {
      ipcServer.stop();
    }

    if(blockPoolManager != null) {
      try {
        this.blockPoolManager.shutDownAll(bposArray);
      } catch (InterruptedException ie) {
        LOG.warn("Received exception in BlockPoolManager#shutDownAll: ", ie);
      }
    }
    
    if (storage != null) {
      try {
        this.storage.unlockAll();
      } catch (IOException ie) {
        LOG.warn("Exception when unlocking storage: " + ie, ie);
      }
    }
    if (data != null) {
      data.shutdown();
    }
    if (metrics != null) {
      metrics.shutdown();
    }
    if (dataNodeInfoBeanName != null) {
      MBeans.unregister(dataNodeInfoBeanName);
      dataNodeInfoBeanName = null;
    }
    if (this.spanReceiverHost != null) {
      this.spanReceiverHost.closeReceivers();
    }
    if (shortCircuitRegistry != null) shortCircuitRegistry.shutdown();
    LOG.info("Shutdown complete.");
    synchronized(this) {
      // it is already false, but setting it again to avoid a findbug warning.
      this.shouldRun = false;
      // Notify the main thread.
      notifyAll();
    }
  }
  
  
  /**
   * Check if there is a disk failure asynchronously and if so, handle the error
   */
  public void checkDiskErrorAsync() {
    synchronized(checkDiskErrorMutex) {
      checkDiskErrorFlag = true;
      if(checkDiskErrorThread == null) {
        startCheckDiskErrorThread();
        checkDiskErrorThread.start();
        LOG.info("Starting CheckDiskError Thread");
      }
    }
  }
  
  private void handleDiskError(String errMsgr) {
    final boolean hasEnoughResources = data.hasEnoughResource();
    LOG.warn("DataNode.handleDiskError: Keep Running: " + hasEnoughResources);
    
    // If we have enough active valid volumes then we do not want to 
    // shutdown the DN completely.
    int dpError = hasEnoughResources ? DatanodeProtocol.DISK_ERROR  
                                     : DatanodeProtocol.FATAL_DISK_ERROR;  
    metrics.incrVolumeFailures();

    //inform NameNodes
    for(BPOfferService bpos: blockPoolManager.getAllNamenodeThreads()) {
      bpos.trySendErrorReport(dpError, errMsgr);
    }
    
    if(hasEnoughResources) {
      scheduleAllBlockReport(0);
      return; // do not shutdown
    }
    
    LOG.warn("DataNode is shutting down: " + errMsgr);
    shouldRun = false;
  }
    
  /** Number of concurrent xceivers per node. */
  @Override // DataNodeMXBean
  public int getXceiverCount() {
    return threadGroup == null ? 0 : threadGroup.activeCount();
  }

  @Override // DataNodeMXBean
  public Map<String, Map<String, Long>> getDatanodeNetworkCounts() {
    return datanodeNetworkCounts.asMap();
  }

  void incrDatanodeNetworkErrors(String host) {
    metrics.incrDatanodeNetworkErrors();

    /*
     * Synchronizing on the whole cache is a big hammer, but since it's only
     * accumulating errors, it should be ok. If this is ever expanded to include
     * non-error stats, then finer-grained concurrency should be applied.
     */
    synchronized (datanodeNetworkCounts) {
      try {
        final Map<String, Long> curCount = datanodeNetworkCounts.get(host);
        curCount.put("networkErrors", curCount.get("networkErrors") + 1L);
        datanodeNetworkCounts.put(host, curCount);
      } catch (ExecutionException e) {
        LOG.warn("failed to increment network error counts for " + host);
      }
    }
  }
  
  int getXmitsInProgress() {
    return xmitsInProgress.get();
  }

  private void reportBadBlock(final BPOfferService bpos,
      final ExtendedBlock block, final String msg) {
    FsVolumeSpi volume = getFSDataset().getVolume(block);
    bpos.reportBadBlocks(
        block, volume.getStorageID(), volume.getStorageType());
    LOG.warn(msg);
  }

  private void transferBlock(ExtendedBlock block, DatanodeInfo[] xferTargets,
      StorageType[] xferTargetStorageTypes) throws IOException {
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
      StringBuilder xfersBuilder = new StringBuilder();
      for (int i = 0; i < numTargets; i++) {
        xfersBuilder.append(xferTargets[i]);
        xfersBuilder.append(" ");
      }
      LOG.info(bpReg + " Starting thread to transfer " + 
               block + " to " + xfersBuilder);                       

      new Daemon(new DataTransfer(xferTargets, xferTargetStorageTypes, block,
          BlockConstructionStage.PIPELINE_SETUP_CREATE, "")).start();
    }
  }

  void transferBlocks(String poolId, Block blocks[],
      DatanodeInfo xferTargets[][], StorageType[][] xferTargetStorageTypes) {
    for (int i = 0; i < blocks.length; i++) {
      try {
        transferBlock(new ExtendedBlock(poolId, blocks[i]), xferTargets[i],
            xferTargetStorageTypes[i]);
      } catch (IOException ie) {
        LOG.warn("Failed to transfer block " + blocks[i], ie);
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
    final ExtendedBlock b;
    final BlockConstructionStage stage;
    final private DatanodeRegistration bpReg;
    final String clientname;
    final CachingStrategy cachingStrategy;

    /**
     * Connect to the first item in the target list.  Pass along the 
     * entire target list, the block, and the data.
     */
    DataTransfer(DatanodeInfo targets[], StorageType[] targetStorageTypes,
        ExtendedBlock b, BlockConstructionStage stage,
        final String clientname)  {
      if (DataTransferProtocol.LOG.isDebugEnabled()) {
        DataTransferProtocol.LOG.debug(getClass().getSimpleName() + ": "
            + b + " (numBytes=" + b.getNumBytes() + ")"
            + ", stage=" + stage
            + ", clientname=" + clientname
            + ", targets=" + Arrays.asList(targets)
            + ", target storage types=" + (targetStorageTypes == null ? "[]" :
            Arrays.asList(targetStorageTypes)));
      }
      this.targets = targets;
      this.targetStorageTypes = targetStorageTypes;
      this.b = b;
      this.stage = stage;
      BPOfferService bpos = blockPoolManager.get(b.getBlockPoolId());
      bpReg = bpos.bpRegistration;
      this.clientname = clientname;
      this.cachingStrategy =
          new CachingStrategy(true, getDnConf().readaheadLength);
    }

    /**
     * Do the deed, write the bytes
     */
    @Override
    public void run() {
      xmitsInProgress.getAndIncrement();
      Socket sock = null;
      DataOutputStream out = null;
      DataInputStream in = null;
      BlockSender blockSender = null;
      final boolean isClient = clientname.length() > 0;
      
      try {
        final String dnAddr = targets[0].getXferAddr(connectToDnViaHostname);
        InetSocketAddress curTarget = NetUtils.createSocketAddr(dnAddr);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to datanode " + dnAddr);
        }
        sock = newSocket();
        NetUtils.connect(sock, curTarget, dnConf.socketTimeout);
        sock.setSoTimeout(targets.length * dnConf.socketTimeout);

        //
        // Header info
        //
        Token<BlockTokenIdentifier> accessToken = BlockTokenSecretManager.DUMMY_TOKEN;
        if (isBlockTokenEnabled) {
          accessToken = blockPoolTokenSecretManager.generateToken(b, 
              EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE));
        }

        long writeTimeout = dnConf.socketWriteTimeout + 
                            HdfsServerConstants.WRITE_TIMEOUT_EXTENSION * (targets.length-1);
        OutputStream unbufOut = NetUtils.getOutputStream(sock, writeTimeout);
        InputStream unbufIn = NetUtils.getInputStream(sock);
        DataEncryptionKeyFactory keyFactory =
          getDataEncryptionKeyFactoryForBlock(b);
        IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut,
          unbufIn, keyFactory, accessToken, bpReg);
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            HdfsConstants.SMALL_BUFFER_SIZE));
        in = new DataInputStream(unbufIn);
        blockSender = new BlockSender(b, 0, b.getNumBytes(), 
            false, false, true, DataNode.this, null, cachingStrategy);
        DatanodeInfo srcNode = new DatanodeInfo(bpReg);

        new Sender(out).writeBlock(b, targetStorageTypes[0], accessToken,
            clientname, targets, targetStorageTypes, srcNode,
            stage, 0, 0, 0, 0, blockSender.getChecksum(), cachingStrategy,
            false, false, null);

        // send data & checksum
        blockSender.sendBlock(out, unbufOut, null);

        // no response necessary
        LOG.info(getClass().getSimpleName() + ": Transmitted " + b
            + " (numBytes=" + b.getNumBytes() + ") to " + curTarget);

        // read ack
        if (isClient) {
          DNTransferAckProto closeAck = DNTransferAckProto.parseFrom(
              PBHelper.vintPrefixed(in));
          if (LOG.isDebugEnabled()) {
            LOG.debug(getClass().getSimpleName() + ": close-ack=" + closeAck);
          }
          if (closeAck.getStatus() != Status.SUCCESS) {
            if (closeAck.getStatus() == Status.ERROR_ACCESS_TOKEN) {
              throw new InvalidBlockTokenException(
                  "Got access token error for connect ack, targets="
                   + Arrays.asList(targets));
            } else {
              throw new IOException("Bad connect ack, targets="
                  + Arrays.asList(targets));
            }
          }
        }
      } catch (IOException ie) {
        LOG.warn(bpReg + ":Failed to transfer " + b + " to " +
            targets[0] + " got ", ie);
        // check if there are any disk problem
        checkDiskErrorAsync();
      } finally {
        xmitsInProgress.getAndDecrement();
        IOUtils.closeStream(blockSender);
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);
      }
    }
  }

  /**
   * Returns a new DataEncryptionKeyFactory that generates a key from the
   * BlockPoolTokenSecretManager, using the block pool ID of the given block.
   *
   * @param block for which the factory needs to create a key
   * @return DataEncryptionKeyFactory for block's block pool ID
   */
  DataEncryptionKeyFactory getDataEncryptionKeyFactoryForBlock(
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
  void closeBlock(ExtendedBlock block, String delHint, String storageUuid) {
    metrics.incrBlocksWritten();
    BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
    if(bpos != null) {
      bpos.notifyNamenodeReceivedBlock(block, delHint, storageUuid);
    } else {
      LOG.warn("Cannot find BPOfferService for reporting block received for bpid="
          + block.getBlockPoolId());
    }
  }

  /** Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  public void runDatanodeDaemon() throws IOException {
    blockPoolManager.startAll();

    // start dataXceiveServer
    dataXceiverServer.start();
    if (localDataXceiverServer != null) {
      localDataXceiverServer.start();
    }
    ipcServer.start();
    startPlugins(conf);
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
        DFS_DATANODE_KERBEROS_PRINCIPAL_KEY);
    return makeInstance(dataLocations, conf, resources);
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
      } catch (IOException ioe) {
        LOG.error("Failed to initialize storage directory " + locationString
            + ". Exception details: " + ioe);
        // Ignore the exception.
        continue;
      } catch (SecurityException se) {
        LOG.error("Failed to initialize storage directory " + locationString
                     + ". Exception details: " + se);
        // Ignore the exception.
        continue;
      }

      locations.add(location);
    }

    return locations;
  }

  /** Instantiate & Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  @VisibleForTesting
  public static DataNode createDataNode(String args[],
                                 Configuration conf) throws IOException {
    return createDataNode(args, conf, null);
  }
  
  /** Instantiate & Start a single datanode daemon and wait for it to finish.
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
        if (blockPoolManager.getAllNamenodeThreads() != null
            && blockPoolManager.getAllNamenodeThreads().length == 0) {
          shouldRun = false;
        }
        // Terminate if shutdown is complete or 2 seconds after all BPs
        // are shutdown.
        synchronized(this) {
          wait(2000);
        }
      } catch (InterruptedException ex) {
        LOG.warn("Received exception in Datanode#join: " + ex);
      }
    }
  }

  // Small wrapper around the DiskChecker class that provides means to mock
  // DiskChecker static methods and unittest DataNode#getDataDirsFromURIs.
  static class DataNodeDiskChecker {
    private final FsPermission expectedPermission;

    public DataNodeDiskChecker(FsPermission expectedPermission) {
      this.expectedPermission = expectedPermission;
    }

    public void checkDir(LocalFileSystem localFS, Path path)
        throws DiskErrorException, IOException {
      DiskChecker.checkDir(localFS, path, expectedPermission);
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
    LocalFileSystem localFS = FileSystem.getLocal(conf);
    FsPermission permission = new FsPermission(
        conf.get(DFS_DATANODE_DATA_DIR_PERMISSION_KEY,
                 DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT));
    DataNodeDiskChecker dataNodeDiskChecker =
        new DataNodeDiskChecker(permission);
    List<StorageLocation> locations =
        checkStorageLocations(dataDirs, localFS, dataNodeDiskChecker);
    DefaultMetricsSystem.initialize("DataNode");

    assert locations.size() > 0 : "number of data directories should be > 0";
    return new DataNode(conf, locations, resources);
  }

  // DataNode ctor expects AbstractList instead of List or Collection...
  static List<StorageLocation> checkStorageLocations(
      Collection<StorageLocation> dataDirs,
      LocalFileSystem localFS, DataNodeDiskChecker dataNodeDiskChecker)
          throws IOException {
    ArrayList<StorageLocation> locations = new ArrayList<StorageLocation>();
    StringBuilder invalidDirs = new StringBuilder();
    for (StorageLocation location : dataDirs) {
      final URI uri = location.getUri();
      try {
        dataNodeDiskChecker.checkDir(localFS, new Path(uri));
        locations.add(location);
      } catch (IOException ioe) {
        LOG.warn("Invalid " + DFS_DATANODE_DATA_DIR_KEY + " "
            + location.getFile() + " : ", ioe);
        invalidDirs.append("\"").append(uri.getPath()).append("\" ");
      }
    }
    if (locations.size() == 0) {
      throw new IOException("All directories in "
          + DFS_DATANODE_DATA_DIR_KEY + " are invalid: "
          + invalidDirs);
    }
    return locations;
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
      LOG.fatal("Exception in secureMain", e);
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

  public Daemon recoverBlocks(
      final String who,
      final Collection<RecoveringBlock> blocks) {
    
    Daemon d = new Daemon(threadGroup, new Runnable() {
      /** Recover a list of blocks. It is run by the primary datanode. */
      @Override
      public void run() {
        for(RecoveringBlock b : blocks) {
          try {
            logRecoverBlock(who, b);
            recoverBlock(b);
          } catch (IOException e) {
            LOG.warn("recoverBlocks FAILED: " + b, e);
          }
        }
      }
    });
    d.start();
    return d;
  }

  // InterDataNodeProtocol implementation
  @Override // InterDatanodeProtocol
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
  throws IOException {
    return data.initReplicaRecovery(rBlock);
  }

  /**
   * Convenience method, which unwraps RemoteException.
   * @throws IOException not a RemoteException.
   */
  private static ReplicaRecoveryInfo callInitReplicaRecovery(
      InterDatanodeProtocol datanode,
      RecoveringBlock rBlock) throws IOException {
    try {
      return datanode.initReplicaRecovery(rBlock);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }

  /**
   * Update replica with the new generation stamp and length.  
   */
  @Override // InterDatanodeProtocol
  public String updateReplicaUnderRecovery(final ExtendedBlock oldBlock,
      final long recoveryId, final long newBlockId, final long newLength)
      throws IOException {
    final String storageID = data.updateReplicaUnderRecovery(oldBlock,
        recoveryId, newBlockId, newLength);
    // Notify the namenode of the updated block info. This is important
    // for HA, since otherwise the standby node may lose track of the
    // block locations until the next block report.
    ExtendedBlock newBlock = new ExtendedBlock(oldBlock);
    newBlock.setGenerationStamp(recoveryId);
    newBlock.setBlockId(newBlockId);
    newBlock.setNumBytes(newLength);
    notifyNamenodeReceivedBlock(newBlock, "", storageID);
    return storageID;
  }

  /** A convenient class used in block recovery */
  static class BlockRecord { 
    final DatanodeID id;
    final InterDatanodeProtocol datanode;
    final ReplicaRecoveryInfo rInfo;
    
    private String storageID;

    BlockRecord(DatanodeID id,
                InterDatanodeProtocol datanode,
                ReplicaRecoveryInfo rInfo) {
      this.id = id;
      this.datanode = datanode;
      this.rInfo = rInfo;
    }

    void updateReplicaUnderRecovery(String bpid, long recoveryId,
                                    long newBlockId, long newLength)
        throws IOException {
      final ExtendedBlock b = new ExtendedBlock(bpid, rInfo);
      storageID = datanode.updateReplicaUnderRecovery(b, recoveryId, newBlockId,
          newLength);
    }

    @Override
    public String toString() {
      return "block:" + rInfo + " node:" + id;
    }
  }

  /** Recover a block */
  private void recoverBlock(RecoveringBlock rBlock) throws IOException {
    ExtendedBlock block = rBlock.getBlock();
    String blookPoolId = block.getBlockPoolId();
    DatanodeID[] datanodeids = rBlock.getLocations();
    List<BlockRecord> syncList = new ArrayList<BlockRecord>(datanodeids.length);
    int errorCount = 0;

    //check generation stamps
    for(DatanodeID id : datanodeids) {
      try {
        BPOfferService bpos = blockPoolManager.get(blookPoolId);
        DatanodeRegistration bpReg = bpos.bpRegistration;
        InterDatanodeProtocol datanode = bpReg.equals(id)?
            this: DataNode.createInterDataNodeProtocolProxy(id, getConf(),
                dnConf.socketTimeout, dnConf.connectToDnViaHostname);
        ReplicaRecoveryInfo info = callInitReplicaRecovery(datanode, rBlock);
        if (info != null &&
            info.getGenerationStamp() >= block.getGenerationStamp() &&
            info.getNumBytes() > 0) {
          syncList.add(new BlockRecord(id, datanode, info));
        }
      } catch (RecoveryInProgressException ripE) {
        InterDatanodeProtocol.LOG.warn(
            "Recovery for replica " + block + " on data-node " + id
            + " is already in progress. Recovery id = "
            + rBlock.getNewGenerationStamp() + " is aborted.", ripE);
        return;
      } catch (IOException e) {
        ++errorCount;
        InterDatanodeProtocol.LOG.warn(
            "Failed to obtain replica info for block (=" + block 
            + ") from datanode (=" + id + ")", e);
      }
    }

    if (errorCount == datanodeids.length) {
      throw new IOException("All datanodes failed: block=" + block
          + ", datanodeids=" + Arrays.asList(datanodeids));
    }

    syncBlock(rBlock, syncList);
  }

  /**
   * Get the NameNode corresponding to the given block pool.
   *
   * @param bpid Block pool Id
   * @return Namenode corresponding to the bpid
   * @throws IOException if unable to get the corresponding NameNode
   */
  public DatanodeProtocolClientSideTranslatorPB getActiveNamenodeForBP(String bpid)
      throws IOException {
    BPOfferService bpos = blockPoolManager.get(bpid);
    if (bpos == null) {
      throw new IOException("No block pool offer service for bpid=" + bpid);
    }
    
    DatanodeProtocolClientSideTranslatorPB activeNN = bpos.getActiveNN();
    if (activeNN == null) {
      throw new IOException(
          "Block pool " + bpid + " has not recognized an active NN");
    }
    return activeNN;
  }

  /** Block synchronization */
  void syncBlock(RecoveringBlock rBlock,
                         List<BlockRecord> syncList) throws IOException {
    ExtendedBlock block = rBlock.getBlock();
    final String bpid = block.getBlockPoolId();
    DatanodeProtocolClientSideTranslatorPB nn =
      getActiveNamenodeForBP(block.getBlockPoolId());

    long recoveryId = rBlock.getNewGenerationStamp();
    boolean isTruncateRecovery = rBlock.getNewBlock() != null;
    long blockId = (isTruncateRecovery) ?
        rBlock.getNewBlock().getBlockId() : block.getBlockId();

    if (LOG.isDebugEnabled()) {
      LOG.debug("block=" + block + ", (length=" + block.getNumBytes()
          + "), syncList=" + syncList);
    }

    // syncList.isEmpty() means that all data-nodes do not have the block
    // or their replicas have 0 length.
    // The block can be deleted.
    if (syncList.isEmpty()) {
      nn.commitBlockSynchronization(block, recoveryId, 0,
          true, true, DatanodeID.EMPTY_ARRAY, null);
      return;
    }

    // Calculate the best available replica state.
    ReplicaState bestState = ReplicaState.RWR;
    long finalizedLength = -1;
    for(BlockRecord r : syncList) {
      assert r.rInfo.getNumBytes() > 0 : "zero length replica";
      ReplicaState rState = r.rInfo.getOriginalReplicaState(); 
      if(rState.getValue() < bestState.getValue())
        bestState = rState;
      if(rState == ReplicaState.FINALIZED) {
        if(finalizedLength > 0 && finalizedLength != r.rInfo.getNumBytes())
          throw new IOException("Inconsistent size of finalized replicas. " +
              "Replica " + r.rInfo + " expected size: " + finalizedLength);
        finalizedLength = r.rInfo.getNumBytes();
      }
    }

    // Calculate list of nodes that will participate in the recovery
    // and the new block size
    List<BlockRecord> participatingList = new ArrayList<BlockRecord>();
    final ExtendedBlock newBlock = new ExtendedBlock(bpid, blockId,
        -1, recoveryId);
    switch(bestState) {
    case FINALIZED:
      assert finalizedLength > 0 : "finalizedLength is not positive";
      for(BlockRecord r : syncList) {
        ReplicaState rState = r.rInfo.getOriginalReplicaState();
        if(rState == ReplicaState.FINALIZED ||
           rState == ReplicaState.RBW &&
                      r.rInfo.getNumBytes() == finalizedLength)
          participatingList.add(r);
      }
      newBlock.setNumBytes(finalizedLength);
      break;
    case RBW:
    case RWR:
      long minLength = Long.MAX_VALUE;
      for(BlockRecord r : syncList) {
        ReplicaState rState = r.rInfo.getOriginalReplicaState();
        if(rState == bestState) {
          minLength = Math.min(minLength, r.rInfo.getNumBytes());
          participatingList.add(r);
        }
      }
      newBlock.setNumBytes(minLength);
      break;
    case RUR:
    case TEMPORARY:
      assert false : "bad replica state: " + bestState;
    }
    if(isTruncateRecovery)
      newBlock.setNumBytes(rBlock.getNewBlock().getNumBytes());

    List<DatanodeID> failedList = new ArrayList<DatanodeID>();
    final List<BlockRecord> successList = new ArrayList<BlockRecord>();
    for(BlockRecord r : participatingList) {
      try {
        r.updateReplicaUnderRecovery(bpid, recoveryId, blockId,
            newBlock.getNumBytes());
        successList.add(r);
      } catch (IOException e) {
        InterDatanodeProtocol.LOG.warn("Failed to updateBlock (newblock="
            + newBlock + ", datanode=" + r.id + ")", e);
        failedList.add(r.id);
      }
    }

    // If any of the data-nodes failed, the recovery fails, because
    // we never know the actual state of the replica on failed data-nodes.
    // The recovery should be started over.
    if(!failedList.isEmpty()) {
      StringBuilder b = new StringBuilder();
      for(DatanodeID id : failedList) {
        b.append("\n  " + id);
      }
      throw new IOException("Cannot recover " + block + ", the following "
          + failedList.size() + " data-nodes failed {" + b + "\n}");
    }

    // Notify the name-node about successfully recovered replicas.
    final DatanodeID[] datanodes = new DatanodeID[successList.size()];
    final String[] storages = new String[datanodes.length];
    for(int i = 0; i < datanodes.length; i++) {
      final BlockRecord r = successList.get(i);
      datanodes[i] = r.id;
      storages[i] = r.storageID;
    }
    nn.commitBlockSynchronization(block,
        newBlock.getGenerationStamp(), newBlock.getNumBytes(), true, false,
        datanodes, storages);
  }
  
  private static void logRecoverBlock(String who, RecoveringBlock rb) {
    ExtendedBlock block = rb.getBlock();
    DatanodeInfo[] targets = rb.getLocations();
    
    LOG.info(who + " calls recoverBlock(" + block
        + ", targets=[" + Joiner.on(", ").join(targets) + "]"
        + ", newGenerationStamp=" + rb.getNewGenerationStamp() + ")");
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
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got: " + id.toString());
        }
        blockPoolTokenSecretManager.checkAccess(id, null, block,
            BlockTokenSecretManager.AccessMode.READ);
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
      final String client) throws IOException {
    final long storedGS;
    final long visible;
    final BlockConstructionStage stage;

    //get replica information
    synchronized(data) {
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
      } else if (data.isValidBlock(b)) {
        stage = BlockConstructionStage.TRANSFER_FINALIZED;
      } else {
        final String r = data.getReplicaString(b.getBlockPoolId(), b.getBlockId());
        throw new IOException(b + " is neither a RBW nor a Finalized, r=" + r);
      }
      visible = data.getReplicaVisibleLength(b);
    }
    //set visible length
    b.setNumBytes(visible);

    if (targets.length > 0) {
      new DataTransfer(targets, targetStorageTypes, b, stage, client).run();
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
  public String getVersion() {
    return VersionInfo.getVersion();
  }
  
  @Override // DataNodeMXBean
  public String getRpcPort(){
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        this.getConf().get(DFS_DATANODE_IPC_ADDRESS_KEY));
    return Integer.toString(ipcAddr.getPort());
  }

  @Override // DataNodeMXBean
  public String getHttpPort(){
    return this.getConf().get("dfs.datanode.info.port");
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
   * Returned information is a JSON representation of a map with 
   * volume name as the key and value is a map of volume attribute 
   * keys to its values
   */
  @Override // DataNodeMXBean
  public String getVolumeInfo() {
    Preconditions.checkNotNull(data, "Storage not yet initialized");
    return JSON.toString(data.getVolumeInfoMap());
  }
  
  @Override // DataNodeMXBean
  public synchronized String getClusterId() {
    return clusterId;
  }
  
  public void refreshNamenodes(Configuration conf) throws IOException {
    blockPoolManager.refreshNamenodes(conf);
  }

  @Override // ClientDatanodeProtocol
  public void refreshNamenodes() throws IOException {
    checkSuperuserPrivilege();
    conf = new Configuration();
    refreshNamenodes(conf);
  }
  
  @Override // ClientDatanodeProtocol
  public void deleteBlockPool(String blockPoolId, boolean force)
      throws IOException {
    checkSuperuserPrivilege();
    LOG.info("deleteBlockPool command received for block pool " + blockPoolId
        + ", force=" + force);
    if (blockPoolManager.get(blockPoolId) != null) {
      LOG.warn("The block pool "+blockPoolId+
          " is still running, cannot be deleted.");
      throw new IOException(
          "The block pool is still running. First do a refreshNamenodes to " +
          "shutdown the block pool service");
    }
   
    data.deleteBlockPool(blockPoolId, force);
  }

  @Override // ClientDatanodeProtocol
  public synchronized void shutdownDatanode(boolean forUpgrade) throws IOException {
    checkSuperuserPrivilege();
    LOG.info("shutdownDatanode command received (upgrade=" + forUpgrade +
        "). Shutting down Datanode...");

    // Shutdown can be called only once.
    if (shutdownInProgress) {
      throw new IOException("Shutdown already in progress.");
    }
    shutdownInProgress = true;
    shutdownForUpgrade = forUpgrade;

    // Asynchronously start the shutdown process so that the rpc response can be
    // sent back.
    Thread shutdownThread = new Thread() {
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
  public DatanodeLocalInfo getDatanodeInfo() {
    long uptime = ManagementFactory.getRuntimeMXBean().getUptime()/1000;
    return new DatanodeLocalInfo(VersionInfo.getVersion(),
        confVersion, uptime);
  }

  @Override // ClientDatanodeProtocol
  public void startReconfiguration() throws IOException {
    checkSuperuserPrivilege();
    startReconfigurationTask();
  }

  @Override // ClientDatanodeProtocol
  public ReconfigurationTaskStatus getReconfigurationStatus() throws IOException {
    checkSuperuserPrivilege();
    return getReconfigurationTaskStatus();
  }

  @Override // ClientDatanodeProtocol
  public void triggerBlockReport(BlockReportOptions options)
      throws IOException {
    checkSuperuserPrivilege();
    for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
      if (bpos != null) {
        for (BPServiceActor actor : bpos.getBPServiceActors()) {
          actor.triggerBlockReport(options);
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

  /**
   * Get current value of the max balancer bandwidth in bytes per second.
   *
   * @return Balancer bandwidth in bytes per second for this datanode.
   */
  public Long getBalancerBandwidth() {
    DataXceiverServer dxcs =
                       (DataXceiverServer) this.dataXceiverServer.getRunnable();
    return dxcs.balanceThrottler.getBandwidth();
  }
  
  public DNConf getDnConf() {
    return dnConf;
  }

  public String getDatanodeUuid() {
    return id == null ? null : id.getDatanodeUuid();
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
  
  /**
   * Check the disk error
   */
  private void checkDiskError() {
    Set<File> unhealthyDataDirs = data.checkDataDir();
    if (unhealthyDataDirs != null && !unhealthyDataDirs.isEmpty()) {
      try {
        // Remove all unhealthy volumes from DataNode.
        removeVolumes(unhealthyDataDirs, false);
      } catch (IOException e) {
        LOG.warn("Error occurred when removing unhealthy storage dirs: "
            + e.getMessage(), e);
      }
      StringBuilder sb = new StringBuilder("DataNode failed volumes:");
      for (File dataDir : unhealthyDataDirs) {
        sb.append(dataDir.getAbsolutePath() + ";");
      }
      handleDiskError(sb.toString());
    }
  }

  /**
   * Starts a new thread which will check for disk error check request 
   * every 5 sec
   */
  private void startCheckDiskErrorThread() {
    checkDiskErrorThread = new Thread(new Runnable() {
          @Override
          public void run() {
            while(shouldRun) {
              boolean tempFlag ;
              synchronized(checkDiskErrorMutex) {
                tempFlag = checkDiskErrorFlag;
                checkDiskErrorFlag = false;
              }
              if(tempFlag) {
                try {
                  checkDiskError();
                } catch (Exception e) {
                  LOG.warn("Unexpected exception occurred while checking disk error  " + e);
                  checkDiskErrorThread = null;
                  return;
                }
                synchronized(checkDiskErrorMutex) {
                  lastDiskErrorCheck = Time.monotonicNow();
                }
              }
              try {
                Thread.sleep(checkDiskErrorInterval);
              } catch (InterruptedException e) {
                LOG.debug("InterruptedException in check disk error thread", e);
                checkDiskErrorThread = null;
                return;
              }
            }
          }
    });
  }
  
  public long getLastDiskErrorCheck() {
    synchronized(checkDiskErrorMutex) {
      return lastDiskErrorCheck;
    }
  }

  @Override
  public SpanReceiverInfo[] listSpanReceivers() throws IOException {
    checkSuperuserPrivilege();
    return spanReceiverHost.listSpanReceivers();
  }

  @Override
  public long addSpanReceiver(SpanReceiverInfo info) throws IOException {
    checkSuperuserPrivilege();
    return spanReceiverHost.addSpanReceiver(info);
  }

  @Override
  public void removeSpanReceiver(long id) throws IOException {
    checkSuperuserPrivilege();
    spanReceiverHost.removeSpanReceiver(id);
  }
}
