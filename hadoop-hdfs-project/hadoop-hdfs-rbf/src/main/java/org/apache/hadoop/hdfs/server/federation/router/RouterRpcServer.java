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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_QUEUE_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_QUEUE_SIZE_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_READER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_READER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_READER_QUEUE_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_READER_QUEUE_SIZE_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ASYNC_HANDLER_COUNT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ASYNC_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ASYNC_RESPONDER_COUNT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ASYNC_RESPONDER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ENABLE_ASYNC;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ENABLE_ASYNC_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DN_REPORT_CACHE_EXPIRE;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DN_REPORT_CACHE_EXPIRE_MS_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_OPTION;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_OPTION_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RouterFederationRename.RouterRenameOption;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncApply;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncCatch;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncComplete;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncForEach;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncReturn;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncTry;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.SCHEDULER_JOURNAL_URI;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil;
import org.apache.hadoop.hdfs.server.federation.router.async.AsyncQuota;
import org.apache.hadoop.hdfs.server.federation.router.async.RouterAsyncClientProtocol;
import org.apache.hadoop.hdfs.server.federation.router.async.RouterAsyncNamenodeProtocol;
import org.apache.hadoop.hdfs.server.federation.router.async.RouterAsyncRpcClient;
import org.apache.hadoop.hdfs.server.federation.router.async.RouterAsyncUserProtocol;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.ApplyFunction;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncCatchFunction;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.CatchFunction;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.BatchedDirectoryListing;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.ReencryptAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.NamenodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.RouterPolicyProvider;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.router.security.RouterSecurityManager;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceProcedureScheduler;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;

/**
 * This class is responsible for handling all of the RPC calls to the It is
 * created, started, and stopped by {@link Router}. It implements the
 * {@link ClientProtocol} to mimic a
 * {@link org.apache.hadoop.hdfs.server.namenode.NameNode NameNode} and proxies
 * the requests to the active
 * {@link org.apache.hadoop.hdfs.server.namenode.NameNode NameNode}.
 */
public class RouterRpcServer extends AbstractService implements ClientProtocol,
    NamenodeProtocol, RefreshUserMappingsProtocol, GetUserMappingsProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterRpcServer.class);
  private ExecutorService asyncRouterHandler;
  private ExecutorService asyncRouterResponder;

  /** Configuration for the RPC server. */
  private Configuration conf;

  /** Router using this RPC server. */
  private final Router router;

  /** Alignment context storing state IDs for all namespaces this router serves. */
  private final RouterStateIdContext routerStateIdContext;

  /** The RPC server that listens to requests from clients. */
  private final Server rpcServer;
  /** The address for this RPC server. */
  private final InetSocketAddress rpcAddress;

  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;

  /** Monitor metrics for the RPC calls. */
  private final RouterRpcMonitor rpcMonitor;

  /** If we use authentication for the connections. */
  private final boolean serviceAuthEnabled;


  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private final ActiveNamenodeResolver namenodeResolver;

  /** Interface to map global name space to HDFS subcluster name spaces. */
  private final FileSubclusterResolver subclusterResolver;

  /** Category of the operation that a thread is executing. */
  private final ThreadLocal<OperationCategory> opCategory = new ThreadLocal<>();

  // Modules implementing groups of RPC calls
  /** Router Quota calls. */
  private final Quota quotaCall;
  /** NamenodeProtocol calls. */
  private final RouterNamenodeProtocol nnProto;
  /** ClientProtocol calls. */
  private final RouterClientProtocol clientProto;
  /** Other protocol calls. */
  private final RouterUserProtocol routerProto;
  /** Router security manager to handle token operations. */
  private RouterSecurityManager securityManager = null;
  /** Super user credentials that a thread may use. */
  private static final ThreadLocal<UserGroupInformation> CUR_USER =
      new ThreadLocal<>();

  /** DN type -> full DN report. */
  private final LoadingCache<DatanodeReportType, DatanodeInfo[]> dnCache;

  /** Specify the option of router federation rename. */
  private RouterRenameOption routerRenameOption;
  /** Schedule the router federation rename jobs. */
  private BalanceProcedureScheduler fedRenameScheduler;
  private boolean enableAsync;

  /**
   * Construct a router RPC server.
   *
   * @param conf HDFS Configuration.
   * @param router A router using this RPC server.
   * @param nnResolver The NN resolver instance to determine active NNs in HA.
   * @param fileResolver File resolver to resolve file paths to subclusters.
   * @throws IOException If the RPC server could not be created.
   */
  @SuppressWarnings("checkstyle:MethodLength")
  public RouterRpcServer(Configuration conf, Router router,
      ActiveNamenodeResolver nnResolver, FileSubclusterResolver fileResolver)
          throws IOException {
    super(RouterRpcServer.class.getName());

    this.conf = conf;
    this.router = router;
    this.namenodeResolver = nnResolver;
    this.subclusterResolver = fileResolver;

    // RPC server settings
    int handlerCount = this.conf.getInt(DFS_ROUTER_HANDLER_COUNT_KEY,
        DFS_ROUTER_HANDLER_COUNT_DEFAULT);

    int readerCount = this.conf.getInt(DFS_ROUTER_READER_COUNT_KEY,
        DFS_ROUTER_READER_COUNT_DEFAULT);

    int handlerQueueSize = this.conf.getInt(DFS_ROUTER_HANDLER_QUEUE_SIZE_KEY,
        DFS_ROUTER_HANDLER_QUEUE_SIZE_DEFAULT);

    this.enableAsync = conf.getBoolean(DFS_ROUTER_RPC_ENABLE_ASYNC,
        DFS_ROUTER_RPC_ENABLE_ASYNC_DEFAULT);
    LOG.info("Router enable async {}", this.enableAsync);
    if (this.enableAsync) {
      initAsyncThreadPool();
    }
    // Override Hadoop Common IPC setting
    int readerQueueSize = this.conf.getInt(DFS_ROUTER_READER_QUEUE_SIZE_KEY,
        DFS_ROUTER_READER_QUEUE_SIZE_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeys.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY,
        readerQueueSize);

    RPC.setProtocolEngine(this.conf, ClientNamenodeProtocolPB.class,
        ProtobufRpcEngine2.class);

    ClientNamenodeProtocolServerSideTranslatorPB
        clientProtocolServerTranslator =
            new ClientNamenodeProtocolServerSideTranslatorPB(this);
    BlockingService clientNNPbService = ClientNamenodeProtocol
        .newReflectiveBlockingService(clientProtocolServerTranslator);

    NamenodeProtocolServerSideTranslatorPB namenodeProtocolXlator =
        new NamenodeProtocolServerSideTranslatorPB(this);
    BlockingService nnPbService = NamenodeProtocolService
        .newReflectiveBlockingService(namenodeProtocolXlator);

    RefreshUserMappingsProtocolServerSideTranslatorPB refreshUserMappingXlator =
        new RefreshUserMappingsProtocolServerSideTranslatorPB(this);
    BlockingService refreshUserMappingService =
        RefreshUserMappingsProtocolProtos.RefreshUserMappingsProtocolService.
        newReflectiveBlockingService(refreshUserMappingXlator);

    GetUserMappingsProtocolServerSideTranslatorPB getUserMappingXlator =
        new GetUserMappingsProtocolServerSideTranslatorPB(this);
    BlockingService getUserMappingService =
        GetUserMappingsProtocolProtos.GetUserMappingsProtocolService.
        newReflectiveBlockingService(getUserMappingXlator);

    InetSocketAddress confRpcAddress = conf.getSocketAddr(
        RBFConfigKeys.DFS_ROUTER_RPC_BIND_HOST_KEY,
        RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY,
        RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_DEFAULT,
        RBFConfigKeys.DFS_ROUTER_RPC_PORT_DEFAULT);
    LOG.info("RPC server binding to {} with {} handlers for Router {}",
        confRpcAddress, handlerCount, this.router.getRouterId());

    // Create security manager
    this.securityManager = new RouterSecurityManager(this.conf);
    routerStateIdContext = new RouterStateIdContext(conf);

    this.rpcServer = new RPC.Builder(this.conf)
        .setProtocol(ClientNamenodeProtocolPB.class)
        .setInstance(clientNNPbService)
        .setBindAddress(confRpcAddress.getHostName())
        .setPort(confRpcAddress.getPort())
        .setNumHandlers(handlerCount)
        .setNumReaders(readerCount)
        .setQueueSizePerHandler(handlerQueueSize)
        .setVerbose(false)
        .setAlignmentContext(routerStateIdContext)
        .setSecretManager(this.securityManager.getSecretManager())
        .build();

    // Add all the RPC protocols that the Router implements
    DFSUtil.addInternalPBProtocol(
        conf, NamenodeProtocolPB.class, nnPbService, this.rpcServer);
    DFSUtil.addInternalPBProtocol(conf, RefreshUserMappingsProtocolPB.class,
        refreshUserMappingService, this.rpcServer);
    DFSUtil.addInternalPBProtocol(conf, GetUserMappingsProtocolPB.class,
        getUserMappingService, this.rpcServer);

    // Set service-level authorization security policy
    this.serviceAuthEnabled = conf.getBoolean(
        HADOOP_SECURITY_AUTHORIZATION, false);
    if (this.serviceAuthEnabled) {
      rpcServer.refreshServiceAcl(conf, new RouterPolicyProvider());
    }

    // We don't want the server to log the full stack trace for some exceptions
    this.rpcServer.addTerseExceptions(
        RemoteException.class,
        SafeModeException.class,
        FileNotFoundException.class,
        FileAlreadyExistsException.class,
        AccessControlException.class,
        LeaseExpiredException.class,
        NotReplicatedYetException.class,
        IOException.class,
        ConnectException.class,
        RetriableException.class);

    this.rpcServer.addSuppressedLoggingExceptions(
        StandbyException.class, UnresolvedPathException.class);

    // The RPC-server port can be ephemeral... ensure we have the correct info
    InetSocketAddress listenAddress = this.rpcServer.getListenerAddress();
    this.rpcAddress = new InetSocketAddress(
        confRpcAddress.getHostName(), listenAddress.getPort());

    if (conf.getBoolean(RBFConfigKeys.DFS_ROUTER_METRICS_ENABLE,
        RBFConfigKeys.DFS_ROUTER_METRICS_ENABLE_DEFAULT)) {
      // Create metrics monitor
      Class<? extends RouterRpcMonitor> rpcMonitorClass = this.conf.getClass(
          RBFConfigKeys.DFS_ROUTER_METRICS_CLASS,
          RBFConfigKeys.DFS_ROUTER_METRICS_CLASS_DEFAULT,
          RouterRpcMonitor.class);
      this.rpcMonitor = ReflectionUtils.newInstance(rpcMonitorClass, conf);
    } else {
      this.rpcMonitor = null;
    }

    // Create the client
    if (this.enableAsync) {
      this.rpcClient = new RouterAsyncRpcClient(this.conf, this.router,
          this.namenodeResolver, this.rpcMonitor, routerStateIdContext);
      this.clientProto = new RouterAsyncClientProtocol(conf, this);
      this.nnProto = new RouterAsyncNamenodeProtocol(this);
      this.routerProto = new RouterAsyncUserProtocol(this);
      this.quotaCall = new AsyncQuota(this.router, this);
    } else {
      this.rpcClient = new RouterRpcClient(this.conf, this.router,
          this.namenodeResolver, this.rpcMonitor, routerStateIdContext);
      this.clientProto = new RouterClientProtocol(conf, this);
      this.nnProto = new RouterNamenodeProtocol(this);
      this.routerProto = new RouterUserProtocol(this);
      this.quotaCall = new Quota(this.router, this);
    }

    long dnCacheExpire = conf.getTimeDuration(
        DN_REPORT_CACHE_EXPIRE,
        DN_REPORT_CACHE_EXPIRE_MS_DEFAULT, TimeUnit.MILLISECONDS);
    this.dnCache = CacheBuilder.newBuilder()
        .build(new DatanodeReportCacheLoader());

    // Actively refresh the dn cache in a configured interval
    Executors
        .newSingleThreadScheduledExecutor()
        .scheduleWithFixedDelay(() -> this.dnCache
                .asMap()
                .keySet()
                .parallelStream()
                .forEach(this.dnCache::refresh),
            0,
            dnCacheExpire, TimeUnit.MILLISECONDS);

    Executors
        .newSingleThreadScheduledExecutor()
        .scheduleWithFixedDelay(this::clearStaleNamespacesInRouterStateIdContext,
            0,
            conf.getLong(RBFConfigKeys.FEDERATION_STORE_MEMBERSHIP_EXPIRATION_MS,
                RBFConfigKeys.FEDERATION_STORE_MEMBERSHIP_EXPIRATION_MS_DEFAULT),
            TimeUnit.MILLISECONDS);

    initRouterFedRename();
  }

  /**
   * Init router async handlers and router async responders.
   */
  public void initAsyncThreadPool() {
    int asyncHandlerCount = conf.getInt(DFS_ROUTER_RPC_ASYNC_HANDLER_COUNT,
        DFS_ROUTER_RPC_ASYNC_HANDLER_COUNT_DEFAULT);
    int asyncResponderCount = conf.getInt(DFS_ROUTER_RPC_ASYNC_RESPONDER_COUNT,
        DFS_ROUTER_RPC_ASYNC_RESPONDER_COUNT_DEFAULT);
    if (asyncRouterHandler == null) {
      LOG.info("init router async handler count: {}", asyncHandlerCount);
      asyncRouterHandler = Executors.newFixedThreadPool(
          asyncHandlerCount, new AsyncThreadFactory("router async handler "));
    }
    if (asyncRouterResponder == null) {
      LOG.info("init router async responder count: {}", asyncResponderCount);
      asyncRouterResponder = Executors.newFixedThreadPool(
          asyncResponderCount, new AsyncThreadFactory("router async responder "));
    }
    AsyncRpcProtocolPBUtil.setWorker(asyncRouterResponder);
  }

  /**
   * Clear expired namespace in the shared RouterStateIdContext.
   */
  private void clearStaleNamespacesInRouterStateIdContext() {
    if (!router.isRouterState(RouterServiceState.RUNNING)) {
      return;
    }
    try {
      final Set<String> resolvedNamespaces = namenodeResolver.getNamespaces()
          .stream()
          .map(FederationNamespaceInfo::getNameserviceId)
          .collect(Collectors.toSet());

      routerStateIdContext.getNamespaces().forEach(namespace -> {
        if (!resolvedNamespaces.contains(namespace)) {
          routerStateIdContext.removeNamespaceStateId(namespace);
        }
      });
    } catch (IOException e) {
      LOG.warn("Could not fetch current list of namespaces.", e);
    }
  }

  /**
   * Init the router federation rename environment. Each router has its own
   * journal path.
   * In HA mode the journal path is:
   *   JOURNAL_BASE/nsId/namenodeId
   * e.g.
   *   /journal/router-namespace/host0
   * In non-ha mode the journal path is based on ip and port:
   *   JOURNAL_BASE/host_port
   * e.g.
   *   /journal/0.0.0.0_8888
   */
  private void initRouterFedRename() throws IOException {
    routerRenameOption = RouterRenameOption.valueOf(
        conf.get(DFS_ROUTER_FEDERATION_RENAME_OPTION,
            DFS_ROUTER_FEDERATION_RENAME_OPTION_DEFAULT).toUpperCase());
    switch (routerRenameOption) {
    case DISTCP:
      RouterFederationRename.checkConfiguration(conf);
      Configuration sConf = new Configuration(conf);
      URI journalUri;
      try {
        journalUri = new URI(sConf.get(SCHEDULER_JOURNAL_URI));
      } catch (URISyntaxException | NullPointerException e) {
        throw new IOException("Bad journal uri. Please check configuration for "
            + SCHEDULER_JOURNAL_URI);
      }
      Path child;
      String nsId = DFSUtil.getNamenodeNameServiceId(conf);
      String namenodeId = HAUtil.getNameNodeId(conf, nsId);
      InetSocketAddress listenAddress = this.rpcServer.getListenerAddress();
      if (nsId == null || namenodeId == null) {
        child = new Path(
            listenAddress.getHostName() + "_" + listenAddress.getPort());
      } else {
        child = new Path(nsId, namenodeId);
      }
      String routerJournal = new Path(journalUri.toString(), child).toString();
      sConf.set(SCHEDULER_JOURNAL_URI, routerJournal);
      fedRenameScheduler = new BalanceProcedureScheduler(sConf);
      fedRenameScheduler.init(true);
      break;
    case NONE:
      fedRenameScheduler = null;
      break;
    default:
      break;
    }
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    this.conf = configuration;

    if (this.rpcMonitor == null) {
      LOG.info("Do not start Router RPC metrics");
    } else {
      this.rpcMonitor.init(this.conf, this, this.router.getStateStore());
    }

    super.serviceInit(configuration);
  }

  @Override
  protected void serviceStart() throws Exception {
    if (this.rpcServer != null) {
      this.rpcServer.start();
      LOG.info("Router RPC up at: {}", this.getRpcAddress());
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.rpcServer != null) {
      this.rpcServer.stop();
    }
    if (rpcMonitor != null) {
      this.rpcMonitor.close();
    }
    if (securityManager != null) {
      this.securityManager.stop();
    }
    if (this.fedRenameScheduler != null) {
      fedRenameScheduler.shutDown();
    }
    super.serviceStop();
  }

  boolean isEnableRenameAcrossNamespace() {
    return routerRenameOption != RouterRenameOption.NONE;
  }

  BalanceProcedureScheduler getFedRenameScheduler() {
    return this.fedRenameScheduler;
  }

  /**
   * Get the routerStateIdContext used by this server.
   * @return routerStateIdContext
   */
  @VisibleForTesting
  public RouterStateIdContext getRouterStateIdContext() {
    return routerStateIdContext;
  }

  /**
   * Get the RPC security manager.
   *
   * @return RPC security manager.
   */
  public RouterSecurityManager getRouterSecurityManager() {
    return this.securityManager;
  }

  /**
   * Get the RPC client to the Namenode.
   *
   * @return RPC clients to the Namenodes.
   */
  public RouterRpcClient getRPCClient() {
    return rpcClient;
  }

  /**
   * Get the subcluster resolver.
   *
   * @return Subcluster resolver.
   */
  public FileSubclusterResolver getSubclusterResolver() {
    return subclusterResolver;
  }

  /**
   * Get the active namenode resolver.
   *
   * @return Active namenode resolver.
   */
  public ActiveNamenodeResolver getNamenodeResolver() {
    return namenodeResolver;
  }

  /**
   * Get the RPC monitor and metrics.
   *
   * @return RPC monitor and metrics.
   */
  public RouterRpcMonitor getRPCMonitor() {
    return rpcMonitor;
  }

  /**
   * Allow access to the client RPC server for testing.
   *
   * @return The RPC server.
   */
  @VisibleForTesting
  public Server getServer() {
    return rpcServer;
  }

  /**
   * Get the RPC address of the service.
   *
   * @return RPC service address.
   */
  public InetSocketAddress getRpcAddress() {
    return rpcAddress;
  }

  /**
   * Check if the Router is in safe mode. We should only see READ, WRITE, and
   * UNCHECKED. It includes a default handler when we haven't implemented an
   * operation. If not supported, it always throws an exception reporting the
   * operation.
   *
   * @param op Category of the operation to check.
   * @param supported If the operation is supported or not. If not, it will
   *                  throw an UnsupportedOperationException.
   * @throws StandbyException If the Router is in safe mode and cannot serve
   *                           client requests.
   * @throws UnsupportedOperationException If the operation is not supported.
   */
  public void checkOperation(OperationCategory op, boolean supported)
      throws StandbyException, UnsupportedOperationException {
    checkOperation(op);

    if (!supported) {
      if (rpcMonitor != null) {
        rpcMonitor.proxyOpNotImplemented();
      }
      String methodName = getMethodName();
      throw new UnsupportedOperationException(
          "Operation \"" + methodName + "\" is not supported");
    }
  }

  /**
   * Check if the Router is in safe mode. We should only see READ, WRITE, and
   * UNCHECKED. This function should be called by all ClientProtocol functions.
   *
   * @param op Category of the operation to check.
   * @throws StandbyException If the Router is in safe mode and cannot serve
   *                           client requests.
   */
  public void checkOperation(OperationCategory op)
      throws StandbyException {
    // Log the function we are currently calling.
    if (rpcMonitor != null) {
      rpcMonitor.startOp();
    }
    // Log the function we are currently calling.
    if (LOG.isDebugEnabled()) {
      String methodName = getMethodName();
      LOG.debug("Proxying operation: {}", methodName);
    }

    // Store the category of the operation category for this thread
    opCategory.set(op);

    // We allow unchecked and read operations to try, fail later
    if (op == OperationCategory.UNCHECKED || op == OperationCategory.READ) {
      return;
    }
    checkSafeMode();
  }

  /**
   * Check if the Router is in safe mode.
   * @throws StandbyException If the Router is in safe mode and cannot serve
   *                          client requests.
   */
  private void checkSafeMode() throws StandbyException {
    if (isSafeMode()) {
      // Throw standby exception, router is not available
      if (rpcMonitor != null) {
        rpcMonitor.routerFailureSafemode();
      }
      OperationCategory op = opCategory.get();
      throw new StandbyException("Router " + router.getRouterId() +
          " is in safe mode and cannot handle " + op + " requests");
    }
  }

  /**
   * Return true if the Router is in safe mode.
   *
   * @return true if the Router is in safe mode.
   */
  boolean isSafeMode() {
    RouterSafemodeService safemodeService = router.getSafemodeService();
    return (safemodeService != null && safemodeService.isInSafeMode());
  }

  /**
   * Get the name of the method that is calling this function.
   *
   * @return Name of the method calling this function.
   */
  static String getMethodName() {
    final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    String methodName = stack[3].getMethodName();
    return methodName;
  }

  /**
   * Invokes the method at default namespace, if default namespace is not
   * available then at the other available namespaces.
   * If the namespace is unavailable, retry with other namespaces.
   * @param <T> expected return type.
   * @param method the remote method.
   * @param clazz the type of return value.
   * @return the response received after invoking method.
   * @throws IOException if there is no namespace available or other ioExceptions.
   */
  <T> T invokeAtAvailableNs(RemoteMethod method, Class<T> clazz)
      throws IOException {
    String nsId = subclusterResolver.getDefaultNamespace();
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    // If no namespace is available, then throw this IOException.
    IOException io = new IOException("No namespace available.");
    // If default Ns is present return result from that namespace.
    if (!nsId.isEmpty()) {
      try {
        return rpcClient.invokeSingle(nsId, method, clazz);
      } catch (IOException ioe) {
        if (!clientProto.isUnavailableSubclusterException(ioe)) {
          LOG.debug("{} exception cannot be retried",
              ioe.getClass().getSimpleName());
          throw ioe;
        }
        // Remove the already tried namespace.
        nss.removeIf(n -> n.getNameserviceId().equals(nsId));
        return invokeOnNs(method, clazz, io, nss);
      }
    }
    return invokeOnNs(method, clazz, io, nss);
  }

  /**
   * Invokes the method at default namespace, if default namespace is not
   * available then at the other available namespaces.
   * If the namespace is unavailable, retry with other namespaces.
   * Asynchronous version of invokeAtAvailableNs method.
   * @param <T> expected return type.
   * @param method the remote method.
   * @param clazz the type of return value.
   * @return the response received after invoking method.
   * @throws IOException if there is no namespace available or other ioExceptions.
   */
  public <T> T invokeAtAvailableNsAsync(RemoteMethod method, Class<T> clazz)
      throws IOException {
    String nsId = subclusterResolver.getDefaultNamespace();
    // If default Ns is not present return result from first namespace.
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    // If no namespace is available, throw IOException.
    IOException io = new IOException("No namespace available.");

    asyncComplete(null);
    if (!nsId.isEmpty()) {
      asyncTry(() -> {
        getRPCClient().invokeSingle(nsId, method, clazz);
      });

      asyncCatch((AsyncCatchFunction<T, IOException>)(res, ioe) -> {
        if (!clientProto.isUnavailableSubclusterException(ioe)) {
          LOG.debug("{} exception cannot be retried",
              ioe.getClass().getSimpleName());
          throw ioe;
        }
        nss.removeIf(n -> n.getNameserviceId().equals(nsId));
        invokeOnNsAsync(method, clazz, io, nss);
      }, IOException.class);
    } else {
      // If not have default NS.
      invokeOnNsAsync(method, clazz, io, nss);
    }
    return asyncReturn(clazz);
  }

  /**
   * Invoke the method sequentially on available namespaces,
   * throw no namespace available exception, if no namespaces are available.
   * @param method the remote method.
   * @param clazz  Class for the return type.
   * @param ioe    IOException .
   * @param nss    List of name spaces in the federation
   * @return the response received after invoking method.
   * @throws IOException if there is no namespace available or other ioExceptions.
   */
  <T> T invokeOnNs(RemoteMethod method, Class<T> clazz, IOException ioe,
      Set<FederationNamespaceInfo> nss) throws IOException {
    if (nss.isEmpty()) {
      throw ioe;
    }
    for (FederationNamespaceInfo fnInfo : nss) {
      String nsId = fnInfo.getNameserviceId();
      LOG.debug("Invoking {} on namespace {}", method, nsId);
      try {
        return rpcClient.invokeSingle(nsId, method, clazz);
      } catch (IOException e) {
        LOG.debug("Failed to invoke {} on namespace {}", method, nsId, e);
        // Ignore the exception and try on other namespace, if the tried
        // namespace is unavailable, else throw the received exception.
        if (!clientProto.isUnavailableSubclusterException(e)) {
          throw e;
        }
      }
    }
    // Couldn't get a response from any of the namespace, throw ioe.
    throw ioe;
  }

  /**
   * Invoke the method sequentially on available namespaces,
   * throw no namespace available exception, if no namespaces are available.
   * Asynchronous version of invokeOnNs method.
   * @param method the remote method.
   * @param clazz  Class for the return type.
   * @param ioe    IOException .
   * @param nss    List of name spaces in the federation
   * @return the response received after invoking method.
   * @throws IOException if there is no namespace available or other ioExceptions.
   */
  <T> T invokeOnNsAsync(RemoteMethod method, Class<T> clazz, IOException ioe,
      Set<FederationNamespaceInfo> nss) throws IOException {
    if (nss.isEmpty()) {
      throw ioe;
    }

    asyncComplete(null);
    Iterator<FederationNamespaceInfo> nsIterator = nss.iterator();
    asyncForEach(nsIterator, (foreach, fnInfo) -> {
      String nsId = fnInfo.getNameserviceId();
      LOG.debug("Invoking {} on namespace {}", method, nsId);
      asyncTry(() -> {
        getRPCClient().invokeSingle(nsId, method, clazz);
        asyncApply(result -> {
          if (result != null) {
            foreach.breakNow();
            return result;
          }
          return null;
        });
      });

      asyncCatch((CatchFunction<T, IOException>)(ret, ex) -> {
        LOG.debug("Failed to invoke {} on namespace {}", method, nsId, ex);
        // Ignore the exception and try on other namespace, if the tried
        // namespace is unavailable, else throw the received exception.
        if (!clientProto.isUnavailableSubclusterException(ex)) {
          throw ex;
        }
        return null;
      }, IOException.class);
    });

    asyncApply(obj -> {
      if (obj == null) {
        // Couldn't get a response from any of the namespace, throw ioe.
        throw ioe;
      }
      return obj;
    });

    return asyncReturn(clazz);
  }

  @Override // ClientProtocol
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return clientProto.getDelegationToken(renewer);
  }

  @Override // ClientProtocol
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    return clientProto.renewDelegationToken(token);
  }

  @Override // ClientProtocol
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    clientProto.cancelDelegationToken(token);
  }

  @Override // ClientProtocol
  public LocatedBlocks getBlockLocations(String src, final long offset,
      final long length) throws IOException {
    return clientProto.getBlockLocations(src, offset, length);
  }

  @Override // ClientProtocol
  public FsServerDefaults getServerDefaults() throws IOException {
    return clientProto.getServerDefaults();
  }

  @Override // ClientProtocol
  public HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag,
      boolean createParent, short replication, long blockSize,
      CryptoProtocolVersion[] supportedVersions, String ecPolicyName,
      String storagePolicy)
      throws IOException {
    return clientProto.create(src, masked, clientName, flag, createParent,
        replication, blockSize, supportedVersions, ecPolicyName, storagePolicy);
  }


  /**
   * Get the location to create a file. It checks if the file already existed
   * in one of the locations.
   *
   * @param src Path of the file to check.
   * @return The remote location for this file.
   * @throws IOException If the file has no creation location.
   */
  RemoteLocation getCreateLocation(final String src) throws IOException {
    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    if (isAsync()) {
      getCreateLocationAsync(src, locations);
      return asyncReturn(RemoteLocation.class);
    }
    return getCreateLocation(src, locations);
  }

  /**
   * Get the location to create a file. It checks if the file already existed
   * in one of the locations.
   *
   * @param src Path of the file to check.
   * @param locations Prefetched locations for the file.
   * @return The remote location for this file.
   * @throws IOException If the file has no creation location.
   */
  RemoteLocation getCreateLocation(
      final String src, final List<RemoteLocation> locations)
      throws IOException {

    if (locations == null || locations.isEmpty()) {
      throw new IOException("Cannot get locations to create " + src);
    }

    RemoteLocation createLocation = locations.get(0);
    if (locations.size() > 1) {
      try {
        RemoteLocation existingLocation = getExistingLocation(src, locations);
        // Forward to the existing location and let the NN handle the error
        if (existingLocation != null) {
          LOG.debug("{} already exists in {}.", src, existingLocation);
          createLocation = existingLocation;
        }
      } catch (FileNotFoundException fne) {
        // Ignore if the file is not found
      }
    }
    return createLocation;
  }

  /**
   * Get the location to create a file. It checks if the file already existed
   * in one of the locations.
   * Asynchronous version of getCreateLocation method.
   *
   * @param src Path of the file to check.
   * @param locations Prefetched locations for the file.
   * @return The remote location for this file.
   * @throws IOException If the file has no creation location.
   */
  public RemoteLocation getCreateLocationAsync(
      final String src, final List<RemoteLocation> locations)
      throws IOException {

    if (locations == null || locations.isEmpty()) {
      throw new IOException("Cannot get locations to create " + src);
    }

    RemoteLocation createLocation = locations.get(0);
    if (locations.size() > 1) {
      asyncTry(() -> {
        getExistingLocationAsync(src, locations);
        asyncApply((ApplyFunction<RemoteLocation, RemoteLocation>) existingLocation -> {
          if (existingLocation != null) {
            LOG.debug("{} already exists in {}.", src, existingLocation);
            return existingLocation;
          }
          return createLocation;
        });
      });
      asyncCatch((o, e) -> createLocation, FileNotFoundException.class);
    } else {
      asyncComplete(createLocation);
    }

    return asyncReturn(RemoteLocation.class);
  }

  /**
   * Gets the remote location where the file exists.
   * @param src the name of file.
   * @param locations all the remote locations.
   * @return the remote location of the file if it exists, else null.
   * @throws IOException in case of any exception.
   */
  private RemoteLocation getExistingLocation(String src,
      List<RemoteLocation> locations) throws IOException {
    RemoteMethod method = new RemoteMethod("getFileInfo",
        new Class<?>[] {String.class}, new RemoteParam());
    Map<RemoteLocation, HdfsFileStatus> results = rpcClient.invokeConcurrent(
        locations, method, true, false, HdfsFileStatus.class);
    for (RemoteLocation loc : locations) {
      if (results.get(loc) != null) {
        return loc;
      }
    }
    return null;
  }

  /**
   * Gets the remote location where the file exists.
   * Asynchronous version of getExistingLocation method.
   * @param src the name of file.
   * @param locations all the remote locations.
   * @return the remote location of the file if it exists, else null.
   * @throws IOException in case of any exception.
   */
  private RemoteLocation getExistingLocationAsync(String src,
      List<RemoteLocation> locations) throws IOException {
    RemoteMethod method = new RemoteMethod("getFileInfo",
        new Class<?>[] {String.class}, new RemoteParam());
    getRPCClient().invokeConcurrent(
        locations, method, true, false, HdfsFileStatus.class);
    asyncApply((ApplyFunction<Map<RemoteLocation, HdfsFileStatus>, Object>) results -> {
      for (RemoteLocation loc : locations) {
        if (results.get(loc) != null) {
          return loc;
        }
      }
      return null;
    });
    return asyncReturn(RemoteLocation.class);
  }

  @Override // ClientProtocol
  public LastBlockWithStatus append(String src, final String clientName,
      final EnumSetWritable<CreateFlag> flag) throws IOException {
    return clientProto.append(src, clientName, flag);
  }

  @Override // ClientProtocol
  public boolean recoverLease(String src, String clientName)
      throws IOException {
    return clientProto.recoverLease(src, clientName);
  }

  @Override // ClientProtocol
  public boolean setReplication(String src, short replication)
      throws IOException {
    return clientProto.setReplication(src, replication);
  }

  @Override // ClientProtocol
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    clientProto.setStoragePolicy(src, policyName);
  }

  @Override // ClientProtocol
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    return clientProto.getStoragePolicies();
  }

  @Override // ClientProtocol
  public void setPermission(String src, FsPermission permissions)
      throws IOException {
    clientProto.setPermission(src, permissions);
  }

  @Override // ClientProtocol
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    clientProto.setOwner(src, username, groupname);
  }

  /**
   * Excluded and favored nodes are not verified and will be ignored by
   * placement policy if they are not in the same nameservice as the file.
   */
  @Override // ClientProtocol
  public LocatedBlock addBlock(String src, String clientName,
      ExtendedBlock previous, DatanodeInfo[] excludedNodes, long fileId,
      String[] favoredNodes, EnumSet<AddBlockFlag> addBlockFlags)
      throws IOException {
    return clientProto.addBlock(src, clientName, previous, excludedNodes,
        fileId, favoredNodes, addBlockFlags);
  }

  /**
   * Excluded nodes are not verified and will be ignored by placement if they
   * are not in the same nameservice as the file.
   */
  @Override // ClientProtocol
  public LocatedBlock getAdditionalDatanode(final String src, final long fileId,
      final ExtendedBlock blk, final DatanodeInfo[] existings,
      final String[] existingStorageIDs, final DatanodeInfo[] excludes,
      final int numAdditionalNodes, final String clientName)
          throws IOException {
    return clientProto.getAdditionalDatanode(src, fileId, blk, existings,
        existingStorageIDs, excludes, numAdditionalNodes, clientName);
  }

  @Override // ClientProtocol
  public void abandonBlock(ExtendedBlock b, long fileId, String src,
      String holder) throws IOException {
    clientProto.abandonBlock(b, fileId, src, holder);
  }

  @Override // ClientProtocol
  public boolean complete(String src, String clientName, ExtendedBlock last,
      long fileId) throws IOException {
    return clientProto.complete(src, clientName, last, fileId);
  }

  @Override // ClientProtocol
  public LocatedBlock updateBlockForPipeline(
      ExtendedBlock block, String clientName) throws IOException {
    return clientProto.updateBlockForPipeline(block, clientName);
  }

  /**
   * Datanode are not verified to be in the same nameservice as the old block.
   * TODO This may require validation.
   */
  @Override // ClientProtocol
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs)
          throws IOException {
    clientProto.updatePipeline(clientName, oldBlock, newBlock, newNodes,
        newStorageIDs);
  }

  @Override // ClientProtocol
  public long getPreferredBlockSize(String src) throws IOException {
    return clientProto.getPreferredBlockSize(src);
  }

  @Deprecated
  @Override // ClientProtocol
  public boolean rename(final String src, final String dst)
      throws IOException {
    return clientProto.rename(src, dst);
  }

  @Override // ClientProtocol
  public void rename2(final String src, final String dst,
      final Options.Rename... options) throws IOException {
    clientProto.rename2(src, dst, options);
  }

  @Override // ClientProtocol
  public void concat(String trg, String[] src) throws IOException {
    clientProto.concat(trg, src);
  }

  @Override // ClientProtocol
  public boolean truncate(String src, long newLength, String clientName)
      throws IOException {
    return clientProto.truncate(src, newLength, clientName);
  }

  @Override // ClientProtocol
  public boolean delete(String src, boolean recursive) throws IOException {
    return clientProto.delete(src, recursive);
  }

  @Override // ClientProtocol
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws IOException {
    return clientProto.mkdirs(src, masked, createParent);
  }

  @Override // ClientProtocol
  public void renewLease(String clientName, List<String> namespaces)
      throws IOException {
    clientProto.renewLease(clientName, namespaces);
  }

  @Override // ClientProtocol
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws IOException {
    return clientProto.getListing(src, startAfter, needLocation);
  }

  @Override
  public BatchedDirectoryListing getBatchedListing(
      String[] srcs, byte[] startAfter, boolean needLocation)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    return clientProto.getFileInfo(src);
  }

  @Override // ClientProtocol
  public boolean isFileClosed(String src) throws IOException {
    return clientProto.isFileClosed(src);
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
    return clientProto.getFileLinkInfo(src);
  }

  @Override // ClientProtocol
  public HdfsLocatedFileStatus getLocatedFileInfo(String src,
      boolean needBlockToken) throws IOException {
    return clientProto.getLocatedFileInfo(src, needBlockToken);
  }

  @Override // ClientProtocol
  public long[] getStats() throws IOException {
    return clientProto.getStats();
  }

  @Override // ClientProtocol
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    return clientProto.getDatanodeReport(type);
  }

  /**
   * Get the datanode report from cache.
   *
   * @param type Type of the datanode.
   * @return List of datanodes.
   * @throws IOException If it cannot get the report.
   */
  DatanodeInfo[] getCachedDatanodeReport(DatanodeReportType type)
      throws IOException {
    try {
      return this.dnCache.get(type);
    } catch (ExecutionException e) {
      LOG.error("Cannot get the DN report for {}", type, e);
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new IOException(cause);
      }
    }
  }

  private DatanodeInfo[] getCachedDatanodeReportImpl(
      final DatanodeReportType type) throws IOException {
    // We need to get the DNs as a privileged user
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    RouterRpcServer.setCurrentUser(loginUser);

    try {
      DatanodeInfo[] dns = clientProto.getDatanodeReport(type);
      LOG.debug("Refresh cached DN report with {} datanodes", dns.length);
      return dns;
    } finally {
      // Reset ugi to remote user for remaining operations.
      RouterRpcServer.resetCurrentUser();
    }
  }

  /**
   * Get the datanode report with a timeout.
   * @param type Type of the datanode.
   * @param requireResponse If we require all the namespaces to report.
   * @param timeOutMs Time out for the reply in milliseconds.
   * @return List of datanodes.
   * @throws IOException If it cannot get the report.
   */
  public DatanodeInfo[] getDatanodeReport(
      DatanodeReportType type, boolean requireResponse, long timeOutMs)
          throws IOException {
    checkOperation(OperationCategory.UNCHECKED);

    Map<String, DatanodeInfo> datanodesMap = new LinkedHashMap<>();
    RemoteMethod method = new RemoteMethod("getDatanodeReport",
        new Class<?>[] {DatanodeReportType.class}, type);

    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, DatanodeInfo[]> results =
        rpcClient.invokeConcurrent(nss, method, requireResponse, false,
            timeOutMs, DatanodeInfo[].class);
    updateDnMap(results, datanodesMap);
    // Map -> Array
    Collection<DatanodeInfo> datanodes = datanodesMap.values();
    return toArray(datanodes, DatanodeInfo.class);
  }

  /**
   * Get the datanode report with a timeout.
   * Asynchronous version of the getDatanodeReport method.
   * @param type Type of the datanode.
   * @param requireResponse If we require all the namespaces to report.
   * @param timeOutMs Time out for the reply in milliseconds.
   * @return List of datanodes.
   * @throws IOException If it cannot get the report.
   */
  public DatanodeInfo[] getDatanodeReportAsync(
      DatanodeReportType type, boolean requireResponse, long timeOutMs)
      throws IOException {
    checkOperation(OperationCategory.UNCHECKED);

    Map<String, DatanodeInfo> datanodesMap = new LinkedHashMap<>();
    RemoteMethod method = new RemoteMethod("getDatanodeReport",
        new Class<?>[] {DatanodeReportType.class}, type);

    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    getRPCClient().invokeConcurrent(nss, method, requireResponse, false,
            timeOutMs, DatanodeInfo[].class);

    asyncApply((ApplyFunction<Map<FederationNamespaceInfo, DatanodeInfo[]>,
        DatanodeInfo[]>) results -> {
        updateDnMap(results, datanodesMap);
        // Map -> Array
        Collection<DatanodeInfo> datanodes = datanodesMap.values();
        return toArray(datanodes, DatanodeInfo.class);
      });
    return asyncReturn(DatanodeInfo[].class);
  }

  @Override // ClientProtocol
  public DatanodeStorageReport[] getDatanodeStorageReport(
      DatanodeReportType type) throws IOException {
    return clientProto.getDatanodeStorageReport(type);
  }

  /**
   * Get the list of datanodes per subcluster.
   *
   * @param type Type of the datanodes to get.
   * @return nsId to datanode list.
   * @throws IOException If the method cannot be invoked remotely.
   */
  public Map<String, DatanodeStorageReport[]> getDatanodeStorageReportMap(
      DatanodeReportType type) throws IOException {
    return getDatanodeStorageReportMap(type, true, -1);
  }

  public Map<String, DatanodeStorageReport[]> getDatanodeStorageReportMapAsync(
      DatanodeReportType type) throws IOException {
    return getDatanodeStorageReportMapAsync(type, true, -1);
  }

  /**
   * Get the list of datanodes per subcluster.
   *
   * @param type Type of the datanodes to get.
   * @param requireResponse If true an exception will be thrown if all calls do
   *          not complete. If false exceptions are ignored and all data results
   *          successfully received are returned.
   * @param timeOutMs Time out for the reply in milliseconds.
   * @return nsId to datanode list.
   * @throws IOException If the method cannot be invoked remotely.
   */
  public Map<String, DatanodeStorageReport[]> getDatanodeStorageReportMap(
      DatanodeReportType type, boolean requireResponse, long timeOutMs)
      throws IOException {

    Map<String, DatanodeStorageReport[]> ret = new LinkedHashMap<>();
    RemoteMethod method = new RemoteMethod("getDatanodeStorageReport",
        new Class<?>[] {DatanodeReportType.class}, type);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, DatanodeStorageReport[]> results =
        rpcClient.invokeConcurrent(
            nss, method, requireResponse, false, timeOutMs, DatanodeStorageReport[].class);
    for (Entry<FederationNamespaceInfo, DatanodeStorageReport[]> entry :
        results.entrySet()) {
      FederationNamespaceInfo ns = entry.getKey();
      String nsId = ns.getNameserviceId();
      DatanodeStorageReport[] result = entry.getValue();
      ret.put(nsId, result);
    }
    return ret;
  }

  /**
   * Get the list of datanodes per subcluster.
   * Asynchronous version of getDatanodeStorageReportMap method.
   * @param type Type of the datanodes to get.
   * @param requireResponse If true an exception will be thrown if all calls do
   *          not complete. If false exceptions are ignored and all data results
   *          successfully received are returned.
   * @param timeOutMs Time out for the reply in milliseconds.
   * @return nsId to datanode list.
   * @throws IOException If the method cannot be invoked remotely.
   */
  public Map<String, DatanodeStorageReport[]> getDatanodeStorageReportMapAsync(
      DatanodeReportType type, boolean requireResponse, long timeOutMs)
      throws IOException {

    Map<String, DatanodeStorageReport[]> ret = new LinkedHashMap<>();
    RemoteMethod method = new RemoteMethod("getDatanodeStorageReport",
        new Class<?>[] {DatanodeReportType.class}, type);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    getRPCClient().invokeConcurrent(
            nss, method, requireResponse, false, timeOutMs, DatanodeStorageReport[].class);

    asyncApply((ApplyFunction<Map<FederationNamespaceInfo, DatanodeStorageReport[]>,
        Map<String, DatanodeStorageReport[]>>) results -> {
        for (Entry<FederationNamespaceInfo, DatanodeStorageReport[]> entry :
            results.entrySet()) {
          FederationNamespaceInfo ns = entry.getKey();
          String nsId = ns.getNameserviceId();
          DatanodeStorageReport[] result = entry.getValue();
          ret.put(nsId, result);
        }
        return ret;
      });
    return asyncReturn(ret.getClass());
  }

  @Override // ClientProtocol
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    return clientProto.setSafeMode(action, isChecked);
  }

  @Override // ClientProtocol
  public boolean restoreFailedStorage(String arg) throws IOException {
    return clientProto.restoreFailedStorage(arg);
  }

  @Override // ClientProtocol
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    return clientProto.saveNamespace(timeWindow, txGap);
  }

  @Override // ClientProtocol
  public long rollEdits() throws IOException {
    return clientProto.rollEdits();
  }

  @Override // ClientProtocol
  public void refreshNodes() throws IOException {
    clientProto.refreshNodes();
  }

  @Override // ClientProtocol
  public void finalizeUpgrade() throws IOException {
    clientProto.finalizeUpgrade();
  }

  @Override // ClientProtocol
  public boolean upgradeStatus() throws IOException {
    return clientProto.upgradeStatus();
  }

  @Override // ClientProtocol
  public RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action)
      throws IOException {
    return clientProto.rollingUpgrade(action);
  }

  @Override // ClientProtocol
  public void metaSave(String filename) throws IOException {
    clientProto.metaSave(filename);
  }

  @Override // ClientProtocol
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    return clientProto.listCorruptFileBlocks(path, cookie);
  }

  @Override // ClientProtocol
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    clientProto.setBalancerBandwidth(bandwidth);
  }

  @Override // ClientProtocol
  public ContentSummary getContentSummary(String path) throws IOException {
    return clientProto.getContentSummary(path);
  }

  @Override // ClientProtocol
  public void fsync(String src, long fileId, String clientName,
      long lastBlockLength) throws IOException {
    clientProto.fsync(src, fileId, clientName, lastBlockLength);
  }

  @Override // ClientProtocol
  public void setTimes(String src, long mtime, long atime) throws IOException {
    clientProto.setTimes(src, mtime, atime);
  }

  @Override // ClientProtocol
  public void createSymlink(String target, String link, FsPermission dirPerms,
      boolean createParent) throws IOException {
    clientProto.createSymlink(target, link, dirPerms, createParent);
  }

  @Override // ClientProtocol
  public String getLinkTarget(String path) throws IOException {
    return clientProto.getLinkTarget(path);
  }

  @Override // ClientProtocol
  public void allowSnapshot(String snapshotRoot) throws IOException {
    clientProto.allowSnapshot(snapshotRoot);
  }

  @Override // ClientProtocol
  public void disallowSnapshot(String snapshot) throws IOException {
    clientProto.disallowSnapshot(snapshot);
  }

  @Override // ClientProtocol
  public void renameSnapshot(String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException {
    clientProto.renameSnapshot(snapshotRoot, snapshotOldName, snapshotNewName);
  }

  @Override // ClientProtocol
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    return clientProto.getSnapshottableDirListing();
  }

  @Override // ClientProtocol
  public SnapshotStatus[] getSnapshotListing(String snapshotRoot)
      throws IOException {
    return clientProto.getSnapshotListing(snapshotRoot);
  }

  @Override // ClientProtocol
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
      String earlierSnapshotName, String laterSnapshotName) throws IOException {
    return clientProto.getSnapshotDiffReport(
        snapshotRoot, earlierSnapshotName, laterSnapshotName);
  }

  @Override // ClientProtocol
  public SnapshotDiffReportListing getSnapshotDiffReportListing(
      String snapshotRoot, String earlierSnapshotName, String laterSnapshotName,
      byte[] startPath, int index) throws IOException {
    return clientProto.getSnapshotDiffReportListing(snapshotRoot,
        earlierSnapshotName, laterSnapshotName, startPath, index);
  }

  @Override // ClientProtocol
  public long addCacheDirective(CacheDirectiveInfo path,
      EnumSet<CacheFlag> flags) throws IOException {
    return clientProto.addCacheDirective(path, flags);
  }

  @Override // ClientProtocol
  public void modifyCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException {
    clientProto.modifyCacheDirective(directive, flags);
  }

  @Override // ClientProtocol
  public void removeCacheDirective(long id) throws IOException {
    clientProto.removeCacheDirective(id);
  }

  @Override // ClientProtocol
  public BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
      long prevId, CacheDirectiveInfo filter) throws IOException {
    return clientProto.listCacheDirectives(prevId, filter);
  }

  @Override // ClientProtocol
  public void addCachePool(CachePoolInfo info) throws IOException {
    clientProto.addCachePool(info);
  }

  @Override // ClientProtocol
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    clientProto.modifyCachePool(info);
  }

  @Override // ClientProtocol
  public void removeCachePool(String cachePoolName) throws IOException {
    clientProto.removeCachePool(cachePoolName);
  }

  @Override // ClientProtocol
  public BatchedEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    return clientProto.listCachePools(prevKey);
  }

  @Override // ClientProtocol
  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    clientProto.modifyAclEntries(src, aclSpec);
  }

  @Override // ClientProtocol
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    clientProto.removeAclEntries(src, aclSpec);
  }

  @Override // ClientProtocol
  public void removeDefaultAcl(String src) throws IOException {
    clientProto.removeDefaultAcl(src);
  }

  @Override // ClientProtocol
  public void removeAcl(String src) throws IOException {
    clientProto.removeAcl(src);
  }

  @Override // ClientProtocol
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    clientProto.setAcl(src, aclSpec);
  }

  @Override // ClientProtocol
  public AclStatus getAclStatus(String src) throws IOException {
    return clientProto.getAclStatus(src);
  }

  @Override // ClientProtocol
  public void createEncryptionZone(String src, String keyName)
      throws IOException {
    clientProto.createEncryptionZone(src, keyName);
  }

  @Override // ClientProtocol
  public EncryptionZone getEZForPath(String src) throws IOException {
    return clientProto.getEZForPath(src);
  }

  @Override // ClientProtocol
  public BatchedEntries<EncryptionZone> listEncryptionZones(long prevId)
      throws IOException {
    return clientProto.listEncryptionZones(prevId);
  }

  @Override // ClientProtocol
  public void reencryptEncryptionZone(String zone, ReencryptAction action)
      throws IOException {
    clientProto.reencryptEncryptionZone(zone, action);
  }

  @Override // ClientProtocol
  public BatchedEntries<ZoneReencryptionStatus> listReencryptionStatus(
      long prevId) throws IOException {
    return clientProto.listReencryptionStatus(prevId);
  }

  @Override // ClientProtocol
  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    clientProto.setXAttr(src, xAttr, flag);
  }

  @Override // ClientProtocol
  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs)
      throws IOException {
    return clientProto.getXAttrs(src, xAttrs);
  }

  @Override // ClientProtocol
  public List<XAttr> listXAttrs(String src) throws IOException {
    return clientProto.listXAttrs(src);
  }

  @Override // ClientProtocol
  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    clientProto.removeXAttr(src, xAttr);
  }

  @Override // ClientProtocol
  public void checkAccess(String path, FsAction mode) throws IOException {
    clientProto.checkAccess(path, mode);
  }

  @Override // ClientProtocol
  public long getCurrentEditLogTxid() throws IOException {
    return clientProto.getCurrentEditLogTxid();
  }

  @Override // ClientProtocol
  public EventBatchList getEditsFromTxid(long txid) throws IOException {
    return clientProto.getEditsFromTxid(txid);
  }

  @Override // ClientProtocol
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    return clientProto.getDataEncryptionKey();
  }

  @Override // ClientProtocol
  public String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    return clientProto.createSnapshot(snapshotRoot, snapshotName);
  }

  @Override // ClientProtocol
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    clientProto.deleteSnapshot(snapshotRoot, snapshotName);
  }

  @Override // ClientProtocol
  public void setQuota(String path, long namespaceQuota, long storagespaceQuota,
      StorageType type) throws IOException {
    clientProto.setQuota(path, namespaceQuota, storagespaceQuota, type);
  }

  @Override // ClientProtocol
  public QuotaUsage getQuotaUsage(String path) throws IOException {
    return clientProto.getQuotaUsage(path);
  }

  @Override // ClientProtocol
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    clientProto.reportBadBlocks(blocks);
  }

  @Override // ClientProtocol
  public void unsetStoragePolicy(String src) throws IOException {
    clientProto.unsetStoragePolicy(src);
  }

  @Override // ClientProtocol
  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    return clientProto.getStoragePolicy(path);
  }

  @Override // ClientProtocol
  public ErasureCodingPolicyInfo[] getErasureCodingPolicies()
      throws IOException {
    return clientProto.getErasureCodingPolicies();
  }

  @Override // ClientProtocol
  public Map<String, String> getErasureCodingCodecs() throws IOException {
    return clientProto.getErasureCodingCodecs();
  }

  @Override // ClientProtocol
  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies) throws IOException {
    return clientProto.addErasureCodingPolicies(policies);
  }

  @Override // ClientProtocol
  public void removeErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    clientProto.removeErasureCodingPolicy(ecPolicyName);
  }

  @Override // ClientProtocol
  public void disableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    clientProto.disableErasureCodingPolicy(ecPolicyName);
  }

  @Override // ClientProtocol
  public void enableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    clientProto.enableErasureCodingPolicy(ecPolicyName);
  }

  @Override // ClientProtocol
  public ErasureCodingPolicy getErasureCodingPolicy(String src)
      throws IOException {
    return clientProto.getErasureCodingPolicy(src);
  }

  @Override // ClientProtocol
  public void setErasureCodingPolicy(String src, String ecPolicyName)
      throws IOException {
    clientProto.setErasureCodingPolicy(src, ecPolicyName);
  }

  @Override // ClientProtocol
  public void unsetErasureCodingPolicy(String src) throws IOException {
    clientProto.unsetErasureCodingPolicy(src);
  }

  @Override
  public ECTopologyVerifierResult getECTopologyResultForPolicies(
      String... policyNames) throws IOException {
    return clientProto.getECTopologyResultForPolicies(policyNames);
  }

  @Override // ClientProtocol
  public ECBlockGroupStats getECBlockGroupStats() throws IOException {
    return clientProto.getECBlockGroupStats();
  }

  @Override // ClientProtocol
  public ReplicatedBlockStats getReplicatedBlockStats() throws IOException {
    return clientProto.getReplicatedBlockStats();
  }

  @Deprecated
  @Override // ClientProtocol
  public BatchedEntries<OpenFileEntry> listOpenFiles(long prevId)
      throws IOException {
    return clientProto.listOpenFiles(prevId);
  }

  @Override // ClientProtocol
  public HAServiceProtocol.HAServiceState getHAServiceState()
      throws IOException {
    return clientProto.getHAServiceState();
  }

  @Override // ClientProtocol
  public BatchedEntries<OpenFileEntry> listOpenFiles(long prevId,
      EnumSet<OpenFilesType> openFilesTypes, String path) throws IOException {
    return clientProto.listOpenFiles(prevId, openFilesTypes, path);
  }

  @Override // ClientProtocol
  public void msync() throws IOException {
    clientProto.msync();
  }

  @Override // ClientProtocol
  public void satisfyStoragePolicy(String path) throws IOException {
    clientProto.satisfyStoragePolicy(path);
  }

  @Override // ClientProtocol
  public DatanodeInfo[] getSlowDatanodeReport() throws IOException {
    return clientProto.getSlowDatanodeReport();
  }

  @Override // ClientProtocol
  public Path getEnclosingRoot(String src) throws IOException {
    return clientProto.getEnclosingRoot(src);
  }

  @Override // NamenodeProtocol
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size,
      long minBlockSize, long hotBlockTimeInterval, StorageType storageType) throws IOException {
    return nnProto.getBlocks(datanode, size, minBlockSize,
            hotBlockTimeInterval, storageType);
  }

  @Override // NamenodeProtocol
  public ExportedBlockKeys getBlockKeys() throws IOException {
    return nnProto.getBlockKeys();
  }

  @Override // NamenodeProtocol
  public long getTransactionID() throws IOException {
    return nnProto.getTransactionID();
  }

  @Override // NamenodeProtocol
  public long getMostRecentCheckpointTxId() throws IOException {
    return nnProto.getMostRecentCheckpointTxId();
  }

  @Override // NamenodeProtocol
  public long getMostRecentNameNodeFileTxId(NNStorage.NameNodeFile nnf)
      throws IOException {
    return nnProto.getMostRecentNameNodeFileTxId(nnf);
  }

  @Override // NamenodeProtocol
  public CheckpointSignature rollEditLog() throws IOException {
    return nnProto.rollEditLog();
  }

  @Override // NamenodeProtocol
  public NamespaceInfo versionRequest() throws IOException {
    return nnProto.versionRequest();
  }

  @Override // NamenodeProtocol
  public void errorReport(NamenodeRegistration registration, int errorCode,
      String msg) throws IOException {
    nnProto.errorReport(registration, errorCode, msg);
  }

  @Override // NamenodeProtocol
  public NamenodeRegistration registerSubordinateNamenode(
      NamenodeRegistration registration) throws IOException {
    return nnProto.registerSubordinateNamenode(registration);
  }

  @Override // NamenodeProtocol
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
      throws IOException {
    return nnProto.startCheckpoint(registration);
  }

  @Override // NamenodeProtocol
  public void endCheckpoint(NamenodeRegistration registration,
      CheckpointSignature sig) throws IOException {
    nnProto.endCheckpoint(registration, sig);
  }

  @Override // NamenodeProtocol
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
      throws IOException {
    return nnProto.getEditLogManifest(sinceTxId);
  }

  @Override // NamenodeProtocol
  public boolean isUpgradeFinalized() throws IOException {
    return nnProto.isUpgradeFinalized();
  }

  @Override // NamenodeProtocol
  public boolean isRollingUpgrade() throws IOException {
    return nnProto.isRollingUpgrade();
  }

  @Override // NamenodeProtocol
  public Long getNextSPSPath() throws IOException {
    return nnProto.getNextSPSPath();
  }

  /**
   * Get the possible locations of a path in the federated cluster.
   * During the get operation, it will do the quota verification.
   *
   * @param path Path to check.
   * @param failIfLocked Fail the request if locked (top mount point).
   * @return Prioritized list of locations in the federated cluster.
   * @throws IOException If the location for this path cannot be determined.
   */
  public List<RemoteLocation> getLocationsForPath(String path,
      boolean failIfLocked) throws IOException {
    return getLocationsForPath(path, failIfLocked, true);
  }

  /**
   * Get the possible locations of a path in the federated cluster.
   *
   * @param path Path to check.
   * @param failIfLocked Fail the request if there is any mount point under
   *                     the path.
   * @param needQuotaVerify If need to do the quota verification.
   * @return Prioritized list of locations in the federated cluster.
   * @throws IOException If the location for this path cannot be determined.
   */
  public List<RemoteLocation> getLocationsForPath(String path,
      boolean failIfLocked, boolean needQuotaVerify) throws IOException {
    try {
      if (failIfLocked) {
        // check if there is any mount point under the path
        final List<String> mountPoints =
            this.subclusterResolver.getMountPoints(path);
        if (mountPoints != null) {
          StringBuilder sb = new StringBuilder();
          sb.append("The operation is not allowed because ");
          if (mountPoints.isEmpty()) {
            sb.append("the path: ")
                .append(path)
                .append(" is a mount point");
          } else {
            sb.append("there are mount points: ")
                .append(String.join(",", mountPoints))
                .append(" under the path: ")
                .append(path);
          }
          throw new AccessControlException(sb.toString());
        }
      }

      // Check the location for this path
      final PathLocation location =
          this.subclusterResolver.getDestinationForPath(path);
      if (location == null) {
        throw new NoLocationException(path, this.subclusterResolver.getClass());
      }

      // We may block some write operations
      if (opCategory.get() == OperationCategory.WRITE) {
        // Check if the path is in a read only mount point
        if (isPathReadOnly(path)) {
          if (this.rpcMonitor != null) {
            this.rpcMonitor.routerFailureReadOnly();
          }
          throw new IOException(path + " is in a read only mount point");
        }

        // Check quota
        if (this.router.isQuotaEnabled() && needQuotaVerify) {
          RouterQuotaUsage quotaUsage = this.router.getQuotaManager()
              .getQuotaUsage(path);
          if (quotaUsage != null) {
            quotaUsage.verifyNamespaceQuota();
            quotaUsage.verifyStoragespaceQuota();
            quotaUsage.verifyQuotaByStorageType();
          }
        }
      }

      // Filter disabled subclusters
      Set<String> disabled = namenodeResolver.getDisabledNamespaces();
      List<RemoteLocation> locs = new ArrayList<>();
      for (RemoteLocation loc : location.getDestinations()) {
        if (!disabled.contains(loc.getNameserviceId())) {
          locs.add(loc);
        }
      }
      if (locs.isEmpty()) {
        throw new NoLocationException(path, this.subclusterResolver.getClass());
      }
      return locs;
    } catch (IOException ioe) {
      if (this.rpcMonitor != null) {
        this.rpcMonitor.routerFailureStateStore();
      }
      if (ioe instanceof StateStoreUnavailableException) {
        checkSafeMode();
      }
      throw ioe;
    }
  }

  /**
   * Check if a path is in a read only mount point.
   *
   * @param path Path to check.
   * @return If the path is in a read only mount point.
   */
  private boolean isPathReadOnly(final String path) {
    MountTable entry = getMountTable(path);
    return entry != null && entry.isReadOnly();
  }

  /**
   * Get the user that is invoking this operation.
   *
   * @return Remote user group information.
   * @throws IOException If we cannot get the user information.
   */
  public static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = CUR_USER.get();
    ugi = (ugi != null) ? ugi : Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  /**
   * Set super user credentials if needed.
   */
  static void setCurrentUser(UserGroupInformation ugi) {
    CUR_USER.set(ugi);
  }

  /**
   * Reset to discard super user credentials.
   */
  static void resetCurrentUser() {
    CUR_USER.set(null);
  }

  /**
   * Merge the outputs from multiple namespaces.
   *
   * @param <T> The type of the objects to merge.
   * @param map Namespace to Output array.
   * @param clazz Class of the values.
   * @return Array with the outputs.
   */
  public static <T> T[] merge(
      Map<FederationNamespaceInfo, T[]> map, Class<T> clazz) {

    // Put all results into a set to avoid repeats
    Set<T> ret = new LinkedHashSet<>();
    for (T[] values : map.values()) {
      if (values != null) {
        for (T val : values) {
          ret.add(val);
        }
      }
    }

    return toArray(ret, clazz);
  }

  /**
   * Convert a set of values into an array.
   * @param <T> The type of the return objects.
   * @param set Input set.
   * @param clazz Class of the values.
   * @return Array with the values in set.
   */
  static <T> T[] toArray(Collection<T> set, Class<T> clazz) {
    @SuppressWarnings("unchecked")
    T[] combinedData = (T[]) Array.newInstance(clazz, set.size());
    combinedData = set.toArray(combinedData);
    return combinedData;
  }

  /**
   * Get quota module implementation.
   * @return Quota module implementation
   */
  public Quota getQuotaModule() {
    return this.quotaCall;
  }

  /**
   * Get ClientProtocol module implementation.
   * @return ClientProtocol implementation
   */
  @VisibleForTesting
  public RouterClientProtocol getClientProtocolModule() {
    return this.clientProto;
  }

  /**
   * Get RPC metrics info.
   * @return The instance of FederationRPCMetrics.
   */
  public FederationRPCMetrics getRPCMetrics() {
    return this.rpcMonitor.getRPCMetrics();
  }

  /**
   * Check if a path should be in all subclusters.
   *
   * @param path Path to check.
   * @return If a path should be in all subclusters.
   */
  public boolean isPathAll(final String path) {
    MountTable entry = getMountTable(path);
    return entry != null && entry.isAll();
  }

  /**
   * Check if a path supports failed subclusters.
   *
   * @param path Path to check.
   * @return If a path should support failed subclusters.
   */
  boolean isPathFaultTolerant(final String path) {
    MountTable entry = getMountTable(path);
    return entry != null && entry.isFaultTolerant();
  }

  private MountTable getMountTable(final String path){
    if (subclusterResolver instanceof MountTableResolver) {
      try {
        MountTableResolver mountTable = (MountTableResolver) subclusterResolver;
        return mountTable.getMountPoint(path);
      } catch (IOException e) {
        LOG.error("Cannot get mount point", e);
      }
    }
    return null;
  }

  /**
   * Check if call needs to be invoked to all the locations. The call is
   * supposed to be invoked in all the locations in case the order of the mount
   * entry is amongst HASH_ALL, RANDOM or SPACE or if the source is itself a
   * mount entry.
   * @param path The path on which the operation need to be invoked.
   * @return true if the call is supposed to invoked on all locations.
   * @throws IOException If an I/O error occurs.
   */
  public boolean isInvokeConcurrent(final String path) throws IOException {
    if (subclusterResolver instanceof MountTableResolver) {
      MountTableResolver mountTableResolver =
          (MountTableResolver) subclusterResolver;
      List<String> mountPoints = mountTableResolver.getMountPoints(path);
      // If this is a mount point, we need to invoke everywhere.
      if (mountPoints != null) {
        return true;
      }
      return isPathAll(path);
    }
    return false;
  }

  @Override
  public void refreshUserToGroupsMappings() throws IOException {
    routerProto.refreshUserToGroupsMappings();
  }

  @Override
  public void refreshSuperUserGroupsConfiguration() throws IOException {
    routerProto.refreshSuperUserGroupsConfiguration();
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    return routerProto.getGroupsForUser(user);
  }

  public int getRouterFederationRenameCount() {
    return clientProto.getRouterFederationRenameCount();
  }

  public int getSchedulerJobCount() {
    if (fedRenameScheduler == null) {
      return 0;
    }
    return fedRenameScheduler.getAllJobs().size();
  }

  public String refreshFairnessPolicyController() {
    return rpcClient.refreshFairnessPolicyController(new Configuration());
  }

  /**
   * Get the slow running datanodes report with a timeout.
   *
   * @param requireResponse If we require all the namespaces to report.
   * @param timeOutMs Time out for the reply in milliseconds.
   * @return List of datanodes.
   * @throws IOException If it cannot get the report.
   */
  public DatanodeInfo[] getSlowDatanodeReport(boolean requireResponse, long timeOutMs)
      throws IOException {
    checkOperation(OperationCategory.UNCHECKED);

    Map<String, DatanodeInfo> datanodesMap = new LinkedHashMap<>();
    RemoteMethod method = new RemoteMethod("getSlowDatanodeReport");

    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, DatanodeInfo[]> results =
        rpcClient.invokeConcurrent(nss, method, requireResponse, false,
            timeOutMs, DatanodeInfo[].class);
    updateDnMap(results, datanodesMap);
    // Map -> Array
    Collection<DatanodeInfo> datanodes = datanodesMap.values();
    return toArray(datanodes, DatanodeInfo.class);
  }

  /**
   * Get the slow running datanodes report with a timeout.
   * Asynchronous version of the getSlowDatanodeReport method.
   *
   * @param requireResponse If we require all the namespaces to report.
   * @param timeOutMs Time out for the reply in milliseconds.
   * @return List of datanodes.
   * @throws IOException If it cannot get the report.
   */
  public DatanodeInfo[] getSlowDatanodeReportAsync(boolean requireResponse, long timeOutMs)
      throws IOException {
    checkOperation(OperationCategory.UNCHECKED);

    Map<String, DatanodeInfo> datanodesMap = new LinkedHashMap<>();
    RemoteMethod method = new RemoteMethod("getSlowDatanodeReport");

    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    getRPCClient().invokeConcurrent(nss, method, requireResponse, false,
            timeOutMs, DatanodeInfo[].class);

    asyncApply((ApplyFunction<Map<FederationNamespaceInfo, DatanodeInfo[]>,
        DatanodeInfo[]>) results -> {
        updateDnMap(results, datanodesMap);
        // Map -> Array
        Collection<DatanodeInfo> datanodes = datanodesMap.values();
        return toArray(datanodes, DatanodeInfo.class);
      });

    return asyncReturn(DatanodeInfo[].class);
  }

  private void updateDnMap(Map<FederationNamespaceInfo, DatanodeInfo[]> results,
      Map<String, DatanodeInfo> datanodesMap) {
    for (Entry<FederationNamespaceInfo, DatanodeInfo[]> entry :
        results.entrySet()) {
      FederationNamespaceInfo ns = entry.getKey();
      DatanodeInfo[] result = entry.getValue();
      for (DatanodeInfo node : result) {
        String nodeId = node.getXferAddr();
        DatanodeInfo dn = datanodesMap.get(nodeId);
        if (dn == null || node.getLastUpdate() > dn.getLastUpdate()) {
          // Add the subcluster as a suffix to the network location
          node.setNetworkLocation(
              NodeBase.PATH_SEPARATOR_STR + ns.getNameserviceId() +
                  node.getNetworkLocation());
          datanodesMap.put(nodeId, node);
        } else {
          LOG.debug("{} is in multiple subclusters", nodeId);
        }
      }
    }
  }

  /**
   * Deals with loading datanode report into the cache and refresh.
   */
  private class DatanodeReportCacheLoader
      extends CacheLoader<DatanodeReportType, DatanodeInfo[]> {

    private ListeningExecutorService executorService;

    DatanodeReportCacheLoader() {
      ThreadFactory threadFactory = new ThreadFactoryBuilder()
          .setNameFormat("DatanodeReport-Cache-Reload")
          .setDaemon(true)
          .build();

      executorService = MoreExecutors.listeningDecorator(
          Executors.newSingleThreadExecutor(threadFactory));
    }

    @Override
    public DatanodeInfo[] load(DatanodeReportType type) throws Exception {
      return getCachedDatanodeReportImpl(type);
    }

    /**
     * Override the reload method to provide an asynchronous implementation,
     * so that the query will not be slowed down by the cache refresh. It
     * will return the old cache value and schedule a background refresh.
     */
    @Override
    public ListenableFuture<DatanodeInfo[]> reload(
        final DatanodeReportType type, DatanodeInfo[] oldValue)
        throws Exception {
      return executorService.submit(() -> load(type));
    }
  }

  public boolean isAsync() {
    return this.enableAsync;
  }

  public Executor getAsyncRouterHandler() {
    return asyncRouterHandler;
  }

  private static class AsyncThreadFactory implements ThreadFactory {
    private final String namePrefix;
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    AsyncThreadFactory(String namePrefix) {
      this.namePrefix = namePrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      return new Thread(r, namePrefix + threadNumber.getAndIncrement());
    }
  }
}
