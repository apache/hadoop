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

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_IP_PROXY_USERS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_HANDLER_RATIO_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_HANDLER_RATIO_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.MAX_PATH_DEPTH;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.MAX_PATH_LENGTH;
import static org.apache.hadoop.util.Time.now;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceProtocolService;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.BatchedDirectoryListing;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FSLimitException;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsPartialListing;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.ReencryptAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.apache.hadoop.hdfs.protocol.XAttrNotFoundException;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeLifelineProtocolProtos.DatanodeLifelineProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.NamenodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ReconfigurationProtocolService;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeLifelineProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeLifelineProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.ReconfigurationProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ReconfigurationProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerFaultInjector;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HttpGetFailedException;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfyManager;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.FinalizeCommand;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.InvalidBlockReportLeaseException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.NodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.RegisterCommand;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.RetryCache;
import org.apache.hadoop.ipc.RetryCache.CacheEntry;
import org.apache.hadoop.ipc.RetryCache.CacheEntryWithPayload;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.RefreshRegistry;
import org.apache.hadoop.ipc.RefreshResponse;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshAuthorizationPolicyProtocolService;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserMappingsProtocolService;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolPB;
import org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.proto.RefreshCallQueueProtocolProtos.RefreshCallQueueProtocolService;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolPB;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshProtocolService;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetUserMappingsProtocolService;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.slf4j.Logger;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;

import javax.annotation.Nonnull;

/**
 * This class is responsible for handling all of the RPC calls to the NameNode.
 * It is created, started, and stopped by {@link NameNode}.
 */
@InterfaceAudience.Private
@VisibleForTesting
public class NameNodeRpcServer implements NamenodeProtocols {
  
  private static final Logger LOG = NameNode.LOG;
  private static final Logger stateChangeLog = NameNode.stateChangeLog;
  private static final Logger blockStateChangeLog = NameNode
      .blockStateChangeLog;
  
  // Dependencies from other parts of NN.
  protected final FSNamesystem namesystem;
  protected final NameNode nn;
  private final NameNodeMetrics metrics;

  private final RetryCache retryCache;

  private final boolean serviceAuthEnabled;

  /** The RPC server that listens to requests from DataNodes */
  private final RPC.Server serviceRpcServer;
  private final InetSocketAddress serviceRPCAddress;

  /** The RPC server that listens to lifeline requests */
  private final RPC.Server lifelineRpcServer;
  private final InetSocketAddress lifelineRPCAddress;
  
  /** The RPC server that listens to requests from clients */
  protected final RPC.Server clientRpcServer;
  protected final InetSocketAddress clientRpcAddress;
  
  private final String minimumDataNodeVersion;

  private final String defaultECPolicyName;

  // Users who can override the client info
  private final String[] ipProxyUsers;

  public NameNodeRpcServer(Configuration conf, NameNode nn)
      throws IOException {
    this.nn = nn;
    this.namesystem = nn.getNamesystem();
    this.retryCache = namesystem.getRetryCache();
    this.metrics = NameNode.getNameNodeMetrics();

    int handlerCount = 
      conf.getInt(DFS_NAMENODE_HANDLER_COUNT_KEY, 
                  DFS_NAMENODE_HANDLER_COUNT_DEFAULT);
    ipProxyUsers = conf.getStrings(DFS_NAMENODE_IP_PROXY_USERS);

    RPC.setProtocolEngine(conf, ClientNamenodeProtocolPB.class,
        ProtobufRpcEngine2.class);

    ClientNamenodeProtocolServerSideTranslatorPB 
       clientProtocolServerTranslator = 
         new ClientNamenodeProtocolServerSideTranslatorPB(this);
     BlockingService clientNNPbService = ClientNamenodeProtocol.
         newReflectiveBlockingService(clientProtocolServerTranslator);

    int maxDataLength = conf.getInt(IPC_MAXIMUM_DATA_LENGTH,
        IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    DatanodeProtocolServerSideTranslatorPB dnProtoPbTranslator = 
        new DatanodeProtocolServerSideTranslatorPB(this, maxDataLength);
    BlockingService dnProtoPbService = DatanodeProtocolService
        .newReflectiveBlockingService(dnProtoPbTranslator);

    DatanodeLifelineProtocolServerSideTranslatorPB lifelineProtoPbTranslator =
        new DatanodeLifelineProtocolServerSideTranslatorPB(this);
    BlockingService lifelineProtoPbService = DatanodeLifelineProtocolService
        .newReflectiveBlockingService(lifelineProtoPbTranslator);

    NamenodeProtocolServerSideTranslatorPB namenodeProtocolXlator = 
        new NamenodeProtocolServerSideTranslatorPB(this);
    BlockingService NNPbService = NamenodeProtocolService
          .newReflectiveBlockingService(namenodeProtocolXlator);
    
    RefreshAuthorizationPolicyProtocolServerSideTranslatorPB refreshAuthPolicyXlator = 
        new RefreshAuthorizationPolicyProtocolServerSideTranslatorPB(this);
    BlockingService refreshAuthService = RefreshAuthorizationPolicyProtocolService
        .newReflectiveBlockingService(refreshAuthPolicyXlator);

    RefreshUserMappingsProtocolServerSideTranslatorPB refreshUserMappingXlator = 
        new RefreshUserMappingsProtocolServerSideTranslatorPB(this);
    BlockingService refreshUserMappingService = RefreshUserMappingsProtocolService
        .newReflectiveBlockingService(refreshUserMappingXlator);

    RefreshCallQueueProtocolServerSideTranslatorPB refreshCallQueueXlator = 
        new RefreshCallQueueProtocolServerSideTranslatorPB(this);
    BlockingService refreshCallQueueService = RefreshCallQueueProtocolService
        .newReflectiveBlockingService(refreshCallQueueXlator);

    GenericRefreshProtocolServerSideTranslatorPB genericRefreshXlator =
        new GenericRefreshProtocolServerSideTranslatorPB(this);
    BlockingService genericRefreshService = GenericRefreshProtocolService
        .newReflectiveBlockingService(genericRefreshXlator);

    GetUserMappingsProtocolServerSideTranslatorPB getUserMappingXlator = 
        new GetUserMappingsProtocolServerSideTranslatorPB(this);
    BlockingService getUserMappingService = GetUserMappingsProtocolService
        .newReflectiveBlockingService(getUserMappingXlator);
    
    HAServiceProtocolServerSideTranslatorPB haServiceProtocolXlator = 
        new HAServiceProtocolServerSideTranslatorPB(this);
    BlockingService haPbService = HAServiceProtocolService
        .newReflectiveBlockingService(haServiceProtocolXlator);

    ReconfigurationProtocolServerSideTranslatorPB reconfigurationProtocolXlator
        = new ReconfigurationProtocolServerSideTranslatorPB(this);
    BlockingService reconfigurationPbService = ReconfigurationProtocolService
        .newReflectiveBlockingService(reconfigurationProtocolXlator);

    InetSocketAddress serviceRpcAddr = nn.getServiceRpcServerAddress(conf);
    if (serviceRpcAddr != null) {
      String bindHost = nn.getServiceRpcServerBindHost(conf);
      if (bindHost == null) {
        bindHost = serviceRpcAddr.getHostName();
      }
      LOG.info("Service RPC server is binding to " + bindHost + ":" +
          serviceRpcAddr.getPort());

      int serviceHandlerCount =
        conf.getInt(DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY,
                    DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT);
      serviceRpcServer = new RPC.Builder(conf)
          .setProtocol(
              org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB.class)
          .setInstance(clientNNPbService)
          .setBindAddress(bindHost)
          .setPort(serviceRpcAddr.getPort())
          .setNumHandlers(serviceHandlerCount)
          .setVerbose(false)
          .setSecretManager(namesystem.getDelegationTokenSecretManager())
          .build();

      // Add all the RPC protocols that the namenode implements
      DFSUtil.addPBProtocol(conf, HAServiceProtocolPB.class, haPbService,
          serviceRpcServer);
      DFSUtil.addPBProtocol(conf, ReconfigurationProtocolPB.class,
          reconfigurationPbService, serviceRpcServer);
      DFSUtil.addPBProtocol(conf, NamenodeProtocolPB.class, NNPbService,
          serviceRpcServer);
      DFSUtil.addPBProtocol(conf, DatanodeProtocolPB.class, dnProtoPbService,
          serviceRpcServer);
      DFSUtil.addPBProtocol(conf, RefreshAuthorizationPolicyProtocolPB.class,
          refreshAuthService, serviceRpcServer);
      DFSUtil.addPBProtocol(conf, RefreshUserMappingsProtocolPB.class, 
          refreshUserMappingService, serviceRpcServer);
      // We support Refreshing call queue here in case the client RPC queue is full
      DFSUtil.addPBProtocol(conf, RefreshCallQueueProtocolPB.class,
          refreshCallQueueService, serviceRpcServer);
      DFSUtil.addPBProtocol(conf, GenericRefreshProtocolPB.class,
          genericRefreshService, serviceRpcServer);
      DFSUtil.addPBProtocol(conf, GetUserMappingsProtocolPB.class, 
          getUserMappingService, serviceRpcServer);

      // Update the address with the correct port
      InetSocketAddress listenAddr = serviceRpcServer.getListenerAddress();
      serviceRPCAddress = new InetSocketAddress(
            serviceRpcAddr.getHostName(), listenAddr.getPort());
      nn.setRpcServiceServerAddress(conf, serviceRPCAddress);
    } else {
      serviceRpcServer = null;
      serviceRPCAddress = null;
    }

    InetSocketAddress lifelineRpcAddr = nn.getLifelineRpcServerAddress(conf);
    if (lifelineRpcAddr != null) {
      RPC.setProtocolEngine(conf, HAServiceProtocolPB.class,
          ProtobufRpcEngine2.class);
      String bindHost = nn.getLifelineRpcServerBindHost(conf);
      if (bindHost == null) {
        bindHost = lifelineRpcAddr.getHostName();
      }
      LOG.info("Lifeline RPC server is binding to {}:{}", bindHost,
          lifelineRpcAddr.getPort());

      int lifelineHandlerCount = conf.getInt(
          DFS_NAMENODE_LIFELINE_HANDLER_COUNT_KEY, 0);
      if (lifelineHandlerCount <= 0) {
        float lifelineHandlerRatio = conf.getFloat(
            DFS_NAMENODE_LIFELINE_HANDLER_RATIO_KEY,
            DFS_NAMENODE_LIFELINE_HANDLER_RATIO_DEFAULT);
        lifelineHandlerCount = Math.max(
            (int)(handlerCount * lifelineHandlerRatio), 1);
      }
      lifelineRpcServer = new RPC.Builder(conf)
          .setProtocol(HAServiceProtocolPB.class)
          .setInstance(haPbService)
          .setBindAddress(bindHost)
          .setPort(lifelineRpcAddr.getPort())
          .setNumHandlers(lifelineHandlerCount)
          .setVerbose(false)
          .setSecretManager(namesystem.getDelegationTokenSecretManager())
          .build();

      DFSUtil.addPBProtocol(conf, DatanodeLifelineProtocolPB.class,
          lifelineProtoPbService, lifelineRpcServer);

      // Update the address with the correct port
      InetSocketAddress listenAddr = lifelineRpcServer.getListenerAddress();
      lifelineRPCAddress = new InetSocketAddress(lifelineRpcAddr.getHostName(),
          listenAddr.getPort());
      nn.setRpcLifelineServerAddress(conf, lifelineRPCAddress);
    } else {
      lifelineRpcServer = null;
      lifelineRPCAddress = null;
    }

    InetSocketAddress rpcAddr = nn.getRpcServerAddress(conf);
    String bindHost = nn.getRpcServerBindHost(conf);
    if (bindHost == null) {
      bindHost = rpcAddr.getHostName();
    }
    LOG.info("RPC server is binding to " + bindHost + ":" + rpcAddr.getPort());

    boolean enableStateContext = conf.getBoolean(
        DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY,
        DFS_NAMENODE_STATE_CONTEXT_ENABLED_DEFAULT);
    LOG.info("Enable NameNode state context:" + enableStateContext);

    GlobalStateIdContext stateIdContext = null;
    if (enableStateContext) {
      stateIdContext = new GlobalStateIdContext(namesystem);
    }

    clientRpcServer = new RPC.Builder(conf)
        .setProtocol(
            org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB.class)
        .setInstance(clientNNPbService)
        .setBindAddress(bindHost)
        .setPort(rpcAddr.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(namesystem.getDelegationTokenSecretManager())
        .setAlignmentContext(stateIdContext)
        .build();

    // Add all the RPC protocols that the namenode implements
    DFSUtil.addPBProtocol(conf, HAServiceProtocolPB.class, haPbService,
        clientRpcServer);
    DFSUtil.addPBProtocol(conf, ReconfigurationProtocolPB.class,
        reconfigurationPbService, clientRpcServer);
    DFSUtil.addPBProtocol(conf, NamenodeProtocolPB.class, NNPbService,
        clientRpcServer);
    DFSUtil.addPBProtocol(conf, DatanodeProtocolPB.class, dnProtoPbService,
        clientRpcServer);
    DFSUtil.addPBProtocol(conf, RefreshAuthorizationPolicyProtocolPB.class, 
        refreshAuthService, clientRpcServer);
    DFSUtil.addPBProtocol(conf, RefreshUserMappingsProtocolPB.class, 
        refreshUserMappingService, clientRpcServer);
    DFSUtil.addPBProtocol(conf, RefreshCallQueueProtocolPB.class,
        refreshCallQueueService, clientRpcServer);
    DFSUtil.addPBProtocol(conf, GenericRefreshProtocolPB.class,
        genericRefreshService, clientRpcServer);
    DFSUtil.addPBProtocol(conf, GetUserMappingsProtocolPB.class, 
        getUserMappingService, clientRpcServer);

    // set service-level authorization security policy
    if (serviceAuthEnabled =
          conf.getBoolean(
            CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      clientRpcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
      if (serviceRpcServer != null) {
        serviceRpcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
      }
      if (lifelineRpcServer != null) {
        lifelineRpcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
      }
    }

    // The rpc-server port can be ephemeral... ensure we have the correct info
    InetSocketAddress listenAddr = clientRpcServer.getListenerAddress();
    clientRpcAddress = new InetSocketAddress(
        rpcAddr.getHostName(), listenAddr.getPort());
    nn.setRpcServerAddress(conf, clientRpcAddress);
    
    minimumDataNodeVersion = conf.get(
        DFSConfigKeys.DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_KEY,
        DFSConfigKeys.DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_DEFAULT);

    defaultECPolicyName = conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY,
        DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY_DEFAULT);

    // Set terse exception whose stack trace won't be logged
    clientRpcServer.addTerseExceptions(SafeModeException.class,
        FileNotFoundException.class,
        HadoopIllegalArgumentException.class,
        FileAlreadyExistsException.class,
        InvalidPathException.class,
        ParentNotDirectoryException.class,
        UnresolvedLinkException.class,
        AlreadyBeingCreatedException.class,
        QuotaExceededException.class,
        RecoveryInProgressException.class,
        AccessControlException.class,
        InvalidToken.class,
        LeaseExpiredException.class,
        NSQuotaExceededException.class,
        DSQuotaExceededException.class,
        QuotaByStorageTypeExceededException.class,
        AclException.class,
        FSLimitException.PathComponentTooLongException.class,
        FSLimitException.MaxDirectoryItemsExceededException.class,
        DisallowedDatanodeException.class,
        XAttrNotFoundException.class);

    clientRpcServer.addSuppressedLoggingExceptions(StandbyException.class,
        UnresolvedPathException.class);

    clientRpcServer.setTracer(nn.tracer);
    if (serviceRpcServer != null) {
      serviceRpcServer.setTracer(nn.tracer);
    }
    if (lifelineRpcServer != null) {
      lifelineRpcServer.setTracer(nn.tracer);
    }
    int[] auxiliaryPorts =
        conf.getInts(DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_KEY);
    if (auxiliaryPorts != null && auxiliaryPorts.length != 0) {
      for (int auxiliaryPort : auxiliaryPorts) {
        this.clientRpcServer.addAuxiliaryListener(auxiliaryPort);
      }
    }
  }

  /** Allow access to the lifeline RPC server for testing */
  @VisibleForTesting
  RPC.Server getLifelineRpcServer() {
    return lifelineRpcServer;
  }

  /** Allow access to the client RPC server for testing */
  @VisibleForTesting
  public RPC.Server getClientRpcServer() {
    return clientRpcServer;
  }
  
  /** Allow access to the service RPC server for testing */
  @VisibleForTesting
  RPC.Server getServiceRpcServer() {
    return serviceRpcServer;
  }
  
  /**
   * Start client and service RPC servers.
   */
  void start() {
    clientRpcServer.start();
    if (serviceRpcServer != null) {
      serviceRpcServer.start();      
    }
    if (lifelineRpcServer != null) {
      lifelineRpcServer.start();
    }
  }
  
  /**
   * Wait until the RPC servers have shutdown.
   */
  void join() throws InterruptedException {
    clientRpcServer.join();
    if (serviceRpcServer != null) {
      serviceRpcServer.join();      
    }
    if (lifelineRpcServer != null) {
      lifelineRpcServer.join();
    }
  }

  /**
   * Stop client and service RPC servers.
   */
  void stop() {
    if (clientRpcServer != null) {
      clientRpcServer.stop();
    }
    if (serviceRpcServer != null) {
      serviceRpcServer.stop();
    }
    if (lifelineRpcServer != null) {
      lifelineRpcServer.stop();
    }
  }

  InetSocketAddress getLifelineRpcAddress() {
    return lifelineRPCAddress;
  }

  InetSocketAddress getServiceRpcAddress() {
    return serviceRPCAddress;
  }

  @VisibleForTesting
  public InetSocketAddress getRpcAddress() {
    return clientRpcAddress;
  }

  @VisibleForTesting
  public Set<InetSocketAddress> getAuxiliaryRpcAddresses() {
    return clientRpcServer.getAuxiliaryListenerAddresses();
  }

  private static UserGroupInformation getRemoteUser() throws IOException {
    return NameNode.getRemoteUser();
  }


  /////////////////////////////////////////////////////
  // NamenodeProtocol
  /////////////////////////////////////////////////////
  @Override // NamenodeProtocol
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size, long
      minBlockSize, long timeInterval)
      throws IOException {
    String operationName = "getBlocks";
    if(size <= 0) {
      throw new IllegalArgumentException(
          "Unexpected not positive size: "+size);
    }
    if(minBlockSize < 0) {
      throw new IllegalArgumentException(
          "Unexpected not positive size: "+size);
    }
    checkNNStartup();
    namesystem.checkSuperuserPrivilege(operationName);
    namesystem.checkNameNodeSafeMode("Cannot execute getBlocks");
    return namesystem.getBlocks(datanode, size, minBlockSize, timeInterval);
  }

  @Override // NamenodeProtocol
  public ExportedBlockKeys getBlockKeys() throws IOException {
    String operationName = "getBlockKeys";
    checkNNStartup();
    namesystem.checkSuperuserPrivilege(operationName);
    return namesystem.getBlockManager().getBlockKeys();
  }

  @Override // NamenodeProtocol
  public void errorReport(NamenodeRegistration registration,
                          int errorCode, 
                          String msg) throws IOException {
    String operationName = "errorReport";
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.UNCHECKED);
    namesystem.checkSuperuserPrivilege(operationName);
    verifyRequest(registration);
    LOG.info("Error report from " + registration + ": " + msg);
    if (errorCode == FATAL) {
      namesystem.releaseBackupNode(registration);
    }
  }

  @Override // NamenodeProtocol
  public NamenodeRegistration registerSubordinateNamenode(
      NamenodeRegistration registration) throws IOException {
    String operationName = "registerSubordinateNamenode";
    checkNNStartup();
    namesystem.checkSuperuserPrivilege(operationName);
    verifyLayoutVersion(registration.getVersion());
    NamenodeRegistration myRegistration = nn.setRegistration();
    namesystem.registerBackupNode(registration, myRegistration);
    return myRegistration;
  }

  @Override // NamenodeProtocol
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
      throws IOException {
    String operationName = "startCheckpoint";
    checkNNStartup();
    namesystem.checkSuperuserPrivilege(operationName);
    verifyRequest(registration);
    if(!nn.isRole(NamenodeRole.NAMENODE))
      throw new IOException("Only an ACTIVE node can invoke startCheckpoint.");

    CacheEntryWithPayload cacheEntry = getCacheEntryWithPayload(null);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return (NamenodeCommand) cacheEntry.getPayload();
    }
    NamenodeCommand ret = null;
    try {
      ret = namesystem.startCheckpoint(registration, nn.setRegistration());
    } finally {
      RetryCache.setState(cacheEntry, ret != null, ret);
    }
    return ret;
  }

  /**
   * Return the current CacheEntry.
   */
  private CacheEntry getCacheEntry() {
    Pair<byte[], Integer> clientInfo =
        NameNode.getClientIdAndCallId(this.ipProxyUsers);
    return RetryCache.waitForCompletion(
        retryCache, clientInfo.getLeft(), clientInfo.getRight());
  }

  /**
   * Return the current CacheEntryWithPayload.
   */
  private CacheEntryWithPayload getCacheEntryWithPayload(Object payload) {
    Pair<byte[], Integer> clientInfo =
        NameNode.getClientIdAndCallId(this.ipProxyUsers);
    return RetryCache.waitForCompletion(retryCache, payload,
        clientInfo.getLeft(), clientInfo.getRight());
  }

  @Override // NamenodeProtocol
  public void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException {
    String operationName = "endCheckpoint";
    checkNNStartup();
    namesystem.checkSuperuserPrivilege(operationName);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
    }
    boolean success = false;
    try {
      namesystem.endCheckpoint(registration, sig);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override // ClientProtocol
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    checkNNStartup();
    return namesystem.getDelegationToken(renewer);
  }

  @Override // ClientProtocol
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    checkNNStartup();
    return namesystem.renewDelegationToken(token);
  }

  @Override // ClientProtocol
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    checkNNStartup();
    namesystem.cancelDelegationToken(token);
  }
  
  @Override // ClientProtocol
  public LocatedBlocks getBlockLocations(String src, 
                                          long offset, 
                                          long length) 
      throws IOException {
    checkNNStartup();
    metrics.incrGetBlockLocations();
    LocatedBlocks locatedBlocks =
        namesystem.getBlockLocations(getClientMachine(), src, offset, length);
    return locatedBlocks;
  }
  
  @Override // ClientProtocol
  public FsServerDefaults getServerDefaults() throws IOException {
    checkNNStartup();
    return namesystem.getServerDefaults();
  }

  @Override // ClientProtocol
  public HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag,
      boolean createParent, short replication, long blockSize,
      CryptoProtocolVersion[] supportedVersions, String ecPolicyName,
      String storagePolicy)
      throws IOException {
    checkNNStartup();
    String clientMachine = getClientMachine();
    stateChangeLog.debug("*DIR* NameNode.create: file {} for {} at {}.",
        src, clientName, clientMachine);
    if (!checkPathLength(src)) {
      throw new IOException("create: Pathname too long.  Limit "
          + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntryWithPayload cacheEntry = getCacheEntryWithPayload(null);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return (HdfsFileStatus) cacheEntry.getPayload();
    }

    HdfsFileStatus status = null;
    try {
      PermissionStatus perm = new PermissionStatus(getRemoteUser()
          .getShortUserName(), null, masked);
      status = namesystem.startFile(src, perm, clientName, clientMachine,
          flag.get(), createParent, replication, blockSize, supportedVersions,
          ecPolicyName, storagePolicy, cacheEntry != null);
    } finally {
      RetryCache.setState(cacheEntry, status != null, status);
    }

    metrics.incrFilesCreated();
    metrics.incrCreateFileOps();
    return status;
  }

  @Override // ClientProtocol
  public LastBlockWithStatus append(String src, String clientName,
      EnumSetWritable<CreateFlag> flag) throws IOException {
    checkNNStartup();
    String clientMachine = getClientMachine();
    stateChangeLog.debug("*DIR* NameNode.append: file {} for {} at {}.",
        src, clientName, clientMachine);
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntryWithPayload cacheEntry = getCacheEntryWithPayload(null);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return (LastBlockWithStatus) cacheEntry.getPayload();
    }

    LastBlockWithStatus info = null;
    boolean success = false;
    try {
      info = namesystem.appendFile(src, clientName, clientMachine, flag.get(),
          cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success, info);
    }
    metrics.incrFilesAppended();
    return info;
  }

  @Override // ClientProtocol
  public boolean recoverLease(String src, String clientName) throws IOException {
    checkNNStartup();
    String clientMachine = getClientMachine();
    return namesystem.recoverLease(src, clientName, clientMachine);
  }

  @Override // ClientProtocol
  public boolean setReplication(String src, short replication) 
    throws IOException {
    checkNNStartup();
    return namesystem.setReplication(src, replication);
  }

  @Override
  public void unsetStoragePolicy(String src)
      throws IOException {
    checkNNStartup();
    stateChangeLog.debug("*DIR* NameNode.unsetStoragePolicy for path: {}", src);
    namesystem.unsetStoragePolicy(src);
  }

  @Override
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    checkNNStartup();
    stateChangeLog.debug("*DIR* NameNode.setStoragePolicy for path: {}, policyName: {}",
        src, policyName);
    namesystem.setStoragePolicy(src, policyName);
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    checkNNStartup();
    stateChangeLog.debug("*DIR* NameNode.getStoragePolicy for path: {}", path);
    return namesystem.getStoragePolicy(path);
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    checkNNStartup();
    return namesystem.getStoragePolicies();
  }

  @Override // ClientProtocol
  public void setPermission(String src, FsPermission permissions)
      throws IOException {
    checkNNStartup();
    namesystem.setPermission(src, permissions);
  }

  @Override // ClientProtocol
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    checkNNStartup();
    namesystem.setOwner(src, username, groupname);
  }
  
  @Override
  public LocatedBlock addBlock(String src, String clientName,
      ExtendedBlock previous, DatanodeInfo[] excludedNodes, long fileId,
      String[] favoredNodes, EnumSet<AddBlockFlag> addBlockFlags)
      throws IOException {
    checkNNStartup();
    LocatedBlock locatedBlock = namesystem.getAdditionalBlock(src, fileId,
        clientName, previous, excludedNodes, favoredNodes, addBlockFlags);
    if (locatedBlock != null) {
      metrics.incrAddBlockOps();
    }
    return locatedBlock;
  }

  @Override // ClientProtocol
  public LocatedBlock getAdditionalDatanode(final String src,
      final long fileId, final ExtendedBlock blk,
      final DatanodeInfo[] existings, final String[] existingStorageIDs,
      final DatanodeInfo[] excludes,
      final int numAdditionalNodes, final String clientName
      ) throws IOException {
    checkNNStartup();
    if (LOG.isDebugEnabled()) {
      LOG.debug("getAdditionalDatanode: src={}, fileId={}, blk={}, existings={}, excludes={}"
          + ", numAdditionalNodes={}, clientName={}", src, fileId, blk, Arrays.asList(existings),
          Arrays.asList(excludes), numAdditionalNodes, clientName);
    }

    metrics.incrGetAdditionalDatanodeOps();

    Set<Node> excludeSet = null;
    if (excludes != null) {
      excludeSet = new HashSet<Node>(excludes.length);
      for (Node node : excludes) {
        excludeSet.add(node);
      }
    }
    LocatedBlock locatedBlock = namesystem.getAdditionalDatanode(src, fileId,
        blk, existings, existingStorageIDs, excludeSet, numAdditionalNodes,
        clientName);
    return locatedBlock;
  }
  /**
   * The client needs to give up on the block.
   */
  @Override // ClientProtocol
  public void abandonBlock(ExtendedBlock b, long fileId, String src,
        String holder) throws IOException {
    checkNNStartup();
    namesystem.abandonBlock(b, fileId, src, holder);
  }

  @Override // ClientProtocol
  public boolean complete(String src, String clientName,
                          ExtendedBlock last,  long fileId)
      throws IOException {
    checkNNStartup();
    return namesystem.completeFile(src, clientName, last, fileId);
  }

  /**
   * The client has detected an error on the specified located blocks 
   * and is reporting them to the server.  For now, the namenode will 
   * mark the block as corrupt.  In the future we might 
   * check the blocks are actually corrupt. 
   */
  @Override // ClientProtocol, DatanodeProtocol
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    checkNNStartup();
    namesystem.reportBadBlocks(blocks);
  }

  @Override // ClientProtocol
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName)
      throws IOException {
    checkNNStartup();
    return namesystem.bumpBlockGenerationStamp(block, clientName);
  }


  @Override // ClientProtocol
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs)
      throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
    }

    boolean success = false;
    try {
      namesystem.updatePipeline(clientName, oldBlock, newBlock, newNodes,
          newStorageIDs, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }
  
  @Override // DatanodeProtocol
  public void commitBlockSynchronization(ExtendedBlock block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets,
      String[] newtargetstorages)
      throws IOException {
    checkNNStartup();
    namesystem.commitBlockSynchronization(block, newgenerationstamp,
        newlength, closeFile, deleteblock, newtargets, newtargetstorages);
  }
  
  @Override // ClientProtocol
  public long getPreferredBlockSize(String filename) 
      throws IOException {
    checkNNStartup();
    return namesystem.getPreferredBlockSize(filename);
  }
    
  @Deprecated
  @Override // ClientProtocol
  public boolean rename(String src, String dst) throws IOException {
    checkNNStartup();
    stateChangeLog.debug("*DIR* NameNode.rename: {} to {}.", src, dst);
    if (!checkPathLength(dst)) {
      throw new IOException("rename: Pathname too long.  Limit "
          + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return true; // Return previous response
    }

    boolean ret = false;
    try {
      ret = namesystem.renameTo(src, dst, cacheEntry != null);
    } finally {
      RetryCache.setState(cacheEntry, ret);
    }
    if (ret) {
      metrics.incrFilesRenamed();
    }
    return ret;
  }
  
  @Override // ClientProtocol
  public void concat(String trg, String[] src) throws IOException {
    checkNNStartup();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.concat: src path {} to target path {}",
          Arrays.toString(src), trg);
    }
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
    }
    boolean success = false;

    try {
      namesystem.concat(trg, src, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }
  
  @Override // ClientProtocol
  public void rename2(String src, String dst, Options.Rename... options)
      throws IOException {
    checkNNStartup();
    stateChangeLog.debug("*DIR* NameNode.rename: {} to {}.", src, dst);
    if (!checkPathLength(dst)) {
      throw new IOException("rename: Pathname too long.  Limit "
          + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
    }
    boolean success = false;
    try {
      namesystem.renameTo(src, dst, cacheEntry != null, options);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
    metrics.incrFilesRenamed();
  }

  @Override // ClientProtocol
  public boolean truncate(String src, long newLength, String clientName)
      throws IOException {
    checkNNStartup();
    stateChangeLog.debug("*DIR* NameNode.truncate: {} to {}", src, newLength);
    String clientMachine = getClientMachine();
    try {
      return namesystem.truncate(
          src, newLength, clientName, clientMachine, now());
    } finally {
      metrics.incrFilesTruncated();
    }
  }

  @Override // ClientProtocol
  public boolean delete(String src, boolean recursive) throws IOException {
    checkNNStartup();
    stateChangeLog.debug("*DIR* Namenode.delete: src={}, recursive={}.", src, recursive);
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return true; // Return previous response
    }

    boolean ret = false;
    try {
      ret = namesystem.delete(src, recursive, cacheEntry != null);
    } finally {
      RetryCache.setState(cacheEntry, ret);
    }
    if (ret) 
      metrics.incrDeleteFileOps();
    return ret;
  }

  /**
   * Check path length does not exceed maximum.  Returns true if
   * length and depth are okay.  Returns false if length is too long 
   * or depth is too great.
   */
  private boolean checkPathLength(String src) {
    Path srcPath = new Path(src);
    return (src.length() <= MAX_PATH_LENGTH &&
            srcPath.depth() <= MAX_PATH_DEPTH);
  }
    
  @Override // ClientProtocol
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws IOException {
    checkNNStartup();
    stateChangeLog.debug("*DIR* NameNode.mkdirs: {}.", src);
    if (!checkPathLength(src)) {
      throw new IOException("mkdirs: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    return namesystem.mkdirs(src,
        new PermissionStatus(getRemoteUser().getShortUserName(),
            null, masked), createParent);
  }

  @Override // ClientProtocol
  public void renewLease(String clientName, List<String> namespaces)
      throws IOException {
    if (namespaces != null && namespaces.size() > 0) {
      LOG.warn("namespaces({}) should be null or empty "
          + "on NameNode side, please check it.", namespaces);
      throw new IOException("namespaces(" + namespaces
          + ") should be null or empty");
    }
    checkNNStartup();
    namesystem.renewLease(clientName);        
  }

  @Override // ClientProtocol
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws IOException {
    checkNNStartup();
    DirectoryListing files = namesystem.getListing(
        src, startAfter, needLocation);
    if (files != null) {
      metrics.incrGetListingOps();
      metrics.incrFilesInGetListingOps(files.getPartialListing().length);
    }
    return files;
  }

  @Override // ClientProtocol
  public BatchedDirectoryListing getBatchedListing(
      String[] srcs,
      byte[] startAfter,
      boolean needLocation) throws IOException {
    checkNNStartup();
    BatchedDirectoryListing batchedListing =
        namesystem.getBatchedListing(srcs, startAfter, needLocation);
    if (batchedListing != null) {
      metrics.incrGetListingOps();
      int numEntries = 0;
      for (HdfsPartialListing partial : batchedListing.getListings()) {
        if (partial.getPartialListing() != null) {
          numEntries += partial.getPartialListing().size();
        }
      }
      metrics.incrFilesInGetListingOps(numEntries);
    }
    return batchedListing;
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    checkNNStartup();
    metrics.incrFileInfoOps();
    return namesystem.getFileInfo(src, true, false, false);
  }

  @Override // ClientProtocol
  public HdfsLocatedFileStatus getLocatedFileInfo(String src,
      boolean needBlockToken) throws IOException {
    checkNNStartup();
    if (needBlockToken) {
      metrics.incrGetBlockLocations();
    } else {
      metrics.incrFileInfoOps();
    }
    return (HdfsLocatedFileStatus)
        namesystem.getFileInfo(src, true, true, needBlockToken);
  }

  @Override // ClientProtocol
  public boolean isFileClosed(String src) throws IOException{
    checkNNStartup();
    return namesystem.isFileClosed(src);
  }
  
  @Override // ClientProtocol
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
    checkNNStartup();
    metrics.incrFileInfoOps();
    return namesystem.getFileInfo(src, false, false, false);
  }
  
  @Override // ClientProtocol
  public long[] getStats() throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.READ);
    return namesystem.getStats();
  }

  @Override // ClientProtocol
  public ReplicatedBlockStats getReplicatedBlockStats() throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.READ);
    return namesystem.getReplicatedBlockStats();
  }

  @Override // ClientProtocol
  public ECBlockGroupStats getECBlockGroupStats() throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.READ);
    return namesystem.getECBlockGroupStats();
  }

  @Override // ClientProtocol
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
  throws IOException {
    checkNNStartup();
    DatanodeInfo results[] = namesystem.datanodeReport(type);
    return results;
  }

  @Override // ClientProtocol
  public DatanodeStorageReport[] getDatanodeStorageReport(
      DatanodeReportType type) throws IOException {
    checkNNStartup();
    final DatanodeStorageReport[] reports = namesystem.getDatanodeStorageReport(type);
    return reports;
  }

  @Override // ClientProtocol
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    checkNNStartup();
    OperationCategory opCategory = OperationCategory.UNCHECKED;
    if (isChecked) {
      if (action == SafeModeAction.SAFEMODE_GET) {
        opCategory = OperationCategory.READ;
      } else {
        opCategory = OperationCategory.WRITE;
      }
    }
    namesystem.checkOperation(opCategory);
    return namesystem.setSafeMode(action);
  }

  @Override // ClientProtocol
  public boolean restoreFailedStorage(String arg) throws IOException { 
    checkNNStartup();
    return namesystem.restoreFailedStorage(arg);
  }

  @Override // ClientProtocol
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    checkNNStartup();
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return true; // Return previous response
    }
    boolean success = false;
    try {
      namesystem.saveNamespace(timeWindow, txGap);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
    return true;
  }
  
  @Override // ClientProtocol
  public long rollEdits() throws AccessControlException, IOException {
    checkNNStartup();
    CheckpointSignature sig = namesystem.rollEditLog();
    return sig.getCurSegmentTxId();
  }

  @Override // ClientProtocol
  public void refreshNodes() throws IOException {
    checkNNStartup();
    namesystem.refreshNodes();
  }

  @Override // NamenodeProtocol
  public long getTransactionID() throws IOException {
    String operationName = "getTransactionID";
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.UNCHECKED);
    namesystem.checkSuperuserPrivilege(operationName);
    return namesystem.getFSImage().getCorrectLastAppliedOrWrittenTxId();
  }
  
  @Override // NamenodeProtocol
  public long getMostRecentCheckpointTxId() throws IOException {
    String operationName = "getMostRecentCheckpointTxId";
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.UNCHECKED);
    namesystem.checkSuperuserPrivilege(operationName);
    return namesystem.getFSImage().getMostRecentCheckpointTxId();
  }
  
  @Override // NamenodeProtocol
  public CheckpointSignature rollEditLog() throws IOException {
    checkNNStartup();
    return namesystem.rollEditLog();
  }
  
  @Override // NamenodeProtocol
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
      throws IOException {
    String operationName = "getEditLogManifest";
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.READ);
    namesystem.checkSuperuserPrivilege(operationName);
    return namesystem.getEditLog().getEditLogManifest(sinceTxId);
  }

  @Override // NamenodeProtocol
  public boolean isUpgradeFinalized() throws IOException {
    String operationName = "isUpgradeFinalized";
    checkNNStartup();
    namesystem.checkSuperuserPrivilege(operationName);
    return namesystem.isUpgradeFinalized();
  }

  @Override // NamenodeProtocol
  public boolean isRollingUpgrade() throws IOException {
    String operationName = "isRollingUpgrade";
    checkNNStartup();
    namesystem.checkSuperuserPrivilege(operationName);
    return namesystem.isRollingUpgrade();
  }
    
  @Override // ClientProtocol
  public void finalizeUpgrade() throws IOException {
    checkNNStartup();
    namesystem.finalizeUpgrade();
  }

  @Override // ClientProtocol
  public boolean upgradeStatus() throws IOException {
    checkNNStartup();
    return namesystem.isUpgradeFinalized();
  }

  @Override // ClientProtocol
  public RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action) throws IOException {
    checkNNStartup();
    LOG.info("rollingUpgrade " + action);
    switch(action) {
    case QUERY:
      return namesystem.queryRollingUpgrade();
    case PREPARE:
      return namesystem.startRollingUpgrade();
    case FINALIZE:
      return namesystem.finalizeRollingUpgrade();
    default:
      throw new UnsupportedActionException(action + " is not yet supported.");
    }
  }

  @Override // ClientProtocol
  public void metaSave(String filename) throws IOException {
    checkNNStartup();
    namesystem.metaSave(filename);
  }

  @Deprecated
  @Override // ClientProtocol
  public BatchedEntries<OpenFileEntry> listOpenFiles(long prevId)
      throws IOException {
    return listOpenFiles(prevId, EnumSet.of(OpenFilesType.ALL_OPEN_FILES),
        OpenFilesIterator.FILTER_PATH_DEFAULT);
  }

  @Override // ClientProtocol
  public BatchedEntries<OpenFileEntry> listOpenFiles(long prevId,
      EnumSet<OpenFilesType> openFilesTypes, String path) throws IOException {
    checkNNStartup();
    return namesystem.listOpenFiles(prevId, openFilesTypes, path);
  }

  @Override // ClientProtocol
  public void msync() throws IOException {
    // Check for write access to ensure that msync only happens on active
    namesystem.checkOperation(OperationCategory.WRITE);
  }

  @Override // ClientProtocol
  public HAServiceState getHAServiceState() throws IOException {
    checkNNStartup();
    return nn.getServiceStatus().getState();
  }

  @Override // ClientProtocol
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    checkNNStartup();
    String[] cookieTab = new String[] { cookie };
    Collection<FSNamesystem.CorruptFileBlockInfo> fbs =
      namesystem.listCorruptFileBlocks(path, cookieTab);

    String[] files = new String[fbs.size()];
    int i = 0;
    for(FSNamesystem.CorruptFileBlockInfo fb: fbs) {
      files[i++] = fb.path;
    }
    return new CorruptFileBlocks(files, cookieTab[0]);
  }

  /**
   * Tell all datanodes to use a new, non-persistent bandwidth value for
   * dfs.datanode.balance.bandwidthPerSec.
   * @param bandwidth Balancer bandwidth in bytes per second for all datanodes.
   * @throws IOException
   */
  @Override // ClientProtocol
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    if (bandwidth > HdfsServerConstants.MAX_BANDWIDTH_PER_DATANODE) {
      throw new IllegalArgumentException(
          "Bandwidth should not exceed maximum limit "
              + HdfsServerConstants.MAX_BANDWIDTH_PER_DATANODE
              + " bytes per second");
    }
    checkNNStartup();
    namesystem.setBalancerBandwidth(bandwidth);
  }
  
  @Override // ClientProtocol
  public ContentSummary getContentSummary(String path) throws IOException {
    checkNNStartup();
    return namesystem.getContentSummary(path);
  }

  @Override // ClientProtocol
  public QuotaUsage getQuotaUsage(String path) throws IOException {
    checkNNStartup();
    return namesystem.getQuotaUsage(path);
  }

  @Override // ClientProtocol
  public void satisfyStoragePolicy(String src) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
    }
    boolean success = false;
    try {
      namesystem.satisfyStoragePolicy(src, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override
  public DatanodeInfo[] getSlowDatanodeReport() throws IOException {
    checkNNStartup();
    return namesystem.slowDataNodesReport();
  }

  @Override // ClientProtocol
  public void setQuota(String path, long namespaceQuota, long storagespaceQuota,
                       StorageType type)
      throws IOException {
    checkNNStartup();
    namesystem.setQuota(path, namespaceQuota, storagespaceQuota, type);
  }
  
  @Override // ClientProtocol
  public void fsync(String src, long fileId, String clientName,
                    long lastBlockLength)
      throws IOException {
    checkNNStartup();
    namesystem.fsync(src, fileId, clientName, lastBlockLength);
  }

  @Override // ClientProtocol
  public void setTimes(String src, long mtime, long atime) 
      throws IOException {
    checkNNStartup();
    namesystem.setTimes(src, mtime, atime);
  }

  @Override // ClientProtocol
  public void createSymlink(String target, String link, FsPermission dirPerms,
      boolean createParent) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
    }

    /* We enforce the MAX_PATH_LENGTH limit even though a symlink target
     * URI may refer to a non-HDFS file system. 
     */
    if (!checkPathLength(link)) {
      throw new IOException("Symlink path exceeds " + MAX_PATH_LENGTH +
                            " character limit");
                            
    }

    final UserGroupInformation ugi = getRemoteUser();

    boolean success = false;
    try {
      PermissionStatus perm = new PermissionStatus(ugi.getShortUserName(),
          null, dirPerms);
      namesystem.createSymlink(target, link, perm, createParent,
          cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override // ClientProtocol
  public String getLinkTarget(String path) throws IOException {
    checkNNStartup();
    metrics.incrGetLinkTargetOps();
    HdfsFileStatus stat = null;
    try {
      stat = namesystem.getFileInfo(path, false, false, false);
    } catch (UnresolvedPathException e) {
      return e.getResolvedPath().toString();
    } catch (UnresolvedLinkException e) {
      // The NameNode should only throw an UnresolvedPathException
      throw new AssertionError("UnresolvedLinkException thrown");
    }
    if (stat == null) {
      throw new FileNotFoundException("File does not exist: " + path);
    } else if (!stat.isSymlink()) {
      throw new IOException("Path " + path + " is not a symbolic link");
    }
    return DFSUtilClient.bytes2String(stat.getSymlinkInBytes());
  }


  @Override // DatanodeProtocol
  public DatanodeRegistration registerDatanode(DatanodeRegistration nodeReg)
      throws IOException {
    checkNNStartup();
    verifySoftwareVersion(nodeReg);
    namesystem.registerDatanode(nodeReg);
    return nodeReg;
  }

  @Override // DatanodeProtocol
  public HeartbeatResponse sendHeartbeat(DatanodeRegistration nodeReg,
      StorageReport[] report, long dnCacheCapacity, long dnCacheUsed,
      int xmitsInProgress, int xceiverCount,
      int failedVolumes, VolumeFailureSummary volumeFailureSummary,
      boolean requestFullBlockReportLease,
      @Nonnull SlowPeerReports slowPeers,
      @Nonnull SlowDiskReports slowDisks)
          throws IOException {
    checkNNStartup();
    verifyRequest(nodeReg);
    return namesystem.handleHeartbeat(nodeReg, report,
        dnCacheCapacity, dnCacheUsed, xceiverCount, xmitsInProgress,
        failedVolumes, volumeFailureSummary, requestFullBlockReportLease,
        slowPeers, slowDisks);
  }

  @Override // DatanodeProtocol
  public DatanodeCommand blockReport(final DatanodeRegistration nodeReg,
        String poolId, final StorageBlockReport[] reports,
        final BlockReportContext context) throws IOException {
    checkNNStartup();
    verifyRequest(nodeReg);
    blockStateChangeLog.debug("*BLOCK* NameNode.blockReport: from {}, reports.length={}.",
        nodeReg, reports.length);
    final BlockManager bm = namesystem.getBlockManager(); 
    boolean noStaleStorages = false;
    try {
      if (bm.checkBlockReportLease(context, nodeReg)) {
        for (int r = 0; r < reports.length; r++) {
          final BlockListAsLongs blocks = reports[r].getBlocks();
          //
          // BlockManager.processReport accumulates information of prior calls
          // for the same node and storage, so the value returned by the last
          // call of this loop is the final updated value for noStaleStorage.
          //
          final int index = r;
          noStaleStorages = bm.runBlockOp(() ->
            bm.processReport(nodeReg, reports[index].getStorage(),
                blocks, context));
        }
      } else {
        throw new InvalidBlockReportLeaseException(context.getReportId(), context.getLeaseId());
      }
    } catch (UnregisteredNodeException une) {
      LOG.warn("Datanode {} is attempting to report but not register yet.",
          nodeReg);
      return RegisterCommand.REGISTER;
    }
    bm.removeBRLeaseIfNeeded(nodeReg, context);

    BlockManagerFaultInjector.getInstance().
        incomingBlockReportRpc(nodeReg, context);

    if (nn.getFSImage().isUpgradeFinalized() &&
        !namesystem.isRollingUpgrade() &&
        nn.isActiveState() &&
        noStaleStorages) {
      return new FinalizeCommand(poolId);
    }

    return null;
  }

  @Override
  public DatanodeCommand cacheReport(DatanodeRegistration nodeReg,
      String poolId, List<Long> blockIds) throws IOException {
    checkNNStartup();
    verifyRequest(nodeReg);
    blockStateChangeLog.debug("*BLOCK* NameNode.cacheReport: from {} {} blocks",
        nodeReg, blockIds.size());
    namesystem.getCacheManager().processCacheReport(nodeReg, blockIds);
    return null;
  }

  @Override // DatanodeProtocol
  public void blockReceivedAndDeleted(final DatanodeRegistration nodeReg,
      String poolId, StorageReceivedDeletedBlocks[] receivedAndDeletedBlocks)
          throws IOException {
    checkNNStartup();
    verifyRequest(nodeReg);
    metrics.incrBlockReceivedAndDeletedOps();
    blockStateChangeLog.debug("*BLOCK* NameNode.blockReceivedAndDeleted: from {} {} blocks.",
        nodeReg, receivedAndDeletedBlocks.length);
    final BlockManager bm = namesystem.getBlockManager();
    for (final StorageReceivedDeletedBlocks r : receivedAndDeletedBlocks) {
      bm.enqueueBlockOp(new Runnable() {
        @Override
        public void run() {
          try {
            namesystem.processIncrementalBlockReport(nodeReg, r);
          } catch (Exception ex) {
            // usually because the node is unregistered/dead.  next heartbeat
            // will correct the problem
            blockStateChangeLog.error(
                "*BLOCK* NameNode.blockReceivedAndDeleted: "
                    + "failed from " + nodeReg + ": " + ex.getMessage());
          }
        }
      });
    }
  }

  @Override // DatanodeProtocol
  public void errorReport(DatanodeRegistration nodeReg,
                          int errorCode, String msg) throws IOException { 
    checkNNStartup();
    String dnName = 
       (nodeReg == null) ? "Unknown DataNode" : nodeReg.toString();

    if (errorCode == DatanodeProtocol.NOTIFY) {
      LOG.info("Error report from " + dnName + ": " + msg);
      return;
    }
    verifyRequest(nodeReg);

    if (errorCode == DatanodeProtocol.DISK_ERROR) {
      LOG.warn("Disk error on " + dnName + ": " + msg);
    } else if (errorCode == DatanodeProtocol.FATAL_DISK_ERROR) {
      LOG.warn("Fatal disk error on " + dnName + ": " + msg);
      namesystem.getBlockManager().getDatanodeManager().removeDatanode(nodeReg);            
    } else {
      LOG.info("Error report from " + dnName + ": " + msg);
    }
  }
    
  @Override // DatanodeProtocol, NamenodeProtocol
  public NamespaceInfo versionRequest() throws IOException {
    checkNNStartup();
    return namesystem.getNamespaceInfo();
  }

  @Override // DatanodeLifelineProtocol
  public void sendLifeline(DatanodeRegistration nodeReg, StorageReport[] report,
      long dnCacheCapacity, long dnCacheUsed, int xmitsInProgress,
      int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary) throws IOException {
    checkNNStartup();
    verifyRequest(nodeReg);
    namesystem.handleLifeline(nodeReg, report, dnCacheCapacity, dnCacheUsed,
        xceiverCount, xmitsInProgress, failedVolumes, volumeFailureSummary);
  }

  /** 
   * Verifies the given registration.
   * 
   * @param nodeReg node registration
   * @throws UnregisteredNodeException if the registration is invalid
   */
  private void verifyRequest(NodeRegistration nodeReg) throws IOException {
    // verify registration ID
    final String id = nodeReg.getRegistrationID();
    final String expectedID = namesystem.getRegistrationID();
    if (!expectedID.equals(id)) {
      LOG.warn("Registration IDs mismatched: the "
          + nodeReg.getClass().getSimpleName() + " ID is " + id
          + " but the expected ID is " + expectedID);
       throw new UnregisteredNodeException(nodeReg);
    }
  }

  @Override // RefreshAuthorizationPolicyProtocol
  public void refreshServiceAcl() throws IOException {
    checkNNStartup();
    if (!serviceAuthEnabled) {
      throw new AuthorizationException("Service Level Authorization not enabled!");
    }

    this.clientRpcServer.refreshServiceAcl(new Configuration(), new HDFSPolicyProvider());
    if (this.serviceRpcServer != null) {
      this.serviceRpcServer.refreshServiceAcl(new Configuration(), new HDFSPolicyProvider());
    }
    namesystem.logAuditEvent(true, "refreshServiceAcl", null);
  }

  @Override // RefreshAuthorizationPolicyProtocol
  public void refreshUserToGroupsMappings() throws IOException {
    LOG.info("Refreshing all user-to-groups mappings. Requested by user: " +
        getRemoteUser().getShortUserName());
    Groups.getUserToGroupsMappingService().refresh();
    namesystem.logAuditEvent(true, "refreshUserToGroupsMappings", null);
  }

  @Override // RefreshAuthorizationPolicyProtocol
  public void refreshSuperUserGroupsConfiguration() throws IOException {
    LOG.info("Refreshing SuperUser proxy group mapping list ");

    ProxyUsers.refreshSuperUserGroupsConfiguration();
    namesystem.logAuditEvent(true, "refreshSuperUserGroupsConfiguration", null);
  }

  @Override // RefreshCallQueueProtocol
  public void refreshCallQueue() throws IOException {
    LOG.info("Refreshing call queue.");

    Configuration conf = new Configuration();
    clientRpcServer.refreshCallQueue(conf);
    if (this.serviceRpcServer != null) {
      serviceRpcServer.refreshCallQueue(conf);
    }
    namesystem.logAuditEvent(true, "refreshCallQueue", null);
  }

  @Override // GenericRefreshProtocol
  public Collection<RefreshResponse> refresh(String identifier, String[] args) {
    // Let the registry handle as needed
    return RefreshRegistry.defaultRegistry().dispatch(identifier, args);
  }
  
  @Override // GetUserMappingsProtocol
  public String[] getGroupsForUser(String user) throws IOException {
    LOG.debug("Getting groups for user {}", user);
    return UserGroupInformation.createRemoteUser(user).getGroupNames();
  }

  @Override // HAServiceProtocol
  public synchronized void monitorHealth() throws HealthCheckFailedException,
      AccessControlException, IOException {
    checkNNStartup();
    nn.monitorHealth();
  }
  
  @Override // HAServiceProtocol
  public synchronized void transitionToActive(StateChangeRequestInfo req) 
      throws ServiceFailedException, AccessControlException, IOException {
    checkNNStartup();
    nn.checkHaStateChange(req);
    nn.transitionToActive();
  }

  @Override // HAServiceProtocol
  public synchronized void transitionToStandby(StateChangeRequestInfo req)
      throws ServiceFailedException, AccessControlException, IOException {
    checkNNStartup();
    // This is to eliminate any race condition between manual transition of
    // namenode into Observer, and ZKFC auto failover election, when the
    // namenode has already participated in the
    // ZKFC election, before transition to Observer state as Standby Node.
    // For more details check : HDFS-14961.
    if (nn.getState().equals(NameNode.OBSERVER_STATE.toString())
        && req.getSource() == RequestSource.REQUEST_BY_ZKFC) {
      throw new AccessControlException(
          "Request from ZK failover controller at " + Server.getRemoteAddress()
              + " denied since the namenode is in Observer state.");
    }
    nn.checkHaStateChange(req);
    nn.transitionToStandby();
  }

  @Override // HAServiceProtocol
  public synchronized void transitionToObserver(StateChangeRequestInfo req)
      throws ServiceFailedException, AccessControlException, IOException {
    checkNNStartup();
    nn.checkHaStateChange(req);
    nn.transitionToObserver();
  }

  @Override // HAServiceProtocol
  public synchronized HAServiceStatus getServiceStatus() 
      throws AccessControlException, ServiceFailedException, IOException {
    checkNNStartup();
    return nn.getServiceStatus();
  }

  /**
   * Verify version.
   * @param version layout version
   * @throws IOException on layout version mismatch
   */
  void verifyLayoutVersion(int version) throws IOException {
    if (version != HdfsServerConstants.NAMENODE_LAYOUT_VERSION)
      throw new IncorrectVersionException(
          HdfsServerConstants.NAMENODE_LAYOUT_VERSION, version, "data node");
  }
  
  private void verifySoftwareVersion(DatanodeRegistration dnReg)
      throws IncorrectVersionException {
    String dnVersion = dnReg.getSoftwareVersion();
    if (VersionUtil.compareVersions(dnVersion, minimumDataNodeVersion) < 0) {
      IncorrectVersionException ive = new IncorrectVersionException(
          minimumDataNodeVersion, dnVersion, "DataNode", "NameNode");
      LOG.warn(ive.getMessage() + " DN: " + dnReg);
      throw ive;
    }
    String nnVersion = VersionInfo.getVersion();
    if (!dnVersion.equals(nnVersion)) {
      String messagePrefix = "Reported DataNode version '" + dnVersion +
          "' of DN " + dnReg + " does not match NameNode version '" +
          nnVersion + "'";
      long nnCTime = nn.getFSImage().getStorage().getCTime();
      long dnCTime = dnReg.getStorageInfo().getCTime();
      if (nnCTime != dnCTime) {
        IncorrectVersionException ive = new IncorrectVersionException(
            messagePrefix + " and CTime of DN ('" + dnCTime +
            "') does not match CTime of NN ('" + nnCTime + "')");
        LOG.warn(ive.toString(), ive);
        throw ive;
      } else {
        LOG.info(messagePrefix +
            ". Note: This is normal during a rolling upgrade.");
      }
    }
  }

  /**
   * Get the actual client's machine.
   */
  private String getClientMachine() {
    return NameNode.getClientMachine(this.ipProxyUsers);
  }

  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    checkNNStartup();
    return namesystem.getBlockManager().generateDataEncryptionKey();
  }

  @Override
  public String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    checkNNStartup();
    LOG.debug("*DIR* NameNode.createSnapshot: Path {} and SnapshotName {}",
        snapshotRoot, snapshotName);
    if (!checkPathLength(snapshotRoot)) {
      throw new IOException("createSnapshot: Pathname too long.  Limit "
          + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntryWithPayload cacheEntry = getCacheEntryWithPayload(null);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return (String) cacheEntry.getPayload();
    }

    metrics.incrCreateSnapshotOps();
    String ret = null;
    try {
      ret = namesystem.createSnapshot(snapshotRoot, snapshotName,
          cacheEntry != null);
    } finally {
      RetryCache.setState(cacheEntry, ret != null, ret);
    }
    return ret;
  }
  
  @Override
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    checkNNStartup();
    LOG.debug("*DIR* NameNode.deleteSnapshot: Path {} and SnapshotName {}",
        snapshotRoot, snapshotName);
    if (snapshotName == null || snapshotName.isEmpty()) {
      throw new IOException("The snapshot name is null or empty.");
    }
    namesystem.checkOperation(OperationCategory.WRITE);
    metrics.incrDeleteSnapshotOps();
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
    }
    boolean success = false;
    try {
      namesystem.deleteSnapshot(snapshotRoot, snapshotName, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override
  // Client Protocol
  public void allowSnapshot(String snapshotRoot) throws IOException {
    checkNNStartup();
    metrics.incrAllowSnapshotOps();
    namesystem.allowSnapshot(snapshotRoot);
  }

  @Override
  // Client Protocol
  public void disallowSnapshot(String snapshot) throws IOException {
    checkNNStartup();
    metrics.incrDisAllowSnapshotOps();
    namesystem.disallowSnapshot(snapshot);
  }

  @Override
  // ClientProtocol
  public void renameSnapshot(String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException {
    checkNNStartup();
    LOG.debug(
        "*DIR* NameNode.renameSnapshot: Snapshot Path {},snapshotOldName {}, snapshotNewName {}",
        snapshotRoot, snapshotOldName, snapshotNewName);
    if (snapshotNewName == null || snapshotNewName.isEmpty()) {
      throw new IOException("The new snapshot name is null or empty.");
    }
    namesystem.checkOperation(OperationCategory.WRITE);
    metrics.incrRenameSnapshotOps();
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
    }
    boolean success = false;
    try {
      namesystem.renameSnapshot(snapshotRoot, snapshotOldName,
          snapshotNewName, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override // Client Protocol
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    checkNNStartup();
    SnapshottableDirectoryStatus[] status = namesystem
        .getSnapshottableDirListing();
    metrics.incrListSnapshottableDirOps();
    return status;
  }

  @Override // Client Protocol
  public SnapshotStatus[] getSnapshotListing(String snapshotRoot)
      throws IOException {
    checkNNStartup();
    SnapshotStatus[] status = namesystem
        .getSnapshotListing(snapshotRoot);
    metrics.incrListSnapshotsOps();
    return status;
  }

  @Override // ClientProtocol
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
      String earlierSnapshotName, String laterSnapshotName) throws IOException {
    checkNNStartup();
    SnapshotDiffReport report = namesystem.getSnapshotDiffReport(snapshotRoot,
        earlierSnapshotName, laterSnapshotName);
    metrics.incrSnapshotDiffReportOps();
    return report;
  }

  @Override // ClientProtocol
  public SnapshotDiffReportListing getSnapshotDiffReportListing(
      String snapshotRoot, String earlierSnapshotName, String laterSnapshotName,
      byte[] startPath, int index) throws IOException {
    checkNNStartup();
    SnapshotDiffReportListing report = namesystem
        .getSnapshotDiffReportListing(snapshotRoot, earlierSnapshotName,
            laterSnapshotName, startPath, index);
    metrics.incrSnapshotDiffReportOps();
    return report;
  }

  @Override // ClientProtocol
  public long addCacheDirective(
      CacheDirectiveInfo path, EnumSet<CacheFlag> flags) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntryWithPayload cacheEntry = getCacheEntryWithPayload(null);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return (Long) cacheEntry.getPayload();
    }

    boolean success = false;
    long ret = 0;
    try {
      ret = namesystem.addCacheDirective(path, flags, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success, ret);
    }
    return ret;
  }

  @Override // ClientProtocol
  public void modifyCacheDirective(
      CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return;
    }

    boolean success = false;
    try {
      namesystem.modifyCacheDirective(directive, flags, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override // ClientProtocol
  public void removeCacheDirective(long id) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return;
    }
    boolean success = false;
    try {
      namesystem.removeCacheDirective(id, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override // ClientProtocol
  public BatchedEntries<CacheDirectiveEntry> listCacheDirectives(long prevId,
      CacheDirectiveInfo filter) throws IOException {
    checkNNStartup();
    if (filter == null) {
      filter = new CacheDirectiveInfo.Builder().build();
    }
    return namesystem.listCacheDirectives(prevId, filter);
  }

  @Override //ClientProtocol
  public void addCachePool(CachePoolInfo info) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
    }
    boolean success = false;
    try {
      namesystem.addCachePool(info, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override // ClientProtocol
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
    }
    boolean success = false;
    try {
      namesystem.modifyCachePool(info, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override // ClientProtocol
  public void removeCachePool(String cachePoolName) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return;
    }
    boolean success = false;
    try {
      namesystem.removeCachePool(cachePoolName, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override // ClientProtocol
  public BatchedEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    checkNNStartup();
    return namesystem.listCachePools(prevKey != null ? prevKey : "");
  }

  @Override // ClientProtocol
  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    checkNNStartup();
    namesystem.modifyAclEntries(src, aclSpec);
  }

  @Override // ClienProtocol
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    checkNNStartup();
    namesystem.removeAclEntries(src, aclSpec);
  }

  @Override // ClientProtocol
  public void removeDefaultAcl(String src) throws IOException {
    checkNNStartup();
    namesystem.removeDefaultAcl(src);
  }

  @Override // ClientProtocol
  public void removeAcl(String src) throws IOException {
    checkNNStartup();
    namesystem.removeAcl(src);
  }

  @Override // ClientProtocol
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    checkNNStartup();
    namesystem.setAcl(src, aclSpec);
  }

  @Override // ClientProtocol
  public AclStatus getAclStatus(String src) throws IOException {
    checkNNStartup();
    return namesystem.getAclStatus(src);
  }
  
  @Override // ClientProtocol
  public void createEncryptionZone(String src, String keyName)
    throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    final CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return;
    }
    boolean success = false;
    try {
      namesystem.createEncryptionZone(src, keyName, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override // ClientProtocol
  public EncryptionZone getEZForPath(String src)
    throws IOException {
    checkNNStartup();
    return namesystem.getEZForPath(src);
  }

  @Override // ClientProtocol
  public BatchedEntries<EncryptionZone> listEncryptionZones(
      long prevId) throws IOException {
    checkNNStartup();
    return namesystem.listEncryptionZones(prevId);
  }

  @Override // ClientProtocol
  public void reencryptEncryptionZone(final String zone,
      final ReencryptAction action) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    final CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return;
    }
    boolean success = false;
    try {
      namesystem.reencryptEncryptionZone(zone, action, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override // ClientProtocol
  public BatchedEntries<ZoneReencryptionStatus> listReencryptionStatus(
      final long prevId) throws IOException {
    checkNNStartup();
    return namesystem.listReencryptionStatus(prevId);
  }

  @Override // ClientProtocol
  public void setErasureCodingPolicy(String src, String ecPolicyName)
      throws IOException {
    checkNNStartup();
    final CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return;
    }
    boolean success = false;
    try {
      if (ecPolicyName == null) {
        ecPolicyName = defaultECPolicyName;
        LOG.debug("No policy name is specified, set the default policy name instead");
      }
      LOG.debug("Set erasure coding policy {} on {}", ecPolicyName, src);
      namesystem.setErasureCodingPolicy(src, ecPolicyName, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override // ClientProtocol
  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
    }
    boolean success = false;
    try {
      namesystem.setXAttr(src, xAttr, flag, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }
  
  @Override // ClientProtocol
  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs) 
      throws IOException {
    checkNNStartup();
    return namesystem.getXAttrs(src, xAttrs);
  }

  @Override // ClientProtocol
  public List<XAttr> listXAttrs(String src) throws IOException {
    checkNNStartup();
    return namesystem.listXAttrs(src);
  }
  
  @Override // ClientProtocol
  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
    }
    boolean success = false;
    try {
      namesystem.removeXAttr(src, xAttr, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  private void checkNNStartup() throws IOException {
    if (!this.nn.isStarted()) {
      String message = NameNode.composeNotStartedMessage(this.nn.getRole());
      throw new RetriableException(message);
    }
  }

  @Override // ClientProtocol
  public void checkAccess(String path, FsAction mode) throws IOException {
    checkNNStartup();
    namesystem.checkAccess(path, mode);
  }

  @Override // ClientProtocol
  public long getCurrentEditLogTxid() throws IOException {
    String operationName = "getCurrentEditLogTxid";
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.READ); // only active
    namesystem.checkSuperuserPrivilege(operationName);
    // if it's not yet open for write, we may be in the process of transitioning
    // from standby to active and may not yet know what the latest committed
    // txid is
    return namesystem.getEditLog().isOpenForWrite() ?
        namesystem.getEditLog().getLastWrittenTxId() : -1;
  }

  private static FSEditLogOp readOp(EditLogInputStream elis)
      throws IOException {
    try {
      return elis.readOp();
      // we can get the below two exceptions if a segment is deleted
      // (because we have accumulated too many edits) or (for the local journal/
      // no-QJM case only) if a in-progress segment is finalized under us ...
      // no need to throw an exception back to the client in this case
    } catch (FileNotFoundException e) {
      LOG.debug("Tried to read from deleted or moved edit log segment", e);
      return null;
    } catch (HttpGetFailedException e) {
      LOG.debug("Tried to read from deleted edit log segment", e);
      return null;
    }
  }

  @Override // ClientProtocol
  public EventBatchList getEditsFromTxid(long txid) throws IOException {
    String operationName = "getEditsFromTxid";
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.READ); // only active
    namesystem.checkSuperuserPrivilege(operationName);
    int maxEventsPerRPC = nn.getConf().getInt(
        DFSConfigKeys.DFS_NAMENODE_INOTIFY_MAX_EVENTS_PER_RPC_KEY,
        DFSConfigKeys.DFS_NAMENODE_INOTIFY_MAX_EVENTS_PER_RPC_DEFAULT);
    FSEditLog log = namesystem.getFSImage().getEditLog();
    long syncTxid = log.getSyncTxId();
    // If we haven't synced anything yet, we can only read finalized
    // segments since we can't reliably determine which txns in in-progress
    // segments have actually been committed (e.g. written to a quorum of JNs).
    // If we have synced txns, we can definitely read up to syncTxid since
    // syncTxid is only updated after a transaction is committed to all
    // journals. (In-progress segments written by old writers are already
    // discarded for us, so if we read any in-progress segments they are
    // guaranteed to have been written by this NameNode.)
    boolean readInProgress = syncTxid > 0;

    // doas the NN login user for the actual operations to get edits.
    // Notably this is necessary when polling from the remote edits via https.
    // We have validated the client is a superuser from the NN RPC, so this
    // running as the login user here is safe.
    EventBatchList ret = SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<EventBatchList>() {
          @Override
          public EventBatchList run() throws IOException {
            return getEventBatchList(syncTxid, txid, log, readInProgress,
                maxEventsPerRPC);
          }
        });
    return ret;
  }

  private EventBatchList getEventBatchList(long syncTxid, long txid,
      FSEditLog log, boolean readInProgress, int maxEventsPerRPC)
      throws IOException {
    List<EventBatch> batches = Lists.newArrayList();
    int totalEvents = 0;
    long maxSeenTxid = -1;
    long firstSeenTxid = -1;

    if (syncTxid > 0 && txid > syncTxid) {
      // we can't read past syncTxid, so there's no point in going any further
      return new EventBatchList(batches, firstSeenTxid, maxSeenTxid, syncTxid);
    }

    Collection<EditLogInputStream> streams = null;
    try {
      streams = log.selectInputStreams(txid, 0, null, readInProgress);
    } catch (IllegalStateException e) { // can happen if we have
      // transitioned out of active and haven't yet transitioned to standby
      // and are using QJM -- the edit log will be closed and this exception
      // will result
      LOG.info("NN is transitioning from active to standby and FSEditLog " +
          "is closed -- could not read edits");
      return new EventBatchList(batches, firstSeenTxid, maxSeenTxid, syncTxid);
    }

    boolean breakOuter = false;
    for (EditLogInputStream elis : streams) {
      // our assumption in this code is the EditLogInputStreams are ordered by
      // starting txid
      try {
        FSEditLogOp op = null;
        while ((op = readOp(elis)) != null) {
          // break out of here in the unlikely event that syncTxid is so
          // out of date that its segment has already been deleted, so the first
          // txid we get is greater than syncTxid
          if (syncTxid > 0 && op.getTransactionId() > syncTxid) {
            breakOuter = true;
            break;
          }

          EventBatch eventBatch = InotifyFSEditLogOpTranslator.translate(op);
          if (eventBatch != null) {
            batches.add(eventBatch);
            totalEvents += eventBatch.getEvents().length;
          }
          if (op.getTransactionId() > maxSeenTxid) {
            maxSeenTxid = op.getTransactionId();
          }
          if (firstSeenTxid == -1) {
            firstSeenTxid = op.getTransactionId();
          }
          if (totalEvents >= maxEventsPerRPC || (syncTxid > 0 &&
              op.getTransactionId() == syncTxid)) {
            // we're done
            breakOuter = true;
            break;
          }
        }
      } finally {
        elis.close();
      }
      if (breakOuter) {
        break;
      }
    }

    return new EventBatchList(batches, firstSeenTxid, maxSeenTxid, syncTxid);
  }

  @Override // ClientProtocol
  public ErasureCodingPolicyInfo[] getErasureCodingPolicies()
      throws IOException {
    checkNNStartup();
    return namesystem.getErasureCodingPolicies();
  }

  @Override // ClientProtocol
  public Map<String, String> getErasureCodingCodecs() throws IOException {
    checkNNStartup();
    return namesystem.getErasureCodingCodecs();
  }

  @Override // ClientProtocol
  public ErasureCodingPolicy getErasureCodingPolicy(String src) throws IOException {
    checkNNStartup();
    return namesystem.getErasureCodingPolicy(src);
  }

  @Override // ClientProtocol
  public void unsetErasureCodingPolicy(String src) throws IOException {
    checkNNStartup();
    final CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return;
    }
    boolean success = false;
    try {
      LOG.debug("Unset erasure coding policy on {}", src);
      namesystem.unsetErasureCodingPolicy(src, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override
  public ECTopologyVerifierResult getECTopologyResultForPolicies(
      String... policyNames) throws IOException {
    return namesystem.getECTopologyResultForPolicies(policyNames);
  }

  @Override
  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies) throws IOException {
    String operationName = "addErasureCodingPolicies";
    checkNNStartup();
    namesystem.checkSuperuserPrivilege(operationName);
    final CacheEntryWithPayload cacheEntry = getCacheEntryWithPayload(null);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return (AddErasureCodingPolicyResponse[]) cacheEntry.getPayload();
    }
    boolean success = false;
    AddErasureCodingPolicyResponse[] responses =
        new AddErasureCodingPolicyResponse[0];
    try {
      responses =
          namesystem.addErasureCodingPolicies(policies, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success, responses);
    }
    return responses;
  }

  @Override
  public void removeErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    String operationName = "removeErasureCodingPolicy";
    checkNNStartup();
    namesystem.checkSuperuserPrivilege(operationName);
    final CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return;
    }
    boolean success = false;
    try {
      namesystem.removeErasureCodingPolicy(ecPolicyName, cacheEntry != null);
      success = true;
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override // ClientProtocol
  public void enableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    String operationName = "enableErasureCodingPolicy";
    checkNNStartup();
    namesystem.checkSuperuserPrivilege(operationName);
    final CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return;
    }
    boolean success = false;
    try {
      success = namesystem.enableErasureCodingPolicy(ecPolicyName,
          cacheEntry != null);
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override // ClientProtocol
  public void disableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    String operationName = "disableErasureCodingPolicy";
    checkNNStartup();
    namesystem.checkSuperuserPrivilege(operationName);
    final CacheEntry cacheEntry = getCacheEntry();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return;
    }
    boolean success = false;
    try {
      success = namesystem.disableErasureCodingPolicy(ecPolicyName,
          cacheEntry != null);
    } finally {
      RetryCache.setState(cacheEntry, success);
    }
  }

  @Override // ReconfigurationProtocol
  public void startReconfiguration() throws IOException {
    checkNNStartup();
    String operationName = "startNamenodeReconfiguration";
    namesystem.checkSuperuserPrivilege(operationName);
    nn.startReconfigurationTask();
    namesystem.logAuditEvent(true, operationName, null);
  }

  @Override // ReconfigurationProtocol
  public ReconfigurationTaskStatus getReconfigurationStatus()
      throws IOException {
    checkNNStartup();
    String operationName = "getNamenodeReconfigurationStatus";
    namesystem.checkSuperuserPrivilege(operationName);
    ReconfigurationTaskStatus status = nn.getReconfigurationTaskStatus();
    namesystem.logAuditEvent(true, operationName, null);
    return status;
  }

  @Override // ReconfigurationProtocol
  public List<String> listReconfigurableProperties() throws IOException {
    checkNNStartup();
    String operationName = "listNamenodeReconfigurableProperties";
    namesystem.checkSuperuserPrivilege(operationName);
    List<String> result = Lists.newArrayList(nn.getReconfigurableProperties());
    namesystem.logAuditEvent(true, operationName, null);
    return result;
  }

  @Override
  public Long getNextSPSPath() throws IOException {
    checkNNStartup();
    String operationName = "getNextSPSPath";
    namesystem.checkSuperuserPrivilege(operationName);
    if (nn.isStandbyState()) {
      throw new StandbyException("Not supported by Standby Namenode.");
    }
    // Check that SPS is enabled externally
    StoragePolicySatisfyManager spsMgr =
        namesystem.getBlockManager().getSPSManager();
    StoragePolicySatisfierMode spsMode = (spsMgr != null ? spsMgr.getMode()
        : StoragePolicySatisfierMode.NONE);
    if (spsMode != StoragePolicySatisfierMode.EXTERNAL) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("SPS service mode is {}, so external SPS service is "
            + "not allowed to fetch the path Ids", spsMode);
      }
      throw new IOException("SPS service mode is " + spsMode + ", so "
          + "external SPS service is not allowed to fetch the path Ids");
    }
    return namesystem.getBlockManager().getSPSManager().getNextPathId();
  }
}
