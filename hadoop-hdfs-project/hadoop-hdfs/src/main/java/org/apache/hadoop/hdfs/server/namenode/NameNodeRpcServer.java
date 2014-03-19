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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.MAX_PATH_DEPTH;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.MAX_PATH_LENGTH;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
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
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceProtocolService;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FSLimitException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.NamenodeProtocolService;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.FinalizeCommand;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.NodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
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
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetUserMappingsProtocolService;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;

/**
 * This class is responsible for handling all of the RPC calls to the NameNode.
 * It is created, started, and stopped by {@link NameNode}.
 */
class NameNodeRpcServer implements NamenodeProtocols {
  
  private static final Log LOG = NameNode.LOG;
  private static final Log stateChangeLog = NameNode.stateChangeLog;
  private static final Log blockStateChangeLog = NameNode.blockStateChangeLog;
  
  // Dependencies from other parts of NN.
  protected final FSNamesystem namesystem;
  protected final NameNode nn;
  private final NameNodeMetrics metrics;
  
  private final boolean serviceAuthEnabled;

  /** The RPC server that listens to requests from DataNodes */
  private final RPC.Server serviceRpcServer;
  private final InetSocketAddress serviceRPCAddress;
  
  /** The RPC server that listens to requests from clients */
  protected final RPC.Server clientRpcServer;
  protected final InetSocketAddress clientRpcAddress;
  
  private final String minimumDataNodeVersion;

  public NameNodeRpcServer(Configuration conf, NameNode nn)
      throws IOException {
    this.nn = nn;
    this.namesystem = nn.getNamesystem();
    this.metrics = NameNode.getNameNodeMetrics();
    
    int handlerCount = 
      conf.getInt(DFS_NAMENODE_HANDLER_COUNT_KEY, 
                  DFS_NAMENODE_HANDLER_COUNT_DEFAULT);

    RPC.setProtocolEngine(conf, ClientNamenodeProtocolPB.class,
        ProtobufRpcEngine.class);

    ClientNamenodeProtocolServerSideTranslatorPB 
       clientProtocolServerTranslator = 
         new ClientNamenodeProtocolServerSideTranslatorPB(this);
     BlockingService clientNNPbService = ClientNamenodeProtocol.
         newReflectiveBlockingService(clientProtocolServerTranslator);
    
    DatanodeProtocolServerSideTranslatorPB dnProtoPbTranslator = 
        new DatanodeProtocolServerSideTranslatorPB(this);
    BlockingService dnProtoPbService = DatanodeProtocolService
        .newReflectiveBlockingService(dnProtoPbTranslator);

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

    GetUserMappingsProtocolServerSideTranslatorPB getUserMappingXlator = 
        new GetUserMappingsProtocolServerSideTranslatorPB(this);
    BlockingService getUserMappingService = GetUserMappingsProtocolService
        .newReflectiveBlockingService(getUserMappingXlator);
    
    HAServiceProtocolServerSideTranslatorPB haServiceProtocolXlator = 
        new HAServiceProtocolServerSideTranslatorPB(this);
    BlockingService haPbService = HAServiceProtocolService
        .newReflectiveBlockingService(haServiceProtocolXlator);
    
    WritableRpcEngine.ensureInitialized();

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
      this.serviceRpcServer = new RPC.Builder(conf)
          .setProtocol(
              org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB.class)
          .setInstance(clientNNPbService)
          .setBindAddress(bindHost)
          .setPort(serviceRpcAddr.getPort()).setNumHandlers(serviceHandlerCount)
          .setVerbose(false)
          .setSecretManager(namesystem.getDelegationTokenSecretManager())
          .build();

      // Add all the RPC protocols that the namenode implements
      DFSUtil.addPBProtocol(conf, HAServiceProtocolPB.class, haPbService,
          serviceRpcServer);
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
    InetSocketAddress rpcAddr = nn.getRpcServerAddress(conf);
    String bindHost = nn.getRpcServerBindHost(conf);
    if (bindHost == null) {
      bindHost = rpcAddr.getHostName();
    }
    LOG.info("RPC server is binding to " + bindHost + ":" + rpcAddr.getPort());

    this.clientRpcServer = new RPC.Builder(conf)
        .setProtocol(
            org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB.class)
        .setInstance(clientNNPbService).setBindAddress(bindHost)
        .setPort(rpcAddr.getPort()).setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(namesystem.getDelegationTokenSecretManager()).build();

    // Add all the RPC protocols that the namenode implements
    DFSUtil.addPBProtocol(conf, HAServiceProtocolPB.class, haPbService,
        clientRpcServer);
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
    }

    // The rpc-server port can be ephemeral... ensure we have the correct info
    InetSocketAddress listenAddr = clientRpcServer.getListenerAddress();
      clientRpcAddress = new InetSocketAddress(
          rpcAddr.getHostName(), listenAddr.getPort());
    nn.setRpcServerAddress(conf, clientRpcAddress);
    
    minimumDataNodeVersion = conf.get(
        DFSConfigKeys.DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_KEY,
        DFSConfigKeys.DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_DEFAULT);

    // Set terse exception whose stack trace won't be logged
    this.clientRpcServer.addTerseExceptions(SafeModeException.class,
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
        AclException.class,
        FSLimitException.PathComponentTooLongException.class,
        FSLimitException.MaxDirectoryItemsExceededException.class);
 }

  /** Allow access to the client RPC server for testing */
  @VisibleForTesting
  RPC.Server getClientRpcServer() {
    return clientRpcServer;
  }
  
  /**
   * Start client and service RPC servers.
   */
  void start() {
    clientRpcServer.start();
    if (serviceRpcServer != null) {
      serviceRpcServer.start();      
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
  }
  
  InetSocketAddress getServiceRpcAddress() {
    return serviceRPCAddress;
  }

  InetSocketAddress getRpcAddress() {
    return clientRpcAddress;
  }

  private static UserGroupInformation getRemoteUser() throws IOException {
    return NameNode.getRemoteUser();
  }


  /////////////////////////////////////////////////////
  // NamenodeProtocol
  /////////////////////////////////////////////////////
  @Override // NamenodeProtocol
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
  throws IOException {
    if(size <= 0) {
      throw new IllegalArgumentException(
        "Unexpected not positive size: "+size);
    }
    namesystem.checkSuperuserPrivilege();
    return namesystem.getBlockManager().getBlocks(datanode, size); 
  }

  @Override // NamenodeProtocol
  public ExportedBlockKeys getBlockKeys() throws IOException {
    namesystem.checkSuperuserPrivilege();
    return namesystem.getBlockManager().getBlockKeys();
  }

  @Override // NamenodeProtocol
  public void errorReport(NamenodeRegistration registration,
                          int errorCode, 
                          String msg) throws IOException {
    namesystem.checkOperation(OperationCategory.UNCHECKED);
    namesystem.checkSuperuserPrivilege();
    verifyRequest(registration);
    LOG.info("Error report from " + registration + ": " + msg);
    if (errorCode == FATAL) {
      namesystem.releaseBackupNode(registration);
    }
  }

  @Override // NamenodeProtocol
  public NamenodeRegistration registerSubordinateNamenode(
      NamenodeRegistration registration) throws IOException {
    namesystem.checkSuperuserPrivilege();
    verifyLayoutVersion(registration.getVersion());
    NamenodeRegistration myRegistration = nn.setRegistration();
    namesystem.registerBackupNode(registration, myRegistration);
    return myRegistration;
  }

  @Override // NamenodeProtocol
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
  throws IOException {
    namesystem.checkSuperuserPrivilege();
    verifyRequest(registration);
    if(!nn.isRole(NamenodeRole.NAMENODE))
      throw new IOException("Only an ACTIVE node can invoke startCheckpoint.");
    return namesystem.startCheckpoint(registration, nn.setRegistration());
  }

  @Override // NamenodeProtocol
  public void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException {
    namesystem.checkSuperuserPrivilege();
    namesystem.endCheckpoint(registration, sig);
  }

  @Override // ClientProtocol
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return namesystem.getDelegationToken(renewer);
  }

  @Override // ClientProtocol
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    return namesystem.renewDelegationToken(token);
  }

  @Override // ClientProtocol
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    namesystem.cancelDelegationToken(token);
  }
  
  @Override // ClientProtocol
  public LocatedBlocks getBlockLocations(String src, 
                                          long offset, 
                                          long length) 
      throws IOException {
    metrics.incrGetBlockLocations();
    return namesystem.getBlockLocations(getClientMachine(), 
                                        src, offset, length);
  }
  
  @Override // ClientProtocol
  public FsServerDefaults getServerDefaults() throws IOException {
    return namesystem.getServerDefaults();
  }

  @Override // ClientProtocol
  public HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag,
      boolean createParent, short replication, long blockSize)
      throws IOException {
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.create: file "
                         +src+" for "+clientName+" at "+clientMachine);
    }
    if (!checkPathLength(src)) {
      throw new IOException("create: Pathname too long.  Limit "
          + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    HdfsFileStatus fileStatus = namesystem.startFile(src, new PermissionStatus(
        getRemoteUser().getShortUserName(), null, masked),
        clientName, clientMachine, flag.get(), createParent, replication,
        blockSize);
    metrics.incrFilesCreated();
    metrics.incrCreateFileOps();
    return fileStatus;
  }

  @Override // ClientProtocol
  public LocatedBlock append(String src, String clientName) 
      throws IOException {
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.append: file "
          +src+" for "+clientName+" at "+clientMachine);
    }
    LocatedBlock info = namesystem.appendFile(src, clientName, clientMachine);
    metrics.incrFilesAppended();
    return info;
  }

  @Override // ClientProtocol
  public boolean recoverLease(String src, String clientName) throws IOException {
    String clientMachine = getClientMachine();
    return namesystem.recoverLease(src, clientName, clientMachine);
  }

  @Override // ClientProtocol
  public boolean setReplication(String src, short replication) 
    throws IOException {  
    return namesystem.setReplication(src, replication);
  }
    
  @Override // ClientProtocol
  public void setPermission(String src, FsPermission permissions)
      throws IOException {
    namesystem.setPermission(src, permissions);
  }

  @Override // ClientProtocol
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    namesystem.setOwner(src, username, groupname);
  }
  
  @Override
  public LocatedBlock addBlock(String src, String clientName,
      ExtendedBlock previous, DatanodeInfo[] excludedNodes, long fileId,
      String[] favoredNodes)
      throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*BLOCK* NameNode.addBlock: file " + src
          + " fileId=" + fileId + " for " + clientName);
    }
    Set<Node> excludedNodesSet = null;
    if (excludedNodes != null) {
      excludedNodesSet = new HashSet<Node>(excludedNodes.length);
      for (Node node : excludedNodes) {
        excludedNodesSet.add(node);
      }
    }
    List<String> favoredNodesList = (favoredNodes == null) ? null
        : Arrays.asList(favoredNodes);
    LocatedBlock locatedBlock = namesystem.getAdditionalBlock(src, fileId,
        clientName, previous, excludedNodesSet, favoredNodesList);
    if (locatedBlock != null)
      metrics.incrAddBlockOps();
    return locatedBlock;
  }

  @Override // ClientProtocol
  public LocatedBlock getAdditionalDatanode(final String src, final ExtendedBlock blk,
      final DatanodeInfo[] existings, final String[] existingStorageIDs,
      final DatanodeInfo[] excludes,
      final int numAdditionalNodes, final String clientName
      ) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("getAdditionalDatanode: src=" + src
          + ", blk=" + blk
          + ", existings=" + Arrays.asList(existings)
          + ", excludes=" + Arrays.asList(excludes)
          + ", numAdditionalNodes=" + numAdditionalNodes
          + ", clientName=" + clientName);
    }

    metrics.incrGetAdditionalDatanodeOps();

    Set<Node> excludeSet = null;
    if (excludes != null) {
      excludeSet = new HashSet<Node>(excludes.length);
      for (Node node : excludes) {
        excludeSet.add(node);
      }
    }
    return namesystem.getAdditionalDatanode(src, blk, existings,
        existingStorageIDs, excludeSet, numAdditionalNodes, clientName);
  }
  /**
   * The client needs to give up on the block.
   */
  @Override // ClientProtocol
  public void abandonBlock(ExtendedBlock b, String src, String holder)
      throws IOException {
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*BLOCK* NameNode.abandonBlock: "
          +b+" of file "+src);
    }
    if (!namesystem.abandonBlock(b, src, holder)) {
      throw new IOException("Cannot abandon block during write to " + src);
    }
  }

  @Override // ClientProtocol
  public boolean complete(String src, String clientName,
                          ExtendedBlock last,  long fileId)
      throws IOException {
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.complete: "
          + src + " fileId=" + fileId +" for " + clientName);
    }
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
    namesystem.reportBadBlocks(blocks);
  }

  @Override // ClientProtocol
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName)
      throws IOException {
    return namesystem.updateBlockForPipeline(block, clientName);
  }


  @Override // ClientProtocol
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs)
      throws IOException {
    namesystem.updatePipeline(clientName, oldBlock, newBlock, newNodes, newStorageIDs);
  }
  
  @Override // DatanodeProtocol
  public void commitBlockSynchronization(ExtendedBlock block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets,
      String[] newtargetstorages)
      throws IOException {
    namesystem.commitBlockSynchronization(block, newgenerationstamp,
        newlength, closeFile, deleteblock, newtargets, newtargetstorages);
  }
  
  @Override // ClientProtocol
  public long getPreferredBlockSize(String filename) 
      throws IOException {
    return namesystem.getPreferredBlockSize(filename);
  }
    
  @Deprecated
  @Override // ClientProtocol
  public boolean rename(String src, String dst) throws IOException {
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    }
    if (!checkPathLength(dst)) {
      throw new IOException("rename: Pathname too long.  Limit "
          + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    boolean ret = namesystem.renameTo(src, dst);
    if (ret) {
      metrics.incrFilesRenamed();
    }
    return ret;
  }
  
  @Override // ClientProtocol
  public void concat(String trg, String[] src) throws IOException {
    namesystem.concat(trg, src);
  }
  
  @Override // ClientProtocol
  public void rename2(String src, String dst, Options.Rename... options)
      throws IOException {
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    }
    if (!checkPathLength(dst)) {
      throw new IOException("rename: Pathname too long.  Limit "
          + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.renameTo(src, dst, options);
    metrics.incrFilesRenamed();
  }

  @Override // ClientProtocol
  public boolean delete(String src, boolean recursive) throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* Namenode.delete: src=" + src
          + ", recursive=" + recursive);
    }
    boolean ret = namesystem.delete(src, recursive);
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
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
    }
    if (!checkPathLength(src)) {
      throw new IOException("mkdirs: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    return namesystem.mkdirs(src,
        new PermissionStatus(getRemoteUser().getShortUserName(),
            null, masked), createParent);
  }

  @Override // ClientProtocol
  public void renewLease(String clientName) throws IOException {
    namesystem.renewLease(clientName);        
  }

  @Override // ClientProtocol
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws IOException {
    DirectoryListing files = namesystem.getListing(
        src, startAfter, needLocation);
    if (files != null) {
      metrics.incrGetListingOps();
      metrics.incrFilesInGetListingOps(files.getPartialListing().length);
    }
    return files;
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileInfo(String src)  throws IOException {
    metrics.incrFileInfoOps();
    return namesystem.getFileInfo(src, true);
  }
  
  @Override // ClientProtocol
  public boolean isFileClosed(String src) throws IOException{
    return namesystem.isFileClosed(src);
  }
  
  @Override // ClientProtocol
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException { 
    metrics.incrFileInfoOps();
    return namesystem.getFileInfo(src, false);
  }
  
  @Override // ClientProtocol
  public long[] getStats() throws IOException {
    namesystem.checkOperation(OperationCategory.READ);
    return namesystem.getStats();
  }

  @Override // ClientProtocol
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
  throws IOException {
    DatanodeInfo results[] = namesystem.datanodeReport(type);
    if (results == null ) {
      throw new IOException("Cannot find datanode report");
    }
    return results;
  }
    
  @Override // ClientProtocol
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
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
    return namesystem.restoreFailedStorage(arg);
  }

  @Override // ClientProtocol
  public void saveNamespace() throws IOException {
    namesystem.saveNamespace();
  }
  
  @Override // ClientProtocol
  public long rollEdits() throws AccessControlException, IOException {
    CheckpointSignature sig = namesystem.rollEditLog();
    return sig.getCurSegmentTxId();
  }

  @Override // ClientProtocol
  public void refreshNodes() throws IOException {
    namesystem.refreshNodes();
  }

  @Override // NamenodeProtocol
  public long getTransactionID() throws IOException {
    namesystem.checkOperation(OperationCategory.UNCHECKED);
    namesystem.checkSuperuserPrivilege();
    return namesystem.getFSImage().getLastAppliedOrWrittenTxId();
  }
  
  @Override // NamenodeProtocol
  public long getMostRecentCheckpointTxId() throws IOException {
    namesystem.checkOperation(OperationCategory.UNCHECKED);
    namesystem.checkSuperuserPrivilege();
    return namesystem.getFSImage().getMostRecentCheckpointTxId();
  }
  
  @Override // NamenodeProtocol
  public CheckpointSignature rollEditLog() throws IOException {
    namesystem.checkSuperuserPrivilege();
    return namesystem.rollEditLog();
  }
  
  @Override // NamenodeProtocol
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
  throws IOException {
    namesystem.checkOperation(OperationCategory.READ);
    namesystem.checkSuperuserPrivilege();
    return namesystem.getEditLog().getEditLogManifest(sinceTxId);
  }
    
  @Override // ClientProtocol
  public void finalizeUpgrade() throws IOException {
    namesystem.finalizeUpgrade();
  }

  @Override // ClientProtocol
  public RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action) throws IOException {
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
    namesystem.metaSave(filename);
  }

  @Override // ClientProtocol
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
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
    namesystem.setBalancerBandwidth(bandwidth);
  }
  
  @Override // ClientProtocol
  public ContentSummary getContentSummary(String path) throws IOException {
    return namesystem.getContentSummary(path);
  }

  @Override // ClientProtocol
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota) 
      throws IOException {
    namesystem.setQuota(path, namespaceQuota, diskspaceQuota);
  }
  
  @Override // ClientProtocol
  public void fsync(String src, String clientName, long lastBlockLength)
      throws IOException {
    namesystem.fsync(src, clientName, lastBlockLength);
  }

  @Override // ClientProtocol
  public void setTimes(String src, long mtime, long atime) 
      throws IOException {
    namesystem.setTimes(src, mtime, atime);
  }

  @Override // ClientProtocol
  public void createSymlink(String target, String link, FsPermission dirPerms,
      boolean createParent) throws IOException {
    metrics.incrCreateSymlinkOps();
    /* We enforce the MAX_PATH_LENGTH limit even though a symlink target 
     * URI may refer to a non-HDFS file system. 
     */
    if (!checkPathLength(link)) {
      throw new IOException("Symlink path exceeds " + MAX_PATH_LENGTH +
                            " character limit");
                            
    }
    if ("".equals(target)) {
      throw new IOException("Invalid symlink target");
    }
    final UserGroupInformation ugi = getRemoteUser();
    namesystem.createSymlink(target, link,
      new PermissionStatus(ugi.getShortUserName(), null, dirPerms), createParent);
  }

  @Override // ClientProtocol
  public String getLinkTarget(String path) throws IOException {
    metrics.incrGetLinkTargetOps();
    HdfsFileStatus stat = null;
    try {
      stat = namesystem.getFileInfo(path, false);
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
    return stat.getSymlink();
  }


  @Override // DatanodeProtocol
  public DatanodeRegistration registerDatanode(DatanodeRegistration nodeReg)
      throws IOException {
    verifySoftwareVersion(nodeReg);
    namesystem.registerDatanode(nodeReg);
    return nodeReg;
  }

  @Override // DatanodeProtocol
  public HeartbeatResponse sendHeartbeat(DatanodeRegistration nodeReg,
      StorageReport[] report, long dnCacheCapacity, long dnCacheUsed,
      int xmitsInProgress, int xceiverCount,
      int failedVolumes) throws IOException {
    verifyRequest(nodeReg);
    return namesystem.handleHeartbeat(nodeReg, report,
        dnCacheCapacity, dnCacheUsed, xceiverCount, xmitsInProgress,
        failedVolumes);
  }

  @Override // DatanodeProtocol
  public DatanodeCommand blockReport(DatanodeRegistration nodeReg,
      String poolId, StorageBlockReport[] reports) throws IOException {
    verifyRequest(nodeReg);
    if(blockStateChangeLog.isDebugEnabled()) {
      blockStateChangeLog.debug("*BLOCK* NameNode.blockReport: "
           + "from " + nodeReg + ", reports.length=" + reports.length);
    }
    final BlockManager bm = namesystem.getBlockManager(); 
    boolean hasStaleStorages = true;
    for(StorageBlockReport r : reports) {
      final BlockListAsLongs blocks = new BlockListAsLongs(r.getBlocks());
      hasStaleStorages = bm.processReport(nodeReg, r.getStorage(), poolId, blocks);
    }

    if (nn.getFSImage().isUpgradeFinalized() &&
        !nn.isStandbyState() &&
        !hasStaleStorages) {
      return new FinalizeCommand(poolId);
    }

    return null;
  }

  @Override
  public DatanodeCommand cacheReport(DatanodeRegistration nodeReg,
      String poolId, List<Long> blockIds) throws IOException {
    verifyRequest(nodeReg);
    if (blockStateChangeLog.isDebugEnabled()) {
      blockStateChangeLog.debug("*BLOCK* NameNode.cacheReport: "
           + "from " + nodeReg + " " + blockIds.size() + " blocks");
    }
    namesystem.getCacheManager().processCacheReport(nodeReg, blockIds);
    return null;
  }

  @Override // DatanodeProtocol
  public void blockReceivedAndDeleted(DatanodeRegistration nodeReg, String poolId,
      StorageReceivedDeletedBlocks[] receivedAndDeletedBlocks) throws IOException {
    verifyRequest(nodeReg);
    metrics.incrBlockReceivedAndDeletedOps();
    if(blockStateChangeLog.isDebugEnabled()) {
      blockStateChangeLog.debug("*BLOCK* NameNode.blockReceivedAndDeleted: "
          +"from "+nodeReg+" "+receivedAndDeletedBlocks.length
          +" blocks.");
    }
    for(StorageReceivedDeletedBlocks r : receivedAndDeletedBlocks) {
      namesystem.processIncrementalBlockReport(nodeReg, poolId, r);
    }
  }
  
  @Override // DatanodeProtocol
  public void errorReport(DatanodeRegistration nodeReg,
                          int errorCode, String msg) throws IOException { 
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
    namesystem.checkSuperuserPrivilege();
    return namesystem.getNamespaceInfo();
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
    if (!serviceAuthEnabled) {
      throw new AuthorizationException("Service Level Authorization not enabled!");
    }

    this.clientRpcServer.refreshServiceAcl(new Configuration(), new HDFSPolicyProvider());
    if (this.serviceRpcServer != null) {
      this.serviceRpcServer.refreshServiceAcl(new Configuration(), new HDFSPolicyProvider());
    }
  }

  @Override // RefreshAuthorizationPolicyProtocol
  public void refreshUserToGroupsMappings() throws IOException {
    LOG.info("Refreshing all user-to-groups mappings. Requested by user: " + 
             getRemoteUser().getShortUserName());
    Groups.getUserToGroupsMappingService().refresh();
  }

  @Override // RefreshAuthorizationPolicyProtocol
  public void refreshSuperUserGroupsConfiguration() {
    LOG.info("Refreshing SuperUser proxy group mapping list ");

    ProxyUsers.refreshSuperUserGroupsConfiguration();
  }

  @Override // RefreshCallQueueProtocol
  public void refreshCallQueue() {
    LOG.info("Refreshing call queue.");

    Configuration conf = new Configuration();
    clientRpcServer.refreshCallQueue(conf);
    if (this.serviceRpcServer != null) {
      serviceRpcServer.refreshCallQueue(conf);
    }
  }
  
  @Override // GetUserMappingsProtocol
  public String[] getGroupsForUser(String user) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting groups for user " + user);
    }
    return UserGroupInformation.createRemoteUser(user).getGroupNames();
  }

  @Override // HAServiceProtocol
  public synchronized void monitorHealth() 
      throws HealthCheckFailedException, AccessControlException {
    nn.monitorHealth();
  }
  
  @Override // HAServiceProtocol
  public synchronized void transitionToActive(StateChangeRequestInfo req) 
      throws ServiceFailedException, AccessControlException {
    nn.checkHaStateChange(req);
    nn.transitionToActive();
  }
  
  @Override // HAServiceProtocol
  public synchronized void transitionToStandby(StateChangeRequestInfo req) 
      throws ServiceFailedException, AccessControlException {
    nn.checkHaStateChange(req);
    nn.transitionToStandby();
  }

  @Override // HAServiceProtocol
  public synchronized HAServiceStatus getServiceStatus() 
      throws AccessControlException, ServiceFailedException {
    return nn.getServiceStatus();
  }

  /**
   * Verify version.
   * 
   * @param version
   * @throws IOException
   */
  void verifyLayoutVersion(int version) throws IOException {
    if (version != HdfsConstants.NAMENODE_LAYOUT_VERSION)
      throw new IncorrectVersionException(
          HdfsConstants.NAMENODE_LAYOUT_VERSION, version, "data node");
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
        LOG.warn(ive);
        throw ive;
      } else {
        LOG.info(messagePrefix +
            ". Note: This is normal during a rolling upgrade.");
      }
    }
  }

  private static String getClientMachine() {
    String clientMachine = NamenodeWebHdfsMethods.getRemoteAddress();
    if (clientMachine == null) { //not a web client
      clientMachine = Server.getRemoteAddress();
    }
    if (clientMachine == null) { //not a RPC client
      clientMachine = "";
    }
    return clientMachine;
  }

  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    return namesystem.getBlockManager().generateDataEncryptionKey();
  }

  @Override
  public String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    if (!checkPathLength(snapshotRoot)) {
      throw new IOException("createSnapshot: Pathname too long.  Limit "
          + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    metrics.incrCreateSnapshotOps();
    return namesystem.createSnapshot(snapshotRoot, snapshotName);
  }
  
  @Override
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    metrics.incrDeleteSnapshotOps();
    namesystem.deleteSnapshot(snapshotRoot, snapshotName);
  }

  @Override
  // Client Protocol
  public void allowSnapshot(String snapshotRoot) throws IOException {
    metrics.incrAllowSnapshotOps();
    namesystem.allowSnapshot(snapshotRoot);
  }

  @Override
  // Client Protocol
  public void disallowSnapshot(String snapshot) throws IOException {
    metrics.incrDisAllowSnapshotOps();
    namesystem.disallowSnapshot(snapshot);
  }

  @Override
  public void renameSnapshot(String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException {
    if (snapshotNewName == null || snapshotNewName.isEmpty()) {
      throw new IOException("The new snapshot name is null or empty.");
    }
    metrics.incrRenameSnapshotOps();
    namesystem.renameSnapshot(snapshotRoot, snapshotOldName, snapshotNewName);
  }

  @Override // Client Protocol
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    SnapshottableDirectoryStatus[] status = namesystem
        .getSnapshottableDirListing();
    metrics.incrListSnapshottableDirOps();
    return status;
  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
      String earlierSnapshotName, String laterSnapshotName) throws IOException {
    SnapshotDiffReport report = namesystem.getSnapshotDiffReport(snapshotRoot,
        earlierSnapshotName, laterSnapshotName);
    metrics.incrSnapshotDiffReportOps();
    return report;
  }

  @Override
  public long addCacheDirective(
      CacheDirectiveInfo path, EnumSet<CacheFlag> flags) throws IOException {
    return namesystem.addCacheDirective(path, flags);
  }

  @Override
  public void modifyCacheDirective(
      CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) throws IOException {
    namesystem.modifyCacheDirective(directive, flags);
  }

  @Override
  public void removeCacheDirective(long id) throws IOException {
    namesystem.removeCacheDirective(id);
  }

  @Override
  public BatchedEntries<CacheDirectiveEntry> listCacheDirectives(long prevId,
      CacheDirectiveInfo filter) throws IOException {
    if (filter == null) {
      filter = new CacheDirectiveInfo.Builder().build();
    }
    return namesystem.listCacheDirectives(prevId, filter);
  }

  @Override
  public void addCachePool(CachePoolInfo info) throws IOException {
    namesystem.addCachePool(info);
  }

  @Override
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    namesystem.modifyCachePool(info);
  }

  @Override
  public void removeCachePool(String cachePoolName) throws IOException {
    namesystem.removeCachePool(cachePoolName);
  }

  @Override
  public BatchedEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    return namesystem.listCachePools(prevKey != null ? prevKey : "");
  }

  @Override
  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    namesystem.modifyAclEntries(src, aclSpec);
  }

  @Override
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    namesystem.removeAclEntries(src, aclSpec);
  }

  @Override
  public void removeDefaultAcl(String src) throws IOException {
    namesystem.removeDefaultAcl(src);
  }

  @Override
  public void removeAcl(String src) throws IOException {
    namesystem.removeAcl(src);
  }

  @Override
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    namesystem.setAcl(src, aclSpec);
  }

  @Override
  public AclStatus getAclStatus(String src) throws IOException {
    return namesystem.getAclStatus(src);
  }
}

