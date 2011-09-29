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

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.MAX_PATH_DEPTH;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.MAX_PATH_LENGTH;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.UpgradeAction;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.NodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.tools.GetUserMappingsProtocol;

/**
 * This class is responsible for handling all of the RPC calls to the NameNode.
 * It is created, started, and stopped by {@link NameNode}.
 */
class NameNodeRpcServer implements NamenodeProtocols {
  
  private static final Log LOG = NameNode.LOG;
  private static final Log stateChangeLog = NameNode.stateChangeLog;
  
  // Dependencies from other parts of NN.
  private final FSNamesystem namesystem;
  protected final NameNode nn;
  private final NameNodeMetrics metrics;
  
  private final boolean serviceAuthEnabled;

  /** The RPC server that listens to requests from DataNodes */
  private final RPC.Server serviceRpcServer;
  private final InetSocketAddress serviceRPCAddress;
  
  /** The RPC server that listens to requests from clients */
  protected final RPC.Server server;
  protected final InetSocketAddress rpcAddress;

  public NameNodeRpcServer(Configuration conf, NameNode nn)
      throws IOException {
    this.nn = nn;
    this.namesystem = nn.getNamesystem();
    this.metrics = NameNode.getNameNodeMetrics();
    
    int handlerCount = 
      conf.getInt(DFS_DATANODE_HANDLER_COUNT_KEY, 
                  DFS_DATANODE_HANDLER_COUNT_DEFAULT);
    InetSocketAddress socAddr = nn.getRpcServerAddress(conf);

    InetSocketAddress dnSocketAddr = nn.getServiceRpcServerAddress(conf);
    if (dnSocketAddr != null) {
      int serviceHandlerCount =
        conf.getInt(DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY,
                    DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT);
      this.serviceRpcServer = RPC.getServer(NamenodeProtocols.class, this,
          dnSocketAddr.getHostName(), dnSocketAddr.getPort(), serviceHandlerCount,
          false, conf, namesystem.getDelegationTokenSecretManager());
      this.serviceRPCAddress = this.serviceRpcServer.getListenerAddress();
      nn.setRpcServiceServerAddress(conf, serviceRPCAddress);
    } else {
      serviceRpcServer = null;
      serviceRPCAddress = null;
    }
    // Add all the RPC protocols that the namenode implements
    this.server = RPC.getServer(ClientProtocol.class, this,
                                socAddr.getHostName(), socAddr.getPort(),
                                handlerCount, false, conf, 
                                namesystem.getDelegationTokenSecretManager());
    this.server.addProtocol(DatanodeProtocol.class, this);
    this.server.addProtocol(NamenodeProtocol.class, this);
    this.server.addProtocol(RefreshAuthorizationPolicyProtocol.class, this);
    this.server.addProtocol(RefreshUserMappingsProtocol.class, this);
    this.server.addProtocol(GetUserMappingsProtocol.class, this);
    

    // set service-level authorization security policy
    if (serviceAuthEnabled =
          conf.getBoolean(
            CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      this.server.refreshServiceAcl(conf, new HDFSPolicyProvider());
      if (this.serviceRpcServer != null) {
        this.serviceRpcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
      }
    }

    // The rpc-server port can be ephemeral... ensure we have the correct info
    this.rpcAddress = this.server.getListenerAddress(); 
    nn.setRpcServerAddress(conf, rpcAddress);
  }
  
  /**
   * Actually start serving requests.
   */
  void start() {
    server.start();  //start RPC server
    if (serviceRpcServer != null) {
      serviceRpcServer.start();      
    }
  }
  
  /**
   * Wait until the RPC server has shut down.
   */
  void join() throws InterruptedException {
    this.server.join();
  }
  
  void stop() {
    if(server != null) server.stop();
    if(serviceRpcServer != null) serviceRpcServer.stop();
  }
  
  InetSocketAddress getServiceRpcAddress() {
    return serviceRPCAddress;
  }

  InetSocketAddress getRpcAddress() {
    return rpcAddress;
  }
  
  @Override // VersionedProtocol
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }
  
  @Override
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
    } else if (protocol.equals(GetUserMappingsProtocol.class.getName())){
      return GetUserMappingsProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
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

    return namesystem.getBlockManager().getBlocks(datanode, size); 
  }

  @Override // NamenodeProtocol
  public ExportedBlockKeys getBlockKeys() throws IOException {
    return namesystem.getBlockManager().getBlockKeys();
  }

  @Override // NamenodeProtocol
  public void errorReport(NamenodeRegistration registration,
                          int errorCode, 
                          String msg) throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    verifyRequest(registration);
    LOG.info("Error report from " + registration + ": " + msg);
    if(errorCode == FATAL)
      namesystem.releaseBackupNode(registration);
  }

  @Override // NamenodeProtocol
  public NamenodeRegistration register(NamenodeRegistration registration)
  throws IOException {
    verifyVersion(registration.getVersion());
    NamenodeRegistration myRegistration = nn.setRegistration();
    namesystem.registerBackupNode(registration, myRegistration);
    return myRegistration;
  }

  @Override // NamenodeProtocol
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
  throws IOException {
    verifyRequest(registration);
    if(!nn.isRole(NamenodeRole.NAMENODE))
      throw new IOException("Only an ACTIVE node can invoke startCheckpoint.");
    return namesystem.startCheckpoint(registration, nn.setRegistration());
  }

  @Override // NamenodeProtocol
  public void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException {
    nn.checkOperation(OperationCategory.CHECKPOINT);
    namesystem.endCheckpoint(registration, sig);
  }

  @Override // ClientProtocol
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    return namesystem.getDelegationToken(renewer);
  }

  @Override // ClientProtocol
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    nn.checkOperation(OperationCategory.WRITE);
    return namesystem.renewDelegationToken(token);
  }

  @Override // ClientProtocol
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    namesystem.cancelDelegationToken(token);
  }
  
  @Override // ClientProtocol
  public LocatedBlocks getBlockLocations(String src, 
                                          long offset, 
                                          long length) 
      throws IOException {
    nn.checkOperation(OperationCategory.READ);
    metrics.incrGetBlockLocations();
    return namesystem.getBlockLocations(getClientMachine(), 
                                        src, offset, length);
  }
  
  @Override // ClientProtocol
  public FsServerDefaults getServerDefaults() throws IOException {
    return namesystem.getServerDefaults();
  }

  @Override // ClientProtocol
  public void create(String src, 
                     FsPermission masked,
                     String clientName, 
                     EnumSetWritable<CreateFlag> flag,
                     boolean createParent,
                     short replication,
                     long blockSize) throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.create: file "
                         +src+" for "+clientName+" at "+clientMachine);
    }
    if (!checkPathLength(src)) {
      throw new IOException("create: Pathname too long.  Limit "
          + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.startFile(src,
        new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
            null, masked),
        clientName, clientMachine, flag.get(), createParent, replication, blockSize);
    metrics.incrFilesCreated();
    metrics.incrCreateFileOps();
  }

  @Override // ClientProtocol
  public LocatedBlock append(String src, String clientName) 
      throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
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
    nn.checkOperation(OperationCategory.WRITE);
    String clientMachine = getClientMachine();
    return namesystem.recoverLease(src, clientName, clientMachine);
  }

  @Override // ClientProtocol
  public boolean setReplication(String src, short replication) 
    throws IOException {  
    nn.checkOperation(OperationCategory.WRITE);
    return namesystem.setReplication(src, replication);
  }
    
  @Override // ClientProtocol
  public void setPermission(String src, FsPermission permissions)
      throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    namesystem.setPermission(src, permissions);
  }

  @Override // ClientProtocol
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    namesystem.setOwner(src, username, groupname);
  }

  @Override // ClientProtocol
  public LocatedBlock addBlock(String src,
                               String clientName,
                               ExtendedBlock previous,
                               DatanodeInfo[] excludedNodes)
      throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*BLOCK* NameNode.addBlock: file "
          +src+" for "+clientName);
    }
    HashMap<Node, Node> excludedNodesSet = null;
    if (excludedNodes != null) {
      excludedNodesSet = new HashMap<Node, Node>(excludedNodes.length);
      for (Node node:excludedNodes) {
        excludedNodesSet.put(node, node);
      }
    }
    LocatedBlock locatedBlock = 
      namesystem.getAdditionalBlock(src, clientName, previous, excludedNodesSet);
    if (locatedBlock != null)
      metrics.incrAddBlockOps();
    return locatedBlock;
  }

  @Override // ClientProtocol
  public LocatedBlock getAdditionalDatanode(final String src, final ExtendedBlock blk,
      final DatanodeInfo[] existings, final DatanodeInfo[] excludes,
      final int numAdditionalNodes, final String clientName
      ) throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    if (LOG.isDebugEnabled()) {
      LOG.debug("getAdditionalDatanode: src=" + src
          + ", blk=" + blk
          + ", existings=" + Arrays.asList(existings)
          + ", excludes=" + Arrays.asList(excludes)
          + ", numAdditionalNodes=" + numAdditionalNodes
          + ", clientName=" + clientName);
    }

    metrics.incrGetAdditionalDatanodeOps();

    HashMap<Node, Node> excludeSet = null;
    if (excludes != null) {
      excludeSet = new HashMap<Node, Node>(excludes.length);
      for (Node node : excludes) {
        excludeSet.put(node, node);
      }
    }
    return namesystem.getAdditionalDatanode(src, blk,
        existings, excludeSet, numAdditionalNodes, clientName);
  }
  /**
   * The client needs to give up on the block.
   */
  @Override // ClientProtocol
  public void abandonBlock(ExtendedBlock b, String src, String holder)
      throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*BLOCK* NameNode.abandonBlock: "
          +b+" of file "+src);
    }
    if (!namesystem.abandonBlock(b, src, holder)) {
      throw new IOException("Cannot abandon block during write to " + src);
    }
  }

  @Override // ClientProtocol
  public boolean complete(String src, String clientName, ExtendedBlock last)
      throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.complete: "
          + src + " for " + clientName);
    }
    return namesystem.completeFile(src, clientName, last);
  }

  /**
   * The client has detected an error on the specified located blocks 
   * and is reporting them to the server.  For now, the namenode will 
   * mark the block as corrupt.  In the future we might 
   * check the blocks are actually corrupt. 
   */
  @Override // ClientProtocol, DatanodeProtocol
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    stateChangeLog.info("*DIR* NameNode.reportBadBlocks");
    for (int i = 0; i < blocks.length; i++) {
      ExtendedBlock blk = blocks[i].getBlock();
      DatanodeInfo[] nodes = blocks[i].getLocations();
      for (int j = 0; j < nodes.length; j++) {
        DatanodeInfo dn = nodes[j];
        namesystem.getBlockManager().findAndMarkBlockAsCorrupt(blk, dn);
      }
    }
  }

  @Override // ClientProtocol
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName)
      throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    return namesystem.updateBlockForPipeline(block, clientName);
  }


  @Override // ClientProtocol
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes)
      throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    namesystem.updatePipeline(clientName, oldBlock, newBlock, newNodes);
  }
  
  @Override // DatanodeProtocol
  public void commitBlockSynchronization(ExtendedBlock block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets)
      throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    namesystem.commitBlockSynchronization(block,
        newgenerationstamp, newlength, closeFile, deleteblock, newtargets);
  }
  
  @Override // ClientProtocol
  public long getPreferredBlockSize(String filename) 
      throws IOException {
    nn.checkOperation(OperationCategory.READ);
    return namesystem.getPreferredBlockSize(filename);
  }
    
  @Deprecated
  @Override // ClientProtocol
  public boolean rename(String src, String dst) throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
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
    nn.checkOperation(OperationCategory.WRITE);
    namesystem.concat(trg, src);
  }
  
  @Override // ClientProtocol
  public void rename(String src, String dst, Options.Rename... options)
      throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
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

  @Deprecated
  @Override // ClientProtocol
  public boolean delete(String src) throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    return delete(src, true);
  }

  @Override // ClientProtocol
  public boolean delete(String src, boolean recursive) throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
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
    nn.checkOperation(OperationCategory.WRITE);
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
    }
    if (!checkPathLength(src)) {
      throw new IOException("mkdirs: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    return namesystem.mkdirs(src,
        new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
            null, masked), createParent);
  }

  @Override // ClientProtocol
  public void renewLease(String clientName) throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    namesystem.renewLease(clientName);        
  }

  @Override // ClientProtocol
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws IOException {
    nn.checkOperation(OperationCategory.READ);
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
    nn.checkOperation(OperationCategory.READ);
    metrics.incrFileInfoOps();
    return namesystem.getFileInfo(src, true);
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException { 
    nn.checkOperation(OperationCategory.READ);
    metrics.incrFileInfoOps();
    return namesystem.getFileInfo(src, false);
  }
  
  @Override
  public long[] getStats() {
    return namesystem.getStats();
  }

  @Override // ClientProtocol
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    nn.checkOperation(OperationCategory.READ);
    DatanodeInfo results[] = namesystem.datanodeReport(type);
    if (results == null ) {
      throw new IOException("Cannot find datanode report");
    }
    return results;
  }
    
  @Override // ClientProtocol
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    // TODO:HA decide on OperationCategory for this
    return namesystem.setSafeMode(action);
  }
  @Override // ClientProtocol
  public boolean restoreFailedStorage(String arg) 
      throws AccessControlException {
    // TODO:HA decide on OperationCategory for this
    return namesystem.restoreFailedStorage(arg);
  }

  @Override // ClientProtocol
  public void saveNamespace() throws IOException {
    // TODO:HA decide on OperationCategory for this
    namesystem.saveNamespace();
  }

  @Override // ClientProtocol
  public void refreshNodes() throws IOException {
    // TODO:HA decide on OperationCategory for this
    namesystem.getBlockManager().getDatanodeManager().refreshNodes(
        new HdfsConfiguration());
  }

  @Override // NamenodeProtocol
  public long getTransactionID() {
    // TODO:HA decide on OperationCategory for this
    return namesystem.getEditLog().getSyncTxId();
  }

  @Override // NamenodeProtocol
  public CheckpointSignature rollEditLog() throws IOException {
    // TODO:HA decide on OperationCategory for this
    return namesystem.rollEditLog();
  }
  
  @Override // NamenodeProtocol
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
  throws IOException {
    // TODO:HA decide on OperationCategory for this
    return namesystem.getEditLog().getEditLogManifest(sinceTxId);
  }
    
  @Override // ClientProtocol
  public void finalizeUpgrade() throws IOException {
    // TODO:HA decide on OperationCategory for this
    namesystem.finalizeUpgrade();
  }

  @Override // ClientProtocol
  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action)
      throws IOException {
    // TODO:HA decide on OperationCategory for this
    return namesystem.distributedUpgradeProgress(action);
  }

  @Override // ClientProtocol
  public void metaSave(String filename) throws IOException {
    // TODO:HA decide on OperationCategory for this
    namesystem.metaSave(filename);
  }
  @Override // ClientProtocol
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    nn.checkOperation(OperationCategory.READ);
    Collection<FSNamesystem.CorruptFileBlockInfo> fbs =
      namesystem.listCorruptFileBlocks(path, cookie);
    
    String[] files = new String[fbs.size()];
    String lastCookie = "";
    int i = 0;
    for(FSNamesystem.CorruptFileBlockInfo fb: fbs) {
      files[i++] = fb.path;
      lastCookie = fb.block.getBlockName();
    }
    return new CorruptFileBlocks(files, lastCookie);
  }

  /**
   * Tell all datanodes to use a new, non-persistent bandwidth value for
   * dfs.datanode.balance.bandwidthPerSec.
   * @param bandwidth Blanacer bandwidth in bytes per second for all datanodes.
   * @throws IOException
   */
  @Override // ClientProtocol
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    // TODO:HA decide on OperationCategory for this
    namesystem.getBlockManager().getDatanodeManager().setBalancerBandwidth(bandwidth);
  }
  
  @Override // ClientProtocol
  public ContentSummary getContentSummary(String path) throws IOException {
    nn.checkOperation(OperationCategory.READ);
    return namesystem.getContentSummary(path);
  }

  @Override // ClientProtocol
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota) 
      throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    namesystem.setQuota(path, namespaceQuota, diskspaceQuota);
  }
  
  @Override // ClientProtocol
  public void fsync(String src, String clientName) throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    namesystem.fsync(src, clientName);
  }

  @Override // ClientProtocol
  public void setTimes(String src, long mtime, long atime) 
      throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
    namesystem.setTimes(src, mtime, atime);
  }

  @Override // ClientProtocol
  public void createSymlink(String target, String link, FsPermission dirPerms,
      boolean createParent) throws IOException {
    nn.checkOperation(OperationCategory.WRITE);
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
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    namesystem.createSymlink(target, link,
      new PermissionStatus(ugi.getShortUserName(), null, dirPerms), createParent);
  }

  @Override // ClientProtocol
  public String getLinkTarget(String path) throws IOException {
    nn.checkOperation(OperationCategory.READ);
    metrics.incrGetLinkTargetOps();
    /* Resolves the first symlink in the given path, returning a
     * new path consisting of the target of the symlink and any 
     * remaining path components from the original path.
     */
    try {
      HdfsFileStatus stat = namesystem.getFileInfo(path, false);
      if (stat != null) {
        // NB: getSymlink throws IOException if !stat.isSymlink() 
        return stat.getSymlink();
      }
    } catch (UnresolvedPathException e) {
      return e.getResolvedPath().toString();
    } catch (UnresolvedLinkException e) {
      // The NameNode should only throw an UnresolvedPathException
      throw new AssertionError("UnresolvedLinkException thrown");
    }
    return null;
  }


  @Override // DatanodeProtocol
  public DatanodeRegistration registerDatanode(DatanodeRegistration nodeReg)
      throws IOException {
    verifyVersion(nodeReg.getVersion());
    namesystem.registerDatanode(nodeReg);
      
    return nodeReg;
  }

  @Override // DatanodeProtocol
  public DatanodeCommand[] sendHeartbeat(DatanodeRegistration nodeReg,
      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
      int xmitsInProgress, int xceiverCount, int failedVolumes)
      throws IOException {
    verifyRequest(nodeReg);
    return namesystem.handleHeartbeat(nodeReg, capacity, dfsUsed, remaining,
        blockPoolUsed, xceiverCount, xmitsInProgress, failedVolumes);
  }

  @Override // DatanodeProtocol
  public DatanodeCommand blockReport(DatanodeRegistration nodeReg,
      String poolId, long[] blocks) throws IOException {
    verifyRequest(nodeReg);
    BlockListAsLongs blist = new BlockListAsLongs(blocks);
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*BLOCK* NameNode.blockReport: "
           + "from " + nodeReg.getName() + " " + blist.getNumberOfBlocks()
           + " blocks");
    }

    namesystem.getBlockManager().processReport(nodeReg, poolId, blist);
    if (nn.getFSImage().isUpgradeFinalized())
      return new DatanodeCommand.Finalize(poolId);
    return null;
  }

  @Override // DatanodeProtocol
  public void blockReceivedAndDeleted(DatanodeRegistration nodeReg, String poolId,
      ReceivedDeletedBlockInfo[] receivedAndDeletedBlocks) throws IOException {
    verifyRequest(nodeReg);
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*BLOCK* NameNode.blockReceivedAndDeleted: "
          +"from "+nodeReg.getName()+" "+receivedAndDeletedBlocks.length
          +" blocks.");
    }
    namesystem.getBlockManager().blockReceivedAndDeleted(
        nodeReg, poolId, receivedAndDeletedBlocks);
  }

  @Override // DatanodeProtocol
  public void errorReport(DatanodeRegistration nodeReg,
                          int errorCode, String msg) throws IOException { 
    String dnName = (nodeReg == null ? "unknown DataNode" : nodeReg.getName());

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
    return namesystem.getNamespaceInfo();
  }

  @Override // DatanodeProtocol
  public UpgradeCommand processUpgradeCommand(UpgradeCommand comm) throws IOException {
    return namesystem.processDistributedUpgradeCommand(comm);
  }

  /** 
   * Verify request.
   * 
   * Verifies correctness of the datanode version, registration ID, and 
   * if the datanode does not need to be shutdown.
   * 
   * @param nodeReg data node registration
   * @throws IOException
   */
  void verifyRequest(NodeRegistration nodeReg) throws IOException {
    verifyVersion(nodeReg.getVersion());
    if (!namesystem.getRegistrationID().equals(nodeReg.getRegistrationID())) {
      LOG.warn("Invalid registrationID - expected: "
          + namesystem.getRegistrationID() + " received: "
          + nodeReg.getRegistrationID());
      throw new UnregisteredNodeException(nodeReg);
    }
  }

  @Override // RefreshAuthorizationPolicyProtocol
  public void refreshServiceAcl() throws IOException {
    if (!serviceAuthEnabled) {
      throw new AuthorizationException("Service Level Authorization not enabled!");
    }

    this.server.refreshServiceAcl(new Configuration(), new HDFSPolicyProvider());
    if (this.serviceRpcServer != null) {
      this.serviceRpcServer.refreshServiceAcl(new Configuration(), new HDFSPolicyProvider());
    }
  }

  @Override // RefreshAuthorizationPolicyProtocol
  public void refreshUserToGroupsMappings() throws IOException {
    LOG.info("Refreshing all user-to-groups mappings. Requested by user: " + 
             UserGroupInformation.getCurrentUser().getShortUserName());
    Groups.getUserToGroupsMappingService().refresh();
  }

  @Override // RefreshAuthorizationPolicyProtocol
  public void refreshSuperUserGroupsConfiguration() {
    LOG.info("Refreshing SuperUser proxy group mapping list ");

    ProxyUsers.refreshSuperUserGroupsConfiguration();
  }
  
  @Override // GetUserMappingsProtocol
  public String[] getGroupsForUser(String user) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting groups for user " + user);
    }
    return UserGroupInformation.createRemoteUser(user).getGroupNames();
  }

  @Override // HAServiceProtocol
  public synchronized void monitorHealth() throws HealthCheckFailedException {
    nn.monitorHealth();
  }
  
  @Override // HAServiceProtocol
  public synchronized void transitionToActive() throws ServiceFailedException {
    nn.transitionToActive();
  }
  
  @Override // HAServiceProtocol
  public synchronized void transitionToStandby() throws ServiceFailedException {
    nn.transitionToStandby();
  }
  
  /**
   * Verify version.
   * 
   * @param version
   * @throws IOException
   */
  void verifyVersion(int version) throws IOException {
    if (version != HdfsConstants.LAYOUT_VERSION)
      throw new IncorrectVersionException(version, "data node");
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
}
