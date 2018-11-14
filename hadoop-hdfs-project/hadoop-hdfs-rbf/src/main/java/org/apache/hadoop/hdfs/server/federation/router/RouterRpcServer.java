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

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_QUEUE_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_QUEUE_SIZE_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_READER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_READER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_READER_QUEUE_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_READER_QUEUE_SIZE_KEY;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.NamenodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolServerSideTranslatorPB;
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
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
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
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;

/**
 * This class is responsible for handling all of the RPC calls to the It is
 * created, started, and stopped by {@link Router}. It implements the
 * {@link ClientProtocol} to mimic a
 * {@link org.apache.hadoop.hdfs.server.namenode.NameNode NameNode} and proxies
 * the requests to the active
 * {@link org.apache.hadoop.hdfs.server.namenode.NameNode NameNode}.
 */
public class RouterRpcServer extends AbstractService
    implements ClientProtocol, NamenodeProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterRpcServer.class);


  /** Configuration for the RPC server. */
  private Configuration conf;

  /** Router using this RPC server. */
  private final Router router;

  /** The RPC server that listens to requests from clients. */
  private final Server rpcServer;
  /** The address for this RPC server. */
  private final InetSocketAddress rpcAddress;

  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;

  /** Monitor metrics for the RPC calls. */
  private final RouterRpcMonitor rpcMonitor;


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

  /**
   * Construct a router RPC server.
   *
   * @param configuration HDFS Configuration.
   * @param router A router using this RPC server.
   * @param nnResolver The NN resolver instance to determine active NNs in HA.
   * @param fileResolver File resolver to resolve file paths to subclusters.
   * @throws IOException If the RPC server could not be created.
   */
  public RouterRpcServer(Configuration configuration, Router router,
      ActiveNamenodeResolver nnResolver, FileSubclusterResolver fileResolver)
          throws IOException {
    super(RouterRpcServer.class.getName());

    this.conf = configuration;
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

    // Override Hadoop Common IPC setting
    int readerQueueSize = this.conf.getInt(DFS_ROUTER_READER_QUEUE_SIZE_KEY,
        DFS_ROUTER_READER_QUEUE_SIZE_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeys.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY,
        readerQueueSize);

    RPC.setProtocolEngine(this.conf, ClientNamenodeProtocolPB.class,
        ProtobufRpcEngine.class);

    ClientNamenodeProtocolServerSideTranslatorPB
        clientProtocolServerTranslator =
            new ClientNamenodeProtocolServerSideTranslatorPB(this);
    BlockingService clientNNPbService = ClientNamenodeProtocol
        .newReflectiveBlockingService(clientProtocolServerTranslator);

    NamenodeProtocolServerSideTranslatorPB namenodeProtocolXlator =
        new NamenodeProtocolServerSideTranslatorPB(this);
    BlockingService nnPbService = NamenodeProtocolService
        .newReflectiveBlockingService(namenodeProtocolXlator);

    InetSocketAddress confRpcAddress = conf.getSocketAddr(
        RBFConfigKeys.DFS_ROUTER_RPC_BIND_HOST_KEY,
        RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY,
        RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_DEFAULT,
        RBFConfigKeys.DFS_ROUTER_RPC_PORT_DEFAULT);
    LOG.info("RPC server binding to {} with {} handlers for Router {}",
        confRpcAddress, handlerCount, this.router.getRouterId());

    this.rpcServer = new RPC.Builder(this.conf)
        .setProtocol(ClientNamenodeProtocolPB.class)
        .setInstance(clientNNPbService)
        .setBindAddress(confRpcAddress.getHostName())
        .setPort(confRpcAddress.getPort())
        .setNumHandlers(handlerCount)
        .setnumReaders(readerCount)
        .setQueueSizePerHandler(handlerQueueSize)
        .setVerbose(false)
        .build();

    // Add all the RPC protocols that the Router implements
    DFSUtil.addPBProtocol(
        conf, NamenodeProtocolPB.class, nnPbService, this.rpcServer);

    // We don't want the server to log the full stack trace for some exceptions
    this.rpcServer.addTerseExceptions(
        RemoteException.class,
        SafeModeException.class,
        FileNotFoundException.class,
        FileAlreadyExistsException.class,
        AccessControlException.class,
        LeaseExpiredException.class,
        NotReplicatedYetException.class,
        IOException.class);

    this.rpcServer.addSuppressedLoggingExceptions(
        StandbyException.class);

    // The RPC-server port can be ephemeral... ensure we have the correct info
    InetSocketAddress listenAddress = this.rpcServer.getListenerAddress();
    this.rpcAddress = new InetSocketAddress(
        confRpcAddress.getHostName(), listenAddress.getPort());

    // Create metrics monitor
    Class<? extends RouterRpcMonitor> rpcMonitorClass = this.conf.getClass(
        RBFConfigKeys.DFS_ROUTER_METRICS_CLASS,
        RBFConfigKeys.DFS_ROUTER_METRICS_CLASS_DEFAULT,
        RouterRpcMonitor.class);
    this.rpcMonitor = ReflectionUtils.newInstance(rpcMonitorClass, conf);

    // Create the client
    this.rpcClient = new RouterRpcClient(this.conf, this.router,
        this.namenodeResolver, this.rpcMonitor);

    // Initialize modules
    this.quotaCall = new Quota(this.router, this);
    this.nnProto = new RouterNamenodeProtocol(this);
    this.clientProto = new RouterClientProtocol(conf, this);
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    this.conf = configuration;

    if (this.rpcMonitor == null) {
      LOG.error("Cannot instantiate Router RPC metrics class");
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
    super.serviceStop();
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
   * @throws SafeModeException If the Router is in safe mode and cannot serve
   *                           client requests.
   * @throws UnsupportedOperationException If the operation is not supported.
   */
  void checkOperation(OperationCategory op, boolean supported)
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
   * @throws SafeModeException If the Router is in safe mode and cannot serve
   *                           client requests.
   */
  void checkOperation(OperationCategory op)
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

    // We allow unchecked and read operations
    if (op == OperationCategory.UNCHECKED || op == OperationCategory.READ) {
      return;
    }

    RouterSafemodeService safemodeService = router.getSafemodeService();
    if (safemodeService != null && safemodeService.isInSafeMode()) {
      // Throw standby exception, router is not available
      if (rpcMonitor != null) {
        rpcMonitor.routerFailureSafemode();
      }
      throw new StandbyException("Router " + router.getRouterId() +
          " is in safe mode and cannot handle " + op + " requests");
    }
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
      CryptoProtocolVersion[] supportedVersions, String ecPolicyName)
      throws IOException {
    return clientProto.create(src, masked, clientName, flag, createParent,
        replication, blockSize, supportedVersions, ecPolicyName);
  }

  /**
   * Get the location to create a file. It checks if the file already existed
   * in one of the locations.
   *
   * @param src Path of the file to check.
   * @return The remote location for this file.
   * @throws IOException If the file has no creation location.
   */
  RemoteLocation getCreateLocation(final String src)
      throws IOException {

    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    if (locations == null || locations.isEmpty()) {
      throw new IOException("Cannot get locations to create " + src);
    }

    RemoteLocation createLocation = locations.get(0);
    if (locations.size() > 1) {
      try {
        // Check if this file already exists in other subclusters
        LocatedBlocks existingLocation = getBlockLocations(src, 0, 1);
        if (existingLocation != null) {
          // Forward to the existing location and let the NN handle the error
          LocatedBlock existingLocationLastLocatedBlock =
              existingLocation.getLastLocatedBlock();
          if (existingLocationLastLocatedBlock == null) {
            // The block has no blocks yet, check for the meta data
            for (RemoteLocation location : locations) {
              RemoteMethod method = new RemoteMethod("getFileInfo",
                  new Class<?>[] {String.class}, new RemoteParam());
              if (rpcClient.invokeSingle(location, method) != null) {
                createLocation = location;
                break;
              }
            }
          } else {
            ExtendedBlock existingLocationLastBlock =
                existingLocationLastLocatedBlock.getBlock();
            String blockPoolId = existingLocationLastBlock.getBlockPoolId();
            createLocation = getLocationForPath(src, true, blockPoolId);
          }
        }
      } catch (FileNotFoundException fne) {
        // Ignore if the file is not found
      }
    }
    return createLocation;
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
  public void renewLease(String clientName) throws IOException {
    clientProto.renewLease(clientName);
  }

  @Override // ClientProtocol
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws IOException {
    return clientProto.getListing(src, startAfter, needLocation);
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
    for (Entry<FederationNamespaceInfo, DatanodeInfo[]> entry :
        results.entrySet()) {
      FederationNamespaceInfo ns = entry.getKey();
      DatanodeInfo[] result = entry.getValue();
      for (DatanodeInfo node : result) {
        String nodeId = node.getXferAddr();
        if (!datanodesMap.containsKey(nodeId)) {
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
    // Map -> Array
    Collection<DatanodeInfo> datanodes = datanodesMap.values();
    return toArray(datanodes, DatanodeInfo.class);
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
   * @return nsId -> datanode list.
   * @throws IOException
   */
  public Map<String, DatanodeStorageReport[]> getDatanodeStorageReportMap(
      DatanodeReportType type) throws IOException {

    Map<String, DatanodeStorageReport[]> ret = new LinkedHashMap<>();
    RemoteMethod method = new RemoteMethod("getDatanodeStorageReport",
        new Class<?>[] {DatanodeReportType.class}, type);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, DatanodeStorageReport[]> results =
        rpcClient.invokeConcurrent(
            nss, method, true, false, DatanodeStorageReport[].class);
    for (Entry<FederationNamespaceInfo, DatanodeStorageReport[]> entry :
        results.entrySet()) {
      FederationNamespaceInfo ns = entry.getKey();
      String nsId = ns.getNameserviceId();
      DatanodeStorageReport[] result = entry.getValue();
      ret.put(nsId, result);
    }
    return ret;
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

  @Override // Client Protocol
  public void allowSnapshot(String snapshotRoot) throws IOException {
    clientProto.allowSnapshot(snapshotRoot);
  }

  @Override // Client Protocol
  public void disallowSnapshot(String snapshot) throws IOException {
    clientProto.disallowSnapshot(snapshot);
  }

  @Override // ClientProtocol
  public void renameSnapshot(String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException {
    clientProto.renameSnapshot(snapshotRoot, snapshotOldName, snapshotNewName);
  }

  @Override // Client Protocol
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    return clientProto.getSnapshottableDirListing();
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

  @Override // ClienProtocol
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

  @Override // NamenodeProtocol
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size,
      long minBlockSize) throws IOException {
    return nnProto.getBlocks(datanode, size, minBlockSize);
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

  /**
   * Locate the location with the matching block pool id.
   *
   * @param path Path to check.
   * @param failIfLocked Fail the request if locked (top mount point).
   * @param blockPoolId The block pool ID of the namespace to search for.
   * @return Prioritized list of locations in the federated cluster.
   * @throws IOException if the location for this path cannot be determined.
   */
  protected RemoteLocation getLocationForPath(
      String path, boolean failIfLocked, String blockPoolId)
          throws IOException {

    final List<RemoteLocation> locations =
        getLocationsForPath(path, failIfLocked);

    String nameserviceId = null;
    Set<FederationNamespaceInfo> namespaces =
        this.namenodeResolver.getNamespaces();
    for (FederationNamespaceInfo namespace : namespaces) {
      if (namespace.getBlockPoolId().equals(blockPoolId)) {
        nameserviceId = namespace.getNameserviceId();
        break;
      }
    }
    if (nameserviceId != null) {
      for (RemoteLocation location : locations) {
        if (location.getNameserviceId().equals(nameserviceId)) {
          return location;
        }
      }
    }
    throw new IOException(
        "Cannot locate a nameservice for block pool " + blockPoolId);
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
  protected List<RemoteLocation> getLocationsForPath(String path,
      boolean failIfLocked) throws IOException {
    return getLocationsForPath(path, failIfLocked, true);
  }

  /**
   * Get the possible locations of a path in the federated cluster.
   *
   * @param path Path to check.
   * @param failIfLocked Fail the request if locked (top mount point).
   * @param needQuotaVerify If need to do the quota verification.
   * @return Prioritized list of locations in the federated cluster.
   * @throws IOException If the location for this path cannot be determined.
   */
  protected List<RemoteLocation> getLocationsForPath(String path,
      boolean failIfLocked, boolean needQuotaVerify) throws IOException {
    try {
      // Check the location for this path
      final PathLocation location =
          this.subclusterResolver.getDestinationForPath(path);
      if (location == null) {
        throw new IOException("Cannot find locations for " + path + " in " +
            this.subclusterResolver.getClass().getSimpleName());
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
      return locs;
    } catch (IOException ioe) {
      if (this.rpcMonitor != null) {
        this.rpcMonitor.routerFailureStateStore();
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
    if (subclusterResolver instanceof MountTableResolver) {
      try {
        MountTableResolver mountTable = (MountTableResolver)subclusterResolver;
        MountTable entry = mountTable.getMountPoint(path);
        if (entry != null && entry.isReadOnly()) {
          return true;
        }
      } catch (IOException e) {
        LOG.error("Cannot get mount point", e);
      }
    }
    return false;
  }

  /**
   * Get the user that is invoking this operation.
   *
   * @return Remote user group information.
   * @throws IOException If we cannot get the user information.
   */
  static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  /**
   * Merge the outputs from multiple namespaces.
   * @param map Namespace -> Output array.
   * @param clazz Class of the values.
   * @return Array with the outputs.
   */
  protected static <T> T[] merge(
      Map<FederationNamespaceInfo, T[]> map, Class<T> clazz) {

    // Put all results into a set to avoid repeats
    Set<T> ret = new LinkedHashSet<>();
    for (T[] values : map.values()) {
      for (T val : values) {
        ret.add(val);
      }
    }

    return toArray(ret, clazz);
  }

  /**
   * Convert a set of values into an array.
   * @param set Input set.
   * @param clazz Class of the values.
   * @return Array with the values in set.
   */
  private static <T> T[] toArray(Collection<T> set, Class<T> clazz) {
    @SuppressWarnings("unchecked")
    T[] combinedData = (T[]) Array.newInstance(clazz, set.size());
    combinedData = set.toArray(combinedData);
    return combinedData;
  }

  /**
   * Get quota module implement.
   */
  public Quota getQuotaModule() {
    return this.quotaCall;
  }

  /**
   * Get RPC metrics info.
   * @return The instance of FederationRPCMetrics.
   */
  public FederationRPCMetrics getRPCMetrics() {
    return this.rpcMonitor.getRPCMetrics();
  }
}
