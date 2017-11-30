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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HANDLER_QUEUE_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_HANDLER_QUEUE_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_READER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_READER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_READER_QUEUE_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_READER_QUEUE_SIZE_KEY;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
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
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
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
public class RouterRpcServer extends AbstractService implements ClientProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterRpcServer.class);


  /** Configuration for the RPC server. */
  private Configuration conf;

  /** Identifier for the super user. */
  private final String superUser;
  /** Identifier for the super group. */
  private final String superGroup;

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


  /**
   * Construct a router RPC server.
   *
   * @param configuration HDFS Configuration.
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

    // User and group for reporting
    this.superUser = System.getProperty("user.name");
    this.superGroup = this.conf.get(
        DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
        DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);

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

    InetSocketAddress confRpcAddress = conf.getSocketAddr(
        DFSConfigKeys.DFS_ROUTER_RPC_BIND_HOST_KEY,
        DFSConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_ROUTER_RPC_ADDRESS_DEFAULT,
        DFSConfigKeys.DFS_ROUTER_RPC_PORT_DEFAULT);
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
    // We don't want the server to log the full stack trace for some exceptions
    this.rpcServer.addTerseExceptions(
        RemoteException.class,
        StandbyException.class,
        SafeModeException.class,
        FileNotFoundException.class,
        FileAlreadyExistsException.class,
        AccessControlException.class,
        LeaseExpiredException.class,
        NotReplicatedYetException.class,
        IOException.class);

    // The RPC-server port can be ephemeral... ensure we have the correct info
    InetSocketAddress listenAddress = this.rpcServer.getListenerAddress();
    this.rpcAddress = new InetSocketAddress(
        confRpcAddress.getHostName(), listenAddress.getPort());

    // Create metrics monitor
    Class<? extends RouterRpcMonitor> rpcMonitorClass = this.conf.getClass(
        DFSConfigKeys.DFS_ROUTER_METRICS_CLASS,
        DFSConfigKeys.DFS_ROUTER_METRICS_CLASS_DEFAULT,
        RouterRpcMonitor.class);
    this.rpcMonitor = ReflectionUtils.newInstance(rpcMonitorClass, conf);

    // Create the client
    this.rpcClient = new RouterRpcClient(this.conf, this.router.getRouterId(),
        this.namenodeResolver, this.rpcMonitor);
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
   *                          client requests.
   * @throws UnsupportedOperationException If the operation is not supported.
   */
  private void checkOperation(OperationCategory op, boolean supported)
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
   *                          client requests.
   */
  private void checkOperation(OperationCategory op) throws StandbyException {
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

    // TODO check Router safe mode and return Standby exception
  }

  @Override // ClientProtocol
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    checkOperation(OperationCategory.WRITE, false);
    return null;
  }

  /**
   * The the delegation token from each name service.
   * @param renewer
   * @return Name service -> Token.
   * @throws IOException
   */
  public Map<FederationNamespaceInfo, Token<DelegationTokenIdentifier>>
      getDelegationTokens(Text renewer) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
    return null;
  }

  @Override // ClientProtocol
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    checkOperation(OperationCategory.WRITE, false);
    return 0;
  }

  @Override // ClientProtocol
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override // ClientProtocol
  public LocatedBlocks getBlockLocations(String src, final long offset,
      final long length) throws IOException {
    checkOperation(OperationCategory.READ);

    List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod remoteMethod = new RemoteMethod("getBlockLocations",
        new Class<?>[] {String.class, long.class, long.class},
        new RemoteParam(), offset, length);
    return (LocatedBlocks) rpcClient.invokeSequential(locations, remoteMethod,
        LocatedBlocks.class, null);
  }

  @Override // ClientProtocol
  public FsServerDefaults getServerDefaults() throws IOException {
    checkOperation(OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getServerDefaults");
    String ns = subclusterResolver.getDefaultNamespace();
    return (FsServerDefaults) rpcClient.invokeSingle(ns, method);
  }

  @Override // ClientProtocol
  public HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag,
      boolean createParent, short replication, long blockSize,
      CryptoProtocolVersion[] supportedVersions, String ecPolicyName)
      throws IOException {
    checkOperation(OperationCategory.WRITE);

    RemoteLocation createLocation = getCreateLocation(src);
    RemoteMethod method = new RemoteMethod("create",
        new Class<?>[] {String.class, FsPermission.class, String.class,
                        EnumSetWritable.class, boolean.class, short.class,
                        long.class, CryptoProtocolVersion[].class,
                        String.class},
        createLocation.getDest(), masked, clientName, flag, createParent,
        replication, blockSize, supportedVersions, ecPolicyName);
    return (HdfsFileStatus) rpcClient.invokeSingle(createLocation, method);
  }

  /**
   * Get the location to create a file. It checks if the file already existed
   * in one of the locations.
   *
   * @param src Path of the file to check.
   * @return The remote location for this file.
   * @throws IOException If the file has no creation location.
   */
  private RemoteLocation getCreateLocation(final String src)
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

  // Medium
  @Override // ClientProtocol
  public LastBlockWithStatus append(String src, final String clientName,
      final EnumSetWritable<CreateFlag> flag) throws IOException {
    checkOperation(OperationCategory.WRITE);

    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("append",
        new Class<?>[] {String.class, String.class, EnumSetWritable.class},
        new RemoteParam(), clientName, flag);
    return (LastBlockWithStatus) rpcClient.invokeSequential(
        locations, method, LastBlockWithStatus.class, null);
  }

  // Low
  @Override // ClientProtocol
  public boolean recoverLease(String src, String clientName)
      throws IOException {
    checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("recoverLease",
        new Class<?>[] {String.class, String.class}, new RemoteParam(),
        clientName);
    Object result = rpcClient.invokeSequential(
        locations, method, Boolean.class, Boolean.TRUE);
    return (boolean) result;
  }

  @Override // ClientProtocol
  public boolean setReplication(String src, short replication)
      throws IOException {
    checkOperation(OperationCategory.WRITE);

    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setReplication",
        new Class<?>[] {String.class, short.class}, new RemoteParam(),
        replication);
    Object result = rpcClient.invokeSequential(
        locations, method, Boolean.class, Boolean.TRUE);
    return (boolean) result;
  }

  @Override
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    checkOperation(OperationCategory.WRITE);

    List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setStoragePolicy",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), policyName);
    rpcClient.invokeSequential(locations, method, null, null);
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    checkOperation(OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getStoragePolicies");
    String ns = subclusterResolver.getDefaultNamespace();
    return (BlockStoragePolicy[]) rpcClient.invokeSingle(ns, method);
  }

  @Override // ClientProtocol
  public void setPermission(String src, FsPermission permissions)
      throws IOException {
    checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setPermission",
        new Class<?>[] {String.class, FsPermission.class},
        new RemoteParam(), permissions);
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setOwner",
        new Class<?>[] {String.class, String.class, String.class},
        new RemoteParam(), username, groupname);
    rpcClient.invokeSequential(locations, method);
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
    checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("addBlock",
        new Class<?>[] {String.class, String.class, ExtendedBlock.class,
                        DatanodeInfo[].class, long.class, String[].class,
                        EnumSet.class},
        new RemoteParam(), clientName, previous, excludedNodes, fileId,
        favoredNodes, addBlockFlags);
    // TODO verify the excludedNodes and favoredNodes are acceptable to this NN
    return (LocatedBlock) rpcClient.invokeSequential(
        locations, method, LocatedBlock.class, null);
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
    checkOperation(OperationCategory.READ);

    final List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("getAdditionalDatanode",
        new Class<?>[] {String.class, long.class, ExtendedBlock.class,
                        DatanodeInfo[].class, String[].class,
                        DatanodeInfo[].class, int.class, String.class},
        new RemoteParam(), fileId, blk, existings, existingStorageIDs, excludes,
        numAdditionalNodes, clientName);
    return (LocatedBlock) rpcClient.invokeSequential(
        locations, method, LocatedBlock.class, null);
  }

  @Override // ClientProtocol
  public void abandonBlock(ExtendedBlock b, long fileId, String src,
      String holder) throws IOException {
    checkOperation(OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("abandonBlock",
        new Class<?>[] {ExtendedBlock.class, long.class, String.class,
                        String.class},
        b, fileId, new RemoteParam(), holder);
    rpcClient.invokeSingle(b, method);
  }

  @Override // ClientProtocol
  public boolean complete(String src, String clientName, ExtendedBlock last,
      long fileId) throws IOException {
    checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("complete",
        new Class<?>[] {String.class, String.class, ExtendedBlock.class,
                        long.class},
        new RemoteParam(), clientName, last, fileId);
    // Complete can return true/false, so don't expect a result
    return ((Boolean) rpcClient.invokeSequential(
        locations, method, Boolean.class, null)).booleanValue();
  }

  @Override // ClientProtocol
  public LocatedBlock updateBlockForPipeline(
      ExtendedBlock block, String clientName) throws IOException {
    checkOperation(OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("updateBlockForPipeline",
        new Class<?>[] {ExtendedBlock.class, String.class},
        block, clientName);
    return (LocatedBlock) rpcClient.invokeSingle(block, method);
  }

  /**
   * Datanode are not verified to be in the same nameservice as the old block.
   * TODO This may require validation.
   */
  @Override // ClientProtocol
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs)
          throws IOException {
    checkOperation(OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("updatePipeline",
        new Class<?>[] {String.class, ExtendedBlock.class, ExtendedBlock.class,
                        DatanodeID[].class, String[].class},
        clientName, oldBlock, newBlock, newNodes, newStorageIDs);
    rpcClient.invokeSingle(oldBlock, method);
  }

  @Override // ClientProtocol
  public long getPreferredBlockSize(String src) throws IOException {
    checkOperation(OperationCategory.READ);

    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("getPreferredBlockSize",
        new Class<?>[] {String.class}, new RemoteParam());
    return ((Long) rpcClient.invokeSequential(
        locations, method, Long.class, null)).longValue();
  }

  /**
   * Determines combinations of eligible src/dst locations for a rename. A
   * rename cannot change the namespace. Renames are only allowed if there is an
   * eligible dst location in the same namespace as the source.
   *
   * @param srcLocations List of all potential source destinations where the
   *          path may be located. On return this list is trimmed to include
   *          only the paths that have corresponding destinations in the same
   *          namespace.
   * @param dst The destination path
   * @return A map of all eligible source namespaces and their corresponding
   *         replacement value.
   * @throws IOException If the dst paths could not be determined.
   */
  private RemoteParam getRenameDestinations(
      final List<RemoteLocation> srcLocations, final String dst)
          throws IOException {

    final List<RemoteLocation> dstLocations = getLocationsForPath(dst, true);
    final Map<RemoteLocation, String> dstMap = new HashMap<>();

    Iterator<RemoteLocation> iterator = srcLocations.iterator();
    while (iterator.hasNext()) {
      RemoteLocation srcLocation = iterator.next();
      RemoteLocation eligibleDst =
          getFirstMatchingLocation(srcLocation, dstLocations);
      if (eligibleDst != null) {
        // Use this dst for this source location
        dstMap.put(srcLocation, eligibleDst.getDest());
      } else {
        // This src destination is not valid, remove from the source list
        iterator.remove();
      }
    }
    return new RemoteParam(dstMap);
  }

  /**
   * Get first matching location.
   *
   * @param location Location we are looking for.
   * @param locations List of locations.
   * @return The first matchin location in the list.
   */
  private RemoteLocation getFirstMatchingLocation(RemoteLocation location,
      List<RemoteLocation> locations) {
    for (RemoteLocation loc : locations) {
      if (loc.getNameserviceId().equals(location.getNameserviceId())) {
        // Return first matching location
        return loc;
      }
    }
    return null;
  }

  @Deprecated
  @Override // ClientProtocol
  public boolean rename(final String src, final String dst)
      throws IOException {
    checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> srcLocations = getLocationsForPath(src, true);
    // srcLocations may be trimmed by getRenameDestinations()
    final List<RemoteLocation> locs = new LinkedList<>(srcLocations);
    RemoteParam dstParam = getRenameDestinations(locs, dst);
    if (locs.isEmpty()) {
      throw new IOException(
          "Rename of " + src + " to " + dst + " is not allowed," +
          " no eligible destination in the same namespace was found.");
    }
    RemoteMethod method = new RemoteMethod("rename",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), dstParam);
    return ((Boolean) rpcClient.invokeSequential(
        locs, method, Boolean.class, Boolean.TRUE)).booleanValue();
  }

  @Override // ClientProtocol
  public void rename2(final String src, final String dst,
      final Options.Rename... options) throws IOException {
    checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> srcLocations = getLocationsForPath(src, true);
    // srcLocations may be trimmed by getRenameDestinations()
    final List<RemoteLocation> locs = new LinkedList<>(srcLocations);
    RemoteParam dstParam = getRenameDestinations(locs, dst);
    if (locs.isEmpty()) {
      throw new IOException(
          "Rename of " + src + " to " + dst + " is not allowed," +
          " no eligible destination in the same namespace was found.");
    }
    RemoteMethod method = new RemoteMethod("rename2",
        new Class<?>[] {String.class, String.class, options.getClass()},
        new RemoteParam(), dstParam, options);
    rpcClient.invokeSequential(locs, method, null, null);
  }

  @Override // ClientProtocol
  public void concat(String trg, String[] src) throws IOException {
    checkOperation(OperationCategory.WRITE);

    // See if the src and target files are all in the same namespace
    LocatedBlocks targetBlocks = getBlockLocations(trg, 0, 1);
    if (targetBlocks == null) {
      throw new IOException("Cannot locate blocks for target file - " + trg);
    }
    LocatedBlock lastLocatedBlock = targetBlocks.getLastLocatedBlock();
    String targetBlockPoolId = lastLocatedBlock.getBlock().getBlockPoolId();
    for (String source : src) {
      LocatedBlocks sourceBlocks = getBlockLocations(source, 0, 1);
      if (sourceBlocks == null) {
        throw new IOException(
            "Cannot located blocks for source file " + source);
      }
      String sourceBlockPoolId =
          sourceBlocks.getLastLocatedBlock().getBlock().getBlockPoolId();
      if (!sourceBlockPoolId.equals(targetBlockPoolId)) {
        throw new IOException("Cannot concatenate source file " + source
            + " because it is located in a different namespace"
            + " with block pool id " + sourceBlockPoolId
            + " from the target file with block pool id "
            + targetBlockPoolId);
      }
    }

    // Find locations in the matching namespace.
    final RemoteLocation targetDestination =
        getLocationForPath(trg, true, targetBlockPoolId);
    String[] sourceDestinations = new String[src.length];
    for (int i = 0; i < src.length; i++) {
      String sourceFile = src[i];
      RemoteLocation location =
          getLocationForPath(sourceFile, true, targetBlockPoolId);
      sourceDestinations[i] = location.getDest();
    }
    // Invoke
    RemoteMethod method = new RemoteMethod("concat",
        new Class<?>[] {String.class, String[].class},
        targetDestination.getDest(), sourceDestinations);
    rpcClient.invokeSingle(targetDestination, method);
  }

  @Override // ClientProtocol
  public boolean truncate(String src, long newLength, String clientName)
      throws IOException {
    checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("truncate",
        new Class<?>[] {String.class, long.class, String.class},
        new RemoteParam(), newLength, clientName);
    return ((Boolean) rpcClient.invokeSequential(locations, method,
        Boolean.class, Boolean.TRUE)).booleanValue();
  }

  @Override // ClientProtocol
  public boolean delete(String src, boolean recursive) throws IOException {
    checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("delete",
        new Class<?>[] {String.class, boolean.class}, new RemoteParam(),
        recursive);
    return ((Boolean) rpcClient.invokeSequential(locations, method,
        Boolean.class, Boolean.TRUE)).booleanValue();
  }

  @Override // ClientProtocol
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws IOException {
    checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    if (locations.size() > 1) {
      // Check if this directory already exists
      try {
        HdfsFileStatus fileStatus = getFileInfo(src);
        if (fileStatus != null) {
          // When existing, the NN doesn't return an exception; return true
          return true;
        }
      } catch (IOException ioe) {
        // Can't query if this file exists or not.
        LOG.error("Error requesting file info for path {} while proxing mkdirs",
            src, ioe);
      }
    }

    RemoteLocation firstLocation = locations.get(0);
    RemoteMethod method = new RemoteMethod("mkdirs",
        new Class<?>[] {String.class, FsPermission.class, boolean.class},
        new RemoteParam(), masked, createParent);
    return ((Boolean) rpcClient.invokeSingle(firstLocation, method))
        .booleanValue();
  }

  @Override // ClientProtocol
  public void renewLease(String clientName) throws IOException {
    checkOperation(OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("renewLease",
        new Class<?>[] {String.class}, clientName);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, false, false);
  }

  @Override // ClientProtocol
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws IOException {
    checkOperation(OperationCategory.READ);

    // Locate the dir and fetch the listing
    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("getListing",
        new Class<?>[] {String.class, startAfter.getClass(), boolean.class},
        new RemoteParam(), startAfter, needLocation);
    Map<RemoteLocation, Object> listings =
        rpcClient.invokeConcurrent(locations, method, false, false);

    Map<String, HdfsFileStatus> nnListing = new TreeMap<>();
    int totalRemainingEntries = 0;
    int remainingEntries = 0;
    boolean namenodeListingExists = false;
    if (listings != null) {
      // Check the subcluster listing with the smallest name
      String lastName = null;
      for (Entry<RemoteLocation, Object> entry : listings.entrySet()) {
        RemoteLocation location = entry.getKey();
        DirectoryListing listing = (DirectoryListing) entry.getValue();
        if (listing == null) {
          LOG.debug("Cannot get listing from {}", location);
        } else {
          totalRemainingEntries += listing.getRemainingEntries();
          HdfsFileStatus[] partialListing = listing.getPartialListing();
          int length = partialListing.length;
          if (length > 0) {
            HdfsFileStatus lastLocalEntry = partialListing[length-1];
            String lastLocalName = lastLocalEntry.getLocalName();
            if (lastName == null || lastName.compareTo(lastLocalName) > 0) {
              lastName = lastLocalName;
            }
          }
        }
      }

      // Add existing entries
      for (Object value : listings.values()) {
        DirectoryListing listing = (DirectoryListing) value;
        if (listing != null) {
          namenodeListingExists = true;
          for (HdfsFileStatus file : listing.getPartialListing()) {
            String filename = file.getLocalName();
            if (totalRemainingEntries > 0 && filename.compareTo(lastName) > 0) {
              // Discarding entries further than the lastName
              remainingEntries++;
            } else {
              nnListing.put(filename, file);
            }
          }
          remainingEntries += listing.getRemainingEntries();
        }
      }
    }

    // Add mount points at this level in the tree
    final List<String> children = subclusterResolver.getMountPoints(src);
    if (children != null) {
      // Get the dates for each mount point
      Map<String, Long> dates = getMountPointDates(src);

      // Create virtual folder with the mount name
      for (String child : children) {
        long date = 0;
        if (dates != null && dates.containsKey(child)) {
          date = dates.get(child);
        }
        // TODO add number of children
        HdfsFileStatus dirStatus = getMountPointStatus(child, 0, date);

        // This may overwrite existing listing entries with the mount point
        // TODO don't add if already there?
        nnListing.put(child, dirStatus);
      }
    }

    if (!namenodeListingExists && nnListing.size() == 0) {
      // NN returns a null object if the directory cannot be found and has no
      // listing. If we didn't retrieve any NN listing data, and there are no
      // mount points here, return null.
      return null;
    }

    // Generate combined listing
    HdfsFileStatus[] combinedData = new HdfsFileStatus[nnListing.size()];
    combinedData = nnListing.values().toArray(combinedData);
    return new DirectoryListing(combinedData, remainingEntries);
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    checkOperation(OperationCategory.READ);

    final List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("getFileInfo",
        new Class<?>[] {String.class}, new RemoteParam());
    HdfsFileStatus ret = (HdfsFileStatus) rpcClient.invokeSequential(
        locations, method, HdfsFileStatus.class, null);

    // If there is no real path, check mount points
    if (ret == null) {
      List<String> children = subclusterResolver.getMountPoints(src);
      if (children != null && !children.isEmpty()) {
        Map<String, Long> dates = getMountPointDates(src);
        long date = 0;
        if (dates != null && dates.containsKey(src)) {
          date = dates.get(src);
        }
        ret = getMountPointStatus(src, children.size(), date);
      }
    }

    return ret;
  }

  @Override // ClientProtocol
  public boolean isFileClosed(String src) throws IOException {
    checkOperation(OperationCategory.READ);

    final List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("isFileClosed",
        new Class<?>[] {String.class}, new RemoteParam());
    return ((Boolean) rpcClient.invokeSequential(
        locations, method, Boolean.class, Boolean.TRUE)).booleanValue();
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
    checkOperation(OperationCategory.READ);

    final List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("getFileLinkInfo",
        new Class<?>[] {String.class}, new RemoteParam());
    return (HdfsFileStatus) rpcClient.invokeSequential(
        locations, method, HdfsFileStatus.class, null);
  }

  @Override // ClientProtocol
  public long[] getStats() throws IOException {
    checkOperation(OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("getStats");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, Object> results =
        rpcClient.invokeConcurrent(nss, method, true, false);
    long[] combinedData = new long[STATS_ARRAY_LENGTH];
    for (Object o : results.values()) {
      long[] data = (long[]) o;
      for (int i = 0; i < combinedData.length && i < data.length; i++) {
        if (data[i] >= 0) {
          combinedData[i] += data[i];
        }
      }
    }
    return combinedData;
  }

  @Override // ClientProtocol
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    checkOperation(OperationCategory.UNCHECKED);
    return getDatanodeReport(type, 0);
  }

  /**
   * Get the datanode report with a timeout.
   * @param type Type of the datanode.
   * @param timeOutMs Time out for the reply in milliseconds.
   * @return List of datanodes.
   * @throws IOException If it cannot get the report.
   */
  public DatanodeInfo[] getDatanodeReport(
      DatanodeReportType type, long timeOutMs) throws IOException {
    checkOperation(OperationCategory.UNCHECKED);

    Map<String, DatanodeInfo> datanodesMap = new LinkedHashMap<>();
    RemoteMethod method = new RemoteMethod("getDatanodeReport",
        new Class<?>[] {DatanodeReportType.class}, type);

    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, Object> results =
        rpcClient.invokeConcurrent(nss, method, true, false, timeOutMs);
    for (Entry<FederationNamespaceInfo, Object> entry : results.entrySet()) {
      FederationNamespaceInfo ns = entry.getKey();
      DatanodeInfo[] result = (DatanodeInfo[]) entry.getValue();
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
    DatanodeInfo[] combinedData = new DatanodeInfo[datanodes.size()];
    combinedData = datanodes.toArray(combinedData);
    return combinedData;
  }

  @Override // ClientProtocol
  public DatanodeStorageReport[] getDatanodeStorageReport(
      DatanodeReportType type) throws IOException {
    checkOperation(OperationCategory.UNCHECKED);

    Map<String, DatanodeStorageReport> datanodesMap = new HashMap<>();
    RemoteMethod method = new RemoteMethod("getDatanodeStorageReport",
        new Class<?>[] {DatanodeReportType.class}, type);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, Object> results =
        rpcClient.invokeConcurrent(nss, method, true, false);
    for (Object r : results.values()) {
      DatanodeStorageReport[] result = (DatanodeStorageReport[]) r;
      for (DatanodeStorageReport node : result) {
        String nodeId = node.getDatanodeInfo().getXferAddr();
        if (!datanodesMap.containsKey(nodeId)) {
          datanodesMap.put(nodeId, node);
        }
        // TODO merge somehow, right now it just takes the first one
      }
    }

    Collection<DatanodeStorageReport> datanodes = datanodesMap.values();
    // TODO sort somehow
    DatanodeStorageReport[] combinedData =
        new DatanodeStorageReport[datanodes.size()];
    combinedData = datanodes.toArray(combinedData);
    return combinedData;
  }

  @Override // ClientProtocol
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    checkOperation(OperationCategory.WRITE);

    // Set safe mode in all the name spaces
    RemoteMethod method = new RemoteMethod("setSafeMode",
        new Class<?>[] {SafeModeAction.class, boolean.class},
        action, isChecked);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, Object> results =
        rpcClient.invokeConcurrent(nss, method, true, true);

    // We only report true if all the name space are in safe mode
    int numSafemode = 0;
    for (Object result : results.values()) {
      if (result instanceof Boolean) {
        boolean safemode = (boolean) result;
        if (safemode) {
          numSafemode++;
        }
      }
    }
    return numSafemode == results.size();
  }

  @Override // ClientProtocol
  public boolean restoreFailedStorage(String arg) throws IOException {
    checkOperation(OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("restoreFailedStorage",
        new Class<?>[] {String.class}, arg);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, Object> ret =
        rpcClient.invokeConcurrent(nss, method, true, false);

    boolean success = true;
    Object obj = ret;
    @SuppressWarnings("unchecked")
    Map<FederationNamespaceInfo, Boolean> results =
        (Map<FederationNamespaceInfo, Boolean>)obj;
    Collection<Boolean> sucesses = results.values();
    for (boolean s : sucesses) {
      if (!s) {
        success = false;
      }
    }
    return success;
  }

  @Override // ClientProtocol
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    checkOperation(OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("saveNamespace",
        new Class<?>[] {Long.class, Long.class}, timeWindow, txGap);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, Object> ret =
        rpcClient.invokeConcurrent(nss, method, true, false);

    boolean success = true;
    Object obj = ret;
    @SuppressWarnings("unchecked")
    Map<FederationNamespaceInfo, Boolean> results =
        (Map<FederationNamespaceInfo, Boolean>)obj;
    Collection<Boolean> sucesses = results.values();
    for (boolean s : sucesses) {
      if (!s) {
        success = false;
      }
    }
    return success;
  }

  @Override // ClientProtocol
  public long rollEdits() throws IOException {
    checkOperation(OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("rollEdits", new Class<?>[] {});
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, Object> ret =
        rpcClient.invokeConcurrent(nss, method, true, false);

    // Return the maximum txid
    long txid = 0;
    Object obj = ret;
    @SuppressWarnings("unchecked")
    Map<FederationNamespaceInfo, Long> results =
        (Map<FederationNamespaceInfo, Long>)obj;
    Collection<Long> txids = results.values();
    for (long t : txids) {
      if (t > txid) {
        txid = t;
      }
    }
    return txid;
  }

  @Override // ClientProtocol
  public void refreshNodes() throws IOException {
    checkOperation(OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("refreshNodes", new Class<?>[] {});
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, true);
  }

  @Override // ClientProtocol
  public void finalizeUpgrade() throws IOException {
    checkOperation(OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("finalizeUpgrade",
        new Class<?>[] {});
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false);
  }

  @Override // ClientProtocol
  public RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action)
      throws IOException {
    checkOperation(OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("rollingUpgrade",
        new Class<?>[] {RollingUpgradeAction.class}, action);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, Object> ret =
        rpcClient.invokeConcurrent(nss, method, true, false);

    // Return the first rolling upgrade info
    RollingUpgradeInfo info = null;
    Object obj = ret;
    @SuppressWarnings("unchecked")
    Map<FederationNamespaceInfo, RollingUpgradeInfo> results =
        (Map<FederationNamespaceInfo, RollingUpgradeInfo>)obj;
    Collection<RollingUpgradeInfo> infos = results.values();
    for (RollingUpgradeInfo infoNs : infos) {
      if (info == null && infoNs != null) {
        info = infoNs;
      }
    }
    return info;
  }

  @Override // ClientProtocol
  public void metaSave(String filename) throws IOException {
    checkOperation(OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("metaSave",
        new Class<?>[] {String.class}, filename);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false);
  }

  @Override // ClientProtocol
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    checkOperation(OperationCategory.READ);

    final List<RemoteLocation> locations = getLocationsForPath(path, false);
    RemoteMethod method = new RemoteMethod("listCorruptFileBlocks",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), cookie);
    return (CorruptFileBlocks) rpcClient.invokeSequential(
        locations, method, CorruptFileBlocks.class, null);
  }

  @Override // ClientProtocol
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    checkOperation(OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("setBalancerBandwidth",
        new Class<?>[] {Long.class}, bandwidth);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false);
  }

  @Override // ClientProtocol
  public ContentSummary getContentSummary(String path) throws IOException {
    checkOperation(OperationCategory.READ);

    // Get the summaries from regular files
    Collection<ContentSummary> summaries = new LinkedList<>();
    FileNotFoundException notFoundException = null;
    try {
      final List<RemoteLocation> locations = getLocationsForPath(path, false);
      RemoteMethod method = new RemoteMethod("getContentSummary",
          new Class<?>[] {String.class}, new RemoteParam());
      @SuppressWarnings("unchecked")
      Map<String, ContentSummary> results =
          (Map<String, ContentSummary>) ((Object)rpcClient.invokeConcurrent(
              locations, method, false, false));
      summaries.addAll(results.values());
    } catch (FileNotFoundException e) {
      notFoundException = e;
    }

    // Add mount points at this level in the tree
    final List<String> children = subclusterResolver.getMountPoints(path);
    if (children != null) {
      for (String child : children) {
        Path childPath = new Path(path, child);
        try {
          ContentSummary mountSummary = getContentSummary(childPath.toString());
          if (mountSummary != null) {
            summaries.add(mountSummary);
          }
        } catch (Exception e) {
          LOG.error("Cannot get content summary for mount {}: {}",
              childPath, e.getMessage());
        }
      }
    }

    // Throw original exception if no original nor mount points
    if (summaries.isEmpty() && notFoundException != null) {
      throw notFoundException;
    }

    return aggregateContentSummary(summaries);
  }

  /**
   * Aggregate content summaries for each subcluster.
   *
   * @param summaries Collection of individual summaries.
   * @return Aggregated content summary.
   */
  private ContentSummary aggregateContentSummary(
      Collection<ContentSummary> summaries) {
    if (summaries.size() == 1) {
      return summaries.iterator().next();
    }

    long length = 0;
    long fileCount = 0;
    long directoryCount = 0;
    long quota = 0;
    long spaceConsumed = 0;
    long spaceQuota = 0;

    for (ContentSummary summary : summaries) {
      length += summary.getLength();
      fileCount += summary.getFileCount();
      directoryCount += summary.getDirectoryCount();
      quota += summary.getQuota();
      spaceConsumed += summary.getSpaceConsumed();
      spaceQuota += summary.getSpaceQuota();
    }

    ContentSummary ret = new ContentSummary.Builder()
        .length(length)
        .fileCount(fileCount)
        .directoryCount(directoryCount)
        .quota(quota)
        .spaceConsumed(spaceConsumed)
        .spaceQuota(spaceQuota)
        .build();
    return ret;
  }

  @Override // ClientProtocol
  public void fsync(String src, long fileId, String clientName,
      long lastBlockLength) throws IOException {
    checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("fsync",
        new Class<?>[] {String.class, long.class, String.class, long.class },
        new RemoteParam(), fileId, clientName, lastBlockLength);
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public void setTimes(String src, long mtime, long atime) throws IOException {
    checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setTimes",
        new Class<?>[] {String.class, long.class, long.class},
        new RemoteParam(), mtime, atime);
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public void createSymlink(String target, String link, FsPermission dirPerms,
      boolean createParent) throws IOException {
    checkOperation(OperationCategory.WRITE);

    // TODO Verify that the link location is in the same NS as the targets
    final List<RemoteLocation> targetLocations =
        getLocationsForPath(target, true);
    final List<RemoteLocation> linkLocations =
        getLocationsForPath(link, true);
    RemoteLocation linkLocation = linkLocations.get(0);
    RemoteMethod method = new RemoteMethod("createSymlink",
        new Class<?>[] {String.class, String.class, FsPermission.class,
                        boolean.class},
        new RemoteParam(), linkLocation.getDest(), dirPerms, createParent);
    rpcClient.invokeSequential(targetLocations, method);
  }

  @Override // ClientProtocol
  public String getLinkTarget(String path) throws IOException {
    checkOperation(OperationCategory.READ);

    final List<RemoteLocation> locations = getLocationsForPath(path, true);
    RemoteMethod method = new RemoteMethod("getLinkTarget",
        new Class<?>[] {String.class}, new RemoteParam());
    return (String) rpcClient.invokeSequential(
        locations, method, String.class, null);
  }

  @Override // Client Protocol
  public void allowSnapshot(String snapshotRoot) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override // Client Protocol
  public void disallowSnapshot(String snapshot) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override // ClientProtocol
  public void renameSnapshot(String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override // Client Protocol
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override // ClientProtocol
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
      String earlierSnapshotName, String laterSnapshotName) throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override // ClientProtocol
  public long addCacheDirective(CacheDirectiveInfo path,
      EnumSet<CacheFlag> flags) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
    return 0;
  }

  @Override // ClientProtocol
  public void modifyCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override // ClientProtocol
  public void removeCacheDirective(long id) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override // ClientProtocol
  public BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
      long prevId, CacheDirectiveInfo filter) throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override // ClientProtocol
  public void addCachePool(CachePoolInfo info) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override // ClientProtocol
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override // ClientProtocol
  public void removeCachePool(String cachePoolName) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override // ClientProtocol
  public BatchedEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override // ClientProtocol
  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    checkOperation(OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("modifyAclEntries",
        new Class<?>[] {String.class, List.class},
        new RemoteParam(), aclSpec);
    rpcClient.invokeSequential(locations, method, null, null);
  }

  @Override // ClienProtocol
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    checkOperation(OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("removeAclEntries",
        new Class<?>[] {String.class, List.class},
        new RemoteParam(), aclSpec);
    rpcClient.invokeSequential(locations, method, null, null);
  }

  @Override // ClientProtocol
  public void removeDefaultAcl(String src) throws IOException {
    checkOperation(OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("removeDefaultAcl",
        new Class<?>[] {String.class}, new RemoteParam());
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public void removeAcl(String src) throws IOException {
    checkOperation(OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("removeAcl",
        new Class<?>[] {String.class}, new RemoteParam());
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    checkOperation(OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod(
        "setAcl", new Class<?>[] {String.class, List.class},
        new RemoteParam(), aclSpec);
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public AclStatus getAclStatus(String src) throws IOException {
    checkOperation(OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("getAclStatus",
        new Class<?>[] {String.class}, new RemoteParam());
    return (AclStatus) rpcClient.invokeSequential(
        locations, method, AclStatus.class, null);
  }

  @Override // ClientProtocol
  public void createEncryptionZone(String src, String keyName)
      throws IOException {
    checkOperation(OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("createEncryptionZone",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), keyName);
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public EncryptionZone getEZForPath(String src) throws IOException {
    checkOperation(OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("getEZForPath",
        new Class<?>[] {String.class}, new RemoteParam());
    return (EncryptionZone) rpcClient.invokeSequential(
        locations, method, EncryptionZone.class, null);
  }

  @Override // ClientProtocol
  public BatchedEntries<EncryptionZone> listEncryptionZones(long prevId)
      throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override // ClientProtocol
  public void reencryptEncryptionZone(String zone, ReencryptAction action)
      throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override // ClientProtocol
  public BatchedEntries<ZoneReencryptionStatus> listReencryptionStatus(
      long prevId) throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override // ClientProtocol
  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    checkOperation(OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setXAttr",
        new Class<?>[] {String.class, XAttr.class, EnumSet.class},
        new RemoteParam(), xAttr, flag);
    rpcClient.invokeSequential(locations, method);
  }

  @SuppressWarnings("unchecked")
  @Override // ClientProtocol
  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs)
      throws IOException {
    checkOperation(OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("getXAttrs",
        new Class<?>[] {String.class, List.class}, new RemoteParam(), xAttrs);
    return (List<XAttr>) rpcClient.invokeSequential(
        locations, method, List.class, null);
  }

  @SuppressWarnings("unchecked")
  @Override // ClientProtocol
  public List<XAttr> listXAttrs(String src) throws IOException {
    checkOperation(OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations = getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("listXAttrs",
        new Class<?>[] {String.class}, new RemoteParam());
    return (List<XAttr>) rpcClient.invokeSequential(
        locations, method, List.class, null);
  }

  @Override // ClientProtocol
  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    checkOperation(OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations = getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("removeXAttr",
        new Class<?>[] {String.class, XAttr.class}, new RemoteParam(), xAttr);
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public void checkAccess(String path, FsAction mode) throws IOException {
    checkOperation(OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations = getLocationsForPath(path, true);
    RemoteMethod method = new RemoteMethod("checkAccess",
        new Class<?>[] {String.class, FsAction.class},
        new RemoteParam(), mode);
    rpcClient.invokeSequential(locations, method);
  }

  @Override // ClientProtocol
  public long getCurrentEditLogTxid() throws IOException {
    checkOperation(OperationCategory.READ);

    RemoteMethod method = new RemoteMethod(
        "getCurrentEditLogTxid", new Class<?>[] {});
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, Object> ret =
        rpcClient.invokeConcurrent(nss, method, true, false);

    // Return the maximum txid
    long txid = 0;
    Object obj = ret;
    @SuppressWarnings("unchecked")
    Map<FederationNamespaceInfo, Long> results =
        (Map<FederationNamespaceInfo, Long>)obj;
    Collection<Long> txids = results.values();
    for (long t : txids) {
      if (t > txid) {
        txid = t;
      }
    }
    return txid;
  }

  @Override // ClientProtocol
  public EventBatchList getEditsFromTxid(long txid) throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override
  public String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    checkOperation(OperationCategory.WRITE);
    return null;
  }

  @Override
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override
  public ErasureCodingPolicyInfo[] getErasureCodingPolicies()
      throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override // ClientProtocol
  public ErasureCodingPolicy getErasureCodingPolicy(String src)
      throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override // ClientProtocol
  public void setErasureCodingPolicy(String src, String ecPolicyName)
      throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override // ClientProtocol
  public void unsetErasureCodingPolicy(String src) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override // ClientProtocol
  public void setQuota(String path, long namespaceQuota, long storagespaceQuota,
      StorageType type) throws IOException {
    checkOperation(OperationCategory.WRITE);

    // TODO assign global replicas instead of applying them to each folder
    final List<RemoteLocation> locations = getLocationsForPath(path, true);
    RemoteMethod method = new RemoteMethod("setQuota",
        new Class<?>[] {String.class, Long.class, Long.class,
            StorageType.class},
        new RemoteParam(), namespaceQuota, storagespaceQuota, type);
    rpcClient.invokeConcurrent(locations, method, false, false);
  }

  @Override // ClientProtocol
  public QuotaUsage getQuotaUsage(String path) throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    checkOperation(OperationCategory.WRITE);

    // Block pool id -> blocks
    Map<String, List<LocatedBlock>> blockLocations = new HashMap<>();
    for (LocatedBlock block : blocks) {
      String bpId = block.getBlock().getBlockPoolId();
      List<LocatedBlock> bpBlocks = blockLocations.get(bpId);
      if (bpBlocks == null) {
        bpBlocks = new LinkedList<>();
        blockLocations.put(bpId, bpBlocks);
      }
      bpBlocks.add(block);
    }

    // Invoke each block pool
    for (Entry<String, List<LocatedBlock>> entry : blockLocations.entrySet()) {
      String bpId = entry.getKey();
      List<LocatedBlock> bpBlocks = entry.getValue();

      LocatedBlock[] bpBlocksArray =
          bpBlocks.toArray(new LocatedBlock[bpBlocks.size()]);
      RemoteMethod method = new RemoteMethod("reportBadBlocks",
          new Class<?>[] {LocatedBlock[].class},
          new Object[] {bpBlocksArray});
      rpcClient.invokeSingleBlockPool(bpId, method);
    }
  }

  @Override
  public void unsetStoragePolicy(String src) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override
  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
    return null;
  }

  @Override
  public void removeErasureCodingPolicy(String arg0) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override
  public void disableErasureCodingPolicy(String arg0) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override
  public void enableErasureCodingPolicy(String arg0) throws IOException {
    checkOperation(OperationCategory.WRITE, false);
  }

  @Override
  public ECBlockGroupStats getECBlockGroupStats() throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override
  public Map<String, String> getErasureCodingCodecs() throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override
  public ReplicatedBlockStats getReplicatedBlockStats() throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override
  public BatchedEntries<OpenFileEntry> listOpenFiles(long arg0)
      throws IOException {
    checkOperation(OperationCategory.READ, false);
    return null;
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
  private RemoteLocation getLocationForPath(
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
   *
   * @param path Path to check.
   * @param failIfLocked Fail the request if locked (top mount point).
   * @return Prioritized list of locations in the federated cluster.
   * @throws IOException If the location for this path cannot be determined.
   */
  private List<RemoteLocation> getLocationsForPath(
      String path, boolean failIfLocked) throws IOException {
    try {
      // Check the location for this path
      final PathLocation location =
          this.subclusterResolver.getDestinationForPath(path);
      if (location == null) {
        throw new IOException("Cannot find locations for " + path + " in " +
            this.subclusterResolver);
      }

      return location.getDestinations();
    } catch (IOException ioe) {
      if (this.rpcMonitor != null) {
        this.rpcMonitor.routerFailureStateStore();
      }
      throw ioe;
    }
  }

  /**
   * Get the modification dates for mount points.
   *
   * @param path Name of the path to start checking dates from.
   * @return Map with the modification dates for all sub-entries.
   */
  private Map<String, Long> getMountPointDates(String path) {
    Map<String, Long> ret = new TreeMap<>();
    // TODO add when we have a Mount Table
    return ret;
  }

  /**
   * Create a new file status for a mount point.
   *
   * @param name Name of the mount point.
   * @param childrenNum Number of children.
   * @param date Map with the dates.
   * @return New HDFS file status representing a mount point.
   */
  private HdfsFileStatus getMountPointStatus(
      String name, int childrenNum, long date) {
    long modTime = date;
    long accessTime = date;
    FsPermission permission = FsPermission.getDirDefault();
    String owner = this.superUser;
    String group = this.superGroup;
    try {
      // TODO support users, it should be the user for the pointed folder
      UserGroupInformation ugi = getRemoteUser();
      owner = ugi.getUserName();
      group = ugi.getPrimaryGroupName();
    } catch (IOException e) {
      LOG.error("Cannot get the remote user: {}", e.getMessage());
    }
    long inodeId = 0;
    return new HdfsFileStatus.Builder()
      .isdir(true)
      .mtime(modTime)
      .atime(accessTime)
      .perm(permission)
      .owner(owner)
      .group(group)
      .symlink(new byte[0])
      .path(DFSUtil.string2Bytes(name))
      .fileId(inodeId)
      .children(childrenNum)
      .build();
  }

  /**
   * Get the name of the method that is calling this function.
   *
   * @return Name of the method calling this function.
   */
  private static String getMethodName() {
    final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    String methodName = stack[3].getMethodName();
    return methodName;
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
}
