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

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SERVER_DEFAULTS_VALIDITY_PERIOD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SERVER_DEFAULTS_VALIDITY_PERIOD_MS_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.FederationUtil.updateMountPointStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
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
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
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
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RouterResolveException;
import org.apache.hadoop.hdfs.server.federation.router.security.RouterSecurityManager;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Module that implements all the RPC calls in {@link ClientProtocol} in the
 * {@link RouterRpcServer}.
 */
public class RouterClientProtocol implements ClientProtocol {
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterClientProtocol.class.getName());

  private final RouterRpcServer rpcServer;
  private final RouterRpcClient rpcClient;
  private final RouterFederationRename rbfRename;
  private final FileSubclusterResolver subclusterResolver;
  private final ActiveNamenodeResolver namenodeResolver;

  /**
   * Caching server defaults so as to prevent redundant calls to namenode,
   * similar to DFSClient, caching saves efforts when router connects
   * to multiple clients.
   */
  private volatile FsServerDefaults serverDefaults;
  private volatile long serverDefaultsLastUpdate;
  private final long serverDefaultsValidityPeriod;

  /** If it requires response from all subclusters. */
  private final boolean allowPartialList;
  /** Time out when getting the mount statistics. */
  private long mountStatusTimeOut;

  /** Identifier for the super user. */
  private String superUser;
  /** Identifier for the super group. */
  private final String superGroup;
  /** Erasure coding calls. */
  private final ErasureCoding erasureCoding;
  /** Cache Admin calls. */
  private final RouterCacheAdmin routerCacheAdmin;
  /** StoragePolicy calls. **/
  private final RouterStoragePolicy storagePolicy;
  /** Snapshot calls. */
  private final RouterSnapshot snapshotProto;
  /** Router security manager to handle token operations. */
  private RouterSecurityManager securityManager = null;

  RouterClientProtocol(Configuration conf, RouterRpcServer rpcServer) {
    this.rpcServer = rpcServer;
    this.rpcClient = rpcServer.getRPCClient();
    this.subclusterResolver = rpcServer.getSubclusterResolver();
    this.namenodeResolver = rpcServer.getNamenodeResolver();

    this.allowPartialList = conf.getBoolean(
        RBFConfigKeys.DFS_ROUTER_ALLOW_PARTIAL_LIST,
        RBFConfigKeys.DFS_ROUTER_ALLOW_PARTIAL_LIST_DEFAULT);
    this.mountStatusTimeOut = conf.getTimeDuration(
        RBFConfigKeys.DFS_ROUTER_CLIENT_MOUNT_TIME_OUT,
        RBFConfigKeys.DFS_ROUTER_CLIENT_MOUNT_TIME_OUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.serverDefaultsValidityPeriod = conf.getTimeDuration(
        DFS_CLIENT_SERVER_DEFAULTS_VALIDITY_PERIOD_MS_KEY,
        DFS_CLIENT_SERVER_DEFAULTS_VALIDITY_PERIOD_MS_DEFAULT,
        TimeUnit.MILLISECONDS);
    // User and group for reporting
    try {
      this.superUser = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException ex) {
      LOG.warn("Unable to get user name. Fall back to system property " +
          "user.name", ex);
      this.superUser = System.getProperty("user.name");
    }
    this.superGroup = conf.get(
        DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
        DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
    this.erasureCoding = new ErasureCoding(rpcServer);
    this.storagePolicy = new RouterStoragePolicy(rpcServer);
    this.snapshotProto = new RouterSnapshot(rpcServer);
    this.routerCacheAdmin = new RouterCacheAdmin(rpcServer);
    this.securityManager = rpcServer.getRouterSecurityManager();
    this.rbfRename = new RouterFederationRename(rpcServer, conf);
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, true);
    return this.securityManager.getDelegationToken(renewer);
  }

  /**
   * The the delegation token from each name service.
   *
   * @param renewer The token renewer.
   * @return Name service to Token.
   * @throws IOException If it cannot get the delegation token.
   */
  public Map<FederationNamespaceInfo, Token<DelegationTokenIdentifier>>
  getDelegationTokens(Text renewer) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
    return null;
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, true);
    return this.securityManager.renewDelegationToken(token);
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, true);
    this.securityManager.cancelDelegationToken(token);
    return;
  }

  @Override
  public LocatedBlocks getBlockLocations(String src, final long offset,
      final long length) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod remoteMethod = new RemoteMethod("getBlockLocations",
        new Class<?>[] {String.class, long.class, long.class},
        new RemoteParam(), offset, length);
    return rpcClient.invokeSequential(locations, remoteMethod,
        LocatedBlocks.class, null);
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);
    long now = Time.monotonicNow();
    if ((serverDefaults == null) || (now - serverDefaultsLastUpdate
        > serverDefaultsValidityPeriod)) {
      RemoteMethod method = new RemoteMethod("getServerDefaults");
      serverDefaults =
          rpcServer.invokeAtAvailableNs(method, FsServerDefaults.class);
      serverDefaultsLastUpdate = now;
    }
    return serverDefaults;
  }

  @Override
  public HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag,
      boolean createParent, short replication, long blockSize,
      CryptoProtocolVersion[] supportedVersions, String ecPolicyName,
      String storagePolicy)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    if (createParent && rpcServer.isPathAll(src)) {
      int index = src.lastIndexOf(Path.SEPARATOR);
      String parent = src.substring(0, index);
      LOG.debug("Creating {} requires creating parent {}", src, parent);
      FsPermission parentPermissions = getParentPermission(masked);
      boolean success = mkdirs(parent, parentPermissions, createParent);
      if (!success) {
        // This shouldn't happen as mkdirs returns true or exception
        LOG.error("Couldn't create parents for {}", src);
      }
    }

    RemoteMethod method = new RemoteMethod("create",
        new Class<?>[] {String.class, FsPermission.class, String.class,
            EnumSetWritable.class, boolean.class, short.class,
            long.class, CryptoProtocolVersion[].class,
            String.class, String.class},
        new RemoteParam(), masked, clientName, flag, createParent,
        replication, blockSize, supportedVersions, ecPolicyName, storagePolicy);
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
    RemoteLocation createLocation = null;
    try {
      createLocation = rpcServer.getCreateLocation(src, locations);
      HdfsFileStatus status = rpcClient.invokeSingle(createLocation, method,
          HdfsFileStatus.class);
      status.setNamespace(createLocation.getNameserviceId());
      return status;
    } catch (IOException ioe) {
      final List<RemoteLocation> newLocations = checkFaultTolerantRetry(
          method, src, ioe, createLocation, locations);
      return rpcClient.invokeSequential(
          newLocations, method, HdfsFileStatus.class, null);
    }
  }

  /**
   * Check if an exception is caused by an unavailable subcluster or not. It
   * also checks the causes.
   * @param ioe IOException to check.
   * @return If caused by an unavailable subcluster. False if they should not be
   *         retried (e.g., NSQuotaExceededException).
   */
  protected static boolean isUnavailableSubclusterException(
      final IOException ioe) {
    if (ioe instanceof ConnectException ||
        ioe instanceof ConnectTimeoutException ||
        ioe instanceof NoNamenodesAvailableException) {
      return true;
    }
    if (ioe.getCause() instanceof IOException) {
      IOException cause = (IOException)ioe.getCause();
      return isUnavailableSubclusterException(cause);
    }
    return false;
  }

  /**
   * Check if a remote method can be retried in other subclusters when it
   * failed in the original destination. This method returns the list of
   * locations to retry in. This is used by fault tolerant mount points.
   * @param method Method that failed and might be retried.
   * @param src Path where the method was invoked.
   * @param ioe Exception that was triggered.
   * @param excludeLoc Location that failed and should be excluded.
   * @param locations All the locations to retry.
   * @return The locations where we should retry (excluding the failed ones).
   * @throws IOException If this path is not fault tolerant or the exception
   *                     should not be retried (e.g., NSQuotaExceededException).
   */
  private List<RemoteLocation> checkFaultTolerantRetry(
      final RemoteMethod method, final String src, final IOException ioe,
      final RemoteLocation excludeLoc, final List<RemoteLocation> locations)
          throws IOException {

    if (!isUnavailableSubclusterException(ioe)) {
      LOG.debug("{} exception cannot be retried",
          ioe.getClass().getSimpleName());
      throw ioe;
    }
    if (!rpcServer.isPathFaultTolerant(src)) {
      LOG.debug("{} does not allow retrying a failed subcluster", src);
      throw ioe;
    }

    final List<RemoteLocation> newLocations;
    if (excludeLoc == null) {
      LOG.error("Cannot invoke {} for {}: {}", method, src, ioe.getMessage());
      newLocations = locations;
    } else {
      LOG.error("Cannot invoke {} for {} in {}: {}",
          method, src, excludeLoc, ioe.getMessage());
      newLocations = new ArrayList<>();
      for (final RemoteLocation loc : locations) {
        if (!loc.equals(excludeLoc)) {
          newLocations.add(loc);
        }
      }
    }
    LOG.info("{} allows retrying failed subclusters in {}", src, newLocations);
    return newLocations;
  }

  @Override
  public LastBlockWithStatus append(String src, final String clientName,
      final EnumSetWritable<CreateFlag> flag) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("append",
        new Class<?>[] {String.class, String.class, EnumSetWritable.class},
        new RemoteParam(), clientName, flag);
    RemoteResult result = rpcClient.invokeSequential(
        method, locations, LastBlockWithStatus.class, null);
    LastBlockWithStatus lbws = (LastBlockWithStatus) result.getResult();
    lbws.getFileStatus().setNamespace(result.getLocation().getNameserviceId());
    return lbws;
  }

  @Override
  public boolean recoverLease(String src, String clientName)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true, false);
    RemoteMethod method = new RemoteMethod("recoverLease",
        new Class<?>[] {String.class, String.class}, new RemoteParam(),
        clientName);
    Object result = rpcClient.invokeSequential(
        locations, method, Boolean.class, null);
    return (boolean) result;
  }

  @Override
  public boolean setReplication(String src, short replication)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setReplication",
        new Class<?>[] {String.class, short.class}, new RemoteParam(),
        replication);
    if (rpcServer.isInvokeConcurrent(src)) {
      return !rpcClient.invokeConcurrent(locations, method, Boolean.class)
          .containsValue(false);
    } else {
      return rpcClient.invokeSequential(locations, method, Boolean.class,
          Boolean.TRUE);
    }
  }

  @Override
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    storagePolicy.setStoragePolicy(src, policyName);
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    return storagePolicy.getStoragePolicies();
  }

  @Override
  public void setPermission(String src, FsPermission permissions)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("setPermission",
        new Class<?>[] {String.class, FsPermission.class},
        new RemoteParam(), permissions);
    if (rpcServer.isInvokeConcurrent(src)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  @Override
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("setOwner",
        new Class<?>[] {String.class, String.class, String.class},
        new RemoteParam(), username, groupname);
    if (rpcServer.isInvokeConcurrent(src)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  /**
   * Excluded and favored nodes are not verified and will be ignored by
   * placement policy if they are not in the same nameservice as the file.
   */
  @Override
  public LocatedBlock addBlock(String src, String clientName,
      ExtendedBlock previous, DatanodeInfo[] excludedNodes, long fileId,
      String[] favoredNodes, EnumSet<AddBlockFlag> addBlockFlags)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("addBlock",
        new Class<?>[] {String.class, String.class, ExtendedBlock.class,
            DatanodeInfo[].class, long.class, String[].class,
            EnumSet.class},
        new RemoteParam(), clientName, previous, excludedNodes, fileId,
        favoredNodes, addBlockFlags);

    final List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, true);
    if (previous != null) {
      return rpcClient.invokeSingle(previous, method, locations, LocatedBlock.class);
    }
    // TODO verify the excludedNodes and favoredNodes are acceptable to this NN
    return rpcClient.invokeSequential(
        locations, method, LocatedBlock.class, null);
  }

  /**
   * Excluded nodes are not verified and will be ignored by placement if they
   * are not in the same nameservice as the file.
   */
  @Override
  public LocatedBlock getAdditionalDatanode(final String src, final long fileId,
      final ExtendedBlock blk, final DatanodeInfo[] existings,
      final String[] existingStorageIDs, final DatanodeInfo[] excludes,
      final int numAdditionalNodes, final String clientName)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getAdditionalDatanode",
        new Class<?>[] {String.class, long.class, ExtendedBlock.class,
            DatanodeInfo[].class, String[].class,
            DatanodeInfo[].class, int.class, String.class},
        new RemoteParam(), fileId, blk, existings, existingStorageIDs, excludes,
        numAdditionalNodes, clientName);

    final List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, false);
    if (blk != null) {
      return rpcClient.invokeSingle(blk, method, locations, LocatedBlock.class);
    }

    return rpcClient.invokeSequential(
        locations, method, LocatedBlock.class, null);
  }

  @Override
  public void abandonBlock(ExtendedBlock b, long fileId, String src,
      String holder) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("abandonBlock",
        new Class<?>[] {ExtendedBlock.class, long.class, String.class,
            String.class},
        b, fileId, new RemoteParam(), holder);

    final List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, false);
    rpcClient.invokeSingle(b, method, locations, Void.class);
  }

  @Override
  public boolean complete(String src, String clientName, ExtendedBlock last,
      long fileId) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("complete",
        new Class<?>[] {String.class, String.class, ExtendedBlock.class,
            long.class},
        new RemoteParam(), clientName, last, fileId);

    final List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, true);
    if (last != null) {
      return rpcClient.invokeSingle(last, method, locations, Boolean.class);
    }

    // Complete can return true/false, so don't expect a result
    return rpcClient.invokeSequential(locations, method, Boolean.class, null);
  }

  @Override
  public LocatedBlock updateBlockForPipeline(
      ExtendedBlock block, String clientName) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("updateBlockForPipeline",
        new Class<?>[] {ExtendedBlock.class, String.class},
        block, clientName);
    return rpcClient.invokeSingle(block, method, LocatedBlock.class);
  }

  /**
   * Datanode are not verified to be in the same nameservice as the old block.
   * TODO This may require validation.
   */
  @Override
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("updatePipeline",
        new Class<?>[] {String.class, ExtendedBlock.class, ExtendedBlock.class,
            DatanodeID[].class, String[].class},
        clientName, oldBlock, newBlock, newNodes, newStorageIDs);
    rpcClient.invokeSingleBlockPool(oldBlock.getBlockPoolId(), method);
  }

  @Override
  public long getPreferredBlockSize(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true, false);
    RemoteMethod method = new RemoteMethod("getPreferredBlockSize",
        new Class<?>[] {String.class}, new RemoteParam());
    return rpcClient.invokeSequential(locations, method, Long.class, null);
  }

  @Deprecated
  @Override
  public boolean rename(final String src, final String dst)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> srcLocations =
        rpcServer.getLocationsForPath(src, true, false);
    final List<RemoteLocation> dstLocations =
        rpcServer.getLocationsForPath(dst, false, false);
    // srcLocations may be trimmed by getRenameDestinations()
    final List<RemoteLocation> locs = new LinkedList<>(srcLocations);
    RemoteParam dstParam = getRenameDestinations(locs, dstLocations);
    if (locs.isEmpty()) {
      return rbfRename.routerFedRename(src, dst, srcLocations, dstLocations);
    }
    RemoteMethod method = new RemoteMethod("rename",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), dstParam);
    if (isMultiDestDirectory(src)) {
      if (locs.size() != srcLocations.size()) {
        throw new IOException("Rename of " + src + " to " + dst + " is not"
            + " allowed. The number of remote locations for both source and"
            + " target should be same.");
      }
      return rpcClient.invokeAll(locs, method);
    } else {
      return rpcClient.invokeSequential(locs, method, Boolean.class,
          Boolean.TRUE);
    }
  }

  @Override
  public void rename2(final String src, final String dst,
      final Options.Rename... options) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> srcLocations =
        rpcServer.getLocationsForPath(src, true, false);
    final List<RemoteLocation> dstLocations =
        rpcServer.getLocationsForPath(dst, false, false);
    // srcLocations may be trimmed by getRenameDestinations()
    final List<RemoteLocation> locs = new LinkedList<>(srcLocations);
    RemoteParam dstParam = getRenameDestinations(locs, dstLocations);
    if (locs.isEmpty()) {
      rbfRename.routerFedRename(src, dst, srcLocations, dstLocations);
      return;
    }
    RemoteMethod method = new RemoteMethod("rename2",
        new Class<?>[] {String.class, String.class, options.getClass()},
        new RemoteParam(), dstParam, options);
    if (isMultiDestDirectory(src)) {
      if (locs.size() != srcLocations.size()) {
        throw new IOException("Rename of " + src + " to " + dst + " is not"
            + " allowed. The number of remote locations for both source and"
            + " target should be same.");
      }
      rpcClient.invokeConcurrent(locs, method);
    } else {
      rpcClient.invokeSequential(locs, method, null, null);
    }
  }

  @Override
  public void concat(String trg, String[] src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

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
        rpcServer.getLocationForPath(trg, true, targetBlockPoolId);
    String[] sourceDestinations = new String[src.length];
    for (int i = 0; i < src.length; i++) {
      String sourceFile = src[i];
      RemoteLocation location =
          rpcServer.getLocationForPath(sourceFile, true, targetBlockPoolId);
      sourceDestinations[i] = location.getDest();
    }
    // Invoke
    RemoteMethod method = new RemoteMethod("concat",
        new Class<?>[] {String.class, String[].class},
        targetDestination.getDest(), sourceDestinations);
    rpcClient.invokeSingle(targetDestination, method, Void.class);
  }

  @Override
  public boolean truncate(String src, long newLength, String clientName)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("truncate",
        new Class<?>[] {String.class, long.class, String.class},
        new RemoteParam(), newLength, clientName);
    // Truncate can return true/false, so don't expect a result
    return rpcClient.invokeSequential(locations, method, Boolean.class,
        null);
  }

  @Override
  public boolean delete(String src, boolean recursive) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true, false);
    RemoteMethod method = new RemoteMethod("delete",
        new Class<?>[] {String.class, boolean.class}, new RemoteParam(),
        recursive);
    if (rpcServer.isPathAll(src)) {
      return rpcClient.invokeAll(locations, method);
    } else {
      return rpcClient.invokeSequential(locations, method,
          Boolean.class, Boolean.TRUE);
    }
  }

  @Override
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("mkdirs",
        new Class<?>[] {String.class, FsPermission.class, boolean.class},
        new RemoteParam(), masked, createParent);

    // Create in all locations
    if (rpcServer.isPathAll(src)) {
      return rpcClient.invokeAll(locations, method);
    }

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
        LOG.error("Error getting file info for {} while proxying mkdirs: {}",
            src, ioe.getMessage());
      }
    }

    final RemoteLocation firstLocation = locations.get(0);
    try {
      return rpcClient.invokeSingle(firstLocation, method, Boolean.class);
    } catch (IOException ioe) {
      final List<RemoteLocation> newLocations = checkFaultTolerantRetry(
          method, src, ioe, firstLocation, locations);
      return rpcClient.invokeSequential(
          newLocations, method, Boolean.class, Boolean.TRUE);
    }
  }

  private Map<String, FederationNamespaceInfo> getAvailableNamespaces()
      throws IOException {
    Map<String, FederationNamespaceInfo> allAvailableNamespaces =
        new HashMap<>();
    namenodeResolver.getNamespaces().forEach(
        k -> allAvailableNamespaces.put(k.getNameserviceId(), k));
    return allAvailableNamespaces;
  }

  /**
   * Try to get a list of FederationNamespaceInfo for renewLease RPC.
   */
  private List<FederationNamespaceInfo> getRenewLeaseNSs(List<String> namespaces)
      throws IOException {
    if (namespaces == null || namespaces.isEmpty()) {
      return new ArrayList<>(namenodeResolver.getNamespaces());
    }
    List<FederationNamespaceInfo> result = new ArrayList<>();
    Map<String, FederationNamespaceInfo> allAvailableNamespaces =
        getAvailableNamespaces();
    for (String namespace : namespaces) {
      if (!allAvailableNamespaces.containsKey(namespace)) {
        return new ArrayList<>(namenodeResolver.getNamespaces());
      } else {
        result.add(allAvailableNamespaces.get(namespace));
      }
    }
    return result;
  }

  @Override
  public void renewLease(String clientName, List<String> namespaces)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("renewLease",
        new Class<?>[] {String.class, List.class}, clientName, null);
    List<FederationNamespaceInfo> nss = getRenewLeaseNSs(namespaces);
    if (nss.size() == 1) {
      rpcClient.invokeSingle(nss.get(0).getNameserviceId(), method);
    } else {
      rpcClient.invokeConcurrent(nss, method, false, false);
    }
  }

  /**
   * For {@link #getListing(String,byte[],boolean) GetLisiting} to sort results.
   */
  private static class GetListingComparator
      implements Comparator<byte[]>, Serializable {
    @Override
    public int compare(byte[] o1, byte[] o2) {
      return DFSUtilClient.compareBytes(o1, o2);
    }
  }

  private static GetListingComparator comparator =
      new GetListingComparator();

  @Override
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    List<RemoteResult<RemoteLocation, DirectoryListing>> listings =
        getListingInt(src, startAfter, needLocation);
    TreeMap<byte[], HdfsFileStatus> nnListing = new TreeMap<>(comparator);
    int totalRemainingEntries = 0;
    int remainingEntries = 0;
    boolean namenodeListingExists = false;
    // Check the subcluster listing with the smallest name to make sure
    // no file is skipped across subclusters
    byte[] lastName = null;
    if (listings != null) {
      for (RemoteResult<RemoteLocation, DirectoryListing> result : listings) {
        if (result.hasException()) {
          IOException ioe = result.getException();
          if (ioe instanceof FileNotFoundException) {
            RemoteLocation location = result.getLocation();
            LOG.debug("Cannot get listing from {}", location);
          } else if (!allowPartialList) {
            throw ioe;
          }
        } else if (result.getResult() != null) {
          DirectoryListing listing = result.getResult();
          totalRemainingEntries += listing.getRemainingEntries();
          HdfsFileStatus[] partialListing = listing.getPartialListing();
          int length = partialListing.length;
          if (length > 0) {
            HdfsFileStatus lastLocalEntry = partialListing[length-1];
            byte[] lastLocalName = lastLocalEntry.getLocalNameInBytes();
            if (lastName == null ||
                comparator.compare(lastName, lastLocalName) > 0) {
              lastName = lastLocalName;
            }
          }
        }
      }

      // Add existing entries
      for (RemoteResult<RemoteLocation, DirectoryListing> result : listings) {
        DirectoryListing listing = result.getResult();
        if (listing != null) {
          namenodeListingExists = true;
          for (HdfsFileStatus file : listing.getPartialListing()) {
            byte[] filename = file.getLocalNameInBytes();
            if (totalRemainingEntries > 0 &&
                comparator.compare(filename, lastName) > 0) {
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
        Path childPath = new Path(src, child);
        HdfsFileStatus dirStatus =
            getMountPointStatus(childPath.toString(), 0, date);

        // if there is no subcluster path, always add mount point
        byte[] bChild = DFSUtil.string2Bytes(child);
        if (lastName == null) {
          nnListing.put(bChild, dirStatus);
        } else {
          if (shouldAddMountPoint(bChild,
                lastName, startAfter, remainingEntries)) {
            // This may overwrite existing listing entries with the mount point
            // TODO don't add if already there?
            nnListing.put(bChild, dirStatus);
          }
        }
      }
      // Update the remaining count to include left mount points
      if (nnListing.size() > 0) {
        byte[] lastListing = nnListing.lastKey();
        for (int i = 0; i < children.size(); i++) {
          byte[] bChild = DFSUtil.string2Bytes(children.get(i));
          if (comparator.compare(bChild, lastListing) > 0) {
            remainingEntries += (children.size() - i);
            break;
          }
        }
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

  @Override
  public BatchedDirectoryListing getBatchedListing(String[] srcs,
      byte[] startAfter, boolean needLocation) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    HdfsFileStatus ret = null;
    IOException noLocationException = null;
    try {
      final List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, false, false);
      RemoteMethod method = new RemoteMethod("getFileInfo",
          new Class<?>[] {String.class}, new RemoteParam());

      // If it's a directory, we check in all locations
      if (rpcServer.isPathAll(src)) {
        ret = getFileInfoAll(locations, method);
      } else {
        // Check for file information sequentially
        ret = rpcClient.invokeSequential(locations, method, HdfsFileStatus.class, null);
      }
    } catch (NoLocationException | RouterResolveException e) {
      noLocationException = e;
    }

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
      } else if (children != null) {
        // The src is a mount point, but there are no files or directories
        ret = getMountPointStatus(src, 0, 0);
      }
    }

    // Can't find mount point for path and the path didn't contain any sub monit points,
    // throw the NoLocationException to client.
    if (ret == null && noLocationException != null) {
      throw noLocationException;
    }

    return ret;
  }

  @Override
  public boolean isFileClosed(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("isFileClosed",
        new Class<?>[] {String.class}, new RemoteParam());
    return rpcClient.invokeSequential(locations, method, Boolean.class,
        null);
  }

  @Override
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("getFileLinkInfo",
        new Class<?>[] {String.class}, new RemoteParam());
    return rpcClient.invokeSequential(locations, method, HdfsFileStatus.class,
        null);
  }

  @Override
  public HdfsLocatedFileStatus getLocatedFileInfo(String src,
      boolean needBlockToken) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("getLocatedFileInfo",
        new Class<?>[] {String.class, boolean.class}, new RemoteParam(),
        needBlockToken);
    return (HdfsLocatedFileStatus) rpcClient.invokeSequential(
        locations, method, HdfsFileStatus.class, null);
  }

  @Override
  public long[] getStats() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("getStats");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, long[]> results =
        rpcClient.invokeConcurrent(nss, method, true, false, long[].class);
    long[] combinedData = new long[STATS_ARRAY_LENGTH];
    for (long[] data : results.values()) {
      for (int i = 0; i < combinedData.length && i < data.length; i++) {
        if (data[i] >= 0) {
          combinedData[i] += data[i];
        }
      }
    }
    return combinedData;
  }

  @Override
  public DatanodeInfo[] getDatanodeReport(HdfsConstants.DatanodeReportType type)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);
    return rpcServer.getDatanodeReport(type, true, 0);
  }

  @Override
  public DatanodeStorageReport[] getDatanodeStorageReport(
      HdfsConstants.DatanodeReportType type) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    Map<String, DatanodeStorageReport[]> dnSubcluster =
        rpcServer.getDatanodeStorageReportMap(type);
    return mergeDtanodeStorageReport(dnSubcluster);
  }

  public DatanodeStorageReport[] getDatanodeStorageReport(
      HdfsConstants.DatanodeReportType type, boolean requireResponse, long timeOutMs)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    Map<String, DatanodeStorageReport[]> dnSubcluster =
        rpcServer.getDatanodeStorageReportMap(type, requireResponse, timeOutMs);
    return mergeDtanodeStorageReport(dnSubcluster);
  }

  private DatanodeStorageReport[] mergeDtanodeStorageReport(
      Map<String, DatanodeStorageReport[]> dnSubcluster) {
    // Avoid repeating machines in multiple subclusters
    Map<String, DatanodeStorageReport> datanodesMap = new LinkedHashMap<>();
    for (DatanodeStorageReport[] dns : dnSubcluster.values()) {
      for (DatanodeStorageReport dn : dns) {
        DatanodeInfo dnInfo = dn.getDatanodeInfo();
        String nodeId = dnInfo.getXferAddr();
        DatanodeStorageReport oldDn = datanodesMap.get(nodeId);
        if (oldDn == null ||
            dnInfo.getLastUpdate() > oldDn.getDatanodeInfo().getLastUpdate()) {
          datanodesMap.put(nodeId, dn);
        } else {
          LOG.debug("{} is in multiple subclusters", nodeId);
        }
      }
    }

    Collection<DatanodeStorageReport> datanodes = datanodesMap.values();
    DatanodeStorageReport[] combinedData =
        new DatanodeStorageReport[datanodes.size()];
    combinedData = datanodes.toArray(combinedData);
    return combinedData;
  }

  @Override
  public boolean setSafeMode(HdfsConstants.SafeModeAction action,
      boolean isChecked) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // Set safe mode in all the name spaces
    RemoteMethod method = new RemoteMethod("setSafeMode",
        new Class<?>[] {HdfsConstants.SafeModeAction.class, boolean.class},
        action, isChecked);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, Boolean> results =
        rpcClient.invokeConcurrent(
            nss, method, true, !isChecked, Boolean.class);

    // We only report true if all the name space are in safe mode
    int numSafemode = 0;
    for (boolean safemode : results.values()) {
      if (safemode) {
        numSafemode++;
      }
    }
    return numSafemode == results.size();
  }

  @Override
  public boolean restoreFailedStorage(String arg) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("restoreFailedStorage",
        new Class<?>[] {String.class}, arg);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, Boolean> ret =
        rpcClient.invokeConcurrent(nss, method, true, false, Boolean.class);

    boolean success = true;
    for (boolean s : ret.values()) {
      if (!s) {
        success = false;
        break;
      }
    }
    return success;
  }

  @Override
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("saveNamespace",
        new Class<?>[] {long.class, long.class}, timeWindow, txGap);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, Boolean> ret =
        rpcClient.invokeConcurrent(nss, method, true, false, boolean.class);

    boolean success = true;
    for (boolean s : ret.values()) {
      if (!s) {
        success = false;
        break;
      }
    }
    return success;
  }

  @Override
  public long rollEdits() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("rollEdits", new Class<?>[] {});
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, Long> ret =
        rpcClient.invokeConcurrent(nss, method, true, false, long.class);

    // Return the maximum txid
    long txid = 0;
    for (long t : ret.values()) {
      if (t > txid) {
        txid = t;
      }
    }
    return txid;
  }

  @Override
  public void refreshNodes() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("refreshNodes", new Class<?>[] {});
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, true);
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("finalizeUpgrade",
        new Class<?>[] {});
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false);
  }

  @Override
  public boolean upgradeStatus() throws IOException {
    String methodName = RouterRpcServer.getMethodName();
    throw new UnsupportedOperationException(
        "Operation \"" + methodName + "\" is not supported");
  }

  @Override
  public RollingUpgradeInfo rollingUpgrade(HdfsConstants.RollingUpgradeAction action)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("rollingUpgrade",
        new Class<?>[] {HdfsConstants.RollingUpgradeAction.class}, action);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, RollingUpgradeInfo> ret =
        rpcClient.invokeConcurrent(
            nss, method, true, false, RollingUpgradeInfo.class);

    // Return the first rolling upgrade info
    RollingUpgradeInfo info = null;
    for (RollingUpgradeInfo infoNs : ret.values()) {
      if (info == null && infoNs != null) {
        info = infoNs;
      }
    }
    return info;
  }

  @Override
  public void metaSave(String filename) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("metaSave",
        new Class<?>[] {String.class}, filename);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false);
  }

  @Override
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(path, false, false);
    RemoteMethod method = new RemoteMethod("listCorruptFileBlocks",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), cookie);
    return rpcClient.invokeSequential(
        locations, method, CorruptFileBlocks.class, null);
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("setBalancerBandwidth",
        new Class<?>[] {long.class}, bandwidth);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false);
  }

  /**
   * Recursively get all the locations for the path.
   * For example, there are some mount points:
   *   /a -> ns0 -> /a
   *   /a/b -> ns1 -> /a/b
   *   /a/b/c -> ns2 -> /a/b/c
   * When the path is '/a', the result of locations should be
   * {ns0 -> [RemoteLocation(/a)], ns1 -> [RemoteLocation(/a/b)], ns2 -> [RemoteLocation(/a/b/c)]}
   * @param path the path to get the locations.
   * @return a map to store all the locations and key is namespace id.
   * @throws IOException
   */
  @VisibleForTesting
  Map<String, List<RemoteLocation>> getAllLocations(String path) throws IOException {
    Map<String, List<RemoteLocation>> locations = new HashMap<>();
    try {
      List<RemoteLocation> parentLocations = rpcServer.getLocationsForPath(path, false, false);
      parentLocations.forEach(
          l -> locations.computeIfAbsent(l.getNameserviceId(), k -> new ArrayList<>()).add(l));
    } catch (NoLocationException | RouterResolveException e) {
      LOG.debug("Cannot find locations for {}.", path);
    }

    final List<String> children = subclusterResolver.getMountPoints(path);
    if (children != null) {
      for (String child : children) {
        String childPath = new Path(path, child).toUri().getPath();
        Map<String, List<RemoteLocation>> childLocations = getAllLocations(childPath);
        childLocations.forEach(
            (k, v) -> locations.computeIfAbsent(k, l -> new ArrayList<>()).addAll(v));
      }
    }
    return locations;
  }

  /**
   * Get all the locations of the path for {@link this#getContentSummary(String)}.
   * For example, there are some mount points:
   *   /a -> ns0 -> /a
   *   /a/b -> ns0 -> /a/b
   *   /a/b/c -> ns1 -> /a/b/c
   * When the path is '/a', the result of locations should be
   * [RemoteLocation('/a', ns0, '/a'), RemoteLocation('/a/b/c', ns1, '/a/b/c')]
   * When the path is '/b', will throw NoLocationException.
   * @param path the path to get content summary
   * @return one list contains all the remote location
   * @throws IOException
   */
  @VisibleForTesting
  List<RemoteLocation> getLocationsForContentSummary(String path) throws IOException {
    // Try to get all the locations of the path.
    final Map<String, List<RemoteLocation>> ns2Locations = getAllLocations(path);
    if (ns2Locations.isEmpty()) {
      throw new NoLocationException(path, subclusterResolver.getClass());
    }

    final List<RemoteLocation> locations = new ArrayList<>();
    // remove the redundancy remoteLocation order by destination.
    ns2Locations.forEach((k, v) -> {
      List<RemoteLocation> sortedList = v.stream().sorted().collect(Collectors.toList());
      int size = sortedList.size();
      for (int i = size - 1; i > -1; i--) {
        RemoteLocation currentLocation = sortedList.get(i);
        if (i == 0) {
          locations.add(currentLocation);
        } else {
          RemoteLocation preLocation = sortedList.get(i - 1);
          if (!currentLocation.getDest().startsWith(preLocation.getDest() + Path.SEPARATOR)) {
            locations.add(currentLocation);
          } else {
            LOG.debug("Ignore redundant location {}, because there is an ancestor location {}",
                currentLocation, preLocation);
          }
        }
      }
    });

    return locations;
  }

  @Override
  public ContentSummary getContentSummary(String path) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // Get the summaries from regular files
    final Collection<ContentSummary> summaries = new ArrayList<>();
    final List<RemoteLocation> locations = getLocationsForContentSummary(path);
    final RemoteMethod method = new RemoteMethod("getContentSummary",
        new Class<?>[] {String.class}, new RemoteParam());
    final List<RemoteResult<RemoteLocation, ContentSummary>> results =
        rpcClient.invokeConcurrent(locations, method,
            false, -1, ContentSummary.class);
    FileNotFoundException notFoundException = null;
    for (RemoteResult<RemoteLocation, ContentSummary> result : results) {
      if (result.hasException()) {
        IOException ioe = result.getException();
        if (ioe instanceof FileNotFoundException) {
          notFoundException = (FileNotFoundException)ioe;
        } else if (!allowPartialList) {
          throw ioe;
        }
      } else if (result.getResult() != null) {
        summaries.add(result.getResult());
      }
    }

    // Throw original exception if no original nor mount points
    if (summaries.isEmpty() && notFoundException != null) {
      throw notFoundException;
    }

    return aggregateContentSummary(summaries);
  }

  @Override
  public void fsync(String src, long fileId, String clientName,
      long lastBlockLength) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true, false);
    RemoteMethod method = new RemoteMethod("fsync",
        new Class<?>[] {String.class, long.class, String.class, long.class },
        new RemoteParam(), fileId, clientName, lastBlockLength);
    rpcClient.invokeSequential(locations, method);
  }

  @Override
  public void setTimes(String src, long mtime, long atime) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("setTimes",
        new Class<?>[] {String.class, long.class, long.class},
        new RemoteParam(), mtime, atime);
    rpcClient.invokeSequential(locations, method);
  }

  @Override
  public void createSymlink(String target, String link, FsPermission dirPerms,
      boolean createParent) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // TODO Verify that the link location is in the same NS as the targets
    final List<RemoteLocation> targetLocations =
        rpcServer.getLocationsForPath(target, true);
    final List<RemoteLocation> linkLocations =
        rpcServer.getLocationsForPath(link, true);
    RemoteLocation linkLocation = linkLocations.get(0);
    RemoteMethod method = new RemoteMethod("createSymlink",
        new Class<?>[] {String.class, String.class, FsPermission.class,
            boolean.class},
        new RemoteParam(), linkLocation.getDest(), dirPerms, createParent);
    rpcClient.invokeSequential(targetLocations, method);
  }

  @Override
  public String getLinkTarget(String path) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(path, false, false);
    RemoteMethod method = new RemoteMethod("getLinkTarget",
        new Class<?>[] {String.class}, new RemoteParam());
    return rpcClient.invokeSequential(locations, method, String.class, null);
  }

  @Override
  public void allowSnapshot(String snapshotRoot) throws IOException {
    snapshotProto.allowSnapshot(snapshotRoot);
  }

  @Override
  public void disallowSnapshot(String snapshot) throws IOException {
    snapshotProto.disallowSnapshot(snapshot);
  }

  @Override
  public void renameSnapshot(String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException {
    snapshotProto.renameSnapshot(
        snapshotRoot, snapshotOldName, snapshotNewName);
  }

  @Override
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    return snapshotProto.getSnapshottableDirListing();
  }

  @Override
  public SnapshotStatus[] getSnapshotListing(String snapshotRoot)
      throws IOException {
    return snapshotProto.getSnapshotListing(snapshotRoot);
  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
      String earlierSnapshotName, String laterSnapshotName) throws IOException {
    return snapshotProto.getSnapshotDiffReport(
        snapshotRoot, earlierSnapshotName, laterSnapshotName);
  }

  @Override
  public SnapshotDiffReportListing getSnapshotDiffReportListing(
      String snapshotRoot, String earlierSnapshotName, String laterSnapshotName,
      byte[] startPath, int index) throws IOException {
    return snapshotProto.getSnapshotDiffReportListing(
        snapshotRoot, earlierSnapshotName, laterSnapshotName, startPath, index);
  }

  @Override
  public long addCacheDirective(CacheDirectiveInfo path,
      EnumSet<CacheFlag> flags) throws IOException {
    return routerCacheAdmin.addCacheDirective(path, flags);
  }

  @Override
  public void modifyCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException {
    routerCacheAdmin.modifyCacheDirective(directive, flags);
  }

  @Override
  public void removeCacheDirective(long id) throws IOException {
    routerCacheAdmin.removeCacheDirective(id);
  }

  @Override
  public BatchedEntries<CacheDirectiveEntry> listCacheDirectives(long prevId,
      CacheDirectiveInfo filter) throws IOException {
    return routerCacheAdmin.listCacheDirectives(prevId, filter);
  }

  @Override
  public void addCachePool(CachePoolInfo info) throws IOException {
    routerCacheAdmin.addCachePool(info);
  }

  @Override
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    routerCacheAdmin.modifyCachePool(info);
  }

  @Override
  public void removeCachePool(String cachePoolName) throws IOException {
    routerCacheAdmin.removeCachePool(cachePoolName);
  }

  @Override
  public BatchedEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    return routerCacheAdmin.listCachePools(prevKey);
  }

  @Override
  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("modifyAclEntries",
        new Class<?>[] {String.class, List.class},
        new RemoteParam(), aclSpec);
    if (rpcServer.isInvokeConcurrent(src)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  @Override
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("removeAclEntries",
        new Class<?>[] {String.class, List.class},
        new RemoteParam(), aclSpec);
    if (rpcServer.isInvokeConcurrent(src)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  @Override
  public void removeDefaultAcl(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("removeDefaultAcl",
        new Class<?>[] {String.class}, new RemoteParam());
    if (rpcServer.isInvokeConcurrent(src)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  @Override
  public void removeAcl(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("removeAcl",
        new Class<?>[] {String.class}, new RemoteParam());
    if (rpcServer.isInvokeConcurrent(src)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  @Override
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod(
        "setAcl", new Class<?>[] {String.class, List.class},
        new RemoteParam(), aclSpec);
    if (rpcServer.isInvokeConcurrent(src)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  @Override
  public AclStatus getAclStatus(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("getAclStatus",
        new Class<?>[] {String.class}, new RemoteParam());
    return rpcClient.invokeSequential(locations, method, AclStatus.class, null);
  }

  @Override
  public void createEncryptionZone(String src, String keyName)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("createEncryptionZone",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), keyName);
    rpcClient.invokeSequential(locations, method);
  }

  @Override
  public EncryptionZone getEZForPath(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("getEZForPath",
        new Class<?>[] {String.class}, new RemoteParam());
    return rpcClient.invokeSequential(
        locations, method, EncryptionZone.class, null);
  }

  @Override
  public BatchedEntries<EncryptionZone> listEncryptionZones(long prevId)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, false);
    return null;
  }

  @Override
  public void reencryptEncryptionZone(String zone, HdfsConstants.ReencryptAction action)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
  }

  @Override
  public BatchedEntries<ZoneReencryptionStatus> listReencryptionStatus(
      long prevId) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, false);
    return null;
  }

  @Override
  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("setXAttr",
        new Class<?>[] {String.class, XAttr.class, EnumSet.class},
        new RemoteParam(), xAttr, flag);
    if (rpcServer.isInvokeConcurrent(src)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("getXAttrs",
        new Class<?>[] {String.class, List.class}, new RemoteParam(), xAttrs);
    return (List<XAttr>) rpcClient.invokeSequential(
        locations, method, List.class, null);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<XAttr> listXAttrs(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("listXAttrs",
        new Class<?>[] {String.class}, new RemoteParam());
    return (List<XAttr>) rpcClient.invokeSequential(
        locations, method, List.class, null);
  }

  @Override
  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("removeXAttr",
        new Class<?>[] {String.class, XAttr.class}, new RemoteParam(), xAttr);
    if (rpcServer.isInvokeConcurrent(src)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  @Override
  public void checkAccess(String path, FsAction mode) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(path, false, false);
    RemoteMethod method = new RemoteMethod("checkAccess",
        new Class<?>[] {String.class, FsAction.class},
        new RemoteParam(), mode);
    rpcClient.invokeSequential(locations, method);
  }

  @Override
  public long getCurrentEditLogTxid() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod(
        "getCurrentEditLogTxid", new Class<?>[] {});
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, Long> ret =
        rpcClient.invokeConcurrent(nss, method, true, false, long.class);

    // Return the maximum txid
    long txid = 0;
    for (long t : ret.values()) {
      if (t > txid) {
        txid = t;
      }
    }
    return txid;
  }

  @Override
  public EventBatchList getEditsFromTxid(long txid) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, false);
    return null;
  }

  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, false);
    return null;
  }

  @Override
  public String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    return snapshotProto.createSnapshot(snapshotRoot, snapshotName);
  }

  @Override
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    snapshotProto.deleteSnapshot(snapshotRoot, snapshotName);
  }

  @Override
  public void setQuota(String path, long namespaceQuota, long storagespaceQuota,
      StorageType type) throws IOException {
    rpcServer.getQuotaModule()
        .setQuota(path, namespaceQuota, storagespaceQuota, type, true);
  }

  @Override
  public QuotaUsage getQuotaUsage(String path) throws IOException {
    return rpcServer.getQuotaModule().getQuotaUsage(path);
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

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
    for (Map.Entry<String, List<LocatedBlock>> entry : blockLocations.entrySet()) {
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
    storagePolicy.unsetStoragePolicy(src);
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    return storagePolicy.getStoragePolicy(path);
  }

  @Override
  public ErasureCodingPolicyInfo[] getErasureCodingPolicies()
      throws IOException {
    return erasureCoding.getErasureCodingPolicies();
  }

  @Override
  public Map<String, String> getErasureCodingCodecs() throws IOException {
    return erasureCoding.getErasureCodingCodecs();
  }

  @Override
  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies) throws IOException {
    return erasureCoding.addErasureCodingPolicies(policies);
  }

  @Override
  public void removeErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    erasureCoding.removeErasureCodingPolicy(ecPolicyName);
  }

  @Override
  public void disableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    erasureCoding.disableErasureCodingPolicy(ecPolicyName);
  }

  @Override
  public void enableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    erasureCoding.enableErasureCodingPolicy(ecPolicyName);
  }

  @Override
  public ErasureCodingPolicy getErasureCodingPolicy(String src)
      throws IOException {
    return erasureCoding.getErasureCodingPolicy(src);
  }

  @Override
  public void setErasureCodingPolicy(String src, String ecPolicyName)
      throws IOException {
    erasureCoding.setErasureCodingPolicy(src, ecPolicyName);
  }

  @Override
  public void unsetErasureCodingPolicy(String src) throws IOException {
    erasureCoding.unsetErasureCodingPolicy(src);
  }

  @Override
  public ECTopologyVerifierResult getECTopologyResultForPolicies(
      String... policyNames) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED, true);
    return erasureCoding.getECTopologyResultForPolicies(policyNames);
  }

  @Override
  public ECBlockGroupStats getECBlockGroupStats() throws IOException {
    return erasureCoding.getECBlockGroupStats();
  }

  @Override
  public ReplicatedBlockStats getReplicatedBlockStats() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getReplicatedBlockStats");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, ReplicatedBlockStats> ret = rpcClient
        .invokeConcurrent(nss, method, true, false, ReplicatedBlockStats.class);
    return ReplicatedBlockStats.merge(ret.values());
  }

  @Deprecated
  @Override
  public BatchedEntries<OpenFileEntry> listOpenFiles(long prevId)
      throws IOException {
    return listOpenFiles(prevId,
        EnumSet.of(OpenFilesIterator.OpenFilesType.ALL_OPEN_FILES),
        OpenFilesIterator.FILTER_PATH_DEFAULT);
  }

  @Override
  public BatchedEntries<OpenFileEntry> listOpenFiles(long prevId,
      EnumSet<OpenFilesIterator.OpenFilesType> openFilesTypes, String path)
          throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, false);
    return null;
  }

  @Override
  public void msync() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, true);
    // Only msync to nameservices with observer reads enabled.
    Set<FederationNamespaceInfo> allNamespaces = namenodeResolver.getNamespaces();
    RemoteMethod method = new RemoteMethod("msync");
    Set<FederationNamespaceInfo> namespacesEligibleForObserverReads = allNamespaces
        .stream()
        .filter(ns -> rpcClient.isNamespaceObserverReadEligible(ns.getNameserviceId()))
        .collect(Collectors.toSet());
    if (namespacesEligibleForObserverReads.isEmpty()) {
      return;
    }
    rpcClient.invokeConcurrent(namespacesEligibleForObserverReads, method);
  }

  @Override
  public void satisfyStoragePolicy(String path) throws IOException {
    storagePolicy.satisfyStoragePolicy(path);
  }

  @Override
  public DatanodeInfo[] getSlowDatanodeReport() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);
    return rpcServer.getSlowDatanodeReport(true, 0);
  }

  @Override
  public HAServiceProtocol.HAServiceState getHAServiceState() {
    if (rpcServer.isSafeMode()) {
      return HAServiceProtocol.HAServiceState.STANDBY;
    }
    return HAServiceProtocol.HAServiceState.ACTIVE;
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
   * @param dstLocations The destination path
   * @return A map of all eligible source namespaces and their corresponding
   *         replacement value.
   * @throws IOException If the dst paths could not be determined.
   */
  private RemoteParam getRenameDestinations(
      final List<RemoteLocation> srcLocations,
      final List<RemoteLocation> dstLocations) throws IOException {

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

  /**
   * Aggregate content summaries for each subcluster.
   * If the mount point has multiple destinations
   * add the quota set value only once.
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
    long snapshotDirectoryCount = 0;
    long snapshotFileCount = 0;
    String ecPolicy = "";

    for (ContentSummary summary : summaries) {
      length += summary.getLength();
      fileCount += summary.getFileCount();
      directoryCount += summary.getDirectoryCount();
      quota = summary.getQuota();
      spaceConsumed += summary.getSpaceConsumed();
      spaceQuota = summary.getSpaceQuota();
      snapshotDirectoryCount += summary.getSnapshotDirectoryCount();
      snapshotFileCount += summary.getSnapshotFileCount();
      // We return from the first response as we assume that the EC policy
      // of each sub-cluster is same.
      if (ecPolicy.isEmpty()) {
        ecPolicy = summary.getErasureCodingPolicy();
      }
    }

    ContentSummary ret = new ContentSummary.Builder()
        .length(length)
        .fileCount(fileCount)
        .directoryCount(directoryCount)
        .quota(quota)
        .spaceConsumed(spaceConsumed)
        .spaceQuota(spaceQuota)
        .erasureCodingPolicy(ecPolicy)
        .snapshotDirectoryCount(snapshotDirectoryCount)
        .snapshotFileCount(snapshotFileCount)
        .build();
    return ret;
  }

  /**
   * Get the file info from all the locations.
   *
   * @param locations Locations to check.
   * @param method The file information method to run.
   * @return The first file info if it's a file, the directory if it's
   *         everywhere.
   * @throws IOException If all the locations throw an exception.
   */
  private HdfsFileStatus getFileInfoAll(final List<RemoteLocation> locations,
      final RemoteMethod method) throws IOException {
    return getFileInfoAll(locations, method, -1);
  }

  /**
   * Get the file info from all the locations.
   *
   * @param locations Locations to check.
   * @param method The file information method to run.
   * @param timeOutMs Time out for the operation in milliseconds.
   * @return The first file info if it's a file, the directory if it's
   *         everywhere.
   * @throws IOException If all the locations throw an exception.
   */
  private HdfsFileStatus getFileInfoAll(final List<RemoteLocation> locations,
      final RemoteMethod method, long timeOutMs) throws IOException {

    // Get the file info from everybody
    Map<RemoteLocation, HdfsFileStatus> results =
        rpcClient.invokeConcurrent(locations, method, false, false, timeOutMs,
            HdfsFileStatus.class);
    int children = 0;
    // We return the first file
    HdfsFileStatus dirStatus = null;
    for (RemoteLocation loc : locations) {
      HdfsFileStatus fileStatus = results.get(loc);
      if (fileStatus != null) {
        children += fileStatus.getChildrenNum();
        if (!fileStatus.isDirectory()) {
          return fileStatus;
        } else if (dirStatus == null) {
          dirStatus = fileStatus;
        }
      }
    }
    if (dirStatus != null) {
      return updateMountPointStatus(dirStatus, children);
    }
    return null;
  }

  /**
   * Get the permissions for the parent of a child with given permissions.
   * Add implicit u+wx permission for parent. This is based on
   * @{FSDirMkdirOp#addImplicitUwx}.
   * @param mask The permission mask of the child.
   * @return The permission mask of the parent.
   */
  private static FsPermission getParentPermission(final FsPermission mask) {
    FsPermission ret = new FsPermission(
        mask.getUserAction().or(FsAction.WRITE_EXECUTE),
        mask.getGroupAction(),
        mask.getOtherAction());
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
  @VisibleForTesting
  HdfsFileStatus getMountPointStatus(
      String name, int childrenNum, long date) {
    long modTime = date;
    long accessTime = date;
    FsPermission permission = FsPermission.getDirDefault();
    String owner = this.superUser;
    String group = this.superGroup;
    EnumSet<HdfsFileStatus.Flags> flags =
        EnumSet.noneOf(HdfsFileStatus.Flags.class);
    if (subclusterResolver instanceof MountTableResolver) {
      try {
        String mName = name.startsWith("/") ? name : "/" + name;
        MountTableResolver mountTable = (MountTableResolver) subclusterResolver;
        MountTable entry = mountTable.getMountPoint(mName);
        if (entry != null) {
          permission = entry.getMode();
          owner = entry.getOwnerName();
          group = entry.getGroupName();

          RemoteMethod method = new RemoteMethod("getFileInfo",
              new Class<?>[] {String.class}, new RemoteParam());
          HdfsFileStatus fInfo = getFileInfoAll(
              entry.getDestinations(), method, mountStatusTimeOut);
          if (fInfo != null) {
            permission = fInfo.getPermission();
            owner = fInfo.getOwner();
            group = fInfo.getGroup();
            childrenNum = fInfo.getChildrenNum();
            flags = DFSUtil
                .getFlags(fInfo.isEncrypted(), fInfo.isErasureCoded(),
                    fInfo.isSnapshotEnabled(), fInfo.hasAcl());
          }
        }
      } catch (IOException e) {
        LOG.error("Cannot get mount point: {}", e.getMessage());
      }
    } else {
      try {
        UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
        owner = ugi.getUserName();
        group = ugi.getPrimaryGroupName();
      } catch (IOException e) {
        String msg = "Cannot get remote user: " + e.getMessage();
        if (UserGroupInformation.isSecurityEnabled()) {
          LOG.error(msg);
        } else {
          LOG.debug(msg);
        }
      }
    }
    long inodeId = 0;
    Path path = new Path(name);
    String nameStr = path.getName();
    return new HdfsFileStatus.Builder()
        .isdir(true)
        .mtime(modTime)
        .atime(accessTime)
        .perm(permission)
        .owner(owner)
        .group(group)
        .symlink(new byte[0])
        .path(DFSUtil.string2Bytes(nameStr))
        .fileId(inodeId)
        .children(childrenNum)
        .flags(flags)
        .build();
  }

  /**
   * Get the modification dates for mount points.
   *
   * @param path Name of the path to start checking dates from.
   * @return Map with the modification dates for all sub-entries.
   */
  private Map<String, Long> getMountPointDates(String path) {
    Map<String, Long> ret = new TreeMap<>();
    if (subclusterResolver instanceof MountTableResolver) {
      try {
        final List<String> children = subclusterResolver.getMountPoints(path);
        for (String child : children) {
          Long modTime = getModifiedTime(ret, path, child);
          ret.put(child, modTime);
        }
      } catch (IOException e) {
        LOG.error("Cannot get mount point", e);
      }
    }
    return ret;
  }

  /**
   * Get modified time for child. If the child is present in mount table it
   * will return the modified time. If the child is not present but subdirs of
   * this child are present then it will return latest modified subdir's time
   * as modified time of the requested child.
   *
   * @param ret contains children and modified times.
   * @param path Name of the path to start checking dates from.
   * @param child child of the requested path.
   * @return modified time.
   */
  private long getModifiedTime(Map<String, Long> ret, String path,
      String child) {
    MountTableResolver mountTable = (MountTableResolver)subclusterResolver;
    String srcPath;
    if (path.equals(Path.SEPARATOR)) {
      srcPath = Path.SEPARATOR + child;
    } else {
      srcPath = path + Path.SEPARATOR + child;
    }
    Long modTime = 0L;
    try {
      // Get mount table entry for the srcPath
      MountTable entry = mountTable.getMountPoint(srcPath);
      // if srcPath is not in mount table but its subdirs are in mount
      // table we will display latest modified subdir date/time.
      if (entry == null) {
        List<MountTable> entries = mountTable.getMounts(srcPath);
        for (MountTable eachEntry : entries) {
          // Get the latest date
          if (ret.get(child) == null ||
              ret.get(child) < eachEntry.getDateModified()) {
            modTime = eachEntry.getDateModified();
          }
        }
      } else {
        modTime = entry.getDateModified();
      }
    } catch (IOException e) {
      LOG.error("Cannot get mount point", e);
    }
    return modTime;
  }

  /**
   * Get listing on remote locations.
   */
  private List<RemoteResult<RemoteLocation, DirectoryListing>> getListingInt(
      String src, byte[] startAfter, boolean needLocation) throws IOException {
    try {
      List<RemoteLocation> locations =
          rpcServer.getLocationsForPath(src, false, false);
      // Locate the dir and fetch the listing.
      if (locations.isEmpty()) {
        return new ArrayList<>();
      }
      RemoteMethod method = new RemoteMethod("getListing",
          new Class<?>[] {String.class, startAfter.getClass(), boolean.class},
          new RemoteParam(), startAfter, needLocation);
      List<RemoteResult<RemoteLocation, DirectoryListing>> listings = rpcClient
          .invokeConcurrent(locations, method, false, -1,
              DirectoryListing.class);
      return listings;
    } catch (NoLocationException | RouterResolveException e) {
      LOG.debug("Cannot get locations for {}, {}.", src, e.getMessage());
      return new ArrayList<>();
    }
  }

  /**
   * Check if we should add the mount point into the total listing.
   * This should be done under either of the two cases:
   * 1) current mount point is between startAfter and cutoff lastEntry.
   * 2) there are no remaining entries from subclusters and this mount
   *    point is bigger than all files from subclusters
   * This is to make sure that the following batch of
   * getListing call will use the correct startAfter, which is lastEntry from
   * subcluster.
   *
   * @param mountPoint to be added mount point inside router
   * @param lastEntry biggest listing from subcluster
   * @param startAfter starting listing from client, used to define listing
   *                   start boundary
   * @param remainingEntries how many entries left from subcluster
   * @return
   */
  private static boolean shouldAddMountPoint(
      byte[] mountPoint, byte[] lastEntry, byte[] startAfter,
      int remainingEntries) {
    if (comparator.compare(mountPoint, startAfter) > 0 &&
        comparator.compare(mountPoint, lastEntry) <= 0) {
      return true;
    }
    if (remainingEntries == 0 &&
        comparator.compare(mountPoint, lastEntry) >= 0) {
      return true;
    }
    return false;
  }

  /**
   * Checks if the path is a directory and is supposed to be present in all
   * subclusters.
   * @param src the source path
   * @return true if the path is directory and is supposed to be present in all
   *         subclusters else false in all other scenarios.
   * @throws IOException if unable to get the file status.
   */
  @VisibleForTesting
  boolean isMultiDestDirectory(String src) throws IOException {
    try {
      if (rpcServer.isPathAll(src)) {
        List<RemoteLocation> locations;
        locations = rpcServer.getLocationsForPath(src, false, false);
        RemoteMethod method = new RemoteMethod("getFileInfo",
            new Class<?>[] {String.class}, new RemoteParam());
        HdfsFileStatus fileStatus = rpcClient.invokeSequential(locations,
            method, HdfsFileStatus.class, null);
        if (fileStatus != null) {
          return fileStatus.isDirectory();
        } else {
          LOG.debug("The destination {} doesn't exist.", src);
        }
      }
    } catch (UnresolvedPathException e) {
      LOG.debug("The destination {} is a symlink.", src);
    }
    return false;
  }

  public int getRouterFederationRenameCount() {
    return rbfRename.getRouterFederationRenameCount();
  }
}
