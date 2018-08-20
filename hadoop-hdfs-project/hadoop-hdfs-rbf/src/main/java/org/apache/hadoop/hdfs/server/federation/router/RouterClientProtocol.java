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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.BatchedRemoteIterator;
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
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Module that implements all the RPC calls in {@link ClientProtocol} in the
 * {@link RouterRpcServer}.
 */
public class RouterClientProtocol implements ClientProtocol {
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterClientProtocol.class.getName());

  private final RouterRpcServer rpcServer;
  private final RouterRpcClient rpcClient;
  private final FileSubclusterResolver subclusterResolver;
  private final ActiveNamenodeResolver namenodeResolver;

  /** Identifier for the super user. */
  private final String superUser;
  /** Identifier for the super group. */
  private final String superGroup;
  /** Erasure coding calls. */
  private final ErasureCoding erasureCoding;

  RouterClientProtocol(Configuration conf, RouterRpcServer rpcServer) {
    this.rpcServer = rpcServer;
    this.rpcClient = rpcServer.getRPCClient();
    this.subclusterResolver = rpcServer.getSubclusterResolver();
    this.namenodeResolver = rpcServer.getNamenodeResolver();

    // User and group for reporting
    this.superUser = System.getProperty("user.name");
    this.superGroup = conf.get(
        DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
        DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
    this.erasureCoding = new ErasureCoding(rpcServer);
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
    return null;
  }

  /**
   * The the delegation token from each name service.
   *
   * @param renewer
   * @return Name service -> Token.
   * @throws IOException
   */
  public Map<FederationNamespaceInfo, Token<DelegationTokenIdentifier>>
  getDelegationTokens(Text renewer) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
    return null;
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
    return 0;
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
  }

  @Override
  public LocatedBlocks getBlockLocations(String src, final long offset,
      final long length) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, false);
    RemoteMethod remoteMethod = new RemoteMethod("getBlockLocations",
        new Class<?>[] {String.class, long.class, long.class},
        new RemoteParam(), offset, length);
    return rpcClient.invokeSequential(locations, remoteMethod,
        LocatedBlocks.class, null);
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getServerDefaults");
    String ns = subclusterResolver.getDefaultNamespace();
    return (FsServerDefaults) rpcClient.invokeSingle(ns, method);
  }

  @Override
  public HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag,
      boolean createParent, short replication, long blockSize,
      CryptoProtocolVersion[] supportedVersions, String ecPolicyName)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    if (createParent && isPathAll(src)) {
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

    RemoteLocation createLocation = rpcServer.getCreateLocation(src);
    RemoteMethod method = new RemoteMethod("create",
        new Class<?>[] {String.class, FsPermission.class, String.class,
            EnumSetWritable.class, boolean.class, short.class,
            long.class, CryptoProtocolVersion[].class,
            String.class},
        createLocation.getDest(), masked, clientName, flag, createParent,
        replication, blockSize, supportedVersions, ecPolicyName);
    return (HdfsFileStatus) rpcClient.invokeSingle(createLocation, method);
  }

  @Override
  public LastBlockWithStatus append(String src, final String clientName,
      final EnumSetWritable<CreateFlag> flag) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("append",
        new Class<?>[] {String.class, String.class, EnumSetWritable.class},
        new RemoteParam(), clientName, flag);
    return rpcClient.invokeSequential(
        locations, method, LastBlockWithStatus.class, null);
  }

  @Override
  public boolean recoverLease(String src, String clientName)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("recoverLease",
        new Class<?>[] {String.class, String.class}, new RemoteParam(),
        clientName);
    Object result = rpcClient.invokeSequential(
        locations, method, Boolean.class, Boolean.TRUE);
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
    Object result = rpcClient.invokeSequential(
        locations, method, Boolean.class, Boolean.TRUE);
    return (boolean) result;
  }

  @Override
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setStoragePolicy",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), policyName);
    rpcClient.invokeSequential(locations, method, null, null);
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getStoragePolicies");
    String ns = subclusterResolver.getDefaultNamespace();
    return (BlockStoragePolicy[]) rpcClient.invokeSingle(ns, method);
  }

  @Override
  public void setPermission(String src, FsPermission permissions)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setPermission",
        new Class<?>[] {String.class, FsPermission.class},
        new RemoteParam(), permissions);
    if (isPathAll(src)) {
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
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setOwner",
        new Class<?>[] {String.class, String.class, String.class},
        new RemoteParam(), username, groupname);
    if (isPathAll(src)) {
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

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("addBlock",
        new Class<?>[] {String.class, String.class, ExtendedBlock.class,
            DatanodeInfo[].class, long.class, String[].class,
            EnumSet.class},
        new RemoteParam(), clientName, previous, excludedNodes, fileId,
        favoredNodes, addBlockFlags);
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

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("getAdditionalDatanode",
        new Class<?>[] {String.class, long.class, ExtendedBlock.class,
            DatanodeInfo[].class, String[].class,
            DatanodeInfo[].class, int.class, String.class},
        new RemoteParam(), fileId, blk, existings, existingStorageIDs, excludes,
        numAdditionalNodes, clientName);
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
    rpcClient.invokeSingle(b, method);
  }

  @Override
  public boolean complete(String src, String clientName, ExtendedBlock last,
      long fileId) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("complete",
        new Class<?>[] {String.class, String.class, ExtendedBlock.class,
            long.class},
        new RemoteParam(), clientName, last, fileId);
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
    return (LocatedBlock) rpcClient.invokeSingle(block, method);
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
    rpcClient.invokeSingle(oldBlock, method);
  }

  @Override
  public long getPreferredBlockSize(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
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
    return rpcClient.invokeSequential(locs, method, Boolean.class,
        Boolean.TRUE);
  }

  @Override
  public void rename2(final String src, final String dst,
      final Options.Rename... options) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> srcLocations =
        rpcServer.getLocationsForPath(src, true, false);
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
    rpcClient.invokeSingle(targetDestination, method);
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
    return rpcClient.invokeSequential(locations, method, Boolean.class,
        Boolean.TRUE);
  }

  @Override
  public boolean delete(String src, boolean recursive) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true, false);
    RemoteMethod method = new RemoteMethod("delete",
        new Class<?>[] {String.class, boolean.class}, new RemoteParam(),
        recursive);
    if (isPathAll(src)) {
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
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("mkdirs",
        new Class<?>[] {String.class, FsPermission.class, boolean.class},
        new RemoteParam(), masked, createParent);

    // Create in all locations
    if (isPathAll(src)) {
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
        LOG.error("Error requesting file info for path {} while proxing mkdirs",
            src, ioe);
      }
    }

    RemoteLocation firstLocation = locations.get(0);
    return (boolean) rpcClient.invokeSingle(firstLocation, method);
  }

  @Override
  public void renewLease(String clientName) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("renewLease",
        new Class<?>[] {String.class}, clientName);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, false, false);
  }

  @Override
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // Locate the dir and fetch the listing
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("getListing",
        new Class<?>[] {String.class, startAfter.getClass(), boolean.class},
        new RemoteParam(), startAfter, needLocation);
    Map<RemoteLocation, DirectoryListing> listings =
        rpcClient.invokeConcurrent(
            locations, method, false, false, DirectoryListing.class);

    Map<String, HdfsFileStatus> nnListing = new TreeMap<>();
    int totalRemainingEntries = 0;
    int remainingEntries = 0;
    boolean namenodeListingExists = false;
    if (listings != null) {
      // Check the subcluster listing with the smallest name
      String lastName = null;
      for (Map.Entry<RemoteLocation, DirectoryListing> entry :
          listings.entrySet()) {
        RemoteLocation location = entry.getKey();
        DirectoryListing listing = entry.getValue();
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

  @Override
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("getFileInfo",
        new Class<?>[] {String.class}, new RemoteParam());

    HdfsFileStatus ret = null;
    // If it's a directory, we check in all locations
    if (isPathAll(src)) {
      ret = getFileInfoAll(locations, method);
    } else {
      // Check for file information sequentially
      ret = rpcClient.invokeSequential(
          locations, method, HdfsFileStatus.class, null);
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
      }
    }

    return ret;
  }

  @Override
  public boolean isFileClosed(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("isFileClosed",
        new Class<?>[] {String.class}, new RemoteParam());
    return rpcClient.invokeSequential(locations, method, Boolean.class,
        Boolean.TRUE);
  }

  @Override
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false);
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
        rpcServer.getLocationsForPath(src, false);
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

    // Avoid repeating machines in multiple subclusters
    Map<String, DatanodeStorageReport> datanodesMap = new LinkedHashMap<>();
    for (DatanodeStorageReport[] dns : dnSubcluster.values()) {
      for (DatanodeStorageReport dn : dns) {
        DatanodeInfo dnInfo = dn.getDatanodeInfo();
        String nodeId = dnInfo.getXferAddr();
        if (!datanodesMap.containsKey(nodeId)) {
          datanodesMap.put(nodeId, dn);
        }
        // TODO merge somehow, right now it just takes the first one
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
        new Class<?>[] {Long.class, Long.class}, timeWindow, txGap);
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
        rpcServer.getLocationsForPath(path, false);
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
        new Class<?>[] {Long.class}, bandwidth);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false);
  }

  @Override
  public ContentSummary getContentSummary(String path) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // Get the summaries from regular files
    Collection<ContentSummary> summaries = new LinkedList<>();
    FileNotFoundException notFoundException = null;
    try {
      final List<RemoteLocation> locations =
          rpcServer.getLocationsForPath(path, false);
      RemoteMethod method = new RemoteMethod("getContentSummary",
          new Class<?>[] {String.class}, new RemoteParam());
      Map<RemoteLocation, ContentSummary> results =
          rpcClient.invokeConcurrent(
              locations, method, false, false, ContentSummary.class);
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

  @Override
  public void fsync(String src, long fileId, String clientName,
      long lastBlockLength) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("fsync",
        new Class<?>[] {String.class, long.class, String.class, long.class },
        new RemoteParam(), fileId, clientName, lastBlockLength);
    rpcClient.invokeSequential(locations, method);
  }

  @Override
  public void setTimes(String src, long mtime, long atime) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
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
        rpcServer.getLocationsForPath(path, true);
    RemoteMethod method = new RemoteMethod("getLinkTarget",
        new Class<?>[] {String.class}, new RemoteParam());
    return rpcClient.invokeSequential(locations, method, String.class, null);
  }

  @Override // Client Protocol
  public void allowSnapshot(String snapshotRoot) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
  }

  @Override // Client Protocol
  public void disallowSnapshot(String snapshot) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
  }

  @Override
  public void renameSnapshot(String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
  }

  @Override
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, false);
    return null;
  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
      String earlierSnapshotName, String laterSnapshotName) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, false);
    return null;
  }

  @Override
  public SnapshotDiffReportListing getSnapshotDiffReportListing(
      String snapshotRoot, String earlierSnapshotName, String laterSnapshotName,
      byte[] startPath, int index) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, false);
    return null;
  }

  @Override
  public long addCacheDirective(CacheDirectiveInfo path,
      EnumSet<CacheFlag> flags) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
    return 0;
  }

  @Override
  public void modifyCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
  }

  @Override
  public void removeCacheDirective(long id) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
  }

  @Override
  public BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
      long prevId, CacheDirectiveInfo filter) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, false);
    return null;
  }

  @Override
  public void addCachePool(CachePoolInfo info) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
  }

  @Override
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
  }

  @Override
  public void removeCachePool(String cachePoolName) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
  }

  @Override
  public BatchedRemoteIterator.BatchedEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, false);
    return null;
  }

  @Override
  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("modifyAclEntries",
        new Class<?>[] {String.class, List.class},
        new RemoteParam(), aclSpec);
    rpcClient.invokeSequential(locations, method, null, null);
  }

  @Override
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("removeAclEntries",
        new Class<?>[] {String.class, List.class},
        new RemoteParam(), aclSpec);
    rpcClient.invokeSequential(locations, method, null, null);
  }

  @Override
  public void removeDefaultAcl(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("removeDefaultAcl",
        new Class<?>[] {String.class}, new RemoteParam());
    rpcClient.invokeSequential(locations, method);
  }

  @Override
  public void removeAcl(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("removeAcl",
        new Class<?>[] {String.class}, new RemoteParam());
    rpcClient.invokeSequential(locations, method);
  }

  @Override
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod(
        "setAcl", new Class<?>[] {String.class, List.class},
        new RemoteParam(), aclSpec);
    rpcClient.invokeSequential(locations, method);
  }

  @Override
  public AclStatus getAclStatus(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false);
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
        rpcServer.getLocationsForPath(src, true);
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
        rpcServer.getLocationsForPath(src, false);
    RemoteMethod method = new RemoteMethod("getEZForPath",
        new Class<?>[] {String.class}, new RemoteParam());
    return rpcClient.invokeSequential(
        locations, method, EncryptionZone.class, null);
  }

  @Override
  public BatchedRemoteIterator.BatchedEntries<EncryptionZone> listEncryptionZones(long prevId)
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
  public BatchedRemoteIterator.BatchedEntries<ZoneReencryptionStatus> listReencryptionStatus(
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
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setXAttr",
        new Class<?>[] {String.class, XAttr.class, EnumSet.class},
        new RemoteParam(), xAttr, flag);
    rpcClient.invokeSequential(locations, method);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false);
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
        rpcServer.getLocationsForPath(src, false);
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
        rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("removeXAttr",
        new Class<?>[] {String.class, XAttr.class}, new RemoteParam(), xAttr);
    rpcClient.invokeSequential(locations, method);
  }

  @Override
  public void checkAccess(String path, FsAction mode) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(path, true);
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
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);
    return null;
  }

  @Override
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
  }

  @Override
  public void setQuota(String path, long namespaceQuota, long storagespaceQuota,
      StorageType type) throws IOException {
    rpcServer.getQuotaModule()
        .setQuota(path, namespaceQuota, storagespaceQuota, type);
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
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, false);
    return null;
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
  public ECBlockGroupStats getECBlockGroupStats() throws IOException {
    return erasureCoding.getECBlockGroupStats();
  }

  @Override
  public ReplicatedBlockStats getReplicatedBlockStats() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, false);
    return null;
  }

  @Deprecated
  @Override
  public BatchedRemoteIterator.BatchedEntries<OpenFileEntry> listOpenFiles(long prevId)
      throws IOException {
    return listOpenFiles(prevId, EnumSet.of(OpenFilesIterator.OpenFilesType.ALL_OPEN_FILES),
        OpenFilesIterator.FILTER_PATH_DEFAULT);
  }

  @Override
  public BatchedRemoteIterator.BatchedEntries<OpenFileEntry> listOpenFiles(long prevId,
      EnumSet<OpenFilesIterator.OpenFilesType> openFilesTypes, String path) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, false);
    return null;
  }

  @Override
  public void satisfyStoragePolicy(String path) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, false);
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

    final List<RemoteLocation> dstLocations =
        rpcServer.getLocationsForPath(dst, true);
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

    // Get the file info from everybody
    Map<RemoteLocation, HdfsFileStatus> results =
        rpcClient.invokeConcurrent(locations, method, HdfsFileStatus.class);

    // We return the first file
    HdfsFileStatus dirStatus = null;
    for (RemoteLocation loc : locations) {
      HdfsFileStatus fileStatus = results.get(loc);
      if (fileStatus != null) {
        if (!fileStatus.isDirectory()) {
          return fileStatus;
        } else if (dirStatus == null) {
          dirStatus = fileStatus;
        }
      }
    }
    return dirStatus;
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
   * Check if a path should be in all subclusters.
   *
   * @param path Path to check.
   * @return If a path should be in all subclusters.
   */
  private boolean isPathAll(final String path) {
    if (subclusterResolver instanceof MountTableResolver) {
      try {
        MountTableResolver mountTable = (MountTableResolver)subclusterResolver;
        MountTable entry = mountTable.getMountPoint(path);
        if (entry != null) {
          return entry.isAll();
        }
      } catch (IOException e) {
        LOG.error("Cannot get mount point", e);
      }
    }
    return false;
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
      UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
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
}
