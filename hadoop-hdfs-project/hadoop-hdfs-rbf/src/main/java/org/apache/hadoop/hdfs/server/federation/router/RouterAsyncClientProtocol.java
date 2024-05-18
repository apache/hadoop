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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RouterResolveException;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static org.apache.hadoop.hdfs.server.federation.router.FederationUtil.updateMountPointStatus;
import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.asyncReturn;
import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.getCompletableFuture;
import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.setCurCompletableFuture;

public class RouterAsyncClientProtocol extends RouterClientProtocol {
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAsyncClientProtocol.class.getName());

  private final RouterSnapshot asyncSnapshotProto;
  private final AsyncErasureCoding asyncErasureCoding;
  private final RouterAsyncCacheAdmin routerAsyncCacheAdmin;
  private volatile FsServerDefaults serverDefaults;
  private final RouterAsyncStoragePolicy asyncstoragePolicy;

  RouterAsyncClientProtocol(Configuration conf, RouterRpcServer rpcServer) {
    super(conf, rpcServer);
    asyncSnapshotProto = new RouterAsyncSnapshot(rpcServer);
    asyncErasureCoding = new AsyncErasureCoding(rpcServer);
    routerAsyncCacheAdmin = new RouterAsyncCacheAdmin(rpcServer);
    asyncstoragePolicy = new RouterAsyncStoragePolicy(rpcServer);
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    long serverDefaultsLastUpdate = getServerDefaultsLastUpdate();
    long serverDefaultsValidityPeriod = getServerDefaultsValidityPeriod();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);
    long now = Time.monotonicNow();
    CompletableFuture<Object> completableFuture = null;
    if ((serverDefaults == null) || (now - serverDefaultsLastUpdate
        > serverDefaultsValidityPeriod)) {
      RemoteMethod method = new RemoteMethod("getServerDefaults");
      serverDefaults =
          rpcServer.invokeAtAvailableNsAsync(method, FsServerDefaults.class);
      completableFuture = getCompletableFuture();
      completableFuture = completableFuture.thenApply(o -> {
        serverDefaults = (FsServerDefaults) o;
        RouterAsyncClientProtocol.super.setServerDefaultsLastUpdate(now);
        return o;
      });
    } else {
      completableFuture =
          CompletableFuture.completedFuture(serverDefaults);
    }
    setCurCompletableFuture(completableFuture);
    return asyncReturn(FsServerDefaults.class);
  }

  @Override
  public HdfsFileStatus create(
      String src, FsPermission masked, String clientName,
      EnumSetWritable<CreateFlag> flag, boolean createParent, short replication,
      long blockSize, CryptoProtocolVersion[] supportedVersions,
      String ecPolicyName, String storagePolicy) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    CompletableFuture<Object> completableFuture = null;
    final Throwable[] throwable = new Throwable[1];
    if (createParent && rpcServer.isPathAll(src)) {
      int index = src.lastIndexOf(Path.SEPARATOR);
      String parent = src.substring(0, index);
      LOG.debug("Creating {} requires creating parent {}", src, parent);
      FsPermission parentPermissions = getParentPermission(masked);
      mkdirs(parent, parentPermissions, createParent);
      completableFuture = getCompletableFuture();
      completableFuture = completableFuture.exceptionally(e -> {
        throwable[0] = e.getCause();
        throw new CompletionException(e.getCause());
      });
    }

    if (completableFuture == null) {
      completableFuture = CompletableFuture.completedFuture(false);
    }
    RemoteMethod method = new RemoteMethod("create",
        new Class<?>[] {String.class, FsPermission.class, String.class,
            EnumSetWritable.class, boolean.class, short.class,
            long.class, CryptoProtocolVersion[].class,
            String.class, String.class},
        new RemoteParam(), masked, clientName, flag, createParent,
        replication, blockSize, supportedVersions, ecPolicyName, storagePolicy);
    final RemoteLocation[] createLocation = new RemoteLocation[1];
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true);
    completableFuture = completableFuture.thenCompose(o -> {
      try {
        rpcServer.getCreateLocationAsync(src, locations);
        return getCompletableFuture();
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    }).thenCompose((Function<Object, CompletionStage<Object>>) o -> {
      createLocation[0] = (RemoteLocation) o;
      try {
        rpcClient.invokeSingle(createLocation[0], method,
            HdfsFileStatus.class);
        return getCompletableFuture().thenApply(o1 -> {
          HdfsFileStatus status = (HdfsFileStatus) o1;
          status.setNamespace(createLocation[0].getNameserviceId());
          return status;
        });
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    }).exceptionally(Throwable::getCause).thenCompose(o -> {
      if (throwable[0] != null) {
        throw new CompletionException(throwable[0]);
      }
      if (o instanceof Throwable) {
        if (o instanceof IOException) {
          try {
            final List<RemoteLocation> newLocations = checkFaultTolerantRetry(
                method, src, (IOException) o, createLocation[0], locations);
            rpcClient.invokeSequential(
                newLocations, method, HdfsFileStatus.class, null);
            return getCompletableFuture();
          } catch (IOException e) {
            throw new CompletionException(e);
          }
        }
        throw new CompletionException((Throwable) o);
      }
      return CompletableFuture.completedFuture(o);
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(HdfsFileStatus.class);
  }

  @Override
  public LastBlockWithStatus append(
      String src, String clientName,
      EnumSetWritable<CreateFlag> flag) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("append",
        new Class<?>[] {String.class, String.class, EnumSetWritable.class},
        new RemoteParam(), clientName, flag);
    rpcClient.invokeSequential(method, locations, LastBlockWithStatus.class, null);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      RemoteResult result = (RemoteResult) o;
      LastBlockWithStatus lbws = (LastBlockWithStatus) result.getResult();
      lbws.getFileStatus().setNamespace(result.getLocation().getNameserviceId());
      return lbws;
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(LastBlockWithStatus.class);
  }

  @Deprecated
  @Override
  public boolean rename(final String src, final String dst)
      throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    RouterFederationRename rbfRename = getRbfRename();
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> srcLocations =
        rpcServer.getLocationsForPath(src, true, false);
    final List<RemoteLocation> dstLocations =
        rpcServer.getLocationsForPath(dst, false, false);
    // srcLocations may be trimmed by getRenameDestinations()
    final List<RemoteLocation> locs = new LinkedList<>(srcLocations);
    RemoteParam dstParam = getRenameDestinations(locs, dstLocations);
    if (locs.isEmpty()) {
      setCurCompletableFuture(
          CompletableFuture.completedFuture(
              rbfRename.routerFedRename(src, dst, srcLocations, dstLocations)));
      return asyncReturn(Boolean.class);
    }
    RemoteMethod method = new RemoteMethod("rename",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), dstParam);
    isMultiDestDirectory(src);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenCompose(o -> {
      Boolean isMultiDest = (Boolean) o;
      if (isMultiDest) {
        if (locs.size() != srcLocations.size()) {
          IOException ioe = new IOException("Rename of " + src + " to " + dst + " is not"
              + " allowed. The number of remote locations for both source and"
              + " target should be same.");
          throw new CompletionException(ioe);
        }
        try {
          rpcClient.invokeAll(locs, method);
          return getCompletableFuture();
        } catch (IOException e) {
          throw new CompletionException(e);
        }
      } else {
        try {
          rpcClient.invokeSequential(locs, method, Boolean.class,
              Boolean.TRUE);
          return getCompletableFuture();
        } catch (IOException e) {
          throw new CompletionException(e);
        }
      }
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(Boolean.class);
  }

  @Override
  public void rename2(final String src, final String dst,
                      final Options.Rename... options) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    RouterFederationRename rbfRename = getRbfRename();
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
    isMultiDestDirectory(src);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenCompose(o -> {
      Boolean isMultiDest = (Boolean) o;
      if (isMultiDest) {
        if (locs.size() != srcLocations.size()) {
          IOException ioe = new IOException("Rename of " + src + " to " + dst + " is not"
              + " allowed. The number of remote locations for both source and"
              + " target should be same.");
          throw new CompletionException(ioe);
        }
        try {
          rpcClient.invokeConcurrent(locs, method);
          return getCompletableFuture();
        } catch (IOException e) {
          throw new CompletionException(e);
        }
      } else {
        try {
          rpcClient.invokeSequential(locs, method, null, null);
          return getCompletableFuture();
        } catch (IOException e) {
          throw new CompletionException(e);
        }
      }
    });
    setCurCompletableFuture(completableFuture);
  }

  @Override
  public void concat(String trg, String[] src) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    List<String> sourceDestinations = new ArrayList<>(src.length);
    // Concat only effects when all files in the same namespace.
    getFileRemoteLocation(trg);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenCompose(o -> {
      RemoteLocation targetDestination = (RemoteLocation) o;
      if (targetDestination == null) {
        throw new CompletionException(
            new IOException("Cannot find target file - " + trg));
      }
      String targetNameService = targetDestination.getNameserviceId();
      AsyncRequestDoWhile<String, RemoteLocation, RemoteLocation> asyncRequestDoWhile =
          new AsyncRequestDoWhile<>();
      try {
        asyncRequestDoWhile.whileFor(Arrays.stream(src).iterator())
            .doAsync(this::getFileRemoteLocation)
            .thenApply((sourceFile, srcLocation) -> {
              if (srcLocation == null) {
                throw new CompletionException(
                    new IOException("Cannot find source file - " + sourceFile));
              }
              sourceDestinations.add(srcLocation.getDest());

              if (!targetNameService.equals(srcLocation.getNameserviceId())) {
                IOException ioe = new IOException("Cannot concatenate source file " + sourceFile
                    + " because it is located in a different namespace" + " with nameservice "
                    + srcLocation.getNameserviceId() + " from the target file with nameservice "
                    + targetNameService);
                throw new CompletionException(ioe);
              }
              return targetDestination;
            }).asyncDoWhile(RemoteLocation.class);
        return getCompletableFuture();
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    });
    // Invoke
    completableFuture = completableFuture.thenCompose(o -> {
      RemoteLocation targetDestination = (RemoteLocation) o;
      try {
        RemoteMethod method = new RemoteMethod("concat",
            new Class<?>[] {String.class, String[].class},
            targetDestination.getDest(), sourceDestinations.toArray(new String[0]));
        rpcClient.invokeSingle(targetDestination, method, Void.class);
        return getCompletableFuture();
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    });
    setCurCompletableFuture(completableFuture);
  }

  @Override
  public RemoteLocation getFileRemoteLocation(String path) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(path, false, false);
    if (locations.size() == 1) {
      setCurCompletableFuture(CompletableFuture.completedFuture(locations.get(0)));
      return asyncReturn(RemoteLocation.class);
    }
    RemoteMethod method =
        new RemoteMethod("getFileInfo", new Class<?>[] {String.class}, new RemoteParam());
    AsyncRequestDoWhile<RemoteLocation, HdfsFileStatus, RemoteLocation> asyncRequestDoWhile =
        new AsyncRequestDoWhile<>();
    return asyncRequestDoWhile.whileFor(locations.listIterator())
        .doAsync(location -> rpcClient.invokeSequential(Collections.singletonList(location),
            method, HdfsFileStatus.class, null))
        .thenApply((remoteLocation, hdfsFileStatus) -> {
          if (hdfsFileStatus != null) {
            asyncRequestDoWhile.breakNow();
            return remoteLocation;
          }
          return null;
        }).asyncDoWhile(RemoteLocation.class);
  }

  @Override
  public boolean mkdirs(String src, FsPermission masked, boolean createParent) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
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

    CompletableFuture<Object> completableFuture
        = CompletableFuture.completedFuture(false);
    if (locations.size() > 1) {
      // Check if this directory already exists
      try {
        getFileInfo(src);
        completableFuture = getCompletableFuture();
        completableFuture = completableFuture.thenApply(o -> {
          HdfsFileStatus fileStatus = (HdfsFileStatus) o;
          if (fileStatus != null) {
            // When existing, the NN doesn't return an exception; return true
            return true;
          }
          return false;
        });
      } catch (IOException ioe) {
        // Can't query if this file exists or not.
        LOG.error("Error getting file info for {} while proxying mkdirs: {}",
            src, ioe.getMessage());
      }
    }

    completableFuture = completableFuture.thenCompose(o -> {
      boolean success = (boolean) o;
      if (success) {
        return CompletableFuture.completedFuture(true);
      }
      final RemoteLocation firstLocation = locations.get(0);
      try {
        rpcClient.invokeSingle(firstLocation, method, Boolean.class);
        return getCompletableFuture();
      } catch (IOException ioe) {
        throw new CompletionException(ioe);
      }
    }).exceptionally(e -> {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        return cause;
      } else {
        throw new CompletionException(cause);
      }
    }).thenCompose(o -> {
      if (o instanceof Boolean) {
        return CompletableFuture.completedFuture(o);
      }
      IOException ioe = (IOException) o;
      final RemoteLocation firstLocation = locations.get(0);
      final List<RemoteLocation> newLocations;
      try {
        newLocations = checkFaultTolerantRetry(
            method, src, ioe, firstLocation, locations);
        rpcClient.invokeSequential(
            newLocations, method, Boolean.class, Boolean.TRUE);
        return getCompletableFuture();
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(Boolean.class);
  }

  @Override
  public DirectoryListing getListing(
      String src, byte[] startAfter, boolean needLocation) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    getListingInt(src, startAfter, needLocation);
    CompletableFuture<Object> completableFuture = getCompletableFuture();

    RouterClientProtocol.GetListingComparator comparator = getComparator();
    TreeMap<byte[], HdfsFileStatus> nnListing = new TreeMap<>(comparator);

    completableFuture = completableFuture.thenApply(new Function<Object, Object>() {
      @Override
      public Object apply(Object o) {
        List<RemoteResult<RemoteLocation, DirectoryListing>> listings =
            (List<RemoteResult<RemoteLocation, DirectoryListing>>) o;
        int totalRemainingEntries = 0;
        int remainingEntries = 0;
        // Check the subcluster listing with the smallest name to make sure
        // no file is skipped across subclusters
        byte[] lastName = null;
        boolean namenodeListingExists = false;
        if (listings != null) {
          for (RemoteResult<RemoteLocation, DirectoryListing> result : listings) {
            if (result.hasException()) {
              IOException ioe = result.getException();
              if (ioe instanceof FileNotFoundException) {
                RemoteLocation location = result.getLocation();
                LOG.debug("Cannot get listing from {}", location);
              } else if (!isAllowPartialList()) {
                throw new CompletionException(ioe);
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
        return new Object[] {remainingEntries, namenodeListingExists, lastName};
      }
    });

    FileSubclusterResolver subclusterResolver = getSubclusterResolver();
    // Add mount points at this level in the tree
    final List<String> children = subclusterResolver.getMountPoints(src);
    if (children != null) {
      // Get the dates for each mount point
      Map<String, Long> dates = getMountPointDates(src);

      // Create virtual folder with the mount name
      for (String child : children) {
        completableFuture = completableFuture.thenCompose(o -> {
          Object[] args = (Object[]) o;
          int remainingEntries = (int) args[0];
          byte[] lastName = (byte[]) args[2];
          long date = 0;
          if (dates != null && dates.containsKey(child)) {
            date = dates.get(child);
          }
          Path childPath = new Path(src, child);
          getMountPointStatus(childPath.toString(), 0, date);
          CompletableFuture<Object> future = getCompletableFuture();
          future = future.thenApply(o1 -> {
            HdfsFileStatus dirStatus = (HdfsFileStatus) o1;
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
            return new Object[] {remainingEntries, args[1], lastName};
          });
          return future;
        });
      }

      completableFuture = completableFuture.thenApply(o -> {
        Object[] args = (Object[]) o;
        int remainingEntries = (int) args[0];
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
        return new Object[] {remainingEntries, args[1], args[2]};
      });

    }

    completableFuture = completableFuture.thenApply(o -> {
      Object[] args = (Object[]) o;
      int remainingEntries = (int) args[0];
      boolean namenodeListingExists = (boolean) args[1];
      if (!namenodeListingExists && nnListing.size() == 0 && children == null) {
        // NN returns a null object if the directory cannot be found and has no
        // listing. If we didn't retrieve any NN listing data, and there are no
        // mount points here, return null.
        return null;
      }

      // Generate combined listing
      HdfsFileStatus[] combinedData = new HdfsFileStatus[nnListing.size()];
      combinedData = nnListing.values().toArray(combinedData);
      return new DirectoryListing(combinedData, remainingEntries);
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(DirectoryListing.class);
  }

  /**
   * Get listing on remote locations.
   */
  @Override
  List<RemoteResult<RemoteLocation, DirectoryListing>> getListingInt(
      String src, byte[] startAfter, boolean needLocation) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    try {
      List<RemoteLocation> locations =
          rpcServer.getLocationsForPath(src, false, false);
      // Locate the dir and fetch the listing.
      if (locations.isEmpty()) {
        setCurCompletableFuture(CompletableFuture.completedFuture(new ArrayList<>()));
        return asyncReturn(List.class);
      }
      RemoteMethod method = new RemoteMethod("getListing",
          new Class<?>[] {String.class, startAfter.getClass(), boolean.class},
          new RemoteParam(), startAfter, needLocation);
      rpcClient.invokeConcurrent(locations, method, false, -1,
              DirectoryListing.class);
    } catch (NoLocationException | RouterResolveException e) {
      LOG.debug("Cannot get locations for {}, {}.", src, e.getMessage());
      setCurCompletableFuture(CompletableFuture.completedFuture(new ArrayList<>()));
    }
    return asyncReturn(List.class);
  }


  HdfsFileStatus getMountPointStatus(
      String name, int childrenNum, long date, boolean setPath) {
    FileSubclusterResolver subclusterResolver = getSubclusterResolver();
    long modTime = date;
    long accessTime = date;
    FsPermission permission = FsPermission.getDirDefault();
    String owner = getSuperUser();
    String group = getSuperGroup();
    EnumSet<HdfsFileStatus.Flags> flags =
        EnumSet.noneOf(HdfsFileStatus.Flags.class);
    CompletableFuture<Object> completableFuture = null;
    if (subclusterResolver instanceof MountTableResolver) {
      try {
        String mName = name.startsWith("/") ? name : "/" + name;
        MountTableResolver mountTable = (MountTableResolver) subclusterResolver;
        MountTable entry = mountTable.getMountPoint(mName);
        if (entry != null) {
          RemoteMethod method = new RemoteMethod("getFileInfo",
              new Class<?>[] {String.class}, new RemoteParam());
          getFileInfoAll(entry.getDestinations(), method, getMountStatusTimeOut());
          completableFuture = getCompletableFuture();
          completableFuture = completableFuture.thenApply(o -> {
            HdfsFileStatus fInfo = (HdfsFileStatus) o;
            if (fInfo != null) {
              return new Object[] {
                  fInfo.getPermission(), fInfo.getOwner(), fInfo.getGroup(),
                  fInfo.getChildrenNum(), DFSUtil
                  .getFlags(fInfo.isEncrypted(), fInfo.isErasureCoded(),
                  fInfo.isSnapshotEnabled(), fInfo.hasAcl())};
            }
            return new Object[] {entry.getMode(), entry.getOwnerName(), entry.getGroupName(),
                childrenNum, flags};
          });
        }
      } catch (IOException e) {
        LOG.error("Cannot get mount point: {}", e.getMessage());
        completableFuture = CompletableFuture.completedFuture(
            new Object[]{permission, owner, group, childrenNum, flags});
      }
    } else {
      try {
        UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
        owner = ugi.getUserName();
        group = ugi.getPrimaryGroupName();
        completableFuture =
            CompletableFuture.completedFuture(new Object[]{permission, owner, group,
                childrenNum, flags});
      } catch (IOException e) {
        String msg = "Cannot get remote user: " + e.getMessage();
        if (UserGroupInformation.isSecurityEnabled()) {
          LOG.error(msg);
        } else {
          LOG.debug(msg);
        }
        completableFuture = CompletableFuture.completedFuture(
            new Object[]{permission, owner, group, childrenNum, flags});
      }
    }

    completableFuture = completableFuture.thenApply(o -> {
      Object[] args = (Object[]) o;
      long inodeId = 0;
      HdfsFileStatus.Builder builder = new HdfsFileStatus.Builder();
      if (setPath) {
        Path path = new Path(name);
        String nameStr = path.getName();
        builder.path(DFSUtil.string2Bytes(nameStr));
      }

      return builder.isdir(true)
          .mtime(modTime)
          .atime(accessTime)
          .perm((FsPermission) args[0])
          .owner((String) args[1])
          .group((String) args[2])
          .symlink(new byte[0])
          .fileId(inodeId)
          .children((Integer) args[3])
          .flags((EnumSet<HdfsFileStatus.Flags>) args[4])
          .build();
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(HdfsFileStatus.class);
  }


  private HdfsFileStatus getFileInfoAll(
      final List<RemoteLocation> locations,
      final RemoteMethod method, long timeOutMs) throws IOException {

    RouterRpcClient rpcClient = getRpcClient();
    // Get the file info from everybody
    rpcClient.invokeConcurrent(locations, method, false, false, timeOutMs,
            HdfsFileStatus.class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<RemoteLocation, HdfsFileStatus> results = (Map<RemoteLocation, HdfsFileStatus>) o;
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
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(HdfsFileStatus.class);
  }

  @Override
  public boolean recoverLease(String src, String clientName) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, true, false);
    RemoteMethod method = new RemoteMethod("recoverLease",
        new Class<?>[] {String.class, String.class}, new RemoteParam(),
        clientName);
    rpcClient.invokeSequential(locations, method, Boolean.class, null);
    return asyncReturn(Boolean.class);
  }

  @Override
  public long[] getStats() throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    ActiveNamenodeResolver namenodeResolver = getNamenodeResolver();
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("getStats");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false, long[].class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, long[]> results
          = (Map<FederationNamespaceInfo, long[]>) o;
      long[] combinedData = new long[STATS_ARRAY_LENGTH];
      for (long[] data : results.values()) {
        for (int i = 0; i < combinedData.length && i < data.length; i++) {
          if (data[i] >= 0) {
            combinedData[i] += data[i];
          }
        }
      }
      return combinedData;
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(long[].class);
  }

  @Override
  public ErasureCodingPolicyInfo[] getErasureCodingPolicies()
      throws IOException {
    return asyncErasureCoding.getErasureCodingPolicies();
  }

  @Override
  public Map<String, String> getErasureCodingCodecs() throws IOException {
    return asyncErasureCoding.getErasureCodingCodecs();
  }

  @Override
  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies) throws IOException {
    return asyncErasureCoding.addErasureCodingPolicies(policies);
  }

  @Override
  public void removeErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    asyncErasureCoding.removeErasureCodingPolicy(ecPolicyName);
  }

  @Override
  public void disableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    asyncErasureCoding.disableErasureCodingPolicy(ecPolicyName);
  }

  @Override
  public void enableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    asyncErasureCoding.enableErasureCodingPolicy(ecPolicyName);
  }

  @Override
  public ErasureCodingPolicy getErasureCodingPolicy(String src)
      throws IOException {
    return asyncErasureCoding.getErasureCodingPolicy(src);
  }

  @Override
  public void setErasureCodingPolicy(String src, String ecPolicyName)
      throws IOException {
    asyncErasureCoding.setErasureCodingPolicy(src, ecPolicyName);
  }

  @Override
  public void unsetErasureCodingPolicy(String src) throws IOException {
    asyncErasureCoding.unsetErasureCodingPolicy(src);
  }

  @Override
  public ECTopologyVerifierResult getECTopologyResultForPolicies(
      String... policyNames) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED, true);
    return asyncErasureCoding.getECTopologyResultForPolicies(policyNames);
  }

  @Override
  public ECBlockGroupStats getECBlockGroupStats() throws IOException {
    return asyncErasureCoding.getECBlockGroupStats();
  }


  @Override
  public ReplicatedBlockStats getReplicatedBlockStats() throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    ActiveNamenodeResolver namenodeResolver = getNamenodeResolver();

    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getReplicatedBlockStats");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true,
        false, ReplicatedBlockStats.class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, ReplicatedBlockStats> ret =
          (Map<FederationNamespaceInfo, ReplicatedBlockStats>) o;
      return ReplicatedBlockStats.merge(ret.values());
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(ReplicatedBlockStats.class);
  }

  @Override
  public DatanodeInfo[] getDatanodeReport(HdfsConstants.DatanodeReportType type)
      throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);
    return rpcServer.getDatanodeReportAsync(type, true, 0);
  }

  @Override
  public DatanodeStorageReport[] getDatanodeStorageReport(
      HdfsConstants.DatanodeReportType type) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    rpcServer.getDatanodeStorageReportMapAsync(type);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<String, DatanodeStorageReport[]> dnSubcluster =
          (Map<String, DatanodeStorageReport[]>) o;
      return mergeDtanodeStorageReport(dnSubcluster);
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(DatanodeStorageReport[].class);
  }

  public DatanodeStorageReport[] getDatanodeStorageReport(
      HdfsConstants.DatanodeReportType type, boolean requireResponse, long timeOutMs)
      throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    rpcServer.getDatanodeStorageReportMapAsync(type, requireResponse, timeOutMs);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<String, DatanodeStorageReport[]> dnSubcluster =
          (Map<String, DatanodeStorageReport[]>) o;
      return mergeDtanodeStorageReport(dnSubcluster);
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(DatanodeStorageReport[].class);
  }

  @Override
  public boolean setSafeMode(
      HdfsConstants.SafeModeAction action, boolean isChecked) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    ActiveNamenodeResolver namenodeResolver = getNamenodeResolver();
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // Set safe mode in all the name spaces
    RemoteMethod method = new RemoteMethod("setSafeMode",
        new Class<?>[] {HdfsConstants.SafeModeAction.class, boolean.class},
        action, isChecked);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(
            nss, method, true, !isChecked, Boolean.class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, Boolean> results
          = (Map<FederationNamespaceInfo, Boolean>) o;
      // We only report true if all the name space are in safe mode
      int numSafemode = 0;
      for (boolean safemode : results.values()) {
        if (safemode) {
          numSafemode++;
        }
      }
      return numSafemode == results.size();
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(Boolean.class);
  }

  @Override
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    ActiveNamenodeResolver namenodeResolver = getNamenodeResolver();
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("saveNamespace",
        new Class<?>[] {long.class, long.class}, timeWindow, txGap);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true,
        false, boolean.class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, Boolean> ret =
          (Map<FederationNamespaceInfo, Boolean>) o;
      boolean success = true;
      for (boolean s : ret.values()) {
        if (!s) {
          success = false;
          break;
        }
      }
      return success;
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(Boolean.class);
  }

  @Override
  public long rollEdits() throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    ActiveNamenodeResolver namenodeResolver = getNamenodeResolver();
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("rollEdits", new Class<?>[] {});
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false, long.class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, Long> ret =
          (Map<FederationNamespaceInfo, Long>) o;
      // Return the maximum txid
      long txid = 0;
      for (long t : ret.values()) {
        if (t > txid) {
          txid = t;
        }
      }
      return txid;
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(Long.class);
  }

  @Override
  public boolean restoreFailedStorage(String arg) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    ActiveNamenodeResolver namenodeResolver = getNamenodeResolver();
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("restoreFailedStorage",
        new Class<?>[] {String.class}, arg);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false, Boolean.class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, Boolean> ret =
          (Map<FederationNamespaceInfo, Boolean>) o;
      boolean success = true;
      for (boolean s : ret.values()) {
        if (!s) {
          success = false;
          break;
        }
      }
      return success;
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(Boolean.class);
  }

  @Override
  public RollingUpgradeInfo rollingUpgrade(
      HdfsConstants.RollingUpgradeAction action) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    ActiveNamenodeResolver namenodeResolver = getNamenodeResolver();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("rollingUpgrade",
        new Class<?>[] {HdfsConstants.RollingUpgradeAction.class}, action);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();

    rpcClient.invokeConcurrent(
            nss, method, true, false, RollingUpgradeInfo.class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, RollingUpgradeInfo> ret =
          (Map<FederationNamespaceInfo, RollingUpgradeInfo>) o;
      // Return the first rolling upgrade info
      RollingUpgradeInfo info = null;
      for (RollingUpgradeInfo infoNs : ret.values()) {
        if (info == null && infoNs != null) {
          info = infoNs;
        }
      }
      return info;
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(RollingUpgradeInfo.class);
  }

  @Override
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    FileSubclusterResolver subclusterResolver = getSubclusterResolver();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    IOException noLocationException = null;
    try {
      final List<RemoteLocation> locations =
          rpcServer.getLocationsForPath(src, false, false);
      RemoteMethod method = new RemoteMethod("getFileInfo",
          new Class<?>[] {String.class}, new RemoteParam());

      // If it's a directory, we check in all locations
      if (rpcServer.isPathAll(src)) {
        getFileInfoAll(locations, method);
      } else {
        // Check for file information sequentially
        rpcClient.invokeSequential(locations,
            method, HdfsFileStatus.class, null);
      }
    } catch (NoLocationException | RouterResolveException e) {
      noLocationException = e;
    }

    CompletableFuture<Object> completableFuture = getCompletableFuture();
    IOException finalNoLocationException = noLocationException;
    completableFuture = completableFuture.thenApply(o -> {
      HdfsFileStatus ret = (HdfsFileStatus) o;
      // If there is no real path, check mount points
      if (ret == null) {
        List<String> children = null;
        try {
          children = subclusterResolver.getMountPoints(src);
        } catch (IOException e) {
          throw new CompletionException(e);
        }
        if (children != null && !children.isEmpty()) {
          Map<String, Long> dates = getMountPointDates(src);
          long date = 0;
          if (dates != null && dates.containsKey(src)) {
            date = dates.get(src);
          }
          ret = getMountPointStatus(src, children.size(), date, false);
        } else if (children != null) {
          // The src is a mount point, but there are no files or directories
          ret = getMountPointStatus(src, 0, 0, false);
        }
      }

      // Can't find mount point for path and the path didn't contain any sub monit points,
      // throw the NoLocationException to client.
      if (ret == null && finalNoLocationException != null) {
        throw new CompletionException(finalNoLocationException);
      }

      return ret;
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(HdfsFileStatus.class);
  }

  private HdfsFileStatus getFileInfoAll(
      final List<RemoteLocation> locations,
      final RemoteMethod method) throws IOException {
    return getFileInfoAll(locations, method, -1);
  }

  @Override
  public ContentSummary getContentSummary(String path) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // Get the summaries from regular files
    final Collection<ContentSummary> summaries = new ArrayList<>();
    final List<RemoteLocation> locations = getLocationsForContentSummary(path);
    final RemoteMethod method = new RemoteMethod("getContentSummary",
        new Class<?>[] {String.class}, new RemoteParam());
    rpcClient.invokeConcurrent(locations, method,
            false, -1, ContentSummary.class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      final List<RemoteResult<RemoteLocation, ContentSummary>> results =
          (List<RemoteResult<RemoteLocation, ContentSummary>>) o;
      FileNotFoundException notFoundException = null;
      for (RemoteResult<RemoteLocation, ContentSummary> result : results) {
        if (result.hasException()) {
          IOException ioe = result.getException();
          if (ioe instanceof FileNotFoundException) {
            notFoundException = (FileNotFoundException)ioe;
          } else if (!isAllowPartialList()) {
            throw new CompletionException(ioe);
          }
        } else if (result.getResult() != null) {
          summaries.add(result.getResult());
        }
      }

      // Throw original exception if no original nor mount points
      if (summaries.isEmpty() && notFoundException != null) {
        throw new CompletionException(notFoundException);
      }

      return aggregateContentSummary(summaries);
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(ContentSummary.class);
  }

  @Override
  public void setQuota(
      String path, long namespaceQuota, long storagespaceQuota,
      StorageType type) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    rpcServer.getQuotaModule()
        .setQuota(path, namespaceQuota, storagespaceQuota, type, true);
  }

  @Override
  public QuotaUsage getQuotaUsage(String path) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    return rpcServer.getQuotaModule().getQuotaUsage(path);
  }

  // todo
  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
    return super.getDelegationToken(renewer);
  }

  //todo
  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
    return super.renewDelegationToken(token);
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
    super.cancelDelegationToken(token);
  }

  @Override
  public String createSnapshot(
      String snapshotRoot, String snapshotName) throws IOException {
    return asyncSnapshotProto.createSnapshot(snapshotRoot, snapshotName);
  }

  @Override
  public void deleteSnapshot(
      String snapshotRoot, String snapshotName) throws IOException {
    asyncSnapshotProto.deleteSnapshot(snapshotRoot, snapshotName);
  }


  @Override
  public void allowSnapshot(String snapshotRoot) throws IOException {
    asyncSnapshotProto.allowSnapshot(snapshotRoot);
  }

  @Override
  public void disallowSnapshot(String snapshot) throws IOException {
    asyncSnapshotProto.disallowSnapshot(snapshot);
  }

  @Override
  public void renameSnapshot(
      String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException {
    asyncSnapshotProto.renameSnapshot(
        snapshotRoot, snapshotOldName, snapshotNewName);
  }

  @Override
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    return asyncSnapshotProto.getSnapshottableDirListing();
  }

  @Override
  public SnapshotStatus[] getSnapshotListing(String snapshotRoot)
      throws IOException {
    return asyncSnapshotProto.getSnapshotListing(snapshotRoot);
  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(
      String snapshotRoot,
      String earlierSnapshotName, String laterSnapshotName) throws IOException {
    return asyncSnapshotProto.getSnapshotDiffReport(
        snapshotRoot, earlierSnapshotName, laterSnapshotName);
  }

  @Override
  public SnapshotDiffReportListing getSnapshotDiffReportListing(
      String snapshotRoot, String earlierSnapshotName, String laterSnapshotName,
      byte[] startPath, int index) throws IOException {
    return asyncSnapshotProto.getSnapshotDiffReportListing(
        snapshotRoot, earlierSnapshotName, laterSnapshotName, startPath, index);
  }

  @Override
  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs) throws IOException {
    RouterRpcClient rpcClient = getRpcClient();
    RouterRpcServer rpcServer = getRpcServer();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("getXAttrs",
        new Class<?>[] {String.class, List.class}, new RemoteParam(), xAttrs);
    rpcClient.invokeSequential(locations, method, List.class, null);
    return asyncReturn(List.class);
  }

  @Override
  public List<XAttr> listXAttrs(String src) throws IOException {
    RouterRpcClient rpcClient = getRpcClient();
    RouterRpcServer rpcServer = getRpcServer();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // TODO handle virtual directories
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("listXAttrs",
        new Class<?>[] {String.class}, new RemoteParam());
    rpcClient.invokeSequential(locations, method, List.class, null);
    return asyncReturn(List.class);
  }

  @Override
  public long getCurrentEditLogTxid() throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    ActiveNamenodeResolver namenodeResolver = getNamenodeResolver();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod(
        "getCurrentEditLogTxid", new Class<?>[] {});
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false, long.class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, Long> ret =
          (Map<FederationNamespaceInfo, Long>) o;
      // Return the maximum txid
      long txid = 0;
      for (long t : ret.values()) {
        if (t > txid) {
          txid = t;
        }
      }
      return txid;
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(Long.class);
  }

  @Override
  public Path getEnclosingRoot(String src) throws IOException {
    Path mountPath = null;
    if (isDefaultNameServiceEnabled()) {
      mountPath = new Path("/");
    }

    FileSubclusterResolver subclusterResolver = getSubclusterResolver();
    if (subclusterResolver instanceof MountTableResolver) {
      MountTableResolver mountTable = (MountTableResolver) subclusterResolver;
      if (mountTable.getMountPoint(src) != null) {
        mountPath = new Path(mountTable.getMountPoint(src).getSourcePath());
      }
    }

    if (mountPath == null) {
      throw new IOException(String.format("No mount point for %s", src));
    }

    getEZForPath(src);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    Path finalMountPath = mountPath;
    completableFuture = completableFuture.thenApply(o -> {
      EncryptionZone zone = (EncryptionZone) o;
      if (zone == null) {
        return finalMountPath;
      } else {
        Path zonePath = new Path(zone.getPath());
        return zonePath.depth() > finalMountPath.depth() ? zonePath : finalMountPath;
      }
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(Path.class);
  }


  @Override
  public long addCacheDirective(CacheDirectiveInfo path,
                                EnumSet<CacheFlag> flags) throws IOException {
    return routerAsyncCacheAdmin.addCacheDirective(path, flags);
  }

  @Override
  public void modifyCacheDirective(CacheDirectiveInfo directive,
                                   EnumSet<CacheFlag> flags) throws IOException {
    routerAsyncCacheAdmin.modifyCacheDirective(directive, flags);
  }

  @Override
  public void removeCacheDirective(long id) throws IOException {
    routerAsyncCacheAdmin.removeCacheDirective(id);
  }

  @Override
  public BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
      long prevId,
      CacheDirectiveInfo filter) throws IOException {
    return routerAsyncCacheAdmin.listCacheDirectives(prevId, filter);
  }

  @Override
  public void addCachePool(CachePoolInfo info) throws IOException {
    routerAsyncCacheAdmin.addCachePool(info);
  }

  @Override
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    routerAsyncCacheAdmin.modifyCachePool(info);
  }

  @Override
  public void removeCachePool(String cachePoolName) throws IOException {
    routerAsyncCacheAdmin.removeCachePool(cachePoolName);
  }

  @Override
  public BatchedRemoteIterator.BatchedEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    return routerAsyncCacheAdmin.listCachePools(prevKey);
  }

  @Override
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    asyncstoragePolicy.setStoragePolicy(src, policyName);
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    return asyncstoragePolicy.getStoragePolicies();
  }


  @Override
  public void unsetStoragePolicy(String src) throws IOException {
    asyncstoragePolicy.unsetStoragePolicy(src);
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    return asyncstoragePolicy.getStoragePolicy(path);
  }

  @Override
  public void satisfyStoragePolicy(String path) throws IOException {
    asyncstoragePolicy.satisfyStoragePolicy(path);
  }

  @Override
  public DatanodeInfo[] getSlowDatanodeReport() throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);
    return rpcServer.getSlowDatanodeReportAsync(true, 0);
  }

  @Override
  public boolean setReplication(String src, short replication)
      throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("setReplication",
        new Class<?>[] {String.class, short.class}, new RemoteParam(),
        replication);
    if (rpcServer.isInvokeConcurrent(src)) {
      rpcClient.invokeConcurrent(locations, method, Boolean.class);
      CompletableFuture<Object> completableFuture = getCompletableFuture();
      completableFuture = completableFuture.thenApply(o -> {
        Map<RemoteLocation, Boolean> results = (Map<RemoteLocation, Boolean>) o;
        return !results.containsValue(false);
      });
      setCurCompletableFuture(completableFuture);
    } else {
      rpcClient.invokeSequential(locations, method, Boolean.class,
          Boolean.TRUE);
    }
    return false;
  }

  public boolean isMultiDestDirectory(String src) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    try {
      if (rpcServer.isPathAll(src)) {
        List<RemoteLocation> locations;
        locations = rpcServer.getLocationsForPath(src, false, false);
        RemoteMethod method = new RemoteMethod("getFileInfo",
            new Class<?>[] {String.class}, new RemoteParam());
        rpcClient.invokeSequential(locations,
            method, HdfsFileStatus.class, null);
        CompletableFuture<Object> completableFuture = getCompletableFuture();
        completableFuture = completableFuture.thenApply(o -> {
          HdfsFileStatus fileStatus = (HdfsFileStatus) o;
          if (fileStatus != null) {
            return fileStatus.isDirectory();
          } else {
            LOG.debug("The destination {} doesn't exist.", src);
          }
          return false;
        });
        setCurCompletableFuture(completableFuture);
        return asyncReturn(Boolean.class);
      }
    } catch (UnresolvedPathException e) {
      LOG.debug("The destination {} is a symlink.", src);
    }
    setCurCompletableFuture(CompletableFuture.completedFuture(false));
    return asyncReturn(Boolean.class);
  }
}
