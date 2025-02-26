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
package org.apache.hadoop.hdfs.server.federation.router.async;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RouterResolveException;
import org.apache.hadoop.hdfs.server.federation.router.NoLocationException;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.RemoteParam;
import org.apache.hadoop.hdfs.server.federation.router.RemoteResult;
import org.apache.hadoop.hdfs.server.federation.router.RouterClientProtocol;
import org.apache.hadoop.hdfs.server.federation.router.RouterFederationRename;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.ApplyFunction;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncApplyFunction;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncCatchFunction;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.CatchFunction;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.security.UserGroupInformation;
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
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.server.federation.router.FederationUtil.updateMountPointStatus;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncApply;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncCatch;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncComplete;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncCompleteWith;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncForEach;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncReturn;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncTry;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.getCompletableFuture;

/**
 * Module that implements all the async RPC calls in {@link ClientProtocol} in the
 * {@link RouterRpcServer}.
 */
public class RouterAsyncClientProtocol extends RouterClientProtocol {
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAsyncClientProtocol.class.getName());

  private final RouterRpcServer rpcServer;
  private final RouterRpcClient rpcClient;
  private final RouterFederationRename rbfRename;
  private final FileSubclusterResolver subclusterResolver;
  private final ActiveNamenodeResolver namenodeResolver;
  /** If it requires response from all subclusters. */
  private final boolean allowPartialList;
  /** Time out when getting the mount statistics. */
  private long mountStatusTimeOut;
  /** Identifier for the super user. */
  private String superUser;
  /** Identifier for the super group. */
  private final String superGroup;
  /**
   * Caching server defaults so as to prevent redundant calls to namenode,
   * similar to DFSClient, caching saves efforts when router connects
   * to multiple clients.
   */
  private volatile FsServerDefaults serverDefaults;

  public RouterAsyncClientProtocol(Configuration conf, RouterRpcServer rpcServer) {
    super(conf, rpcServer);
    this.rpcServer = rpcServer;
    this.rpcClient = rpcServer.getRPCClient();
    this.rbfRename = getRbfRename();
    this.subclusterResolver = getSubclusterResolver();
    this.namenodeResolver = getNamenodeResolver();
    this.allowPartialList = isAllowPartialList();
    this.mountStatusTimeOut = getMountStatusTimeOut();
    this.superUser = getSuperUser();
    this.superGroup = getSuperGroup();
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);
    long now = Time.monotonicNow();
    if ((serverDefaults == null) || (now - getServerDefaultsLastUpdate()
        > getServerDefaultsValidityPeriod())) {
      RemoteMethod method = new RemoteMethod("getServerDefaults");
      rpcServer.invokeAtAvailableNsAsync(method, FsServerDefaults.class);
      asyncApply(o -> {
        serverDefaults = (FsServerDefaults) o;
        setServerDefaultsLastUpdate(now);
        return serverDefaults;
      });
    } else {
      asyncComplete(serverDefaults);
    }
    return asyncReturn(FsServerDefaults.class);
  }

  @Override
  public HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag,
      boolean createParent, short replication, long blockSize,
      CryptoProtocolVersion[] supportedVersions, String ecPolicyName,
      String storagePolicy) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    if (createParent && rpcServer.isPathAll(src)) {
      int index = src.lastIndexOf(Path.SEPARATOR);
      String parent = src.substring(0, index);
      LOG.debug("Creating {} requires creating parent {}", src, parent);
      FsPermission parentPermissions = getParentPermission(masked);
      mkdirs(parent, parentPermissions, createParent);
      asyncApply((ApplyFunction<Boolean, Boolean>) success -> {
        if (!success) {
          // This shouldn't happen as mkdirs returns true or exception
          LOG.error("Couldn't create parents for {}", src);
        }
        return success;
      });
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
    final RemoteLocation[] createLocation = new RemoteLocation[1];
    asyncTry(() -> {
      rpcServer.getCreateLocationAsync(src, locations);
      asyncApply((AsyncApplyFunction<RemoteLocation, Object>) remoteLocation -> {
        createLocation[0] = remoteLocation;
        rpcClient.invokeSingle(remoteLocation, method, HdfsFileStatus.class);
        asyncApply((ApplyFunction<HdfsFileStatus, Object>) status -> {
          status.setNamespace(remoteLocation.getNameserviceId());
          return status;
        });
      });
    });
    asyncCatch((AsyncCatchFunction<Object, IOException>) (o, ioe) -> {
      final List<RemoteLocation> newLocations = checkFaultTolerantRetry(
          method, src, ioe, createLocation[0], locations);
      rpcClient.invokeSequential(
          newLocations, method, HdfsFileStatus.class, null);
    }, IOException.class);

    return asyncReturn(HdfsFileStatus.class);
  }

  @Override
  public LastBlockWithStatus append(
      String src, String clientName,
      EnumSetWritable<CreateFlag> flag) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, true);
    RemoteMethod method = new RemoteMethod("append",
        new Class<?>[] {String.class, String.class, EnumSetWritable.class},
        new RemoteParam(), clientName, flag);
    rpcClient.invokeSequential(method, locations, LastBlockWithStatus.class, null);
    asyncApply((ApplyFunction<RemoteResult, LastBlockWithStatus>) result -> {
      LastBlockWithStatus lbws = (LastBlockWithStatus) result.getResult();
      lbws.getFileStatus().setNamespace(result.getLocation().getNameserviceId());
      return lbws;
    });
    return asyncReturn(LastBlockWithStatus.class);
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
      asyncComplete(
          rbfRename.routerFedRename(src, dst, srcLocations, dstLocations));
      return asyncReturn(Boolean.class);
    }
    RemoteMethod method = new RemoteMethod("rename",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), dstParam);
    isMultiDestDirectory(src);
    asyncApply((AsyncApplyFunction<Boolean, Boolean>) isMultiDestDirectory -> {
      if (isMultiDestDirectory) {
        if (locs.size() != srcLocations.size()) {
          throw new IOException("Rename of " + src + " to " + dst + " is not"
              + " allowed. The number of remote locations for both source and"
              + " target should be same.");
        }
        rpcClient.invokeAll(locs, method);
      } else {
        rpcClient.invokeSequential(locs, method, Boolean.class,
            Boolean.TRUE);
      }
    });
    return asyncReturn(Boolean.class);
  }

  @Override
  public void rename2(
      final String src, final String dst,
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
    isMultiDestDirectory(src);
    asyncApply((AsyncApplyFunction<Boolean, Boolean>) isMultiDestDirectory -> {
      if (isMultiDestDirectory) {
        if (locs.size() != srcLocations.size()) {
          throw new IOException("Rename of " + src + " to " + dst + " is not"
              + " allowed. The number of remote locations for both source and"
              + " target should be same.");
        }
        rpcClient.invokeConcurrent(locs, method);
      } else {
        rpcClient.invokeSequential(locs, method, null, null);
      }
    });
  }

  @Override
  public void concat(String trg, String[] src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    // Concat only effects when all files in the same namespace.
    getFileRemoteLocation(trg);
    asyncApply((AsyncApplyFunction<RemoteLocation, Object>) targetDestination -> {
      if (targetDestination == null) {
        throw new IOException("Cannot find target file - " + trg);
      }
      String targetNameService = targetDestination.getNameserviceId();
      String[] sourceDestinations = new String[src.length];
      int[] index = new int[1];
      asyncForEach(Arrays.stream(src).iterator(), (forEachRun, sourceFile) -> {
        getFileRemoteLocation(sourceFile);
        asyncApply((ApplyFunction<RemoteLocation, Object>) srcLocation -> {
          if (srcLocation == null) {
            throw new IOException("Cannot find source file - " + sourceFile);
          }
          sourceDestinations[index[0]++] = srcLocation.getDest();
          if (!targetNameService.equals(srcLocation.getNameserviceId())) {
            throw new IOException("Cannot concatenate source file " + sourceFile
                + " because it is located in a different namespace" + " with nameservice "
                + srcLocation.getNameserviceId() + " from the target file with nameservice "
                + targetNameService);
          }
          return null;
        });
      });
      asyncApply((AsyncApplyFunction<Object, Object>) o -> {
        // Invoke
        RemoteMethod method = new RemoteMethod("concat",
            new Class<?>[] {String.class, String[].class},
            targetDestination.getDest(), sourceDestinations);
        rpcClient.invokeSingle(targetDestination, method, Void.class);
      });
    });
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

    asyncComplete(false);
    if (locations.size() > 1) {
      // Check if this directory already exists
      asyncTry(() -> {
        getFileInfo(src);
        asyncApply((ApplyFunction<HdfsFileStatus, Boolean>) fileStatus -> {
          if (fileStatus != null) {
            // When existing, the NN doesn't return an exception; return true
            return true;
          }
          return false;
        });
      });
      asyncCatch((ret, ex) -> {
        // Can't query if this file exists or not.
        LOG.error("Error getting file info for {} while proxying mkdirs: {}",
            src, ex.getMessage());
        return false;
      }, IOException.class);
    }

    final RemoteLocation firstLocation = locations.get(0);
    asyncApply((AsyncApplyFunction<Boolean, Boolean>) success -> {
      if (success) {
        asyncComplete(true);
        return;
      }
      asyncTry(() -> {
        rpcClient.invokeSingle(firstLocation, method, Boolean.class);
      });

      asyncCatch((AsyncCatchFunction<Object, IOException>) (o, ioe) -> {
        final List<RemoteLocation> newLocations = checkFaultTolerantRetry(
            method, src, ioe, firstLocation, locations);
        rpcClient.invokeSequential(
            newLocations, method, Boolean.class, Boolean.TRUE);
      }, IOException.class);
    });

    return asyncReturn(Boolean.class);
  }

  @Override
  public DirectoryListing getListing(
      String src, byte[] startAfter, boolean needLocation) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);
    GetListingComparator comparator = RouterClientProtocol.getComparator();
    getListingInt(src, startAfter, needLocation);
    asyncApply((AsyncApplyFunction<List<RemoteResult<RemoteLocation, DirectoryListing>>, Object>)
        listings -> {
        TreeMap<byte[], HdfsFileStatus> nnListing = new TreeMap<>(comparator);
        int totalRemainingEntries = 0;
        final int[] remainingEntries = {0};
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
                  remainingEntries[0]++;
                } else {
                  nnListing.put(filename, file);
                }
              }
              remainingEntries[0] += listing.getRemainingEntries();
            }
          }
        }

        // Add mount points at this level in the tree
        final List<String> children = subclusterResolver.getMountPoints(src);
        if (children != null) {
          // Get the dates for each mount point
          Map<String, Long> dates = getMountPointDates(src);
          byte[] finalLastName = lastName;
          asyncForEach(children.iterator(), (forEachRun, child) -> {
            long date = 0;
            if (dates != null && dates.containsKey(child)) {
              date = dates.get(child);
            }
            Path childPath = new Path(src, child);
            getMountPointStatus(childPath.toString(), 0, date);
            asyncApply((ApplyFunction<HdfsFileStatus, Object>) dirStatus -> {
              // if there is no subcluster path, always add mount point
              byte[] bChild = DFSUtil.string2Bytes(child);
              if (finalLastName == null) {
                nnListing.put(bChild, dirStatus);
              } else {
                if (shouldAddMountPoint(bChild,
                    finalLastName, startAfter, remainingEntries[0])) {
                  // This may overwrite existing listing entries with the mount point
                  // TODO don't add if already there?
                  nnListing.put(bChild, dirStatus);
                }
              }
              return null;
            });
          });
          asyncApply(o -> {
            // Update the remaining count to include left mount points
            if (nnListing.size() > 0) {
              byte[] lastListing = nnListing.lastKey();
              for (int i = 0; i < children.size(); i++) {
                byte[] bChild = DFSUtil.string2Bytes(children.get(i));
                if (comparator.compare(bChild, lastListing) > 0) {
                  remainingEntries[0] += (children.size() - i);
                  break;
                }
              }
            }
            return null;
          });
        }
        asyncComplete(namenodeListingExists);
        asyncApply((ApplyFunction<Boolean, Object>) exists -> {
          if (!exists && nnListing.size() == 0 && children == null) {
            // NN returns a null object if the directory cannot be found and has no
            // listing. If we didn't retrieve any NN listing data, and there are no
            // mount points here, return null.
            return null;
          }

          // Generate combined listing
          HdfsFileStatus[] combinedData = new HdfsFileStatus[nnListing.size()];
          combinedData = nnListing.values().toArray(combinedData);
          return new DirectoryListing(combinedData, remainingEntries[0]);
        });
      });
    return asyncReturn(DirectoryListing.class);
  }

  /**
   * Get listing on remote locations.
   */
  @Override
  protected List<RemoteResult<RemoteLocation, DirectoryListing>> getListingInt(
      String src, byte[] startAfter, boolean needLocation) throws IOException {
    List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    // Locate the dir and fetch the listing.
    if (locations.isEmpty()) {
      asyncComplete(new ArrayList<>());
      return asyncReturn(List.class);
    }
    asyncTry(() -> {
      RemoteMethod method = new RemoteMethod("getListing",
          new Class<?>[] {String.class, startAfter.getClass(), boolean.class},
          new RemoteParam(), startAfter, needLocation);
      rpcClient.invokeConcurrent(locations, method, false, -1,
          DirectoryListing.class);
    });
    asyncCatch((CatchFunction<List, RouterResolveException>) (o, e) -> {
      LOG.debug("Cannot get locations for {}, {}.", src, e.getMessage());
      LOG.info("Cannot get locations for {}, {}.", src, e.getMessage());
      return new ArrayList<>();
    }, RouterResolveException.class);
    return asyncReturn(List.class);
  }


  @Override
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final IOException[] noLocationException = new IOException[1];
    asyncTry(() -> {
      final List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, false, false);
      RemoteMethod method = new RemoteMethod("getFileInfo",
          new Class<?>[] {String.class}, new RemoteParam());
      // If it's a directory, we check in all locations
      if (rpcServer.isPathAll(src)) {
        getFileInfoAll(locations, method);
      } else {
        // Check for file information sequentially
        rpcClient.invokeSequential(locations, method, HdfsFileStatus.class, null);
      }
    });
    asyncCatch((o, e) -> {
      if (e instanceof NoLocationException
          || e instanceof RouterResolveException) {
        noLocationException[0] = e;
      }
      throw e;
    }, IOException.class);

    asyncApply((AsyncApplyFunction<HdfsFileStatus, Object>) ret -> {
      // If there is no real path, check mount points
      if (ret == null) {
        List<String> children = subclusterResolver.getMountPoints(src);
        if (children != null && !children.isEmpty()) {
          Map<String, Long> dates = getMountPointDates(src);
          long date = 0;
          if (dates != null && dates.containsKey(src)) {
            date = dates.get(src);
          }
          getMountPointStatus(src, children.size(), date, false);
        } else if (children != null) {
          // The src is a mount point, but there are no files or directories
          getMountPointStatus(src, 0, 0, false);
        } else {
          asyncComplete(null);
        }
        asyncApply((ApplyFunction<HdfsFileStatus, HdfsFileStatus>) result -> {
          // Can't find mount point for path and the path didn't contain any sub monit points,
          // throw the NoLocationException to client.
          if (result == null && noLocationException[0] != null) {
            throw noLocationException[0];
          }

          return result;
        });
      } else {
        asyncComplete(ret);
      }
    });

    return asyncReturn(HdfsFileStatus.class);
  }

  @Override
  public RemoteLocation getFileRemoteLocation(String path) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations = rpcServer.getLocationsForPath(path, false, false);
    if (locations.size() == 1) {
      asyncComplete(locations.get(0));
      return asyncReturn(RemoteLocation.class);
    }

    asyncForEach(locations.iterator(), (forEachRun, location) -> {
      RemoteMethod method =
          new RemoteMethod("getFileInfo", new Class<?>[] {String.class}, new RemoteParam());
      rpcClient.invokeSequential(Collections.singletonList(location), method,
          HdfsFileStatus.class, null);
      asyncApply((ApplyFunction<HdfsFileStatus, RemoteLocation>) ret -> {
        if (ret != null) {
          forEachRun.breakNow();
          return location;
        }
        return null;
      });
    });

    return asyncReturn(RemoteLocation.class);
  }

  @Override
  public HdfsFileStatus getMountPointStatus(
      String name, int childrenNum, long date, boolean setPath) {
    long modTime = date;
    long accessTime = date;
    final FsPermission[] permission = new FsPermission[]{FsPermission.getDirDefault()};
    final String[] owner = new String[]{this.superUser};
    final String[] group = new String[]{this.superGroup};
    final int[] childrenNums = new int[]{childrenNum};
    final EnumSet<HdfsFileStatus.Flags>[] flags =
        new EnumSet[]{EnumSet.noneOf(HdfsFileStatus.Flags.class)};
    asyncComplete(null);
    if (getSubclusterResolver() instanceof MountTableResolver) {
      asyncTry(() -> {
        String mName = name.startsWith("/") ? name : "/" + name;
        MountTableResolver mountTable = (MountTableResolver) subclusterResolver;
        MountTable entry = mountTable.getMountPoint(mName);
        if (entry != null) {
          permission[0] = entry.getMode();
          owner[0] = entry.getOwnerName();
          group[0] = entry.getGroupName();

          RemoteMethod method = new RemoteMethod("getFileInfo",
              new Class<?>[] {String.class}, new RemoteParam());
          getFileInfoAll(
              entry.getDestinations(), method, mountStatusTimeOut);
          asyncApply((ApplyFunction<HdfsFileStatus, HdfsFileStatus>) fInfo -> {
            if (fInfo != null) {
              permission[0] = fInfo.getPermission();
              owner[0] = fInfo.getOwner();
              group[0] = fInfo.getGroup();
              childrenNums[0] = fInfo.getChildrenNum();
              flags[0] = DFSUtil
                  .getFlags(fInfo.isEncrypted(), fInfo.isErasureCoded(),
                      fInfo.isSnapshotEnabled(), fInfo.hasAcl());
            }
            return fInfo;
          });
        }
      });
      asyncCatch((CatchFunction<HdfsFileStatus, IOException>) (status, e) -> {
        LOG.error("Cannot get mount point: {}", e.getMessage());
        return status;
      }, IOException.class);
    } else {
      try {
        UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
        owner[0] = ugi.getUserName();
        group[0] = ugi.getPrimaryGroupName();
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
    HdfsFileStatus.Builder builder = new HdfsFileStatus.Builder();
    asyncApply((ApplyFunction<HdfsFileStatus, HdfsFileStatus>) status -> {
      if (setPath) {
        Path path = new Path(name);
        String nameStr = path.getName();
        builder.path(DFSUtil.string2Bytes(nameStr));
      }

      return builder.isdir(true)
          .mtime(modTime)
          .atime(accessTime)
          .perm(permission[0])
          .owner(owner[0])
          .group(group[0])
          .symlink(new byte[0])
          .fileId(inodeId)
          .children(childrenNums[0])
          .flags(flags[0])
          .build();
    });
    return asyncReturn(HdfsFileStatus.class);
  }

  @Override
  protected HdfsFileStatus getFileInfoAll(final List<RemoteLocation> locations,
      final RemoteMethod method, long timeOutMs) throws IOException {

    asyncComplete(null);
    // Get the file info from everybody
    rpcClient.invokeConcurrent(locations, method, false, false, timeOutMs,
        HdfsFileStatus.class);
    asyncApply(res -> {
      Map<RemoteLocation, HdfsFileStatus> results = (Map<RemoteLocation, HdfsFileStatus>) res;
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
    return asyncReturn(HdfsFileStatus.class);
  }

  @Override
  public boolean recoverLease(String src, String clientName) throws IOException {
    super.recoverLease(src, clientName);
    return asyncReturn(boolean.class);
  }

  @Override
  public long[] getStats() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("getStats");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false, long[].class);
    asyncApply(o -> {
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
    return asyncReturn(long[].class);
  }

  @Override
  public ReplicatedBlockStats getReplicatedBlockStats() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getReplicatedBlockStats");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true,
        false, ReplicatedBlockStats.class);
    asyncApply(o -> {
      Map<FederationNamespaceInfo, ReplicatedBlockStats> ret =
          (Map<FederationNamespaceInfo, ReplicatedBlockStats>) o;
      return ReplicatedBlockStats.merge(ret.values());
    });
    return asyncReturn(ReplicatedBlockStats.class);
  }

  @Override
  public DatanodeInfo[] getDatanodeReport(HdfsConstants.DatanodeReportType type)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);
    return rpcServer.getDatanodeReportAsync(type, true, 0);
  }

  @Override
  public DatanodeInfo[] getSlowDatanodeReport() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);
    return rpcServer.getSlowDatanodeReportAsync(true, 0);
  }

  @Override
  public DatanodeStorageReport[] getDatanodeStorageReport(
      HdfsConstants.DatanodeReportType type) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    rpcServer.getDatanodeStorageReportMapAsync(type);
    asyncApply((ApplyFunction< Map<String, DatanodeStorageReport[]>, DatanodeStorageReport[]>)
        dnSubcluster -> mergeDtanodeStorageReport(dnSubcluster));
    return asyncReturn(DatanodeStorageReport[].class);
  }

  public DatanodeStorageReport[] getDatanodeStorageReport(
      HdfsConstants.DatanodeReportType type, boolean requireResponse, long timeOutMs)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    rpcServer.getDatanodeStorageReportMapAsync(type, requireResponse, timeOutMs);
    asyncApply((ApplyFunction< Map<String, DatanodeStorageReport[]>, DatanodeStorageReport[]>)
        dnSubcluster -> mergeDtanodeStorageReport(dnSubcluster));
    return asyncReturn(DatanodeStorageReport[].class);
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
    rpcClient.invokeConcurrent(
        nss, method, true, !isChecked, Boolean.class);

    asyncApply(o -> {
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
    return asyncReturn(Boolean.class);
  }

  @Override
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("saveNamespace",
        new Class<?>[] {long.class, long.class}, timeWindow, txGap);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true,
        false, boolean.class);

    asyncApply(o -> {
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
    return asyncReturn(Boolean.class);
  }

  @Override
  public long rollEdits() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("rollEdits", new Class<?>[] {});
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false, long.class);
    asyncApply(o -> {
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
    return asyncReturn(long.class);
  }

  @Override
  public boolean restoreFailedStorage(String arg) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);

    RemoteMethod method = new RemoteMethod("restoreFailedStorage",
        new Class<?>[] {String.class}, arg);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false, Boolean.class);
    asyncApply(o -> {
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
    return asyncReturn(boolean.class);
  }

  @Override
  public RollingUpgradeInfo rollingUpgrade(HdfsConstants.RollingUpgradeAction action)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("rollingUpgrade",
        new Class<?>[] {HdfsConstants.RollingUpgradeAction.class}, action);
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();

    rpcClient.invokeConcurrent(
        nss, method, true, false, RollingUpgradeInfo.class);
    asyncApply(o -> {
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
    return asyncReturn(RollingUpgradeInfo.class);
  }

  @Override
  public ContentSummary getContentSummary(String path) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // Get the summaries from regular files
    final Collection<ContentSummary> summaries = new ArrayList<>();
    final List<RemoteLocation> locations = getLocationsForContentSummary(path);
    final RemoteMethod method = new RemoteMethod("getContentSummary",
        new Class<?>[] {String.class}, new RemoteParam());
    rpcClient.invokeConcurrent(locations, method,
        false, -1, ContentSummary.class);

    asyncApply(o -> {
      final List<RemoteResult<RemoteLocation, ContentSummary>> results =
          (List<RemoteResult<RemoteLocation, ContentSummary>>) o;

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
    });

    return asyncReturn(ContentSummary.class);
  }

  @Override
  public long getCurrentEditLogTxid() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod(
        "getCurrentEditLogTxid", new Class<?>[] {});
    final Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(nss, method, true, false, long.class);

    asyncApply(o -> {
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
    return asyncReturn(long.class);
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
      asyncCompleteWith(CompletableFuture.completedFuture(null));
      return;
    }
    rpcClient.invokeConcurrent(namespacesEligibleForObserverReads, method);
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
      rpcClient.invokeConcurrent(locations, method, Boolean.class);
      asyncApply(o -> {
        Map<RemoteLocation, Boolean> results = (Map<RemoteLocation, Boolean>) o;
        return !results.containsValue(false);
      });
    } else {
      rpcClient.invokeSequential(locations, method, Boolean.class,
          Boolean.TRUE);
    }
    return asyncReturn(boolean.class);
  }

  /**
   * Checks if the path is a directory and is supposed to be present in all
   * subclusters.
   * @param src the source path
   * @return true if the path is directory and is supposed to be present in all
   *         subclusters else false in all other scenarios.
   * @throws IOException if unable to get the file status.
   */
  @Override
  public boolean isMultiDestDirectory(String src) throws IOException {
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
        asyncCompleteWith(completableFuture);
        return asyncReturn(Boolean.class);
      }
    } catch (UnresolvedPathException e) {
      LOG.debug("The destination {} is a symlink.", src);
    }
    asyncCompleteWith(CompletableFuture.completedFuture(false));
    return asyncReturn(Boolean.class);
  }
}
