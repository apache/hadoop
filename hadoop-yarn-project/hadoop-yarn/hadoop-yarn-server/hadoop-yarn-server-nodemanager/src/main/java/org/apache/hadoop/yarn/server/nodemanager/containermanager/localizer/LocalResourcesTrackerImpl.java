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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRecoveredEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceReleaseEvent;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;


/**
 * A collection of {@link LocalizedResource}s all of same
 * {@link LocalResourceVisibility}.
 * 
 */

class LocalResourcesTrackerImpl implements LocalResourcesTracker {

  static final Logger LOG =
       LoggerFactory.getLogger(LocalResourcesTrackerImpl.class);
  private static final String RANDOM_DIR_REGEX = "-?\\d+";
  private static final Pattern RANDOM_DIR_PATTERN = Pattern
      .compile(RANDOM_DIR_REGEX);

  private final String user;
  private final ApplicationId appId;
  private final Dispatcher dispatcher;
  @VisibleForTesting
  final ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc;
  private Configuration conf;
  private LocalDirsHandlerService dirsHandler;
  /*
   * This flag controls whether this resource tracker uses hierarchical
   * directories or not. For PRIVATE and PUBLIC resource trackers it
   * will be set whereas for APPLICATION resource tracker it would
   * be false.
   */
  private final boolean useLocalCacheDirectoryManager;
  private ConcurrentHashMap<Path, LocalCacheDirectoryManager> directoryManagers;
  /*
   * It is used to keep track of resource into hierarchical directory
   * while it is getting downloaded. It is useful for reference counting
   * in case resource localization fails.
   */
  private ConcurrentHashMap<LocalResourceRequest, Path>
    inProgressLocalResourcesMap;
  /*
   * starting with 10 to accommodate 0-9 directories created as a part of
   * LocalCacheDirectoryManager. So there will be one unique number generator
   * per APPLICATION, USER and PUBLIC cache.
   */
  private AtomicLong uniqueNumberGenerator = new AtomicLong(9);
  private NMStateStoreService stateStore;

  public LocalResourcesTrackerImpl(String user, ApplicationId appId,
      Dispatcher dispatcher, boolean useLocalCacheDirectoryManager,
      Configuration conf, NMStateStoreService stateStore,
      LocalDirsHandlerService dirHandler) {
    this(user, appId, dispatcher,
        new ConcurrentHashMap<LocalResourceRequest, LocalizedResource>(),
        useLocalCacheDirectoryManager, conf, stateStore, dirHandler);
  }

  LocalResourcesTrackerImpl(String user, ApplicationId appId,
      Dispatcher dispatcher,
      ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc,
      boolean useLocalCacheDirectoryManager, Configuration conf,
      NMStateStoreService stateStore, LocalDirsHandlerService dirHandler) {
    this.appId = appId;
    this.user = user;
    this.dispatcher = dispatcher;
    this.localrsrc = localrsrc;
    this.useLocalCacheDirectoryManager = useLocalCacheDirectoryManager;
    if (this.useLocalCacheDirectoryManager) {
      directoryManagers =
          new ConcurrentHashMap<>();
      inProgressLocalResourcesMap =
          new ConcurrentHashMap<>();
    }
    this.conf = conf;
    this.stateStore = stateStore;
    this.dirsHandler = dirHandler;
  }

  /*
   * Synchronizing this method for avoiding races due to multiple ResourceEvent's
   * coming to LocalResourcesTracker from Public/Private localizer and
   * Resource Localization Service.
   */
  @Override
  public synchronized void handle(ResourceEvent event) {
    LocalResourceRequest req = event.getLocalResourceRequest();
    LocalizedResource rsrc = localrsrc.get(req);
    switch (event.getType()) {
    case LOCALIZED:
      if (useLocalCacheDirectoryManager) {
        inProgressLocalResourcesMap.remove(req);
      }
      break;
    case REQUEST:
      if (rsrc != null && (!isResourcePresent(rsrc))) {
        LOG.info("Resource " + rsrc.getLocalPath()
            + " is missing, localizing it again");
        removeResource(req);
        rsrc = null;
      }
      if (null == rsrc) {
        rsrc = new LocalizedResource(req, dispatcher);
        localrsrc.put(req, rsrc);
      }
      break;
    case RELEASE:
      if (null == rsrc) {
        // The container sent a release event on a resource which 
        // 1) Failed
        // 2) Removed for some reason (ex. disk is no longer accessible)
        ResourceReleaseEvent relEvent = (ResourceReleaseEvent) event;
        LOG.info("Container " + relEvent.getContainer()
            + " sent RELEASE event on a resource request " + req
            + " not present in cache.");
        return;
      }
      break;
    case LOCALIZATION_FAILED:
      /*
       * If resource localization fails then Localized resource will be
       * removed from local cache.
       */
      removeResource(req);
      break;
    case RECOVERED:
      if (rsrc != null) {
        LOG.warn("Ignoring attempt to recover existing resource " + rsrc);
        return;
      }
      rsrc = recoverResource(req, (ResourceRecoveredEvent) event);
      localrsrc.put(req, rsrc);
      break;
    }

    if (rsrc == null) {
      LOG.warn("Received " + event.getType() + " event for request " + req
          + " but localized resource is missing");
      return;
    }
    rsrc.handle(event);

    // Remove the resource if its downloading and its reference count has
    // become 0 after RELEASE. This maybe because a container was killed while
    // localizing and no other container is referring to the resource.
    // NOTE: This should NOT be done for public resources since the
    //       download is not associated with a container-specific localizer.
    if (event.getType() == ResourceEventType.RELEASE) {
      if (rsrc.getState() == ResourceState.DOWNLOADING &&
          rsrc.getRefCount() <= 0 &&
          rsrc.getRequest().getVisibility() != LocalResourceVisibility.PUBLIC) {
        removeResource(req);
      }
    }

    if (event.getType() == ResourceEventType.LOCALIZED) {
      if (rsrc.getLocalPath() != null) {
        try {
          stateStore.finishResourceLocalization(user, appId,
              buildLocalizedResourceProto(rsrc));
        } catch (IOException ioe) {
          LOG.error("Error storing resource state for " + rsrc, ioe);
        }
      } else {
        LOG.warn("Resource " + rsrc + " localized without a location");
      }
    }
  }

  private LocalizedResource recoverResource(LocalResourceRequest req,
      ResourceRecoveredEvent event) {
    // unique number for a resource is the directory of the resource
    Path localDir = event.getLocalPath().getParent();
    long rsrcId = Long.parseLong(localDir.getName());

    // update ID generator to avoid conflicts with existing resources
    while (true) {
      long currentRsrcId = uniqueNumberGenerator.get();
      long nextRsrcId = Math.max(currentRsrcId, rsrcId);
      if (uniqueNumberGenerator.compareAndSet(currentRsrcId, nextRsrcId)) {
        break;
      }
    }

    incrementFileCountForLocalCacheDirectory(localDir.getParent());

    return new LocalizedResource(req, dispatcher);
  }

  private LocalizedResourceProto buildLocalizedResourceProto(
      LocalizedResource rsrc) {
    return LocalizedResourceProto.newBuilder()
        .setResource(buildLocalResourceProto(rsrc.getRequest()))
        .setLocalPath(rsrc.getLocalPath().toString())
        .setSize(rsrc.getSize())
        .build();
  }

  private LocalResourceProto buildLocalResourceProto(LocalResource lr) {
    LocalResourcePBImpl lrpb;
    if (!(lr instanceof LocalResourcePBImpl)) {
      lr = LocalResource.newInstance(lr.getResource(), lr.getType(),
          lr.getVisibility(), lr.getSize(), lr.getTimestamp(),
          lr.getPattern());
    }
    lrpb = (LocalResourcePBImpl) lr;
    return lrpb.getProto();
  }

  public void incrementFileCountForLocalCacheDirectory(Path cacheDir) {
    if (useLocalCacheDirectoryManager) {
      Path cacheRoot = LocalCacheDirectoryManager.getCacheDirectoryRoot(
          cacheDir);
      if (cacheRoot != null) {
        LocalCacheDirectoryManager dir = directoryManagers.get(cacheRoot);
        if (dir == null) {
          dir = new LocalCacheDirectoryManager(conf);
          LocalCacheDirectoryManager otherDir =
              directoryManagers.putIfAbsent(cacheRoot, dir);
          if (otherDir != null) {
            dir = otherDir;
          }
        }
        if (cacheDir.equals(cacheRoot)) {
          dir.incrementFileCountForPath("");
        } else {
          String dirStr = cacheDir.toUri().getRawPath();
          String rootStr = cacheRoot.toUri().getRawPath();
          dir.incrementFileCountForPath(
              dirStr.substring(rootStr.length() + 1));
        }
      }
    }
  }

  /*
   * Update the file-count statistics for a local cache-directory.
   * This will retrieve the localized path for the resource from
   * 1) inProgressRsrcMap if the resource was under localization and it
   * failed.
   * 2) LocalizedResource if the resource is already localized.
   * From this path it will identify the local directory under which the
   * resource was localized. Then rest of the path will be used to decrement
   * file count for the HierarchicalSubDirectory pointing to this relative
   * path.
   */
  private void decrementFileCountForLocalCacheDirectory(LocalResourceRequest req,
      LocalizedResource rsrc) {
    if ( useLocalCacheDirectoryManager) {
      Path rsrcPath = null;
      if (inProgressLocalResourcesMap.containsKey(req)) {
        // This happens when localization of a resource fails.
        rsrcPath = inProgressLocalResourcesMap.remove(req);
      } else if (rsrc != null && rsrc.getLocalPath() != null) {
        rsrcPath = rsrc.getLocalPath().getParent().getParent();
      }
      if (rsrcPath != null) {
        Path parentPath = new Path(rsrcPath.toUri().getRawPath());
        while (!directoryManagers.containsKey(parentPath)) {
          parentPath = parentPath.getParent();
          if ( parentPath == null) {
            return;
          }
        }
        if ( parentPath != null) {
          String parentDir = parentPath.toUri().getRawPath().toString();
          LocalCacheDirectoryManager dir = directoryManagers.get(parentPath);
          String rsrcDir = rsrcPath.toUri().getRawPath(); 
          if (rsrcDir.equals(parentDir)) {
            dir.decrementFileCountForPath("");
          } else {
            dir.decrementFileCountForPath(
              rsrcDir.substring(
              parentDir.length() + 1));
          }
        }
      }
    }
  }

/**
   * This module checks if the resource which was localized is already present
   * or not
   * 
   * @param rsrc
   * @return true/false based on resource is present or not
   */
  public boolean isResourcePresent(LocalizedResource rsrc) {
    boolean ret = true;
    if (rsrc.getState() == ResourceState.LOCALIZED) {
      File file = new File(rsrc.getLocalPath().toUri().getRawPath().
        toString());
      if (!file.exists()) {
        ret = false;
      } else if (dirsHandler != null) {
        ret = checkLocalResource(rsrc);
      }
    }
    return ret;
  }

  /**
   * Check if the rsrc is Localized on a good dir.
   *
   * @param rsrc
   * @return
   */
  @VisibleForTesting
  boolean checkLocalResource(LocalizedResource rsrc) {
    List<String> localDirs = dirsHandler.getLocalDirsForRead();
    for (String dir : localDirs) {
      if (isParent(rsrc.getLocalPath().toUri().getPath(), dir)) {
        return true;
      } else {
        continue;
      }
    }
    return false;
  }

  /**
   * @param path
   * @param parentdir
   * @return true if parentdir is parent of path else false.
   */
  private boolean isParent(String path, String parentdir) {
    // Add separator if not present.
    if (path.charAt(path.length() - 1) != File.separatorChar) {
      path += File.separator;
    }
    return path.startsWith(parentdir);
  }

  @Override
  public boolean remove(LocalizedResource rem, DeletionService delService) {
 // current synchronization guaranteed by crude RLS event for cleanup
    LocalizedResource rsrc = localrsrc.get(rem.getRequest());
    if (null == rsrc) {
      LOG.error("Attempt to remove absent resource: " + rem.getRequest()
          + " from " + getUser());
      return true;
    }
    if (rsrc.getRefCount() > 0
        || ResourceState.DOWNLOADING.equals(rsrc.getState()) || rsrc != rem) {
      // internal error
      LOG.error("Attempt to remove resource: " + rsrc
          + " with non-zero refcount");
      return false;
    } else { // ResourceState is LOCALIZED or INIT
      if (ResourceState.LOCALIZED.equals(rsrc.getState())) {
        FileDeletionTask deletionTask = new FileDeletionTask(delService,
            getUser(), getPathToDelete(rsrc.getLocalPath()), null);
        delService.delete(deletionTask);
      }
      removeResource(rem.getRequest());
      LOG.info("Removed " + rsrc.getLocalPath() + " from localized cache");
      return true;
    }
  }

  private void removeResource(LocalResourceRequest req) {
    LocalizedResource rsrc = localrsrc.remove(req);
    decrementFileCountForLocalCacheDirectory(req, rsrc);
    if (rsrc != null) {
      Path localPath = rsrc.getLocalPath();
      if (localPath != null) {
        try {
          stateStore.removeLocalizedResource(user, appId, localPath);
        } catch (IOException e) {
          LOG.error("Unable to remove resource " + rsrc + " from state store",
              e);
        }
      }
    }
  }

  /**
   * Returns the path up to the random directory component.
   */
  private Path getPathToDelete(Path localPath) {
    Path delPath = localPath.getParent();
    String name = delPath.getName();
    Matcher matcher = RANDOM_DIR_PATTERN.matcher(name);
    if (matcher.matches()) {
      return delPath;
    } else {
      LOG.warn("Random directory component did not match. " +
      		"Deleting localized path only");
      return localPath;
    }
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public Iterator<LocalizedResource> iterator() {
    return localrsrc.values().iterator();
  }

  /**
   * @return {@link Path} absolute path for localization which includes local
   *         directory path and the relative hierarchical path (if use local
   *         cache directory manager is enabled)
   * 
   * @param {@link LocalResourceRequest} Resource localization request to
   *        localize the resource.
   * @param {@link Path} local directory path
   * @param {@link DeletionService} Deletion Service to delete existing
   *        path for localization.
   */
  @Override
  public Path getPathForLocalization(LocalResourceRequest req,
      Path localDirPath, DeletionService delService) {
    Path rPath = localDirPath;
    if (useLocalCacheDirectoryManager && localDirPath != null) {

      if (!directoryManagers.containsKey(localDirPath)) {
        directoryManagers.putIfAbsent(localDirPath,
          new LocalCacheDirectoryManager(conf));
      }
      LocalCacheDirectoryManager dir = directoryManagers.get(localDirPath);

      rPath = localDirPath;
      String hierarchicalPath = dir.getRelativePathForLocalization();
      // For most of the scenarios we will get root path only which
      // is an empty string
      if (!hierarchicalPath.isEmpty()) {
        rPath = new Path(localDirPath, hierarchicalPath);
      }
      inProgressLocalResourcesMap.put(req, rPath);
    }

    while (true) {
      Path uniquePath = new Path(rPath,
          Long.toString(uniqueNumberGenerator.incrementAndGet()));
      File file = new File(uniquePath.toUri().getRawPath());
      if (!file.exists()) {
        rPath = uniquePath;
        break;
      }
      // If the directory already exists, delete it and move to next one.
      LOG.warn("Directory " + uniquePath + " already exists, " +
          "try next one.");
      if (delService != null) {
        FileDeletionTask deletionTask = new FileDeletionTask(delService,
            getUser(), uniquePath, null);
        delService.delete(deletionTask);
      }
    }

    Path localPath = new Path(rPath, req.getPath().getName());
    LocalizedResource rsrc = localrsrc.get(req);
    if (rsrc == null) {
      LOG.warn("Resource " + req + " has been removed"
          + " and will no longer be localized");
      return null;
    }
    rsrc.setLocalPath(localPath);
    LocalResource lr = LocalResource.newInstance(req.getResource(),
        req.getType(), req.getVisibility(), req.getSize(),
        req.getTimestamp());
    try {
      stateStore.startResourceLocalization(user, appId,
          ((LocalResourcePBImpl) lr).getProto(), localPath);
    } catch (IOException e) {
      LOG.error("Unable to record localization start for " + rsrc, e);
    }
    return rPath;
  }

  @Override
  public LocalizedResource getLocalizedResource(LocalResourceRequest request) {
    return localrsrc.get(request);
  }

  @VisibleForTesting
  LocalCacheDirectoryManager getDirectoryManager(Path localDirPath) {
    LocalCacheDirectoryManager mgr = null;
    if (useLocalCacheDirectoryManager) {
      mgr = directoryManagers.get(localDirPath);
    }
    return mgr;
  }

  @VisibleForTesting
  LocalDirsHandlerService getDirsHandler() {
    return dirsHandler;
  }
}
