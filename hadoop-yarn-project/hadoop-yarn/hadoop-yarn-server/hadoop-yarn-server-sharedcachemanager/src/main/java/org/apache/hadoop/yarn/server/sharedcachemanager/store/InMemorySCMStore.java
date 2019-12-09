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

package org.apache.hadoop.yarn.server.sharedcachemanager.store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.sharedcache.SharedCacheUtil;
import org.apache.hadoop.yarn.server.sharedcachemanager.AppChecker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A thread safe version of an in-memory SCM store. The thread safety is
 * implemented with two key pieces: (1) at the mapping level a ConcurrentHashMap
 * is used to allow concurrency to resources and their associated references,
 * and (2) a key level lock is used to ensure mutual exclusion between any
 * operation that accesses a resource with the same key. <br>
 * <br>
 * To ensure safe key-level locking, we use the original string key and intern
 * it weakly using hadoop's <code>StringInterner</code>. It avoids the pitfalls
 * of using built-in String interning. The interned strings are also weakly
 * referenced, so it can be garbage collected once it is done. And there is
 * little risk of keys being available for other parts of the code so they can
 * be used as locks accidentally. <br>
 * <br>
 * Resources in the in-memory store are evicted based on a time staleness
 * criteria. If a resource is not referenced (i.e. used) for a given period, it
 * is designated as a stale resource and is considered evictable.
 */
@Private
@Evolving
public class InMemorySCMStore extends SCMStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(InMemorySCMStore.class);

  private final Map<String, SharedCacheResource> cachedResources =
      new ConcurrentHashMap<String, SharedCacheResource>();
  private Collection<ApplicationId> initialApps =
      new ArrayList<ApplicationId>();
  private final Object initialAppsLock = new Object();
  private long startTime;
  private int stalenessMinutes;
  private ScheduledExecutorService scheduler;
  private int initialDelayMin;
  private int checkPeriodMin;

  public InMemorySCMStore() {
    super(InMemorySCMStore.class.getName());
  }

  @VisibleForTesting
  public InMemorySCMStore(AppChecker appChecker) {
    super(InMemorySCMStore.class.getName(), appChecker);
  }

  private String intern(String key) {
    return StringInterner.weakIntern(key);
  }

  /**
   * The in-memory store bootstraps itself from the shared cache entries that
   * exist in HDFS.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    this.startTime = System.currentTimeMillis();
    this.initialDelayMin = getInitialDelay(conf);
    this.checkPeriodMin = getCheckPeriod(conf);
    this.stalenessMinutes = getStalenessPeriod(conf);

    bootstrap(conf);

    ThreadFactory tf =
        new ThreadFactoryBuilder().setNameFormat("InMemorySCMStore")
            .build();
    scheduler = HadoopExecutors.newSingleThreadScheduledExecutor(tf);

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // start composed services first
    super.serviceStart();

    // Get initial list of running applications
    LOG.info("Getting the active app list to initialize the in-memory scm store");
    synchronized (initialAppsLock) {
      initialApps = appChecker.getActiveApplications();
    }
    LOG.info(initialApps.size() + " apps recorded as active at this time");

    Runnable task = new AppCheckTask(appChecker);
    scheduler.scheduleAtFixedRate(task, initialDelayMin, checkPeriodMin,
        TimeUnit.MINUTES);
    LOG.info("Scheduled the in-memory scm store app check task to run every "
        + checkPeriodMin + " minutes.");
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping the " + InMemorySCMStore.class.getSimpleName()
        + " service.");
    if (scheduler != null) {
      LOG.info("Shutting down the background thread.");
      scheduler.shutdownNow();
      try {
        if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
          LOG.warn("Gave up waiting for the app check task to shutdown.");
        }
      } catch (InterruptedException e) {
        LOG.warn(
            "The InMemorySCMStore was interrupted while shutting down the "
                + "app check task.", e);
      }
      LOG.info("The background thread stopped.");
    }
    super.serviceStop();
  }

  private void bootstrap(Configuration conf) throws IOException {
    Map<String, String> initialCachedResources =
        getInitialCachedResources(FileSystem.get(conf), conf);
    LOG.info("Bootstrapping from " + initialCachedResources.size()
        + " cache resources located in the file system");
    Iterator<Map.Entry<String, String>> it =
        initialCachedResources.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> e = it.next();
      String key = intern(e.getKey());
      String fileName = e.getValue();
      SharedCacheResource resource = new SharedCacheResource(fileName);
      // we don't hold the lock for this as it is done as part of serviceInit
      cachedResources.put(key, resource);
      // clear out the initial resource to reduce the footprint
      it.remove();
    }
    LOG.info("Bootstrapping complete");
  }

  @VisibleForTesting
  Map<String, String> getInitialCachedResources(FileSystem fs,
      Configuration conf) throws IOException {
    // get the root directory for the shared cache
    String location =
        conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
            YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);
    Path root = new Path(location);
    try {
      fs.getFileStatus(root);
    } catch (FileNotFoundException e) {
      String message =
          "The shared cache root directory " + location + " was not found";
      LOG.error(message);
      throw (IOException)new FileNotFoundException(message)
          .initCause(e);
    }

    int nestedLevel = SharedCacheUtil.getCacheDepth(conf);
    // now traverse individual directories and process them
    // the directory structure is specified by the nested level parameter
    // (e.g. 9/c/d/<checksum>/file)
    String pattern = SharedCacheUtil.getCacheEntryGlobPattern(nestedLevel+1);

    LOG.info("Querying for all individual cached resource files");
    FileStatus[] entries = fs.globStatus(new Path(root, pattern));
    int numEntries = entries == null ? 0 : entries.length;
    LOG.info("Found " + numEntries + " files: processing for one resource per "
        + "key");

    Map<String, String> initialCachedEntries = new HashMap<String, String>();
    if (entries != null) {
      for (FileStatus entry : entries) {
        Path file = entry.getPath();
        String fileName = file.getName();
        if (entry.isFile()) {
          // get the parent to get the checksum
          Path parent = file.getParent();
          if (parent != null) {
            // the name of the immediate parent directory is the checksum
            String key = parent.getName();
            // make sure we insert only one file per checksum whichever comes
            // first
            if (initialCachedEntries.containsKey(key)) {
              LOG.warn("Key " + key + " is already mapped to file "
                  + initialCachedEntries.get(key) + "; file " + fileName
                  + " will not be added");
            } else {
              initialCachedEntries.put(key, fileName);
            }
          }
        }
      }
    }
    LOG.info("A total of " + initialCachedEntries.size()
        + " files are now mapped");
    return initialCachedEntries;
  }

  /**
   * Adds the given resource to the store under the key and the filename. If the
   * entry is already found, it returns the existing filename. It represents the
   * state of the store at the time of this query. The entry may change or even
   * be removed once this method returns. The caller should be prepared to
   * handle that situation.
   * 
   * @return the filename of the newly inserted resource or that of the existing
   *         resource
   */
  @Override
  public String addResource(String key, String fileName) {
    String interned = intern(key);
    synchronized (interned) {
      SharedCacheResource resource = cachedResources.get(interned);
      if (resource == null) {
        resource = new SharedCacheResource(fileName);
        cachedResources.put(interned, resource);
      }
      return resource.getFileName();
    }
  }

  /**
   * Adds the provided resource reference to the cache resource under the key,
   * and updates the access time. If it returns a non-null value, the caller may
   * safely assume that the resource will not be removed at least until the app
   * in this resource reference has terminated.
   * 
   * @return the filename of the resource, or null if the resource is not found
   */
  @Override
  public String addResourceReference(String key,
      SharedCacheResourceReference ref) {
    String interned = intern(key);
    synchronized (interned) {
      SharedCacheResource resource = cachedResources.get(interned);
      if (resource == null) { // it's not mapped
        return null;
      }
      resource.addReference(ref);
      resource.updateAccessTime();
      return resource.getFileName();
    }
  }

  /**
   * Returns the list of resource references currently registered under the
   * cache entry. If the list is empty, it returns an empty collection. The
   * returned collection is unmodifiable and a snapshot of the information at
   * the time of the query. The state may change after this query returns. The
   * caller should handle the situation that some or all of these resource
   * references are no longer relevant.
   * 
   * @return the collection that contains the resource references associated
   *         with the resource; or an empty collection if no resource references
   *         are registered under this resource
   */
  @Override
  public Collection<SharedCacheResourceReference> getResourceReferences(String key) {
    String interned = intern(key);
    synchronized (interned) {
      SharedCacheResource resource = cachedResources.get(interned);
      if (resource == null) {
        return Collections.emptySet();
      }
      Set<SharedCacheResourceReference> refs =
          new HashSet<SharedCacheResourceReference>(
              resource.getResourceReferences());
      return Collections.unmodifiableSet(refs);
    }
  }

  /**
   * Removes the provided resource reference from the resource. If the resource
   * does not exist, nothing will be done.
   */
  @Override
  public boolean removeResourceReference(String key, SharedCacheResourceReference ref,
      boolean updateAccessTime) {
    String interned = intern(key);
    synchronized (interned) {
      boolean removed = false;
      SharedCacheResource resource = cachedResources.get(interned);
      if (resource != null) {
        Set<SharedCacheResourceReference> resourceRefs =
            resource.getResourceReferences();
        removed = resourceRefs.remove(ref);
        if (updateAccessTime) {
          resource.updateAccessTime();
        }
      }
      return removed;
    }
  }

  /**
   * Removes the provided collection of resource references from the resource.
   * If the resource does not exist, nothing will be done.
   */
  @Override
  public void removeResourceReferences(String key,
      Collection<SharedCacheResourceReference> refs, boolean updateAccessTime) {
    String interned = intern(key);
    synchronized (interned) {
      SharedCacheResource resource = cachedResources.get(interned);
      if (resource != null) {
        Set<SharedCacheResourceReference> resourceRefs =
            resource.getResourceReferences();
        resourceRefs.removeAll(refs);
        if (updateAccessTime) {
          resource.updateAccessTime();
        }
      }
    }
  }

  /**
   * Provides atomicity for the method.
   */
  @Override
  public void cleanResourceReferences(String key) throws YarnException {
    String interned = intern(key);
    synchronized (interned) {
      super.cleanResourceReferences(key);
    }
  }

  /**
   * Removes the given resource from the store. Returns true if the resource is
   * found and removed or if the resource is not found. Returns false if it was
   * unable to remove the resource because the resource reference list was not
   * empty.
   */
  @Override
  public boolean removeResource(String key) {
    String interned = intern(key);
    synchronized (interned) {
      SharedCacheResource resource = cachedResources.get(interned);
      if (resource == null) {
        return true;
      }

      if (!resource.getResourceReferences().isEmpty()) {
        return false;
      }
      // no users
      cachedResources.remove(interned);
      return true;
    }
  }

  /**
   * Obtains the access time for a resource. It represents the view of the
   * resource at the time of the query. The value may have been updated at a
   * later point.
   * 
   * @return the access time of the resource if found; -1 if the resource is not
   *         found
   */
  @VisibleForTesting
  long getAccessTime(String key) {
    String interned = intern(key);
    synchronized (interned) {
      SharedCacheResource resource = cachedResources.get(interned);
      return resource == null ? -1 : resource.getAccessTime();
    }
  }

  @Override
  public boolean isResourceEvictable(String key, FileStatus file) {
    synchronized (initialAppsLock) {
      if (initialApps.size() > 0) {
        return false;
      }
    }

    long staleTime =
        System.currentTimeMillis()
            - TimeUnit.MINUTES.toMillis(this.stalenessMinutes);
    long accessTime = getAccessTime(key);
    if (accessTime == -1) {
      // check modification time
      long modTime = file.getModificationTime();
      // if modification time is older then the store startup time, we need to
      // just use the store startup time as the last point of certainty
      long lastUse = modTime < this.startTime ? this.startTime : modTime;
      return lastUse < staleTime;
    } else {
      // check access time
      return accessTime < staleTime;
    }
  }

  private static int getStalenessPeriod(Configuration conf) {
    int stalenessMinutes =
        conf.getInt(YarnConfiguration.IN_MEMORY_STALENESS_PERIOD_MINS,
            YarnConfiguration.DEFAULT_IN_MEMORY_STALENESS_PERIOD_MINS);
    // non-positive value is invalid; use the default
    if (stalenessMinutes <= 0) {
      throw new HadoopIllegalArgumentException("Non-positive staleness value: "
          + stalenessMinutes
          + ". The staleness value must be greater than zero.");
    }
    return stalenessMinutes;
  }

  private static int getInitialDelay(Configuration conf) {
    int initialMinutes =
        conf.getInt(YarnConfiguration.IN_MEMORY_INITIAL_DELAY_MINS,
            YarnConfiguration.DEFAULT_IN_MEMORY_INITIAL_DELAY_MINS);
    // non-positive value is invalid; use the default
    if (initialMinutes <= 0) {
      throw new HadoopIllegalArgumentException(
          "Non-positive initial delay value: " + initialMinutes
              + ". The initial delay value must be greater than zero.");
    }
    return initialMinutes;
  }

  private static int getCheckPeriod(Configuration conf) {
    int checkMinutes =
        conf.getInt(YarnConfiguration.IN_MEMORY_CHECK_PERIOD_MINS,
            YarnConfiguration.DEFAULT_IN_MEMORY_CHECK_PERIOD_MINS);
    // non-positive value is invalid; use the default
    if (checkMinutes <= 0) {
      throw new HadoopIllegalArgumentException(
          "Non-positive check period value: " + checkMinutes
              + ". The check period value must be greater than zero.");
    }
    return checkMinutes;
  }

  @Private
  @Evolving
  class AppCheckTask implements Runnable {

    private final AppChecker taskAppChecker;

    public AppCheckTask(AppChecker appChecker) {
      this.taskAppChecker = appChecker;
    }

    @Override
    public void run() {
      try {
        LOG.info("Checking the initial app list for finished applications.");
        synchronized (initialAppsLock) {
          if (initialApps.isEmpty()) {
            // we're fine, no-op; there are no active apps that were running at
            // the time of the service start
          } else {
            LOG.info("Looking into " + initialApps.size()
                + " apps to see if they are still active");
            Iterator<ApplicationId> it = initialApps.iterator();
            while (it.hasNext()) {
              ApplicationId id = it.next();
              try {
                if (!taskAppChecker.isApplicationActive(id)) {
                  // remove it from the list
                  it.remove();
                }
              } catch (YarnException e) {
                LOG.warn("Exception while checking the app status;"
                    + " will leave the entry in the list", e);
                // continue
              }
            }
          }
          LOG.info("There are now " + initialApps.size()
              + " entries in the list");
        }
      } catch (Throwable e) {
        LOG.error(
            "Unexpected exception thrown during in-memory store app check task."
                + " Rescheduling task.", e);
      }

    }
  }
}
