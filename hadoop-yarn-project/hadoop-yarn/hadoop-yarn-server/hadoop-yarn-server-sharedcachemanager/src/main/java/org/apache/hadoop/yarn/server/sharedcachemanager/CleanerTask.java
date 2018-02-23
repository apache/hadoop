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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.sharedcache.SharedCacheUtil;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.CleanerMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The task that runs and cleans up the shared cache area for stale entries and
 * orphaned files. It is expected that only one cleaner task runs at any given
 * point in time.
 */
@Private
@Evolving
class CleanerTask implements Runnable {
  private static final String RENAMED_SUFFIX = "-renamed";
  private static final Logger LOG =
      LoggerFactory.getLogger(CleanerTask.class);

  private final String location;
  private final long sleepTime;
  private final int nestedLevel;
  private final Path root;
  private final FileSystem fs;
  private final SCMStore store;
  private final CleanerMetrics metrics;
  private final Lock cleanerTaskLock;

  /**
   * Creates a cleaner task based on the configuration. This is provided for
   * convenience.
   *
   * @param conf
   * @param store
   * @param metrics
   * @param cleanerTaskLock lock that ensures a serial execution of cleaner
   *                        task
   * @return an instance of a CleanerTask
   */
  public static CleanerTask create(Configuration conf, SCMStore store,
      CleanerMetrics metrics, Lock cleanerTaskLock) {
    try {
      // get the root directory for the shared cache
      String location =
          conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
              YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);

      long sleepTime =
          conf.getLong(YarnConfiguration.SCM_CLEANER_RESOURCE_SLEEP_MS,
              YarnConfiguration.DEFAULT_SCM_CLEANER_RESOURCE_SLEEP_MS);
      int nestedLevel = SharedCacheUtil.getCacheDepth(conf);
      FileSystem fs = FileSystem.get(conf);

      return new CleanerTask(location, sleepTime, nestedLevel, fs, store,
          metrics, cleanerTaskLock);
    } catch (IOException e) {
      LOG.error("Unable to obtain the filesystem for the cleaner service", e);
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Creates a cleaner task based on the root directory location and the
   * filesystem.
   */
  CleanerTask(String location, long sleepTime, int nestedLevel, FileSystem fs,
      SCMStore store, CleanerMetrics metrics, Lock cleanerTaskLock) {
    this.location = location;
    this.sleepTime = sleepTime;
    this.nestedLevel = nestedLevel;
    this.root = new Path(location);
    this.fs = fs;
    this.store = store;
    this.metrics = metrics;
    this.cleanerTaskLock = cleanerTaskLock;
  }

  @Override
  public void run() {
    if (!this.cleanerTaskLock.tryLock()) {
      // there is already another task running
      LOG.warn("A cleaner task is already running. "
          + "This scheduled cleaner task will do nothing.");
      return;
    }

    try {
      if (!fs.exists(root)) {
        LOG.error("The shared cache root " + location + " was not found. "
            + "The cleaner task will do nothing.");
        return;
      }

      // we're now ready to process the shared cache area
      process();
    } catch (Throwable e) {
      LOG.error("Unexpected exception while initializing the cleaner task. "
          + "This task will do nothing,", e);
    } finally {
      // this is set to false regardless of if it is a scheduled or on-demand
      // task
      this.cleanerTaskLock.unlock();
    }
  }

  /**
   * Sweeps and processes the shared cache area to clean up stale and orphaned
   * files.
   */
  void process() {
    // mark the beginning of the run in the metrics
    metrics.reportCleaningStart();
    try {
      // now traverse individual directories and process them
      // the directory structure is specified by the nested level parameter
      // (e.g. 9/c/d/<checksum>)
      String pattern = SharedCacheUtil.getCacheEntryGlobPattern(nestedLevel);
      FileStatus[] resources =
          fs.globStatus(new Path(root, pattern));
      int numResources = resources == null ? 0 : resources.length;
      LOG.info("Processing " + numResources + " resources in the shared cache");
      long beginMs = System.currentTimeMillis();
      if (resources != null) {
        for (FileStatus resource : resources) {
          // check for interruption so it can abort in a timely manner in case
          // of shutdown
          if (Thread.currentThread().isInterrupted()) {
            LOG.warn("The cleaner task was interrupted. Aborting.");
            break;
          }

          if (resource.isDirectory()) {
            processSingleResource(resource);
          } else {
            LOG.warn("Invalid file at path " + resource.getPath().toString()
                +
                " when a directory was expected");
          }
          // add sleep time between cleaning each directory if it is non-zero
          if (sleepTime > 0) {
            Thread.sleep(sleepTime);
          }
        }
      }
      long endMs = System.currentTimeMillis();
      long durationMs = endMs - beginMs;
      LOG.info("Processed " + numResources + " resource(s) in " + durationMs +
          " ms.");
    } catch (IOException e1) {
      LOG.error("Unable to complete the cleaner task", e1);
    } catch (InterruptedException e2) {
      Thread.currentThread().interrupt(); // restore the interrupt
    }
  }

  /**
   * Returns a path for the root directory for the shared cache.
   */
  Path getRootPath() {
    return root;
  }

  /**
   * Processes a single shared cache resource directory.
   */
  void processSingleResource(FileStatus resource) {
    Path path = resource.getPath();
    // indicates the processing status of the resource
    ResourceStatus resourceStatus = ResourceStatus.INIT;

    // first, if the path ends with the renamed suffix, it indicates the
    // directory was moved (as stale) but somehow not deleted (probably due to
    // SCM failure); delete the directory
    if (path.toString().endsWith(RENAMED_SUFFIX)) {
      LOG.info("Found a renamed directory that was left undeleted at " +
          path.toString() + ". Deleting.");
      try {
        if (fs.delete(path, true)) {
          resourceStatus = ResourceStatus.DELETED;
        }
      } catch (IOException e) {
        LOG.error("Error while processing a shared cache resource: " + path, e);
      }
    } else {
      // this is the path to the cache resource directory
      // the directory name is the resource key (i.e. a unique identifier)
      String key = path.getName();

      try {
        store.cleanResourceReferences(key);
      } catch (YarnException e) {
        LOG.error("Exception thrown while removing dead appIds.", e);
      }

      if (store.isResourceEvictable(key, resource)) {
        try {
          /*
           * TODO See YARN-2663: There is a race condition between
           * store.removeResource(key) and
           * removeResourceFromCacheFileSystem(path) operations because they do
           * not happen atomically and resources can be uploaded with different
           * file names by the node managers.
           */
          // remove the resource from scm (checks for appIds as well)
          if (store.removeResource(key)) {
            // remove the resource from the file system
            boolean deleted = removeResourceFromCacheFileSystem(path);
            if (deleted) {
              resourceStatus = ResourceStatus.DELETED;
            } else {
              LOG.error("Failed to remove path from the file system."
                  + " Skipping this resource: " + path);
              resourceStatus = ResourceStatus.ERROR;
            }
          } else {
            // we did not delete the resource because it contained application
            // ids
            resourceStatus = ResourceStatus.PROCESSED;
          }
        } catch (IOException e) {
          LOG.error(
              "Failed to remove path from the file system. Skipping this resource: "
                  + path, e);
          resourceStatus = ResourceStatus.ERROR;
        }
      } else {
        resourceStatus = ResourceStatus.PROCESSED;
      }
    }

    // record the processing
    switch (resourceStatus) {
    case DELETED:
      metrics.reportAFileDelete();
      break;
    case PROCESSED:
      metrics.reportAFileProcess();
      break;
    case ERROR:
      metrics.reportAFileError();
      break;
    default:
      LOG.error("Cleaner encountered an invalid status (" + resourceStatus
          + ") while processing resource: " + path.getName());
    }
  }

  private boolean removeResourceFromCacheFileSystem(Path path)
      throws IOException {
    // rename the directory to make the delete atomic
    Path renamedPath = new Path(path.toString() + RENAMED_SUFFIX);
    if (fs.rename(path, renamedPath)) {
      // the directory can be removed safely now
      // log the original path
      LOG.info("Deleting " + path.toString());
      return fs.delete(renamedPath, true);
    } else {
      // we were unable to remove it for some reason: it's best to leave
      // it at that
      LOG.error("We were not able to rename the directory to "
          + renamedPath.toString() + ". We will leave it intact.");
    }
    return false;
  }

  /**
   * A status indicating what happened with the processing of a given cache
   * resource.
   */
  private enum ResourceStatus {
    INIT,
    /** Resource was successfully processed, but not deleted **/
    PROCESSED,
    /** Resource was successfully deleted **/
    DELETED,
    /** The cleaner task ran into an error while processing the resource **/
    ERROR
  }
}
