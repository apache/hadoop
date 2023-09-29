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
import java.lang.management.ManagementFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.CleanerMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The cleaner service that maintains the shared cache area, and cleans up stale
 * entries on a regular basis.
 */
@Private
@Evolving
public class CleanerService extends CompositeService {
  /**
   * The name of the global cleaner lock that the cleaner creates to indicate
   * that a cleaning process is in progress.
   */
  public static final String GLOBAL_CLEANER_PID = ".cleaner_pid";

  private static final Logger LOG =
      LoggerFactory.getLogger(CleanerService.class);

  private Configuration conf;
  private CleanerMetrics metrics;
  private ScheduledExecutorService scheduledExecutor;
  private final SCMStore store;
  private final Lock cleanerTaskLock;

  public CleanerService(SCMStore store) {
    super("CleanerService");
    this.store = store;
    this.cleanerTaskLock = new ReentrantLock();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.conf = conf;

    // create scheduler executor service that services the cleaner tasks
    // use 2 threads to accommodate the on-demand tasks and reduce the chance of
    // back-to-back runs
    ThreadFactory tf =
        new ThreadFactoryBuilder().setNameFormat("Shared cache cleaner").build();
    scheduledExecutor = HadoopExecutors.newScheduledThreadPool(2, tf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    if (!writeGlobalCleanerPidFile()) {
      throw new YarnException("The global cleaner pid file already exists! " +
          "It appears there is another CleanerService running in the cluster");
    }

    this.metrics = CleanerMetrics.getInstance();

    // Start dependent services (i.e. AppChecker)
    super.serviceStart();

    Runnable task =
        CleanerTask.create(conf, store, metrics, cleanerTaskLock);
    long periodInMinutes = getPeriod(conf);
    scheduledExecutor.scheduleAtFixedRate(task, getInitialDelay(conf),
        periodInMinutes, TimeUnit.MINUTES);
    LOG.info("Scheduled the shared cache cleaner task to run every "
        + periodInMinutes + " minutes.");
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Shutting down the background thread.");
    scheduledExecutor.shutdownNow();
    try {
      if (scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.info("The background thread stopped.");
      } else {
        LOG.warn("Gave up waiting for the cleaner task to shutdown.");
      }
    } catch (InterruptedException e) {
      LOG.warn("The cleaner service was interrupted while shutting down the task.",
          e);
    }

    removeGlobalCleanerPidFile();

    super.serviceStop();
  }

  /**
   * Execute an on-demand cleaner task.
   */
  protected void runCleanerTask() {
    Runnable task =
        CleanerTask.create(conf, store, metrics, cleanerTaskLock);
    // this is a non-blocking call (it simply submits the task to the executor
    // queue and returns)
    this.scheduledExecutor.execute(task);
  }

  /**
   * To ensure there are not multiple instances of the SCM running on a given
   * cluster, a global pid file is used. This file contains the hostname of the
   * machine that owns the pid file.
   *
   * @return true if the pid file was written, false otherwise
   * @throws YarnException
   */
  private boolean writeGlobalCleanerPidFile() throws YarnException {
    String root =
        conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
            YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);
    Path pidPath = new Path(root, GLOBAL_CLEANER_PID);
    try {
      FileSystem fs = FileSystem.get(this.conf);

      if (fs.exists(pidPath)) {
        return false;
      }

      FSDataOutputStream os = fs.create(pidPath, false);
      // write the hostname and the process id in the global cleaner pid file
      final String ID = ManagementFactory.getRuntimeMXBean().getName();
      os.writeUTF(ID);
      os.close();
      // add it to the delete-on-exit to ensure it gets deleted when the JVM
      // exits
      fs.deleteOnExit(pidPath);
    } catch (IOException e) {
      throw new YarnException(e);
    }
    LOG.info("Created the global cleaner pid file at " + pidPath.toString());
    return true;
  }

  private void removeGlobalCleanerPidFile() {
    try {
      FileSystem fs = FileSystem.get(this.conf);
      String root =
          conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
              YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);

      Path pidPath = new Path(root, GLOBAL_CLEANER_PID);


      fs.delete(pidPath, false);
      LOG.info("Removed the global cleaner pid file at " + pidPath.toString());
    } catch (IOException e) {
      LOG.error(
          "Unable to remove the global cleaner pid file! The file may need "
              + "to be removed manually.", e);
    }
  }

  private static int getInitialDelay(Configuration conf) {
    int initialDelayInMinutes =
        conf.getInt(YarnConfiguration.SCM_CLEANER_INITIAL_DELAY_MINS,
            YarnConfiguration.DEFAULT_SCM_CLEANER_INITIAL_DELAY_MINS);
    // negative value is invalid; use the default
    if (initialDelayInMinutes < 0) {
      throw new HadoopIllegalArgumentException("Negative initial delay value: "
          + initialDelayInMinutes
          + ". The initial delay must be greater than zero.");
    }
    return initialDelayInMinutes;
  }

  private static int getPeriod(Configuration conf) {
    int periodInMinutes =
        conf.getInt(YarnConfiguration.SCM_CLEANER_PERIOD_MINS,
            YarnConfiguration.DEFAULT_SCM_CLEANER_PERIOD_MINS);
    // non-positive value is invalid; use the default
    if (periodInMinutes <= 0) {
      throw new HadoopIllegalArgumentException("Non-positive period value: "
          + periodInMinutes
          + ". The cleaner period must be greater than or equal to zero.");
    }
    return periodInMinutes;
  }
}
