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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * The class which provides functionality of checking the health of the local
 * directories of a node. This specifically manages nodemanager-local-dirs and
 * nodemanager-log-dirs by periodically checking their health.
 */
public class LocalDirsHandlerService extends AbstractService {

  private static Log LOG = LogFactory.getLog(LocalDirsHandlerService.class);

  /** Timer used to schedule disk health monitoring code execution */
  private Timer dirsHandlerScheduler;
  private long diskHealthCheckInterval;
  private boolean isDiskHealthCheckerEnabled;
  /**
   * Minimum fraction of disks to be healthy for the node to be healthy in
   * terms of disks. This applies to nm-local-dirs and nm-log-dirs.
   */
  private float minNeededHealthyDisksFactor;

  private MonitoringTimerTask monitoringTimerTask;

  /** Local dirs to store localized files in */
  private DirectoryCollection localDirs = null;

  /** storage for container logs*/
  private DirectoryCollection logDirs = null;

  /**
   * Everybody should go through this LocalDirAllocator object for read/write
   * of any local path corresponding to {@link YarnConfiguration#NM_LOCAL_DIRS}
   * instead of creating his/her own LocalDirAllocator objects
   */ 
  private LocalDirAllocator localDirsAllocator;
  /**
   * Everybody should go through this LocalDirAllocator object for read/write
   * of any local path corresponding to {@link YarnConfiguration#NM_LOG_DIRS}
   * instead of creating his/her own LocalDirAllocator objects
   */ 
  private LocalDirAllocator logDirsAllocator;

  /** when disk health checking code was last run */
  private long lastDisksCheckTime;

  /**
   * Class which is used by the {@link Timer} class to periodically execute the
   * disks' health checker code.
   */
  private final class MonitoringTimerTask extends TimerTask {

    public MonitoringTimerTask(Configuration conf) {
      localDirs = new DirectoryCollection(
          conf.getTrimmedStrings(YarnConfiguration.NM_LOCAL_DIRS));
      logDirs = new DirectoryCollection(
          conf.getTrimmedStrings(YarnConfiguration.NM_LOG_DIRS));
      localDirsAllocator =
          new LocalDirAllocator(YarnConfiguration.NM_LOCAL_DIRS);
      logDirsAllocator = new LocalDirAllocator(YarnConfiguration.NM_LOG_DIRS);
    }

    @Override
    public void run() {
      boolean newFailure = false;
      if (localDirs.checkDirs()) {
        newFailure = true;
      }
      if (logDirs.checkDirs()) {
        newFailure = true;
      }

      if (newFailure) {
        LOG.info("Disk(s) failed. " + getDisksHealthReport());
        updateDirsInConfiguration();
        if (!areDisksHealthy()) {
          // Just log.
          LOG.error("Most of the disks failed. " + getDisksHealthReport());
        }
      }
      lastDisksCheckTime = System.currentTimeMillis();
    }
  }

  public LocalDirsHandlerService() {
    super(LocalDirsHandlerService.class.getName());
  }

  /**
   * Method which initializes the timertask and its interval time.
   */
  @Override
  public void init(Configuration config) {
    // Clone the configuration as we may do modifications to dirs-list
    Configuration conf = new Configuration(config);
    diskHealthCheckInterval = conf.getLong(
        YarnConfiguration.NM_DISK_HEALTH_CHECK_INTERVAL_MS,
        YarnConfiguration.DEFAULT_NM_DISK_HEALTH_CHECK_INTERVAL_MS);
    monitoringTimerTask = new MonitoringTimerTask(conf);
    isDiskHealthCheckerEnabled = conf.getBoolean(
        YarnConfiguration.NM_DISK_HEALTH_CHECK_ENABLE, true);
    minNeededHealthyDisksFactor = conf.getFloat(
        YarnConfiguration.NM_MIN_HEALTHY_DISKS_FRACTION,
        YarnConfiguration.DEFAULT_NM_MIN_HEALTHY_DISKS_FRACTION);
    lastDisksCheckTime = System.currentTimeMillis();
    super.init(conf);
  }

  /**
   * Method used to start the disk health monitoring, if enabled.
   */
  @Override
  public void start() {
    if (isDiskHealthCheckerEnabled) {
      dirsHandlerScheduler = new Timer("DiskHealthMonitor-Timer", true);
      // Start the timer task for disk health checking immediately and
      // then run periodically at interval time.
      dirsHandlerScheduler.scheduleAtFixedRate(monitoringTimerTask, 0,
                                                   diskHealthCheckInterval);
    }
    super.start();
  }

  /**
   * Method used to terminate the disk health monitoring service.
   */
  @Override
  public void stop() {
    if (dirsHandlerScheduler != null) {
      dirsHandlerScheduler.cancel();
    }
    super.stop();
  }

  /**
   * @return the good/valid local directories based on disks' health
   */
  public List<String> getLocalDirs() {
    return localDirs.getGoodDirs();
  }

  /**
   * @return the good/valid log directories based on disks' health
   */
  public List<String> getLogDirs() {
    return logDirs.getGoodDirs();
  }

  /**
   * @return the health report of nm-local-dirs and nm-log-dirs
   */
  public String getDisksHealthReport() {
    if (!isDiskHealthCheckerEnabled) {
      return "";
    }

    StringBuilder report = new StringBuilder();
    List<String> failedLocalDirsList = localDirs.getFailedDirs();
    List<String> failedLogDirsList = logDirs.getFailedDirs();
    int numLocalDirs = localDirs.getGoodDirs().size()
        + failedLocalDirsList.size();
    int numLogDirs = logDirs.getGoodDirs().size() + failedLogDirsList.size();
    if (!failedLocalDirsList.isEmpty()) {
      report.append(failedLocalDirsList.size() + "/" + numLocalDirs
          + " local-dirs turned bad: "
          + StringUtils.join(",", failedLocalDirsList) + ";");
    }
    if (!failedLogDirsList.isEmpty()) {
      report.append(failedLogDirsList.size() + "/" + numLogDirs
          + " log-dirs turned bad: "
          + StringUtils.join(",", failedLogDirsList));
    }
    return report.toString();
  }

  /**
   * The minimum fraction of number of disks needed to be healthy for a node to
   * be considered healthy in terms of disks is configured using
   * {@link YarnConfiguration#NM_MIN_HEALTHY_DISKS_FRACTION}, with a default
   * value of {@link YarnConfiguration#DEFAULT_NM_MIN_HEALTHY_DISKS_FRACTION}.
   * @return <em>false</em> if either (a) more than the allowed percentage of
   * nm-local-dirs failed or (b) more than the allowed percentage of
   * nm-log-dirs failed.
   */
  public boolean areDisksHealthy() {
    if (!isDiskHealthCheckerEnabled) {
      return true;
    }

    int goodDirs = getLocalDirs().size();
    int failedDirs = localDirs.getFailedDirs().size();
    int totalConfiguredDirs = goodDirs + failedDirs;
    if (goodDirs/(float)totalConfiguredDirs < minNeededHealthyDisksFactor) {
      return false; // Not enough healthy local-dirs
    }

    goodDirs = getLogDirs().size();
    failedDirs = logDirs.getFailedDirs().size();
    totalConfiguredDirs = goodDirs + failedDirs;
    if (goodDirs/(float)totalConfiguredDirs < minNeededHealthyDisksFactor) {
      return false; // Not enough healthy log-dirs
    }

    return true;
  }

  public long getLastDisksCheckTime() {
    return lastDisksCheckTime;
  }

  /**
   * Set good local dirs and good log dirs in the configuration so that the
   * LocalDirAllocator objects will use this updated configuration only.
   */
  private void updateDirsInConfiguration() {
    Configuration conf = getConfig();
    List<String> localDirs = getLocalDirs();
    conf.setStrings(YarnConfiguration.NM_LOCAL_DIRS,
                    localDirs.toArray(new String[localDirs.size()]));
    List<String> logDirs = getLogDirs();
    synchronized(conf) {
      conf.setStrings(YarnConfiguration.NM_LOG_DIRS,
                      logDirs.toArray(new String[logDirs.size()]));
    }
  }

  public Path getLocalPathForWrite(String pathStr) throws IOException {
    Configuration conf = getConfig();
    Path path = null;
    synchronized (conf) {
      path = localDirsAllocator.getLocalPathForWrite(pathStr, conf);
    }
    return path;
  }

  public Path getLocalPathForWrite(String pathStr, long size,
      boolean checkWrite) throws IOException {
    Configuration conf = getConfig();
    Path path = null;
    synchronized (conf) {
      path = localDirsAllocator.getLocalPathForWrite(pathStr, size, conf,
                                                     checkWrite);
    }
    return path;
  }

  public Path getLogPathForWrite(String pathStr, boolean checkWrite)
      throws IOException {
    Configuration conf = getConfig();
    Path path = null;
    synchronized (conf) {
      path = logDirsAllocator.getLocalPathForWrite(pathStr,
        LocalDirAllocator.SIZE_UNKNOWN, conf, checkWrite);
    }
    return path;
  }

  public Path getLogPathToRead(String pathStr) throws IOException {
    Configuration conf = getConfig();
    Path path = null;
    synchronized (conf) {
      path = logDirsAllocator.getLocalPathToRead(pathStr, conf);
    }
    return path;
  }
}
