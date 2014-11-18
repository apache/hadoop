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
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

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
  
  private static String FILE_SCHEME = "file";

  /**
   * Class which is used by the {@link Timer} class to periodically execute the
   * disks' health checker code.
   */
  private final class MonitoringTimerTask extends TimerTask {

    public MonitoringTimerTask(Configuration conf) throws YarnRuntimeException {
      float maxUsableSpacePercentagePerDisk =
          conf.getFloat(
            YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE,
            YarnConfiguration.DEFAULT_NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE);
      long minFreeSpacePerDiskMB =
          conf.getLong(YarnConfiguration.NM_MIN_PER_DISK_FREE_SPACE_MB,
            YarnConfiguration.DEFAULT_NM_MIN_PER_DISK_FREE_SPACE_MB);
      localDirs =
          new DirectoryCollection(
            validatePaths(conf
              .getTrimmedStrings(YarnConfiguration.NM_LOCAL_DIRS)),
            maxUsableSpacePercentagePerDisk, minFreeSpacePerDiskMB);
      logDirs =
          new DirectoryCollection(
            validatePaths(conf.getTrimmedStrings(YarnConfiguration.NM_LOG_DIRS)),
            maxUsableSpacePercentagePerDisk, minFreeSpacePerDiskMB);
      localDirsAllocator = new LocalDirAllocator(
          YarnConfiguration.NM_LOCAL_DIRS);
      logDirsAllocator = new LocalDirAllocator(YarnConfiguration.NM_LOG_DIRS);
    }

    @Override
    public void run() {
      checkDirs();
    }
  }

  public LocalDirsHandlerService() {
    super(LocalDirsHandlerService.class.getName());
  }

  /**
   * Method which initializes the timertask and its interval time.
   * 
   */
  @Override
  protected void serviceInit(Configuration config) throws Exception {
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
    super.serviceInit(conf);

    FileContext localFs;
    try {
      localFs = FileContext.getLocalFSFileContext(config);
    } catch (IOException e) {
      throw new YarnRuntimeException("Unable to get the local filesystem", e);
    }
    FsPermission perm = new FsPermission((short)0755);
    boolean createSucceeded = localDirs.createNonExistentDirs(localFs, perm);
    createSucceeded &= logDirs.createNonExistentDirs(localFs, perm);
    if (!createSucceeded) {
      updateDirsAfterTest();
    }

    // Check the disk health immediately to weed out bad directories
    // before other init code attempts to use them.
    checkDirs();
  }

  /**
   * Method used to start the disk health monitoring, if enabled.
   */
  @Override
  protected void serviceStart() throws Exception {
    if (isDiskHealthCheckerEnabled) {
      dirsHandlerScheduler = new Timer("DiskHealthMonitor-Timer", true);
      dirsHandlerScheduler.scheduleAtFixedRate(monitoringTimerTask,
          diskHealthCheckInterval, diskHealthCheckInterval);
    }
    super.serviceStart();
  }

  /**
   * Method used to terminate the disk health monitoring service.
   */
  @Override
  protected void serviceStop() throws Exception {
    if (dirsHandlerScheduler != null) {
      dirsHandlerScheduler.cancel();
    }
    super.serviceStop();
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
   * @return the local directories which have no disk space
   */
  public List<String> getDiskFullLocalDirs() {
    return localDirs.getFullDirs();
  }

  /**
   * @return the log directories that have no disk space
   */
  public List<String> getDiskFullLogDirs() {
    return logDirs.getFullDirs();
  }

  /**
   * Function to get the local dirs which should be considered when cleaning up
   * resources. Contains the good local dirs and the local dirs that have reached
   * the disk space limit
   *
   * @return the local dirs which should be considered for cleaning up
   */
  public List<String> getLocalDirsForCleanup() {
    return DirectoryCollection.concat(localDirs.getGoodDirs(),
        localDirs.getFullDirs());
  }

  /**
   * Function to get the log dirs which should be considered when cleaning up
   * resources. Contains the good log dirs and the log dirs that have reached
   * the disk space limit
   *
   * @return the log dirs which should be considered for cleaning up
   */
  public List<String> getLogDirsForCleanup() {
    return DirectoryCollection.concat(logDirs.getGoodDirs(),
        logDirs.getFullDirs());
  }

  /**
   * Function to generate a report on the state of the disks.
   *
   * @param listGoodDirs
   *          flag to determine whether the report should report the state of
   *          good dirs or failed dirs
   * @return the health report of nm-local-dirs and nm-log-dirs
   */
  public String getDisksHealthReport(boolean listGoodDirs) {
    if (!isDiskHealthCheckerEnabled) {
      return "";
    }

    StringBuilder report = new StringBuilder();
    List<String> failedLocalDirsList = localDirs.getFailedDirs();
    List<String> failedLogDirsList = logDirs.getFailedDirs();
    List<String> goodLocalDirsList = localDirs.getGoodDirs();
    List<String> goodLogDirsList = logDirs.getGoodDirs();
    int numLocalDirs = goodLocalDirsList.size() + failedLocalDirsList.size();
    int numLogDirs = goodLogDirsList.size() + failedLogDirsList.size();
    if (!listGoodDirs) {
      if (!failedLocalDirsList.isEmpty()) {
        report.append(failedLocalDirsList.size() + "/" + numLocalDirs
            + " local-dirs are bad: "
            + StringUtils.join(",", failedLocalDirsList) + "; ");
      }
      if (!failedLogDirsList.isEmpty()) {
        report.append(failedLogDirsList.size() + "/" + numLogDirs
            + " log-dirs are bad: " + StringUtils.join(",", failedLogDirsList));
      }
    } else {
      report.append(goodLocalDirsList.size() + "/" + numLocalDirs
          + " local-dirs are good: " + StringUtils.join(",", goodLocalDirsList)
          + "; ");
      report.append(goodLogDirsList.size() + "/" + numLogDirs
          + " log-dirs are good: " + StringUtils.join(",", goodLogDirsList));

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
  private void updateDirsAfterTest() {

    Configuration conf = getConfig();
    List<String> localDirs = getLocalDirs();
    conf.setStrings(YarnConfiguration.NM_LOCAL_DIRS,
                    localDirs.toArray(new String[localDirs.size()]));
    List<String> logDirs = getLogDirs();
    conf.setStrings(YarnConfiguration.NM_LOG_DIRS,
                      logDirs.toArray(new String[logDirs.size()]));
    if (!areDisksHealthy()) {
      // Just log.
      LOG.error("Most of the disks failed. " + getDisksHealthReport(false));
    }
  }

  private void logDiskStatus(boolean newDiskFailure, boolean diskTurnedGood) {
    if (newDiskFailure) {
      String report = getDisksHealthReport(false);
      LOG.info("Disk(s) failed: " + report);
    }
    if (diskTurnedGood) {
      String report = getDisksHealthReport(true);
      LOG.info("Disk(s) turned good: " + report);
    }

  }

  private void checkDirs() {
    boolean disksStatusChange = false;
    Set<String> failedLocalDirsPreCheck =
        new HashSet<String>(localDirs.getFailedDirs());
    Set<String> failedLogDirsPreCheck =
        new HashSet<String>(logDirs.getFailedDirs());

    if (localDirs.checkDirs()) {
      disksStatusChange = true;
    }
    if (logDirs.checkDirs()) {
      disksStatusChange = true;
    }

    Set<String> failedLocalDirsPostCheck =
        new HashSet<String>(localDirs.getFailedDirs());
    Set<String> failedLogDirsPostCheck =
        new HashSet<String>(logDirs.getFailedDirs());

    boolean disksFailed = false;
    boolean disksTurnedGood = false;

    disksFailed =
        disksTurnedBad(failedLocalDirsPreCheck, failedLocalDirsPostCheck);
    disksTurnedGood =
        disksTurnedGood(failedLocalDirsPreCheck, failedLocalDirsPostCheck);

    // skip check if we have new failed or good local dirs since we're going to
    // log anyway
    if (!disksFailed) {
      disksFailed =
          disksTurnedBad(failedLogDirsPreCheck, failedLogDirsPostCheck);
    }
    if (!disksTurnedGood) {
      disksTurnedGood =
          disksTurnedGood(failedLogDirsPreCheck, failedLogDirsPostCheck);
    }

    logDiskStatus(disksFailed, disksTurnedGood);

    if (disksStatusChange) {
      updateDirsAfterTest();
    }

    lastDisksCheckTime = System.currentTimeMillis();
  }

  private boolean disksTurnedBad(Set<String> preCheckFailedDirs,
      Set<String> postCheckDirs) {
    boolean disksFailed = false;
    for (String dir : postCheckDirs) {
      if (!preCheckFailedDirs.contains(dir)) {
        disksFailed = true;
        break;
      }
    }
    return disksFailed;
  }

  private boolean disksTurnedGood(Set<String> preCheckDirs,
      Set<String> postCheckDirs) {
    boolean disksTurnedGood = false;
    for (String dir : preCheckDirs) {
      if (!postCheckDirs.contains(dir)) {
        disksTurnedGood = true;
        break;
      }
    }
    return disksTurnedGood;
  }

  public Path getLocalPathForWrite(String pathStr) throws IOException {
    return localDirsAllocator.getLocalPathForWrite(pathStr, getConfig());
  }

  public Path getLocalPathForWrite(String pathStr, long size,
      boolean checkWrite) throws IOException {
    return localDirsAllocator.getLocalPathForWrite(pathStr, size, getConfig(),
                                                   checkWrite);
  }

  public Path getLogPathForWrite(String pathStr, boolean checkWrite)
      throws IOException {
    return logDirsAllocator.getLocalPathForWrite(pathStr,
        LocalDirAllocator.SIZE_UNKNOWN, getConfig(), checkWrite);
  }

  public Path getLogPathToRead(String pathStr) throws IOException {
    return logDirsAllocator.getLocalPathToRead(pathStr, getConfig());
  }
  
  public static String[] validatePaths(String[] paths) {
    ArrayList<String> validPaths = new ArrayList<String>();
    for (int i = 0; i < paths.length; ++i) {
      try {
        URI uriPath = (new Path(paths[i])).toUri();
        if (uriPath.getScheme() == null
            || uriPath.getScheme().equals(FILE_SCHEME)) {
          validPaths.add(new Path(uriPath.getPath()).toString());
        } else {
          LOG.warn(paths[i] + " is not a valid path. Path should be with "
              + FILE_SCHEME + " scheme or without scheme");
          throw new YarnRuntimeException(paths[i]
              + " is not a valid path. Path should be with " + FILE_SCHEME
              + " scheme or without scheme");
        }
      } catch (IllegalArgumentException e) {
        LOG.warn(e.getMessage());
        throw new YarnRuntimeException(paths[i]
            + " is not a valid path. Path should be with " + FILE_SCHEME
            + " scheme or without scheme");
      }
    }
    String[] arrValidPaths = new String[validPaths.size()];
    validPaths.toArray(arrValidPaths);
    return arrValidPaths;
  }
}
