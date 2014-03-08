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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

/**
 * Manages a list of local storage directories.
 */
class DirectoryCollection {
  private static final Log LOG = LogFactory.getLog(DirectoryCollection.class);

  // Good local storage directories
  private List<String> localDirs;
  private List<String> failedDirs;
  private int numFailures;
  
  private float diskUtilizationPercentageCutoff;
  private long diskUtilizationSpaceCutoff;

  /**
   * Create collection for the directories specified. No check for free space.
   * 
   * @param dirs
   *          directories to be monitored
   */
  public DirectoryCollection(String[] dirs) {
    this(dirs, 100.0F, 0);
  }

  /**
   * Create collection for the directories specified. Users must specify the
   * maximum percentage of disk utilization allowed. Minimum amount of disk
   * space is not checked.
   * 
   * @param dirs
   *          directories to be monitored
   * @param utilizationPercentageCutOff
   *          percentage of disk that can be used before the dir is taken out of
   *          the good dirs list
   * 
   */
  public DirectoryCollection(String[] dirs, float utilizationPercentageCutOff) {
    this(dirs, utilizationPercentageCutOff, 0);
  }

  /**
   * Create collection for the directories specified. Users must specify the
   * minimum amount of free space that must be available for the dir to be used.
   * 
   * @param dirs
   *          directories to be monitored
   * @param utilizationSpaceCutOff
   *          minimum space, in MB, that must be available on the disk for the
   *          dir to be marked as good
   * 
   */
  public DirectoryCollection(String[] dirs, long utilizationSpaceCutOff) {
    this(dirs, 100.0F, utilizationSpaceCutOff);
  }

  /**
   * Create collection for the directories specified. Users must specify the
   * maximum percentage of disk utilization allowed and the minimum amount of
   * free space that must be available for the dir to be used. If either check
   * fails the dir is removed from the good dirs list.
   * 
   * @param dirs
   *          directories to be monitored
   * @param utilizationPercentageCutOff
   *          percentage of disk that can be used before the dir is taken out of
   *          the good dirs list
   * @param utilizationSpaceCutOff
   *          minimum space, in MB, that must be available on the disk for the
   *          dir to be marked as good
   * 
   */
  public DirectoryCollection(String[] dirs, 
      float utilizationPercentageCutOff,
      long utilizationSpaceCutOff) {
    localDirs = new CopyOnWriteArrayList<String>(dirs);
    failedDirs = new CopyOnWriteArrayList<String>();
    diskUtilizationPercentageCutoff = utilizationPercentageCutOff;
    diskUtilizationSpaceCutoff = utilizationSpaceCutOff;
    diskUtilizationPercentageCutoff =
        utilizationPercentageCutOff < 0.0F ? 0.0F
            : (utilizationPercentageCutOff > 100.0F ? 100.0F
                : utilizationPercentageCutOff);
    diskUtilizationSpaceCutoff =
        utilizationSpaceCutOff < 0 ? 0 : utilizationSpaceCutOff;
  }

  /**
   * @return the current valid directories 
   */
  synchronized List<String> getGoodDirs() {
    return Collections.unmodifiableList(localDirs);
  }

  /**
   * @return the failed directories
   */
  synchronized List<String> getFailedDirs() {
    return Collections.unmodifiableList(failedDirs);
  }

  /**
   * @return total the number of directory failures seen till now
   */
  synchronized int getNumFailures() {
    return numFailures;
  }

  /**
   * Create any non-existent directories and parent directories, updating the
   * list of valid directories if necessary.
   * @param localFs local file system to use
   * @param perm absolute permissions to use for any directories created
   * @return true if there were no errors, false if at least one error occurred
   */
  synchronized boolean createNonExistentDirs(FileContext localFs,
      FsPermission perm) {
    boolean failed = false;
    for (final String dir : localDirs) {
      try {
        createDir(localFs, new Path(dir), perm);
      } catch (IOException e) {
        LOG.warn("Unable to create directory " + dir + " error " +
            e.getMessage() + ", removing from the list of valid directories.");
        localDirs.remove(dir);
        failedDirs.add(dir);
        numFailures++;
        failed = true;
      }
    }
    return !failed;
  }

  /**
   * Check the health of current set of local directories, updating the list
   * of valid directories if necessary.
   * @return <em>true</em> if there is a new disk-failure identified in
   *         this checking. <em>false</em> otherwise.
   */
  synchronized boolean checkDirs() {
    int oldNumFailures = numFailures;
    HashSet<String> checkFailedDirs = new HashSet<String>();
    for (final String dir : localDirs) {
      try {
        File testDir = new File(dir);
        DiskChecker.checkDir(testDir);
        if (isDiskUsageUnderPercentageLimit(testDir)) {
          LOG.warn("Directory " + dir
              + " error, used space above threshold of "
              + diskUtilizationPercentageCutoff
              + "%, removing from the list of valid directories.");
          checkFailedDirs.add(dir);
        } else if (isDiskFreeSpaceWithinLimit(testDir)) {
          LOG.warn("Directory " + dir + " error, free space below limit of "
              + diskUtilizationSpaceCutoff
              + "MB, removing from the list of valid directories.");
          checkFailedDirs.add(dir);
        }
      } catch (DiskErrorException de) {
        LOG.warn("Directory " + dir + " error " + de.getMessage()
            + ", removing from the list of valid directories.");
        checkFailedDirs.add(dir);
      }
    }
    for (String dir : checkFailedDirs) {
      localDirs.remove(dir);
      failedDirs.add(dir);
      numFailures++;
    }
    return numFailures > oldNumFailures;
  }
  
  private boolean isDiskUsageUnderPercentageLimit(File dir) {
    float freePercentage =
        100 * (dir.getUsableSpace() / (float) dir.getTotalSpace());
    float usedPercentage = 100.0F - freePercentage;
    if (usedPercentage > diskUtilizationPercentageCutoff
        || usedPercentage >= 100.0F) {
      return true;
    }
    return false;
  }

  private boolean isDiskFreeSpaceWithinLimit(File dir) {
    long freeSpace = dir.getUsableSpace() / (1024 * 1024);
    if (freeSpace < this.diskUtilizationSpaceCutoff) {
      return true;
    }
    return false;
  }

  private void createDir(FileContext localFs, Path dir, FsPermission perm)
      throws IOException {
    if (dir == null) {
      return;
    }
    try {
      localFs.getFileStatus(dir);
    } catch (FileNotFoundException e) {
      createDir(localFs, dir.getParent(), perm);
      localFs.mkdir(dir, perm, false);
      if (!perm.equals(perm.applyUMask(localFs.getUMask()))) {
        localFs.setPermission(dir, perm);
      }
    }
  }
  
  public float getDiskUtilizationPercentageCutoff() {
    return diskUtilizationPercentageCutoff;
  }

  public void setDiskUtilizationPercentageCutoff(
      float diskUtilizationPercentageCutoff) {
    this.diskUtilizationPercentageCutoff =
        diskUtilizationPercentageCutoff < 0.0F ? 0.0F
            : (diskUtilizationPercentageCutoff > 100.0F ? 100.0F
                : diskUtilizationPercentageCutoff);
  }

  public long getDiskUtilizationSpaceCutoff() {
    return diskUtilizationSpaceCutoff;
  }

  public void setDiskUtilizationSpaceCutoff(long diskUtilizationSpaceCutoff) {
    diskUtilizationSpaceCutoff =
        diskUtilizationSpaceCutoff < 0 ? 0 : diskUtilizationSpaceCutoff;
    this.diskUtilizationSpaceCutoff = diskUtilizationSpaceCutoff;
  }
}
