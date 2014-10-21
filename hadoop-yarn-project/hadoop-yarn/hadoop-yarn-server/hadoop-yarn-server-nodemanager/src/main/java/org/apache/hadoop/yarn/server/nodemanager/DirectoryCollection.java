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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.DiskChecker;

/**
 * Manages a list of local storage directories.
 */
class DirectoryCollection {
  private static final Log LOG = LogFactory.getLog(DirectoryCollection.class);

  public enum DiskErrorCause {
    DISK_FULL, OTHER
  }

  static class DiskErrorInformation {
    DiskErrorCause cause;
    String message;

    DiskErrorInformation(DiskErrorCause cause, String message) {
      this.cause = cause;
      this.message = message;
    }
  }

  /**
   * Returns a merged list which contains all the elements of l1 and l2
   * @param l1 the first list to be included
   * @param l2 the second list to be included
   * @return a new list containing all the elements of the first and second list
   */
  static List<String> concat(List<String> l1, List<String> l2) {
    List<String> ret = new ArrayList<String>(l1.size() + l2.size());
    ret.addAll(l1);
    ret.addAll(l2);
    return ret;
  }

  // Good local storage directories
  private List<String> localDirs;
  private List<String> errorDirs;
  private List<String> fullDirs;

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
    errorDirs = new CopyOnWriteArrayList<String>();
    fullDirs = new CopyOnWriteArrayList<String>();

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
    return Collections.unmodifiableList(
        DirectoryCollection.concat(errorDirs, fullDirs));
  }

  /**
   * @return the directories that have used all disk space
   */

  synchronized List<String> getFullDirs() {
    return fullDirs;
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
        errorDirs.add(dir);
        numFailures++;
        failed = true;
      }
    }
    return !failed;
  }

  /**
   * Check the health of current set of local directories(good and failed),
   * updating the list of valid directories if necessary.
   *
   * @return <em>true</em> if there is a new disk-failure identified in this
   *         checking or a failed directory passes the disk check <em>false</em>
   *         otherwise.
   */
  synchronized boolean checkDirs() {
    boolean setChanged = false;
    Set<String> preCheckGoodDirs = new HashSet<String>(localDirs);
    Set<String> preCheckFullDirs = new HashSet<String>(fullDirs);
    Set<String> preCheckOtherErrorDirs = new HashSet<String>(errorDirs);
    List<String> failedDirs = DirectoryCollection.concat(errorDirs, fullDirs);
    List<String> allLocalDirs =
        DirectoryCollection.concat(localDirs, failedDirs);

    Map<String, DiskErrorInformation> dirsFailedCheck = testDirs(allLocalDirs);

    localDirs.clear();
    errorDirs.clear();
    fullDirs.clear();

    for (Map.Entry<String, DiskErrorInformation> entry : dirsFailedCheck
      .entrySet()) {
      String dir = entry.getKey();
      DiskErrorInformation errorInformation = entry.getValue();
      switch (entry.getValue().cause) {
      case DISK_FULL:
        fullDirs.add(entry.getKey());
        break;
      case OTHER:
        errorDirs.add(entry.getKey());
        break;
      }
      if (preCheckGoodDirs.contains(dir)) {
        LOG.warn("Directory " + dir + " error, " + errorInformation.message
            + ", removing from list of valid directories");
        setChanged = true;
        numFailures++;
      }
    }
    for (String dir : allLocalDirs) {
      if (!dirsFailedCheck.containsKey(dir)) {
        localDirs.add(dir);
        if (preCheckFullDirs.contains(dir)
            || preCheckOtherErrorDirs.contains(dir)) {
          setChanged = true;
          LOG.info("Directory " + dir
              + " passed disk check, adding to list of valid directories.");
        }
      }
    }
    Set<String> postCheckFullDirs = new HashSet<String>(fullDirs);
    Set<String> postCheckOtherDirs = new HashSet<String>(errorDirs);
    for (String dir : preCheckFullDirs) {
      if (postCheckOtherDirs.contains(dir)) {
        LOG.warn("Directory " + dir + " error "
            + dirsFailedCheck.get(dir).message);
      }
    }

    for (String dir : preCheckOtherErrorDirs) {
      if (postCheckFullDirs.contains(dir)) {
        LOG.warn("Directory " + dir + " error "
            + dirsFailedCheck.get(dir).message);
      }
    }
    return setChanged;
  }

  Map<String, DiskErrorInformation> testDirs(List<String> dirs) {
    HashMap<String, DiskErrorInformation> ret =
        new HashMap<String, DiskErrorInformation>();
    for (final String dir : dirs) {
      String msg;
      try {
        File testDir = new File(dir);
        DiskChecker.checkDir(testDir);
        if (isDiskUsageOverPercentageLimit(testDir)) {
          msg =
              "used space above threshold of "
                  + diskUtilizationPercentageCutoff
                  + "%";
          ret.put(dir,
            new DiskErrorInformation(DiskErrorCause.DISK_FULL, msg));
          continue;
        } else if (isDiskFreeSpaceUnderLimit(testDir)) {
          msg =
              "free space below limit of " + diskUtilizationSpaceCutoff
                  + "MB";
          ret.put(dir,
            new DiskErrorInformation(DiskErrorCause.DISK_FULL, msg));
          continue;
        }

        // create a random dir to make sure fs isn't in read-only mode
        verifyDirUsingMkdir(testDir);
      } catch (IOException ie) {
        ret.put(dir,
          new DiskErrorInformation(DiskErrorCause.OTHER, ie.getMessage()));
      }
    }
    return ret;
  }

  /**
   * Function to test whether a dir is working correctly by actually creating a
   * random directory.
   *
   * @param dir
   *          the dir to test
   */
  private void verifyDirUsingMkdir(File dir) throws IOException {

    String randomDirName = RandomStringUtils.randomAlphanumeric(5);
    File target = new File(dir, randomDirName);
    int i = 0;
    while (target.exists()) {

      randomDirName = RandomStringUtils.randomAlphanumeric(5) + i;
      target = new File(dir, randomDirName);
      i++;
    }
    try {
      DiskChecker.checkDir(target);
    } finally {
      FileUtils.deleteQuietly(target);
    }
  }

  private boolean isDiskUsageOverPercentageLimit(File dir) {
    float freePercentage =
        100 * (dir.getUsableSpace() / (float) dir.getTotalSpace());
    float usedPercentage = 100.0F - freePercentage;
    return (usedPercentage > diskUtilizationPercentageCutoff
        || usedPercentage >= 100.0F);
  }

  private boolean isDiskFreeSpaceUnderLimit(File dir) {
    long freeSpace = dir.getUsableSpace() / (1024 * 1024);
    return freeSpace < this.diskUtilizationSpaceCutoff;
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
