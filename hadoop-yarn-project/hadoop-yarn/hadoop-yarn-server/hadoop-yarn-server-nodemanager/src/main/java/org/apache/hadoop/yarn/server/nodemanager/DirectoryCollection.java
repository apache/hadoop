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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.DiskValidator;
import org.apache.hadoop.util.DiskValidatorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.google.common.annotations.VisibleForTesting;

/**
 * Manages a list of local storage directories.
 */
public class DirectoryCollection {
  private static final Logger LOG =
       LoggerFactory.getLogger(DirectoryCollection.class);

  private final Configuration conf;
  private final DiskValidator diskValidator;
  /**
   * The enum defines disk failure type.
   */
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
   * The interface provides a callback when localDirs is changed.
   */
  public interface DirsChangeListener {
    void onDirsChanged();
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
  private Map<String, DiskErrorInformation> directoryErrorInfo;

  // read/write lock for accessing above directories.
  private final ReadLock readLock;
  private final WriteLock writeLock;

  private int numFailures;

  private float diskUtilizationPercentageCutoffHigh;
  private float diskUtilizationPercentageCutoffLow;
  private long diskUtilizationSpaceCutoff;

  private int goodDirsDiskUtilizationPercentage;

  private Set<DirsChangeListener> dirsChangeListeners;

  /**
   * Create collection for the directories specified. No check for free space.
   * 
   * @param dirs
   *          directories to be monitored
   */
  public DirectoryCollection(String[] dirs) {
    this(dirs, 100.0F, 100.0F, 0);
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
    this(dirs, utilizationPercentageCutOff, utilizationPercentageCutOff, 0);
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
    this(dirs, 100.0F, 100.0F, utilizationSpaceCutOff);
  }

  /**
   * Create collection for the directories specified. Users must specify the
   * maximum percentage of disk utilization allowed and the minimum amount of
   * free space that must be available for the dir to be used. If either check
   * fails the dir is removed from the good dirs list.
   * 
   * @param dirs
   *          directories to be monitored
   * @param utilizationPercentageCutOffHigh
   *          percentage of disk that can be used before the dir is taken out of
   *          the good dirs list
   * @param utilizationPercentageCutOffLow
   *          percentage of disk that can be used when the dir is moved from
   *          the bad dirs list to the good dirs list
   * @param utilizationSpaceCutOff
   *          minimum space, in MB, that must be available on the disk for the
   *          dir to be marked as good
   * 
   */
  public DirectoryCollection(String[] dirs,
      float utilizationPercentageCutOffHigh,
      float utilizationPercentageCutOffLow,
      long utilizationSpaceCutOff) {
    conf = new YarnConfiguration();
    try {
      diskValidator = DiskValidatorFactory.getInstance(
          conf.get(YarnConfiguration.DISK_VALIDATOR,
              YarnConfiguration.DEFAULT_DISK_VALIDATOR));
      LOG.info("Disk Validator: " + YarnConfiguration.DISK_VALIDATOR +
          " is loaded.");
    } catch (Exception e) {
      throw new YarnRuntimeException(e);
    }

    localDirs = new CopyOnWriteArrayList<>(dirs);
    errorDirs = new CopyOnWriteArrayList<>();
    fullDirs = new CopyOnWriteArrayList<>();
    directoryErrorInfo = new ConcurrentHashMap<>();

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    diskUtilizationPercentageCutoffHigh = Math.max(0.0F, Math.min(100.0F,
        utilizationPercentageCutOffHigh));
    diskUtilizationPercentageCutoffLow = Math.max(0.0F, Math.min(
        diskUtilizationPercentageCutoffHigh, utilizationPercentageCutOffLow));
    diskUtilizationSpaceCutoff =
        utilizationSpaceCutOff < 0 ? 0 : utilizationSpaceCutOff;

    dirsChangeListeners = Collections.newSetFromMap(
        new ConcurrentHashMap<DirsChangeListener, Boolean>());
  }

  void registerDirsChangeListener(
      DirsChangeListener listener) {
    if (dirsChangeListeners.add(listener)) {
      listener.onDirsChanged();
    }
  }

  void deregisterDirsChangeListener(
      DirsChangeListener listener) {
    dirsChangeListeners.remove(listener);
  }

  /**
   * @return the current valid directories 
   */
  List<String> getGoodDirs() {
    this.readLock.lock();
    try {
      return Collections.unmodifiableList(localDirs);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * @return the failed directories
   */
  List<String> getFailedDirs() {
    this.readLock.lock();
    try {
      return Collections.unmodifiableList(
          DirectoryCollection.concat(errorDirs, fullDirs));
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * @return the directories that have used all disk space
   */
  List<String> getFullDirs() {
    this.readLock.lock();
    try {
      return Collections.unmodifiableList(fullDirs);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * @return the directories that have errors - many not have appropriate permissions
   * or other disk validation checks might have failed in {@link DiskValidator}
   *
   */
  @InterfaceStability.Evolving
  List<String> getErroredDirs() {
    this.readLock.lock();
    try {
      return Collections.unmodifiableList(errorDirs);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * @return total the number of directory failures seen till now
   */
  int getNumFailures() {
    this.readLock.lock();
    try {
      return numFailures;
    }finally {
      this.readLock.unlock();
    }
  }

  /**
   *
   * @param dirName Absolute path of Directory for which error diagnostics are needed
   * @return DiskErrorInformation - disk error diagnostics for the specified directory
   *         null - the disk associated with the directory has passed disk utilization checks
   *         /error validations in {@link DiskValidator}
   *
   */
  @InterfaceStability.Evolving
  DiskErrorInformation getDirectoryErrorInfo(String dirName) {
    this.readLock.lock();
    try {
      return directoryErrorInfo.get(dirName);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   *
   * @param dirName Absolute path of Directory for which the disk has been marked as unhealthy
   * @return Check if disk associated with the directory is unhealthy
   */
  @InterfaceStability.Evolving
  boolean isDiskUnHealthy(String dirName) {
    this.readLock.lock();
    try {
      return directoryErrorInfo.containsKey(dirName);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Create any non-existent directories and parent directories, updating the
   * list of valid directories if necessary.
   * @param localFs local file system to use
   * @param perm absolute permissions to use for any directories created
   * @return true if there were no errors, false if at least one error occurred
   */
  boolean createNonExistentDirs(FileContext localFs,
      FsPermission perm) {
    boolean failed = false;
    List<String> localDirectories = null;
    this.readLock.lock();
    try {
      localDirectories = new ArrayList<>(localDirs);
    } finally {
      this.readLock.unlock();
    }
    for (final String dir : localDirectories) {
      try {
        createDir(localFs, new Path(dir), perm);
      } catch (IOException e) {
        LOG.warn("Unable to create directory " + dir + " error " +
            e.getMessage() + ", removing from the list of valid directories.");
        this.writeLock.lock();
        try {
          localDirs.remove(dir);
          errorDirs.add(dir);
          directoryErrorInfo.put(dir,
              new DiskErrorInformation(DiskErrorCause.OTHER,
                  "Cannot create directory : " + dir + ", error " + e.getMessage()));
          numFailures++;
        } finally {
          this.writeLock.unlock();
        }
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
  boolean checkDirs() {
    boolean setChanged = false;
    Set<String> preCheckGoodDirs = null;
    Set<String> preCheckFullDirs = null;
    Set<String> preCheckOtherErrorDirs = null;
    List<String> failedDirs = null;
    List<String> allLocalDirs = null;
    this.readLock.lock();
    try {
      preCheckGoodDirs = new HashSet<String>(localDirs);
      preCheckFullDirs = new HashSet<String>(fullDirs);
      preCheckOtherErrorDirs = new HashSet<String>(errorDirs);
      failedDirs = DirectoryCollection.concat(errorDirs, fullDirs);
      allLocalDirs = DirectoryCollection.concat(localDirs, failedDirs);
    } finally {
      this.readLock.unlock();
    }

    // move testDirs out of any lock as it could wait for very long time in
    // case of busy IO
    Map<String, DiskErrorInformation> dirsFailedCheck = testDirs(allLocalDirs,
        preCheckGoodDirs);

    this.writeLock.lock();
    try {
      localDirs.clear();
      errorDirs.clear();
      fullDirs.clear();
      directoryErrorInfo.clear();

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
        default:
          LOG.warn(entry.getValue().cause + " is unknown for disk error.");
          break;
        }
        directoryErrorInfo.put(entry.getKey(), errorInformation);

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
      setGoodDirsDiskUtilizationPercentage();
      if (setChanged) {
        for (DirsChangeListener listener : dirsChangeListeners) {
          listener.onDirsChanged();
        }
      }
      return setChanged;
    } finally {
      this.writeLock.unlock();
    }
  }

  Map<String, DiskErrorInformation> testDirs(List<String> dirs,
      Set<String> goodDirs) {
    HashMap<String, DiskErrorInformation> ret =
        new HashMap<String, DiskErrorInformation>();
    for (final String dir : dirs) {
      String msg;
      try {
        File testDir = new File(dir);
        diskValidator.checkStatus(testDir);
        float diskUtilizationPercentageCutoff = goodDirs.contains(dir) ?
            diskUtilizationPercentageCutoffHigh : diskUtilizationPercentageCutoffLow;
        if (isDiskUsageOverPercentageLimit(testDir,
            diskUtilizationPercentageCutoff)) {
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
      diskValidator.checkStatus(target);
    } finally {
      FileUtils.deleteQuietly(target);
    }
  }

  private boolean isDiskUsageOverPercentageLimit(File dir,
      float diskUtilizationPercentageCutoff) {
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
      try {
        localFs.mkdir(dir, perm, false);
      } catch (FileAlreadyExistsException ex) {
        // do nothing as other threads could in creating the same directory.
      }
      if (!perm.equals(perm.applyUMask(localFs.getUMask()))) {
        localFs.setPermission(dir, perm);
      }
    }
  }

  @VisibleForTesting
  float getDiskUtilizationPercentageCutoffHigh() {
    return diskUtilizationPercentageCutoffHigh;
  }

  @VisibleForTesting
  float getDiskUtilizationPercentageCutoffLow() {
    return diskUtilizationPercentageCutoffLow;
  }

  public void setDiskUtilizationPercentageCutoff(
      float utilizationPercentageCutOffHigh,
      float utilizationPercentageCutOffLow) {
    diskUtilizationPercentageCutoffHigh = Math.max(0.0F, Math.min(100.0F,
        utilizationPercentageCutOffHigh));
    diskUtilizationPercentageCutoffLow = Math.max(0.0F, Math.min(
        diskUtilizationPercentageCutoffHigh, utilizationPercentageCutOffLow));
  }

  public long getDiskUtilizationSpaceCutoff() {
    return diskUtilizationSpaceCutoff;
  }

  public void setDiskUtilizationSpaceCutoff(long diskUtilizationSpaceCutoff) {
    diskUtilizationSpaceCutoff =
        diskUtilizationSpaceCutoff < 0 ? 0 : diskUtilizationSpaceCutoff;
    this.diskUtilizationSpaceCutoff = diskUtilizationSpaceCutoff;
  }

  private void setGoodDirsDiskUtilizationPercentage() {

    long totalSpace = 0;
    long usableSpace = 0;

    for (String dir : localDirs) {
      File f = new File(dir);
      if (!f.isDirectory()) {
        continue;
      }
      totalSpace += f.getTotalSpace();
      usableSpace += f.getUsableSpace();
    }
    if (totalSpace != 0) {
      long tmp = ((totalSpace - usableSpace) * 100) / totalSpace;
      if (Integer.MIN_VALUE < tmp && Integer.MAX_VALUE > tmp) {
        goodDirsDiskUtilizationPercentage = (int) tmp;
      }
    } else {
      // got no good dirs
      goodDirsDiskUtilizationPercentage = 0;
    }
  }

  public int getGoodDirsDiskUtilizationPercentage() {
    return goodDirsDiskUtilizationPercentage;
  }
}
