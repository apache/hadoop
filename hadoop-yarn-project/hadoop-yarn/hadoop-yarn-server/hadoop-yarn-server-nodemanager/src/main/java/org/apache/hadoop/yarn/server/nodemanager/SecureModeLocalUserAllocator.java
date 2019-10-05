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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LocalUserInfo {
  String localUser;
  int localUserIndex;
  int appCount;
  int fileOpCount;
  int logHandlingCount;
  
  public LocalUserInfo(String user, int userIndex) {
    localUser = user;
    localUserIndex = userIndex;
    appCount = 0;
    fileOpCount = 0;
    logHandlingCount = 0;
  }
}

/**
 * Allocate local user to an appUser from a pool of precreated local users.
 * Maintains the appUser to local user mapping, until:
 * a) all applications of the appUser is finished;
 * b) all FileDeletionTask for that appUser is executed;
 * c) all log aggregation/handling requests for appUser's applications are done
 *
 * If DeletionService is set, during deallocation, we will check if appcache
 * folder for the app user exists, if yes queue a FileDeletionTask.
 *
 * For now allocation is only maintained in memory so it does not support
 * node manager recovery mode.
 */
public class SecureModeLocalUserAllocator {
  public static final String NONEXISTUSER = "nonexistuser";
  private static final Logger LOG =
      LoggerFactory.getLogger(SecureModeLocalUserAllocator.class);
  private static SecureModeLocalUserAllocator instance;
  private Map<String, LocalUserInfo> appUserToLocalUser;
  private ArrayList<Boolean> allocated;
  private int localUserCount;
  private String localUserPrefix;
  private DeletionService delService;
  private String[] nmLocalDirs;
  private static FileContext lfs = getLfs();

  private static FileContext getLfs() {
    try {
      return FileContext.getLocalFSFileContext();
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  SecureModeLocalUserAllocator(Configuration conf) {
    if (conf.getBoolean(YarnConfiguration.NM_RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED)) {
      String errMsg = "Invalidate configuration combination: " +
          YarnConfiguration.NM_RECOVERY_ENABLED + "=true, " +
          YarnConfiguration.NM_SECURE_MODE_USE_POOL_USER + "=true";
      throw new RuntimeException(errMsg);
    }
    localUserPrefix = conf.get(
        YarnConfiguration.NM_SECURE_MODE_POOL_USER_PREFIX,
        YarnConfiguration.DEFAULT_NM_SECURE_MODE_POOL_USER_PREFIX);
    localUserCount = conf.getInt(
        YarnConfiguration.NM_SECURE_MODE_POOL_USER_COUNT,
        YarnConfiguration.DEFAULT_NM_SECURE_MODE_POOL_USER_COUNT);
    if (localUserCount == -1) {
      localUserCount = conf.getInt(YarnConfiguration.NM_VCORES,
          YarnConfiguration.DEFAULT_NM_VCORES);
    }
    allocated = new ArrayList<Boolean>(localUserCount);
    appUserToLocalUser = new HashMap<String, LocalUserInfo>(localUserCount);
    for (int i=0; i<localUserCount; ++i) {
      allocated.add(false);
    }
    nmLocalDirs = conf.getTrimmedStrings(YarnConfiguration.NM_LOCAL_DIRS);
  }

  public static SecureModeLocalUserAllocator getInstance(Configuration conf) {
    if (instance == null) {
      synchronized (SecureModeLocalUserAllocator.class) {
        if (instance == null) {
          instance = new SecureModeLocalUserAllocator(conf);
        }
      }
    }
    return instance;
  }

  public void setDeletionService(DeletionService delService) {
    this.delService = delService;
  }

  /**
   * Get allocated local user for the appUser.
   */
  public String getRunAsLocalUser(String appUser) {
    if (!appUserToLocalUser.containsKey(appUser)) {
      LOG.error("Cannot find runas local user for appUser " + appUser +
          ", return " + NONEXISTUSER);
      return NONEXISTUSER;
    }
    return appUserToLocalUser.get(appUser).localUser;
  }

  /**
   * Allocate a local user for appUser to run application appId
   */
  synchronized public void allocate(String appUser, String appId) {
    LOG.info("Allocating local user for " + appUser + " for " + appId);
    if (!checkAndAllocateAppUser(appUser)) {
      return;
    }
    LocalUserInfo localUserInfo = appUserToLocalUser.get(appUser);
    localUserInfo.appCount++;
    LOG.info("Incremented appCount for appUser " + appUser +
          " to " + localUserInfo.appCount);
  }

  /**
   * Deallocate local user for appUser for application appId.
   */
  synchronized public void deallocate(String appUser, String appId) {
    LOG.info("Deallocating local user for " + appUser + " for " + appId);
    if (!appUserToLocalUser.containsKey(appUser)) {
      // This should never happen
      String errMsg = "deallocate: No local user allocation for appUser " +
          appUser + ", appId " + appId;
      LOG.error(errMsg);
      return;
    }

    LocalUserInfo localUserInfo = appUserToLocalUser.get(appUser);
    localUserInfo.appCount--;
    LOG.info("Decremented appCount for appUser " + appUser +
        " to " + localUserInfo.appCount);

    checkAndDeallocateAppUser(appUser, localUserInfo);
  }

  /**
   * Increment reference count for pending file operations
   *
   * Note that it is allowed to call incrementFileOpCount() before allocate().
   * For example, during node manager restarts if there are old folders created
   * by these local pool users, we should allocate the same named local user to
   * the requested appUser, so that the local user is not allocated to new
   * containers to use until the old folder deletions are done.
   * The old app folders clean up code is in:
   * ResourceLocalizationService.cleanUpFilesPerUserDir()
   */
  synchronized public void incrementFileOpCount(String appUser) {
    if (!checkAndAllocateAppUser(appUser)) {
      return;
    }
    LocalUserInfo localUserInfo = appUserToLocalUser.get(appUser);
    localUserInfo.fileOpCount++;
    LOG.info("Incremented fileOpCount for appuser " + appUser +
        " to " + localUserInfo.fileOpCount);
  }

  /**
   * Decrement reference count for pending file operations
   */
  synchronized public void decrementFileOpCount(String appUser) {
    if (!appUserToLocalUser.containsKey(appUser)) {
      String errMsg =
          "decrementFileOpCount: No local user allocation for appUser " +
          appUser;
      LOG.error(errMsg);
      return;
    }
    LocalUserInfo localUserInfo = appUserToLocalUser.get(appUser);
    localUserInfo.fileOpCount--;
    LOG.info("Decremented fileOpCount for appUser " + appUser +
        " to " + localUserInfo.fileOpCount);

    checkAndDeallocateAppUser(appUser, localUserInfo);
  }

  /**
   * Increment pending log handling (or aggregation) per application
   */
  synchronized public void incrementLogHandlingCount(String appUser) {
    if (!appUserToLocalUser.containsKey(appUser)) {
      String errMsg =
          "incrementLogHandlingCount: No local user allocation for appUser " +
          appUser;
      LOG.error(errMsg);
      return;
    }
    LocalUserInfo localUserInfo = appUserToLocalUser.get(appUser);
    localUserInfo.logHandlingCount++;
    LOG.info("Incremented logHandlingCount for appUser " + appUser +
        " to " + localUserInfo.logHandlingCount);
  }

  /**
   * Decrement pending log handling (or aggregation) per application
   */
  synchronized public void decrementLogHandlingCount(String appUser) {
    if (!appUserToLocalUser.containsKey(appUser)) {
      String errMsg =
          "decrementLogHandlingCount: No local user allocation for appUser " +
          appUser;
      LOG.error(errMsg);
      return;
    }
    LocalUserInfo localUserInfo = appUserToLocalUser.get(appUser);
    localUserInfo.logHandlingCount--;
    LOG.info("Decremented logHandlingCount for appUser " + appUser +
        " to " + localUserInfo.logHandlingCount);

    checkAndDeallocateAppUser(appUser, localUserInfo);
  }

  /**
   * Check if a given user name is local pool user
   * if yes return index, otherwise return -1
   */
  private int localUserIndex(String user) {
    int result = -1;
    if (!user.startsWith(localUserPrefix)) {
      return result;
    }
    try {
      result = Integer.parseInt(user.substring(localUserPrefix.length()));
    }
    catch(NumberFormatException e) {
      result = -1;
    }
    if (result >= localUserCount) {
      result = -1;
    }
    return result;
  }

  /**
   * check if the appUser mapping exists, if not then allocate a local user.
   * return true if appUser mapping exists or created,
   * return false if not able to allocate local user.
   */
  private boolean checkAndAllocateAppUser(String appUser) {
    if (appUserToLocalUser.containsKey(appUser)) {
      // If appUser exists, don't need to allocate again
      return true;
    }

    LOG.info("Allocating local user for appUser " + appUser);
    // check if the appUser is one of the local user, if yes just use it
    int index = localUserIndex(appUser);
    if (index == -1) {
      // otherwise find the first empty slot in the pool of local users
      for (int i=0; i<localUserCount; ++i) {
        if (!allocated.get(i)) {
          index = i;
          break;
        }
      }
    }
    if (index == -1) {
      String errMsg = "Cannot allocate local users from a pool of " +
          localUserCount; 
      LOG.error(errMsg);
      return false;
    }
    else {
      allocated.set(index, true);
    }
    appUserToLocalUser.put(appUser,
        new LocalUserInfo(localUserPrefix + index, index));
    LOG.info("Allocated local user index " + index + " for appUser "
        + appUser);
    return true;
  }

  private void checkAndDeallocateAppUser(String appUser, LocalUserInfo localUserInfo) {
    if (localUserInfo.fileOpCount <= 0 &&
        localUserInfo.appCount <= 0 &&
        localUserInfo.logHandlingCount <= 0) {
      String localUser = appUserToLocalUser.remove(appUser).localUser;
      allocated.set(localUserInfo.localUserIndex, false);
      if (delService != null) {
        // check if node manager usercache/<appUser>/appcache folder exists
        for (String localDir : nmLocalDirs) {
          Path usersDir = new Path(localDir, ContainerLocalizer.USERCACHE);
          Path userDir = new Path(usersDir, appUser);
          Path userAppCacheDir = new Path(userDir, ContainerLocalizer.APPCACHE);
          FileStatus status;
          try {
            status = lfs.getFileStatus(userAppCacheDir);
          }
          catch(FileNotFoundException fs) {
            status = null;
          }
          catch(IOException ie) {
            String msg = "Could not get file status for local dir " + userDir;
            LOG.warn(msg, ie);
            throw new YarnRuntimeException(msg, ie);
          }
          if (status != null) {
            FileDeletionTask delTask = new FileDeletionTask(delService, localUser,
                userAppCacheDir, null);
            delService.delete(delTask);
          }
        }
      }
      LOG.info("Deallocated local user index " + localUserInfo.localUserIndex +
          " for appUser " + appUser);
    }
  }
}