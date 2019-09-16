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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
    localUserCount = conf.getInt(YarnConfiguration.NM_VCORES,
        YarnConfiguration.DEFAULT_NM_VCORES);
    allocated = new ArrayList<Boolean>(localUserCount);
    appUserToLocalUser = new HashMap<String, LocalUserInfo>(localUserCount);
    for (int i=0; i<localUserCount; ++i) {
      allocated.add(false);
    }
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
    checkAndAllocateAppUser(appUser);
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
   */
  synchronized public void incrementFileOpCount(String appUser) {
    if (!appUserToLocalUser.containsKey(appUser)) {
      String msg =
          "incrementPendingFileOp: No local user allocation for appUser " +
          appUser;
      LOG.warn(msg);
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
      LOG.warn(errMsg);
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
      LOG.warn(errMsg);
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
      LOG.warn(errMsg);
      return;
    }
    LocalUserInfo localUserInfo = appUserToLocalUser.get(appUser);
    localUserInfo.logHandlingCount--;
    LOG.info("Decremented logHandlingCount for appUser " + appUser +
        " to " + localUserInfo.logHandlingCount);

    checkAndDeallocateAppUser(appUser, localUserInfo);
  }

  private void checkAndAllocateAppUser(String appUser) {
    if (appUserToLocalUser.containsKey(appUser)) {
      // If appUser exists, don't need to allocate again
      return;
    }

    LOG.info("Allocating local user for appUser " + appUser);
    // find the first empty slot in the pool of local users
    int index = -1;
    for (int i=0; i<localUserCount; ++i) {
      if (!allocated.get(i)) {
        index = i;
        allocated.set(i, true);
        break;
      }
    }
    if (index == -1) {
      String errMsg = "Cannot allocate local users from a pool of " +
          localUserCount; 
      LOG.error(errMsg);
      return;
    }
    appUserToLocalUser.put(appUser,
        new LocalUserInfo(localUserPrefix + index, index));
    LOG.info("Allocated local user index " + index + " for appUser "
        + appUser);
  }

  private void checkAndDeallocateAppUser(String appUser, LocalUserInfo localUserInfo) {
    if (localUserInfo.fileOpCount <= 0 &&
        localUserInfo.appCount <= 0 &&
        localUserInfo.logHandlingCount <= 0) {
      appUserToLocalUser.remove(appUser);
      allocated.set(localUserInfo.localUserIndex, false);
      LOG.info("Deallocated local user index " + localUserInfo.localUserIndex +
          " for appUser " + appUser);
    }
  }
}