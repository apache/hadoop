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

package org.apache.hadoop.ozone.om.lock;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.lock.LockManager;

/**
 * Provides different locks to handle concurrency in OzoneMaster.
 * We also maintain lock hierarchy, based on the weight.
 *
 * <table>
 *   <caption></caption>
 *   <tr>
 *     <td><b> WEIGHT </b></td> <td><b> LOCK </b></td>
 *   </tr>
 *   <tr>
 *     <td> 0 </td> <td> S3 Bucket Lock </td>
 *   </tr>
 *   <tr>
 *     <td> 1 </td> <td> Volume Lock </td>
 *   </tr>
 *   <tr>
 *     <td> 2 </td> <td> Bucket Lock </td>
 *   </tr>
 *   <tr>
 *     <td> 3 </td> <td> User Lock </td>
 *   </tr>
 *   <tr>
 *     <td> 4 </td> <td> S3 Secret Lock</td>
 *   </tr>
 *   <tr>
 *     <td> 5 </td> <td> Prefix Lock </td>
 *   </tr>
 * </table>
 *
 * One cannot obtain a lower weight lock while holding a lock with higher
 * weight. The other way around is possible. <br>
 * <br>
 * <p>
 * For example:
 * <br>
 * {@literal ->} acquire volume lock (will work)<br>
 *   {@literal +->} acquire bucket lock (will work)<br>
 *     {@literal +-->} acquire s3 bucket lock (will throw Exception)<br>
 * </p>
 * <br>
 */

public class OzoneManagerLock {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerLock.class);

  private final LockManager<String> manager;
  private final ThreadLocal<Short> lockSet = ThreadLocal.withInitial(
      () -> Short.valueOf((short)0));


  /**
   * Creates new OzoneManagerLock instance.
   * @param conf Configuration object
   */
  public OzoneManagerLock(Configuration conf) {
    manager = new LockManager<>(conf);
  }

  /**
   * Acquire lock on resource.
   *
   * For S3_Bucket, VOLUME, BUCKET type resource, same thread acquiring lock
   * again is allowed.
   *
   * For USER, PREFIX, S3_SECRET type resource, same thread acquiring lock
   * again is not allowed.
   *
   * Special Note for UserLock: Single thread can acquire single user lock/
   * multi user lock. But not both at the same time.
   * @param resourceName - Resource name on which user want to acquire lock.
   * @param resource - Type of the resource.
   */
  public void acquireLock(String resourceName, Resource resource) {
    if (!resource.canLock(lockSet.get())) {
      String errorMessage = getErrorMessage(resource);
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage);
    } else {
      manager.lock(resourceName);
      LOG.debug("Acquired {} lock on resource {}", resource.name,
          resourceName);
      lockSet.set(resource.setLock(lockSet.get()));
    }
  }

  private String getErrorMessage(Resource resource) {
    return "Thread '" + Thread.currentThread().getName() + "' cannot " +
        "acquire " + resource.name + " lock while holding " +
        getCurrentLocks().toString() + " lock(s).";

  }

  private List<String> getCurrentLocks() {
    List<String> currentLocks = new ArrayList<>();
    int i=0;
    short lockSetVal = lockSet.get();
    for (Resource value : Resource.values()) {
      if (value.isLevelLocked(lockSetVal)) {
        currentLocks.add(value.getName());
      }
    }
    return currentLocks;
  }

  /**
   * Acquire lock on multiple users.
   * @param firstUser
   * @param secondUser
   */
  public void acquireMultiUserLock(String firstUser, String secondUser) {
    Resource resource = Resource.USER;
    if (!resource.canLock(lockSet.get())) {
      String errorMessage = getErrorMessage(resource);
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage);
    } else {
      // When acquiring multiple user locks, the reason for doing lexical
      // order comparision is to avoid deadlock scenario.

      // Example: 1st thread acquire lock(ozone, hdfs)
      // 2nd thread acquire lock(hdfs, ozone).
      // If we don't acquire user locks in an order, there can be a deadlock.
      // 1st thread acquired lock on ozone, waiting for lock on hdfs, 2nd
      // thread acquired lock on hdfs, waiting for lock on ozone.
      // To avoid this when we acquire lock on multiple users, we acquire
      // locks in lexical order, which can help us to avoid dead locks.
      // Now if first thread acquires lock on hdfs, 2nd thread wait for lock
      // on hdfs, and first thread acquires lock on ozone. Once after first
      // thread releases user locks, 2nd thread acquires them.

      int compare = firstUser.compareTo(secondUser);
      String temp;

      // Order the user names in sorted order. Swap them.
      if (compare > 0) {
        temp = secondUser;
        secondUser = firstUser;
        firstUser = temp;
      }

      if (compare == 0) {
        // both users are equal.
        manager.lock(firstUser);
      } else {
        manager.lock(firstUser);
        try {
          manager.lock(secondUser);
        } catch (Exception ex) {
          // We got an exception acquiring 2nd user lock. Release already
          // acquired user lock, and throw exception to the user.
          manager.unlock(firstUser);
          throw ex;
        }
      }
      LOG.debug("Acquired {} lock on resource {} and {}", resource.name,
          firstUser, secondUser);
      lockSet.set(resource.setLock(lockSet.get()));
    }
  }



  /**
   * Release lock on multiple users.
   * @param firstUser
   * @param secondUser
   */
  public void releaseMultiUserLock(String firstUser, String secondUser) {
    Resource resource = Resource.USER;
    int compare = firstUser.compareTo(secondUser);

    String temp;

    // Order the user names in sorted order. Swap them.
    if (compare > 0) {
      temp = secondUser;
      secondUser = firstUser;
      firstUser = temp;
    }

    if (compare == 0) {
      // both users are equal.
      manager.unlock(firstUser);
    } else {
      manager.unlock(firstUser);
      manager.unlock(secondUser);
    }
    LOG.debug("Release {} lock on resource {} and {}", resource.name,
        firstUser, secondUser);
    lockSet.set(resource.clearLock(lockSet.get()));
  }


  public void releaseLock(String resourceName, Resource resource) {

    // TODO: Not checking release of higher order level lock happened while
    // releasing lower order level lock, as for that we need counter for
    // locks, as some locks support acquiring lock again.
    manager.unlock(resourceName);
    // clear lock
    LOG.debug("Release {}, lock on resource {}", resource.name,
        resource.name, resourceName);
    lockSet.set(resource.clearLock(lockSet.get()));

  }

  /**
   * Resource defined in Ozone.
   */
  public enum Resource {
    // For S3 Bucket need to allow only for S3, that should be means only 1.
    S3_BUCKET((byte) 0, "S3_BUCKET"), // = 1

    // For volume need to allow both s3 bucket and volume. 01 + 10 = 11 (3)
    VOLUME((byte) 1, "VOLUME"), // = 2

    // For bucket we need to allow both s3 bucket, volume and bucket. Which
    // is equal to 100 + 010 + 001 = 111 = 4 + 2 + 1 = 7
    BUCKET((byte) 2, "BUCKET"), // = 4

    // For user we need to allow s3 bucket, volume, bucket and user lock.
    // Which is 8  4 + 2 + 1 = 15
    USER((byte) 3, "USER"), // 15

    S3_SECRET((byte) 4, "S3_SECRET"), // 31
    PREFIX((byte) 5, "PREFIX"); //63

    // level of the resource
    private byte lockLevel;

    // This will tell the value, till which we can allow locking.
    private short mask;

    // This value will help during setLock, and also will tell whether we can
    // re-acquire lock or not.
    private short setMask;

    // Name of the resource.
    private String name;

    Resource(byte pos, String name) {
      this.lockLevel = pos;
      this.mask = (short) (Math.pow(2, lockLevel + 1) - 1);
      this.setMask = (short) Math.pow(2, lockLevel);
      this.name = name;
    }

    boolean canLock(short lockSetVal) {

      // For USER, S3_SECRET and  PREFIX we shall not allow re-acquire locks at
      // from single thread. 2nd condition is we have acquired one of these
      // locks, but after that trying to acquire a lock with less than equal of
      // lockLevel, we should disallow.
      if (((USER.setMask & lockSetVal) == USER.setMask ||
          (S3_SECRET.setMask & lockSetVal) == S3_SECRET.setMask ||
          (PREFIX.setMask & lockSetVal) == PREFIX.setMask)
          && setMask <= lockSetVal) {
        return false;
      }


      // Our mask is the summation of bits of all previous possible locks. In
      // other words it is the largest possible value for that bit position.

      // For example for Volume lock, bit position is 1, and mask is 3. Which
      // is the largest value that can be represented with 2 bits is 3.
      // Therefore if lockSet is larger than mask we have to return false i.e
      // some other higher order lock has been acquired.

      return lockSetVal <= mask;
    }

    /**
     * Set Lock bits in lockSetVal.
     *
     * @param lockSetVal
     * @return Updated value which has set lock bits.
     */
    short setLock(short lockSetVal) {
      return (short) (lockSetVal | setMask);
    }

    /**
     * Clear lock from lockSetVal.
     *
     * @param lockSetVal
     * @return Updated value which has cleared lock bits.
     */
    short clearLock(short lockSetVal) {
      return (short) (lockSetVal & ~setMask);
    }

    /**
     * Return true, if this level is locked, else false.
     * @param lockSetVal
     */
    boolean isLevelLocked(short lockSetVal) {
      return (lockSetVal & setMask) == setMask;
    }

    String getName() {
      return name;
    }

    short getMask() {
      return mask;
    }
  }

}

