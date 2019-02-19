/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.lock.LockManager;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_S3_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_USER_PREFIX;

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
 *     <td> 0 </td> <td> User Lock </td>
 *   </tr>
 *   <tr>
 *     <td> 1 </td> <td> Volume Lock </td>
 *   </tr>
 *   <tr>
 *     <td> 2 </td> <td> Bucket Lock </td>
 *   </tr>
 * </table>
 *
 * One cannot obtain a lower weight lock while holding a lock with higher
 * weight. The other way around is possible. <br>
 * <br>
 * <p>
 * For example:
 * <br>
 * {@literal ->} acquireVolumeLock (will work)<br>
 *   {@literal +->} acquireBucketLock (will work)<br>
 *     {@literal +-->} acquireUserLock (will throw Exception)<br>
 * </p>
 * <br>
 * To acquire a user lock you should not hold any Volume/Bucket lock. Similarly
 * to acquire a Volume lock you should not hold any Bucket lock.
 */
public final class OzoneManagerLock {

  private static final String VOLUME_LOCK = "volumeLock";
  private static final String BUCKET_LOCK = "bucketLock";
  private static final String S3_BUCKET_LOCK = "s3BucketLock";
  private static final String S3_SECRET_LOCK = "s3SecretetLock";

  private final LockManager<String> manager;

  // To maintain locks held by current thread.
  private final ThreadLocal<Map<String, AtomicInteger>> myLocks =
      ThreadLocal.withInitial(
          () -> ImmutableMap.of(
              VOLUME_LOCK, new AtomicInteger(0),
              BUCKET_LOCK, new AtomicInteger(0),
              S3_BUCKET_LOCK, new AtomicInteger(0),
              S3_SECRET_LOCK, new AtomicInteger(0)
          )
      );

  /**
   * Creates new OzoneManagerLock instance.
   * @param conf Configuration object
   */
  public OzoneManagerLock(Configuration conf) {
    manager = new LockManager<>(conf);
  }

  /**
   * Acquires user lock on the given resource.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until the
   * lock has been acquired.
   *
   * @param user User on which the lock has to be acquired
   */
  public void acquireUserLock(String user) {
    // Calling thread should not hold any volume or bucket lock.
    if (hasAnyVolumeLock() || hasAnyBucketLock() || hasAnyS3Lock()) {
      throw new RuntimeException(
          "Thread '" + Thread.currentThread().getName() +
              "' cannot acquire user lock" +
              " while holding volume, bucket or S3 bucket lock(s).");
    }
    manager.lock(OM_USER_PREFIX + user);
  }

  /**
   * Releases the user lock on given resource.
   */
  public void releaseUserLock(String user) {
    manager.unlock(OM_USER_PREFIX + user);
  }

  /**
   * Acquires volume lock on the given resource.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until the
   * lock has been acquired.
   *
   * @param volume Volume on which the lock has to be acquired
   */
  public void acquireVolumeLock(String volume) {
    // Calling thread should not hold any bucket lock.
    // You can take an Volume while holding S3 bucket lock, since
    // semantically an S3 bucket maps to the ozone volume. So we check here
    // only if ozone bucket lock is taken.
    if (hasAnyBucketLock()) {
      throw new RuntimeException(
          "Thread '" + Thread.currentThread().getName() +
              "' cannot acquire volume lock while holding bucket lock(s).");
    }
    manager.lock(OM_KEY_PREFIX + volume);
    myLocks.get().get(VOLUME_LOCK).incrementAndGet();
  }

  /**
   * Releases the volume lock on given resource.
   */
  public void releaseVolumeLock(String volume) {
    manager.unlock(OM_KEY_PREFIX + volume);
    myLocks.get().get(VOLUME_LOCK).decrementAndGet();
  }

  /**
   * Acquires S3 Bucket lock on the given resource.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until the lock has
   * been acquired.
   *
   * @param s3BucketName S3Bucket Name on which the lock has to be acquired
   */
  public void acquireS3Lock(String s3BucketName) {
    // Calling thread should not hold any bucket lock.
    // You can take an Volume while holding S3 bucket lock, since
    // semantically an S3 bucket maps to the ozone volume. So we check here
    // only if ozone bucket lock is taken.
    if (hasAnyBucketLock()) {
      throw new RuntimeException(
          "Thread '" + Thread.currentThread().getName() +
              "' cannot acquire S3 bucket lock while holding Ozone bucket " +
              "lock(s).");
    }
    manager.lock(OM_S3_PREFIX + s3BucketName);
    myLocks.get().get(S3_BUCKET_LOCK).incrementAndGet();
  }

  /**
   * Releases the volume lock on given resource.
   */
  public void releaseS3Lock(String s3BucketName) {
    manager.unlock(OM_S3_PREFIX + s3BucketName);
    myLocks.get().get(S3_BUCKET_LOCK).decrementAndGet();
  }

  /**
   * Acquires bucket lock on the given resource.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until the
   * lock has been acquired.
   *
   * @param bucket Bucket on which the lock has to be acquired
   */
  public void acquireBucketLock(String volume, String bucket) {
    manager.lock(OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket);
    myLocks.get().get(BUCKET_LOCK).incrementAndGet();
  }

  /**
   * Releases the bucket lock on given resource.
   */
  public void releaseBucketLock(String volume, String bucket) {
    manager.unlock(OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket);
    myLocks.get().get(BUCKET_LOCK).decrementAndGet();
  }

  /**
   * Returns true if the current thread holds any volume lock.
   * @return true if current thread holds volume lock, else false
   */
  private boolean hasAnyVolumeLock() {
    return myLocks.get().get(VOLUME_LOCK).get() != 0;
  }

  /**
   * Returns true if the current thread holds any bucket lock.
   * @return true if current thread holds bucket lock, else false
   */
  private boolean hasAnyBucketLock() {
    return myLocks.get().get(BUCKET_LOCK).get() != 0;
  }

  private boolean hasAnyS3Lock() {
    return myLocks.get().get(S3_BUCKET_LOCK).get() != 0;
  }

  public void acquireS3SecretLock(String awsAccessId) {
    if (hasAnyS3SecretLock()) {
      throw new RuntimeException(
          "Thread '" + Thread.currentThread().getName() +
              "' cannot acquire S3 Secret lock while holding S3 " +
              "awsAccessKey lock(s).");
    }
    manager.lock(awsAccessId);
    myLocks.get().get(S3_SECRET_LOCK).incrementAndGet();
  }

  private boolean hasAnyS3SecretLock() {
    return myLocks.get().get(S3_SECRET_LOCK).get() != 0;
  }

  public void releaseS3SecretLock(String awsAccessId) {
    manager.unlock(awsAccessId);
    myLocks.get().get(S3_SECRET_LOCK).decrementAndGet();
  }
}
