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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Contains test-cases to verify OzoneManagerLock.
 */
public class TestOzoneManagerLock {

  @Test(timeout = 1000)
  public void testDifferentUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireUserLock("userOne");
    lock.acquireUserLock("userTwo");
    lock.releaseUserLock("userOne");
    lock.releaseUserLock("userTwo");
    Assert.assertTrue(true);
  }

  @Test
  public void testSameUserLock() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireUserLock("userOne");
    AtomicBoolean gotLock = new AtomicBoolean(false);
    new Thread(() -> {
      lock.acquireUserLock("userOne");
      gotLock.set(true);
      lock.releaseUserLock("userOne");
    }).start();
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    // Since the new thread is trying to get lock on same user, it will wait.
    Assert.assertFalse(gotLock.get());
    lock.releaseUserLock("userOne");
    // Since we have released the lock, the new thread should have the lock
    // now
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    Assert.assertTrue(gotLock.get());
  }

  @Test(timeout = 1000)
  public void testDifferentVolumeLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireVolumeLock("volOne");
    lock.acquireVolumeLock("volTwo");
    lock.releaseVolumeLock("volOne");
    lock.releaseVolumeLock("volTwo");
    Assert.assertTrue(true);
  }

  @Test
  public void testSameVolumeLock() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireVolumeLock("volOne");
    AtomicBoolean gotLock = new AtomicBoolean(false);
    new Thread(() -> {
      lock.acquireVolumeLock("volOne");
      gotLock.set(true);
      lock.releaseVolumeLock("volOne");
    }).start();
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    // Since the new thread is trying to get lock on same user, it will wait.
    Assert.assertFalse(gotLock.get());
    lock.releaseVolumeLock("volOne");
    // Since we have released the lock, the new thread should have the lock
    // now
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    Assert.assertTrue(gotLock.get());
  }

  @Test(timeout = 1000)
  public void testDifferentBucketLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireBucketLock("volOne", "bucketOne");
    lock.acquireBucketLock("volOne", "bucketTwo");
    lock.releaseBucketLock("volOne", "bucketTwo");
    lock.releaseBucketLock("volOne", "bucketOne");
    Assert.assertTrue(true);
  }

  @Test
  public void testSameBucketLock() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireBucketLock("volOne", "bucketOne");
    AtomicBoolean gotLock = new AtomicBoolean(false);
    new Thread(() -> {
      lock.acquireBucketLock("volOne", "bucketOne");
      gotLock.set(true);
      lock.releaseBucketLock("volOne", "bucketOne");
    }).start();
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    // Since the new thread is trying to get lock on same user, it will wait.
    Assert.assertFalse(gotLock.get());
    lock.releaseBucketLock("volOne", "bucketOne");
    // Since we have released the lock, the new thread should have the lock
    // now
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    Assert.assertTrue(gotLock.get());
  }

  @Test(timeout = 1000)
  public void testVolumeLockAfterUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireUserLock("userOne");
    lock.acquireVolumeLock("volOne");
    lock.releaseVolumeLock("volOne");
    lock.releaseUserLock("userOne");
    Assert.assertTrue(true);
  }

  @Test(timeout = 1000)
  public void testBucketLockAfterVolumeLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireVolumeLock("volOne");
    lock.acquireBucketLock("volOne", "bucketOne");
    lock.releaseBucketLock("volOne", "bucketOne");
    lock.releaseVolumeLock("volOne");
    Assert.assertTrue(true);
  }

  @Test(timeout = 1000)
  public void testBucketLockAfterVolumeLockAfterUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireUserLock("userOne");
    lock.acquireVolumeLock("volOne");
    lock.acquireBucketLock("volOne", "bucketOne");
    lock.releaseBucketLock("volOne", "bucketOne");
    lock.releaseVolumeLock("volOne");
    lock.releaseUserLock("userOne");
    Assert.assertTrue(true);
  }

  @Test
  public void testUserLockAfterVolumeLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireVolumeLock("volOne");
    try {
      lock.acquireUserLock("userOne");
      Assert.fail();
    } catch (RuntimeException ex) {
      String msg =
          "cannot acquire user lock while holding " +
              "volume, bucket or S3 bucket lock(s).";
      Assert.assertTrue(ex.getMessage().contains(msg));
    }
    lock.releaseVolumeLock("volOne");
    Assert.assertTrue(true);
  }

  @Test
  public void testVolumeLockAfterBucketLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireBucketLock("volOne", "bucketOne");
    try {
      lock.acquireVolumeLock("volOne");
      Assert.fail();
    } catch (RuntimeException ex) {
      String msg =
          "cannot acquire volume lock while holding bucket lock(s).";
      Assert.assertTrue(ex.getMessage().contains(msg));
    }
    lock.releaseBucketLock("volOne", "bucketOne");
    Assert.assertTrue(true);
  }


}