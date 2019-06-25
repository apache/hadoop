/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.lock;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import static org.junit.Assert.fail;

/**
 * Class tests OzoneManagerLock.
 */
public class TestOzoneManagerLock {
  @Test
  public void acquireResourceLock() {
    String resourceName;
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      resourceName = generateResourceLockName(resource);
      testResourceLock(resourceName, resource);
    }
  }

  private void testResourceLock(String resourceName,
      OzoneManagerLock.Resource resource) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireLock(resourceName, resource);
    lock.releaseLock(resourceName, resource);
    Assert.assertTrue(true);
  }

  @Test
  public void reacquireResourceLock() {
    String resourceName;
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      resourceName = generateResourceLockName(resource);
      testResourceReacquireLock(resourceName, resource);
    }
  }

  private void testResourceReacquireLock(String resourceName,
      OzoneManagerLock.Resource resource) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    // Lock re-acquire not allowed by same thread.
    if (resource == OzoneManagerLock.Resource.USER ||
        resource == OzoneManagerLock.Resource.S3_SECRET ||
        resource == OzoneManagerLock.Resource.PREFIX){
      lock.acquireLock(resourceName, resource);
      try {
        lock.acquireLock(resourceName, resource);
        fail("reacquireResourceLock failed");
      } catch (RuntimeException ex) {
        String message = "cannot acquire " + resource.getName() + " lock " +
            "while holding [" + resource.getName() + "] lock(s).";
        Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
      }
      lock.releaseLock(resourceName, resource);
      Assert.assertTrue(true);
    } else {
      lock.acquireLock(resourceName, resource);
      lock.acquireLock(resourceName, resource);
      lock.releaseLock(resourceName, resource);
      lock.releaseLock(resourceName, resource);
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testLockingOrder() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String resourceName;

    // What this test does is iterate all resources. For each resource
    // acquire lock, and then in inner loop acquire all locks with higher
    // lock level, finally release the locks.
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      Stack<ResourceInfo> stack = new Stack<>();
      resourceName = generateResourceLockName(resource);
      lock.acquireLock(resourceName, resource);
      stack.push(new ResourceInfo(resourceName, resource));
      for (OzoneManagerLock.Resource higherResource :
          OzoneManagerLock.Resource.values()) {
        if (higherResource.getMask() > resource.getMask()) {
          resourceName = generateResourceLockName(higherResource);
          lock.acquireLock(resourceName, higherResource);
          stack.push(new ResourceInfo(resourceName, higherResource));
        }
      }
      // Now release locks
      while (!stack.empty()) {
        ResourceInfo resourceInfo = stack.pop();
        lock.releaseLock(resourceInfo.getLockName(),
            resourceInfo.getResource());
      }
    }
    Assert.assertTrue(true);
  }

  @Test
  public void testLockViolationsWithOneHigherLevelLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      for (OzoneManagerLock.Resource higherResource :
          OzoneManagerLock.Resource.values()) {
        if (higherResource.getMask() > resource.getMask()) {
          String resourceName = generateResourceLockName(higherResource);
          lock.acquireLock(resourceName, higherResource);
          try {
            lock.acquireLock(generateResourceLockName(resource), resource);
            fail("testLockViolationsWithOneHigherLevelLock failed");
          } catch (RuntimeException ex) {
            String message = "cannot acquire " + resource.getName() + " lock " +
                "while holding [" + higherResource.getName() + "] lock(s).";
            Assert.assertTrue(ex.getMessage(),
                ex.getMessage().contains(message));
          }
          lock.releaseLock(resourceName, higherResource);
        }
      }
    }
  }

  @Test
  public void testLockViolations() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String resourceName;

    // What this test does is iterate all resources. For each resource
    // acquire an higher level lock above the resource, and then take the the
    // lock. This should fail. Like that it tries all error combinations.
    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      Stack<ResourceInfo> stack = new Stack<>();
      List<String> currentLocks = new ArrayList<>();
      Queue<ResourceInfo> queue = new LinkedList<>();
      for (OzoneManagerLock.Resource higherResource :
          OzoneManagerLock.Resource.values()) {
        if (higherResource.getMask() > resource.getMask()) {
          resourceName = generateResourceLockName(higherResource);
          lock.acquireLock(resourceName, higherResource);
          stack.push(new ResourceInfo(resourceName, higherResource));
          currentLocks.add(higherResource.getName());
          queue.add(new ResourceInfo(resourceName, higherResource));
          // try to acquire lower level lock
          try {
            resourceName = generateResourceLockName(resource);
            lock.acquireLock(resourceName, resource);
          } catch (RuntimeException ex) {
            String message = "cannot acquire " + resource.getName() + " lock " +
                "while holding " + currentLocks.toString() + " lock(s).";
            Assert.assertTrue(ex.getMessage(),
                ex.getMessage().contains(message));
          }
        }
      }

      // Now release locks
      while (!stack.empty()) {
        ResourceInfo resourceInfo = stack.pop();
        lock.releaseLock(resourceInfo.getLockName(),
            resourceInfo.getResource());
      }
    }
  }

  @Test
  public void releaseLockWithOutAcquiringLock() {
    String userLock =
        OzoneManagerLockUtil.generateResourceLockName(
            OzoneManagerLock.Resource.USER, "user3");
    OzoneManagerLock lock =
        new OzoneManagerLock(new OzoneConfiguration());
    try {
      lock.releaseLock(userLock, OzoneManagerLock.Resource.USER);
      fail("releaseLockWithOutAcquiringLock failed");
    } catch (IllegalMonitorStateException ex) {
      String message = "Releasing lock on resource $user3 without acquiring " +
          "lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
  }


  private String generateResourceLockName(OzoneManagerLock.Resource resource) {
    if (resource == OzoneManagerLock.Resource.BUCKET) {
      return OzoneManagerLockUtil.generateBucketLockName(
          UUID.randomUUID().toString(), UUID.randomUUID().toString());
    } else {
      return OzoneManagerLockUtil.generateResourceLockName(resource,
          UUID.randomUUID().toString());
    }
  }


  /**
   * Class used to store locked resource info.
   */
  public class ResourceInfo {
    private String lockName;
    private OzoneManagerLock.Resource resource;

    ResourceInfo(String resourceName, OzoneManagerLock.Resource resource) {
      this.lockName = resourceName;
      this.resource = resource;
    }

    public String getLockName() {
      return lockName;
    }

    public OzoneManagerLock.Resource getResource() {
      return resource;
    }
  }

  @Test
  public void acquireMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String oldUserLock = OzoneManagerLockUtil.generateResourceLockName(
        OzoneManagerLock.Resource.USER, "user1");
    String newUserLock = OzoneManagerLockUtil.generateResourceLockName(
        OzoneManagerLock.Resource.USER, "user2");
    lock.acquireMultiUserLock(oldUserLock, newUserLock);
    lock.releaseMultiUserLock(oldUserLock, newUserLock);
    Assert.assertTrue(true);
  }

  @Test
  public void reAcquireMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String oldUserLock = OzoneManagerLockUtil.generateResourceLockName(
        OzoneManagerLock.Resource.USER, "user1");
    String newUserLock = OzoneManagerLockUtil.generateResourceLockName(
        OzoneManagerLock.Resource.USER, "user2");
    lock.acquireMultiUserLock(oldUserLock, newUserLock);
    try {
      lock.acquireMultiUserLock(oldUserLock, newUserLock);
      fail("reAcquireMultiUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER lock while holding [USER] lock(s).";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseMultiUserLock(oldUserLock, newUserLock);
  }

  @Test
  public void acquireMultiUserLockAfterUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String oldUserLock = OzoneManagerLockUtil.generateResourceLockName(
        OzoneManagerLock.Resource.USER, "user1");
    String newUserLock = OzoneManagerLockUtil.generateResourceLockName(
        OzoneManagerLock.Resource.USER, "user2");
    String userLock = OzoneManagerLockUtil.generateResourceLockName(
        OzoneManagerLock.Resource.USER, "user3");
    lock.acquireLock(userLock, OzoneManagerLock.Resource.USER);
    try {
      lock.acquireMultiUserLock(oldUserLock, newUserLock);
      fail("acquireMultiUserLockAfterUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER lock while holding [USER] lock(s).";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseLock(userLock, OzoneManagerLock.Resource.USER);
  }

  @Test
  public void acquireUserLockAfterMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String oldUserLock = OzoneManagerLockUtil.generateResourceLockName(
        OzoneManagerLock.Resource.USER, "user1");
    String newUserLock = OzoneManagerLockUtil.generateResourceLockName(
        OzoneManagerLock.Resource.USER, "user2");
    String userLock = OzoneManagerLockUtil.generateResourceLockName(
        OzoneManagerLock.Resource.USER, "user3");
    lock.acquireMultiUserLock(oldUserLock, newUserLock);
    try {
      lock.acquireLock(userLock, OzoneManagerLock.Resource.USER);
      fail("acquireUserLockAfterMultiUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER lock while holding [USER] lock(s).";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseMultiUserLock(oldUserLock, newUserLock);
  }

  @Test
  public void testLockResourceParallel() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    for (OzoneManagerLock.Resource resource :
        OzoneManagerLock.Resource.values()) {
      final String resourceName = generateResourceLockName(resource);
      lock.acquireLock(resourceName, resource);

      AtomicBoolean gotLock = new AtomicBoolean(false);
      new Thread(() -> {
        lock.acquireLock(resourceName, resource);
        gotLock.set(true);
        lock.releaseLock(resourceName, resource);
      }).start();
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      // Since the new thread is trying to get lock on same volume,
      // it will wait.
      Assert.assertFalse(gotLock.get());
      lock.releaseLock(resourceName, OzoneManagerLock.Resource.VOLUME);
      // Since we have released the lock, the new thread should have the lock
      // now.
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      Assert.assertTrue(gotLock.get());
    }

  }

  @Test
  public void testMultiLockResourceParallel() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    String oldUserLock = OzoneManagerLockUtil.generateResourceLockName(
        OzoneManagerLock.Resource.USER, "user1");
    String newUserLock = OzoneManagerLockUtil.generateResourceLockName(
        OzoneManagerLock.Resource.USER, "user2");

    lock.acquireMultiUserLock(oldUserLock, newUserLock);

    AtomicBoolean gotLock = new AtomicBoolean(false);
    new Thread(() -> {
      lock.acquireMultiUserLock(newUserLock, oldUserLock);
      gotLock.set(true);
      lock.releaseMultiUserLock(newUserLock, oldUserLock);
    }).start();
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    // Since the new thread is trying to get lock on same volume, it will wait.
    Assert.assertFalse(gotLock.get());
    lock.releaseMultiUserLock(oldUserLock, newUserLock);
    // Since we have released the lock, the new thread should have the lock
    // now.
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    Assert.assertTrue(gotLock.get());
  }
}
