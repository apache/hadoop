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

package org.apache.hadoop.ozone.lock;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test-cases to test LockManager.
 */
public class TestLockManager {

  @Test(timeout = 1000)
  public void testWithDifferentResource() {
    LockManager<String> manager = new LockManager<>(new OzoneConfiguration());
    manager.lock("/resourceOne");
    // This should work, as they are different resource.
    manager.lock("/resourceTwo");
    manager.unlock("/resourceOne");
    manager.unlock("/resourceTwo");
    Assert.assertTrue(true);
  }

  @Test
  public void testWithSameResource() throws Exception {
    LockManager<String> manager = new LockManager<>(new OzoneConfiguration());
    manager.lock("/resourceOne");
    AtomicBoolean gotLock = new AtomicBoolean(false);
    new Thread(() -> {
      manager.lock("/resourceOne");
      gotLock.set(true);
      manager.unlock("/resourceOne");
    }).start();
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    // Since the new thread is trying to get lock on same object, it will wait.
    Assert.assertFalse(gotLock.get());
    manager.unlock("/resourceOne");
    // Since we have released the lock, the new thread should have the lock
    // now
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    Assert.assertTrue(gotLock.get());
  }

}