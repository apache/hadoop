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
package org.apache.hadoop.util;

import org.junit.Assert;
import org.junit.Test;

public class TestShutdownHookManager {

  @Test
  public void shutdownHookManager() {
    ShutdownHookManager mgr = ShutdownHookManager.get();
    Assert.assertNotNull(mgr);
    Assert.assertEquals(0, mgr.getShutdownHooksInOrder().size());
    Runnable hook1 = new Runnable() {
      @Override
      public void run() {
      }
    };
    Runnable hook2 = new Runnable() {
      @Override
      public void run() {
      }
    };

    mgr.addShutdownHook(hook1, 0);
    Assert.assertTrue(mgr.hasShutdownHook(hook1));
    Assert.assertEquals(1, mgr.getShutdownHooksInOrder().size());
    Assert.assertEquals(hook1, mgr.getShutdownHooksInOrder().get(0));
    mgr.removeShutdownHook(hook1);
    Assert.assertFalse(mgr.hasShutdownHook(hook1));

    mgr.addShutdownHook(hook1, 0);
    Assert.assertTrue(mgr.hasShutdownHook(hook1));
    Assert.assertEquals(1, mgr.getShutdownHooksInOrder().size());
    Assert.assertTrue(mgr.hasShutdownHook(hook1));
    Assert.assertEquals(1, mgr.getShutdownHooksInOrder().size());

    mgr.addShutdownHook(hook2, 1);
    Assert.assertTrue(mgr.hasShutdownHook(hook1));
    Assert.assertTrue(mgr.hasShutdownHook(hook2));
    Assert.assertEquals(2, mgr.getShutdownHooksInOrder().size());
    Assert.assertEquals(hook2, mgr.getShutdownHooksInOrder().get(0));
    Assert.assertEquals(hook1, mgr.getShutdownHooksInOrder().get(1));

  }
}
