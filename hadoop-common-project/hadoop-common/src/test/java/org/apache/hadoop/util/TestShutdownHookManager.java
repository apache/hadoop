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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;

public class TestShutdownHookManager {
  static final Logger LOG =
      LoggerFactory.getLogger(TestShutdownHookManager.class.getName());

  @Test
  public void shutdownHookManager() {
    ShutdownHookManager mgr = ShutdownHookManager.get();
    Assert.assertNotNull(mgr);
    Assert.assertEquals(0, mgr.getShutdownHooksInOrder().size());
    Runnable hook1 = new Runnable() {
      @Override
      public void run() {
        LOG.info("Shutdown hook1 complete.");
      }
    };
    Runnable hook2 = new Runnable() {
      @Override
      public void run() {
        LOG.info("Shutdown hook2 complete.");
      }
    };

    Runnable hook3 = new Runnable() {
      @Override
      public void run() {
        try {
          sleep(3000);
          LOG.info("Shutdown hook3 complete.");
        } catch (InterruptedException ex) {
          LOG.info("Shutdown hook3 interrupted exception:",
              ExceptionUtils.getStackTrace(ex));
          Assert.fail("Hook 3 should not timeout.");
        }
      }
    };

    Runnable hook4 = new Runnable() {
      @Override
      public void run() {
        try {
          sleep(3500);
          LOG.info("Shutdown hook4 complete.");
          Assert.fail("Hook 4 should timeout");
        } catch (InterruptedException ex) {
          LOG.info("Shutdown hook4 interrupted exception:",
              ExceptionUtils.getStackTrace(ex));
        }
      }
    };

    mgr.addShutdownHook(hook1, 0);
    Assert.assertTrue(mgr.hasShutdownHook(hook1));
    Assert.assertEquals(1, mgr.getShutdownHooksInOrder().size());
    Assert.assertEquals(hook1, mgr.getShutdownHooksInOrder().get(0).getHook());
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
    Assert.assertEquals(hook2, mgr.getShutdownHooksInOrder().get(0).getHook());
    Assert.assertEquals(hook1, mgr.getShutdownHooksInOrder().get(1).getHook());

    // Test hook finish without timeout
    mgr.addShutdownHook(hook3, 2, 4, TimeUnit.SECONDS);
    Assert.assertTrue(mgr.hasShutdownHook(hook3));
    Assert.assertEquals(hook3, mgr.getShutdownHooksInOrder().get(0).getHook());
    Assert.assertEquals(4, mgr.getShutdownHooksInOrder().get(0).getTimeout());

    // Test hook finish with timeout
    mgr.addShutdownHook(hook4, 3, 2, TimeUnit.SECONDS);
    Assert.assertTrue(mgr.hasShutdownHook(hook4));
    Assert.assertEquals(hook4, mgr.getShutdownHooksInOrder().get(0).getHook());
    Assert.assertEquals(2, mgr.getShutdownHooksInOrder().get(0).getTimeout());
    LOG.info("Shutdown starts here");
  }
}
