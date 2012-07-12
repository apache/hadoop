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
package org.apache.hadoop.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.test.MultithreadedTestUtil.TestingThread;
import org.apache.hadoop.test.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.util.Time;

public class TestMultithreadedTestUtil {

  private static final String FAIL_MSG =
    "Inner thread fails an assert";

  @Test
  public void testNoErrors() throws Exception {
    final AtomicInteger threadsRun = new AtomicInteger();

    TestContext ctx = new TestContext();
    for (int i = 0; i < 3; i++) {
      ctx.addThread(new TestingThread(ctx) {
        @Override
        public void doWork() throws Exception {
          threadsRun.incrementAndGet();
        }
      });
    }
    assertEquals(0, threadsRun.get());
    ctx.startThreads();
    long st = Time.now();
    ctx.waitFor(30000);
    long et = Time.now();

    // All threads should have run
    assertEquals(3, threadsRun.get());
    // Test shouldn't have waited the full 30 seconds, since
    // the threads exited faster than that.
    assertTrue("Test took " + (et - st) + "ms",
        et - st < 5000);
  }

  @Test
  public void testThreadFails() throws Exception {
    TestContext ctx = new TestContext();
    ctx.addThread(new TestingThread(ctx) {
      @Override
      public void doWork() throws Exception {
        fail(FAIL_MSG);
      }
    });
    ctx.startThreads();
    long st = Time.now();
    try {
      ctx.waitFor(30000);
      fail("waitFor did not throw");
    } catch (RuntimeException rte) {
      // expected
      assertEquals(FAIL_MSG, rte.getCause().getMessage());
    }
    long et = Time.now();
    // Test shouldn't have waited the full 30 seconds, since
    // the thread throws faster than that
    assertTrue("Test took " + (et - st) + "ms",
        et - st < 5000);
  }

  @Test
  public void testThreadThrowsCheckedException() throws Exception {
    TestContext ctx = new TestContext();
    ctx.addThread(new TestingThread(ctx) {
      @Override
      public void doWork() throws Exception {
        throw new IOException("my ioe");
      }
    });
    ctx.startThreads();
    long st = Time.now();
    try {
      ctx.waitFor(30000);
      fail("waitFor did not throw");
    } catch (RuntimeException rte) {
      // expected
      assertEquals("my ioe", rte.getCause().getMessage());
    }
    long et = Time.now();
    // Test shouldn't have waited the full 30 seconds, since
    // the thread throws faster than that
    assertTrue("Test took " + (et - st) + "ms",
        et - st < 5000);
  }

  @Test
  public void testRepeatingThread() throws Exception {
    final AtomicInteger counter = new AtomicInteger();

    TestContext ctx = new TestContext();
    ctx.addThread(new RepeatingTestThread(ctx) {
      @Override
      public void doAnAction() throws Exception {
        counter.incrementAndGet();
      }
    });
    ctx.startThreads();
    long st = Time.now();
    ctx.waitFor(3000);
    ctx.stop();
    long et = Time.now();
    long elapsed = et - st;

    // Test should have waited just about 3 seconds
    assertTrue("Test took " + (et - st) + "ms",
        Math.abs(elapsed - 3000) < 500);
    // Counter should have been incremented lots of times in 3 full seconds
    assertTrue("Counter value = " + counter.get(),
        counter.get() > 1000);
  }

}
