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
package org.apache.hadoop.yarn.sls.scheduler;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestTaskRunner {
  private TaskRunner runner;

  @Before
  public void setUp() {
    runner = new TaskRunner();
    runner.setQueueSize(5);
  }

  @After
  public void cleanUp() throws InterruptedException {
    runner.stop();
  }

  public static class SingleTask extends TaskRunner.Task {
    public static CountDownLatch latch = new CountDownLatch(1);
    public static boolean first;

    public SingleTask(long startTime) {
      super.init(startTime);
    }

    @Override
    public void firstStep() {
      if (first) {
        Assert.fail();
      }
      first = true;
      latch.countDown();
    }

    @Override
    public void middleStep() {
      Assert.fail();
    }

    @Override
    public void lastStep() {
      Assert.fail();
    }
  }

  @Test
  public void testSingleTask() throws Exception {
    runner.start();
    runner.schedule(new SingleTask(0));
    SingleTask.latch.await(5000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(SingleTask.first);
  }

  public static class DualTask extends TaskRunner.Task {
    public static CountDownLatch latch = new CountDownLatch(1);
    public static boolean first;
    public static boolean last;

    public DualTask(long startTime, long endTime, long interval) {
      super.init(startTime, endTime, interval);
    }

    @Override
    public void firstStep() {
      if (first) {
        Assert.fail();
      }
      first = true;
    }

    @Override
    public void middleStep() {
      Assert.fail();
    }

    @Override
    public void lastStep() {
      if (last) {
        Assert.fail();
      }
      last = true;
      latch.countDown();
    }
  }

  @Test
  public void testDualTask() throws Exception {
    runner.start();
    runner.schedule(new DualTask(0, 10, 10));
    DualTask.latch.await(5000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(DualTask.first);
    Assert.assertTrue(DualTask.last);
  }

  public static class TriTask extends TaskRunner.Task {
    public static CountDownLatch latch = new CountDownLatch(1);
    public static boolean first;
    public static boolean middle;
    public static boolean last;

    public TriTask(long startTime, long endTime, long interval) {
      super.init(startTime, endTime, interval);
    }

    @Override
    public void firstStep() {
      if (first) {
        Assert.fail();
      }
      first = true;
    }

    @Override
    public void middleStep() {
      if (middle) {
        Assert.fail();
      }
      middle = true;
    }

    @Override
    public void lastStep() {
      if (last) {
        Assert.fail();
      }
      last = true;
      latch.countDown();
    }
  }

  @Test
  public void testTriTask() throws Exception {
    runner.start();
    runner.schedule(new TriTask(0, 10, 5));
    TriTask.latch.await(5000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(TriTask.first);
    Assert.assertTrue(TriTask.middle);
    Assert.assertTrue(TriTask.last);
  }

  public static class MultiTask extends TaskRunner.Task {
    public static CountDownLatch latch = new CountDownLatch(1);
    public static boolean first;
    public static int middle;
    public static boolean last;

    public MultiTask(long startTime, long endTime, long interval) {
      super.init(startTime, endTime, interval);
    }

    @Override
    public void firstStep() {
      if (first) {
        Assert.fail();
      }
      first = true;
    }

    @Override
    public void middleStep() {
      middle++;
    }

    @Override
    public void lastStep() {
      if (last) {
        Assert.fail();
      }
      last = true;
      latch.countDown();
    }
  }

  @Test
  public void testMultiTask() throws Exception {
    runner.start();
    runner.schedule(new MultiTask(0, 20, 5));
    MultiTask.latch.await(5000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(MultiTask.first);
    Assert.assertEquals((20 - 0) / 5 - 2 + 1, MultiTask.middle);
    Assert.assertTrue(MultiTask.last);
  }


  public static class PreStartTask extends TaskRunner.Task {
    public static CountDownLatch latch = new CountDownLatch(1);
    public static boolean first;

    public PreStartTask(long startTime) {
      super.init(startTime);
    }

    @Override
    public void firstStep() {
      if (first) {
        Assert.fail();
      }
      first = true;
      latch.countDown();
    }

    @Override
    public void middleStep() {
    }

    @Override
    public void lastStep() {
    }
  }

  @Test
  public void testPreStartQueueing() throws Exception {
    runner.schedule(new PreStartTask(210));
    Thread.sleep(210);
    runner.start();
    long startedAt = System.currentTimeMillis();
    PreStartTask.latch.await(5000, TimeUnit.MILLISECONDS);
    long runAt = System.currentTimeMillis();
    Assert.assertTrue(PreStartTask.first);
    Assert.assertTrue(runAt - startedAt >= 200);
  }

}
