/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.resetCounters;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_failed_to_grab_task_lost_race;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_failed_to_grab_task_owned;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_preempt_task;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_task_acquired;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_task_acquired_rescan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog.TaskState;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;



public class TestSplitLogWorker {
  private static final Log LOG = LogFactory.getLog(TestSplitLogWorker.class);
  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
  }
  private final static HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private ZooKeeperWatcher zkw;
  private SplitLogWorker slw;

  private void waitForCounter(AtomicLong ctr, long oldval, long newval,
      long timems) {
    assertTrue("ctr=" + ctr.get() + ", oldval=" + oldval + ", newval=" + newval,
      waitForCounterBoolean(ctr, oldval, newval, timems));
  }

  private boolean waitForCounterBoolean(AtomicLong ctr, long oldval, long newval,
      long timems) {
    assert ctr.get() == oldval;
    long curt = System.currentTimeMillis();
    long endt = curt + timems;
    while (curt < endt) {
      if (ctr.get() == oldval) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
        }
        curt = System.currentTimeMillis();
      } else {
        assertEquals(newval, ctr.get());
        return true;
      }
    }
    return false;
  }

  @Before
  public void setup() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "split-log-worker-tests", null);
    ZKUtil.deleteChildrenRecursively(zkw, zkw.baseZNode);
    ZKUtil.createAndFailSilent(zkw, zkw.baseZNode);
    assertTrue(ZKUtil.checkExists(zkw, zkw.baseZNode) != -1);
    LOG.debug(zkw.baseZNode + " created");
    ZKUtil.createAndFailSilent(zkw, zkw.splitLogZNode);
    assertTrue(ZKUtil.checkExists(zkw, zkw.splitLogZNode) != -1);
    LOG.debug(zkw.splitLogZNode + " created");
    resetCounters();

  }

  @After
  public void teardown() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  SplitLogWorker.TaskExecutor neverEndingTask =
    new SplitLogWorker.TaskExecutor() {

      @Override
      public Status exec(String name, CancelableProgressable p) {
        while (true) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            return Status.PREEMPTED;
          }
          if (!p.progress()) {
            return Status.PREEMPTED;
          }
        }
      }

  };

  @Test
  public void testAcquireTaskAtStartup() throws Exception {
    LOG.info("testAcquireTaskAtStartup");

    zkw.getZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "tatas"),
        TaskState.TASK_UNASSIGNED.get("mgr"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    SplitLogWorker slw = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(),
      "rs", neverEndingTask);
    slw.start();
    try {
      waitForCounter(tot_wkr_task_acquired, 0, 1, 100);
      assertTrue(TaskState.TASK_OWNED.equals(ZKUtil.getData(zkw,
        ZKSplitLog.getEncodedNodeName(zkw, "tatas")), "rs"));
    } finally {
      stopSplitLogWorker(slw);
    }
  }

  private void stopSplitLogWorker(final SplitLogWorker slw)
  throws InterruptedException {
    if (slw != null) {
      slw.stop();
      slw.worker.join(3000);
      if (slw.worker.isAlive()) {
        assertTrue(("Could not stop the worker thread slw=" + slw) == null);
      }
    }
  }

  @Test
  public void testRaceForTask() throws Exception {
    LOG.info("testRaceForTask");

    zkw.getZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "trft"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    SplitLogWorker slw1 = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(),
        "svr1", neverEndingTask);
    SplitLogWorker slw2 = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(),
        "svr2", neverEndingTask);
    slw1.start();
    slw2.start();
    try {
      waitForCounter(tot_wkr_task_acquired, 0, 1, 1000);
      boolean first =
        waitForCounterBoolean(tot_wkr_failed_to_grab_task_owned, 0, 1, 1000);
      boolean second =
        waitForCounterBoolean(tot_wkr_failed_to_grab_task_lost_race, 0, 1, 100);
      assertTrue(first || second);
      assertTrue(TaskState.TASK_OWNED.equals(ZKUtil.getData(zkw,
        ZKSplitLog.getEncodedNodeName(zkw, "trft")), "svr1") ||
        TaskState.TASK_OWNED.equals(ZKUtil.getData(zkw,
            ZKSplitLog.getEncodedNodeName(zkw, "trft")), "svr2"));
    } finally {
      stopSplitLogWorker(slw1);
      stopSplitLogWorker(slw2);
    }
  }

  @Test
  public void testPreemptTask() throws Exception {
    LOG.info("testPreemptTask");

    SplitLogWorker slw = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(),
        "tpt_svr", neverEndingTask);
    slw.start();
    try {
      Thread.yield(); // let the worker start
      Thread.sleep(100);

      // this time create a task node after starting the splitLogWorker
      zkw.getZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "tpt_task"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

      waitForCounter(tot_wkr_task_acquired, 0, 1, 1000);
      assertEquals(1, slw.taskReadySeq);
      assertTrue(TaskState.TASK_OWNED.equals(ZKUtil.getData(zkw,
        ZKSplitLog.getEncodedNodeName(zkw, "tpt_task")), "tpt_svr"));

      ZKUtil.setData(zkw, ZKSplitLog.getEncodedNodeName(zkw, "tpt_task"),
        TaskState.TASK_UNASSIGNED.get("manager"));
      waitForCounter(tot_wkr_preempt_task, 0, 1, 1000);
    } finally {
      stopSplitLogWorker(slw);
    }
  }

  @Test
  public void testMultipleTasks() throws Exception {
    LOG.info("testMultipleTasks");
    SplitLogWorker slw = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(),
        "tmt_svr", neverEndingTask);
    slw.start();
    try {
      Thread.yield(); // let the worker start
      Thread.sleep(100);

      zkw.getZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "tmt_task"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

      waitForCounter(tot_wkr_task_acquired, 0, 1, 1000);
      // now the worker is busy doing the above task

      // create another task
      zkw.getZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "tmt_task_2"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

      // preempt the first task, have it owned by another worker
      ZKUtil.setData(zkw, ZKSplitLog.getEncodedNodeName(zkw, "tmt_task"),
        TaskState.TASK_OWNED.get("another-worker"));
      waitForCounter(tot_wkr_preempt_task, 0, 1, 1000);

      waitForCounter(tot_wkr_task_acquired, 1, 2, 1000);
      assertEquals(2, slw.taskReadySeq);
      assertTrue(TaskState.TASK_OWNED.equals(ZKUtil.getData(zkw,
        ZKSplitLog.getEncodedNodeName(zkw, "tmt_task_2")), "tmt_svr"));
    } finally {
      stopSplitLogWorker(slw);
    }
  }

  @Test
  public void testRescan() throws Exception {
    LOG.info("testRescan");
    slw = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(),
        "svr", neverEndingTask);
    slw.start();
    Thread.yield(); // let the worker start
    Thread.sleep(100);

    zkw.getZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "task"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    waitForCounter(tot_wkr_task_acquired, 0, 1, 1000);
    // now the worker is busy doing the above task

    // preempt the task, have it owned by another worker
    ZKUtil.setData(zkw, ZKSplitLog.getEncodedNodeName(zkw, "task"),
        TaskState.TASK_UNASSIGNED.get("manager"));
    waitForCounter(tot_wkr_preempt_task, 0, 1, 1000);

    // create a RESCAN node
    zkw.getZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "RESCAN"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT_SEQUENTIAL);

    waitForCounter(tot_wkr_task_acquired, 1, 2, 1000);
    // RESCAN node might not have been processed if the worker became busy
    // with the above task. preempt the task again so that now the RESCAN
    // node is processed
    ZKUtil.setData(zkw, ZKSplitLog.getEncodedNodeName(zkw, "task"),
        TaskState.TASK_UNASSIGNED.get("manager"));
    waitForCounter(tot_wkr_preempt_task, 1, 2, 1000);
    waitForCounter(tot_wkr_task_acquired_rescan, 0, 1, 1000);

    List<String> nodes = ZKUtil.listChildrenNoWatch(zkw, zkw.splitLogZNode);
    LOG.debug(nodes);
    int num = 0;
    for (String node : nodes) {
      num++;
      if (node.startsWith("RESCAN")) {
        assertTrue(TaskState.TASK_DONE.equals(ZKUtil.getData(zkw,
            ZKSplitLog.getEncodedNodeName(zkw, node)), "svr"));
      }
    }
    assertEquals(2, num);
  }
}
