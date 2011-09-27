/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.master.TestActiveMasterManager.NodeDeletionListener;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestZooKeeperNodeTracker {
  private static final Log LOG = LogFactory.getLog(TestZooKeeperNodeTracker.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final static Random rand = new Random();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  /**
   * Test that we can interrupt a node that is blocked on a wait.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test public void testInterruptible() throws IOException, InterruptedException {
    Abortable abortable = new StubAbortable();
    ZooKeeperWatcher zk = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
      "testInterruptible", abortable);
    final TestTracker tracker = new TestTracker(zk, "/xyz", abortable);
    tracker.start();
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          tracker.blockUntilAvailable();
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted", e);
        }
      }
    };
    t.start();
    while (!t.isAlive()) Threads.sleep(1);
    tracker.stop();
    t.join();
    // If it wasn't interruptible, we'd never get to here.
  }

  @Test
  public void testNodeTracker() throws Exception {
    Abortable abortable = new StubAbortable();
    ZooKeeperWatcher zk = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "testNodeTracker", abortable);
    ZKUtil.createAndFailSilent(zk, zk.baseZNode);

    final String node =
      ZKUtil.joinZNode(zk.baseZNode, new Long(rand.nextLong()).toString());

    final byte [] dataOne = Bytes.toBytes("dataOne");
    final byte [] dataTwo = Bytes.toBytes("dataTwo");

    // Start a ZKNT with no node currently available
    TestTracker localTracker = new TestTracker(zk, node, abortable);
    localTracker.start();
    zk.registerListener(localTracker);

    // Make sure we don't have a node
    assertNull(localTracker.getData(false));

    // Spin up a thread with another ZKNT and have it block
    WaitToGetDataThread thread = new WaitToGetDataThread(zk, node);
    thread.start();

    // Verify the thread doesn't have a node
    assertFalse(thread.hasData);

    // Now, start a new ZKNT with the node already available
    TestTracker secondTracker = new TestTracker(zk, node, null);
    secondTracker.start();
    zk.registerListener(secondTracker);

    // Put up an additional zk listener so we know when zk event is done
    TestingZKListener zkListener = new TestingZKListener(zk, node);
    zk.registerListener(zkListener);
    assertEquals(0, zkListener.createdLock.availablePermits());

    // Create a completely separate zk connection for test triggers and avoid
    // any weird watcher interactions from the test
    final ZooKeeper zkconn = new ZooKeeper(
        ZKConfig.getZKQuorumServersString(TEST_UTIL.getConfiguration()), 60000,
        new StubWatcher());

    // Add the node with data one
    zkconn.create(node, dataOne, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    // Wait for the zk event to be processed
    zkListener.waitForCreation();
    thread.join();

    // Both trackers should have the node available with data one
    assertNotNull(localTracker.getData(false));
    assertNotNull(localTracker.blockUntilAvailable());
    assertTrue(Bytes.equals(localTracker.getData(false), dataOne));
    assertTrue(thread.hasData);
    assertTrue(Bytes.equals(thread.tracker.getData(false), dataOne));
    LOG.info("Successfully got data one");

    // Make sure it's available and with the expected data
    assertNotNull(secondTracker.getData(false));
    assertNotNull(secondTracker.blockUntilAvailable());
    assertTrue(Bytes.equals(secondTracker.getData(false), dataOne));
    LOG.info("Successfully got data one with the second tracker");

    // Drop the node
    zkconn.delete(node, -1);
    zkListener.waitForDeletion();

    // Create a new thread but with the existing thread's tracker to wait
    TestTracker threadTracker = thread.tracker;
    thread = new WaitToGetDataThread(zk, node, threadTracker);
    thread.start();

    // Verify other guys don't have data
    assertFalse(thread.hasData);
    assertNull(secondTracker.getData(false));
    assertNull(localTracker.getData(false));
    LOG.info("Successfully made unavailable");

    // Create with second data
    zkconn.create(node, dataTwo, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    // Wait for the zk event to be processed
    zkListener.waitForCreation();
    thread.join();

    // All trackers should have the node available with data two
    assertNotNull(localTracker.getData(false));
    assertNotNull(localTracker.blockUntilAvailable());
    assertTrue(Bytes.equals(localTracker.getData(false), dataTwo));
    assertNotNull(secondTracker.getData(false));
    assertNotNull(secondTracker.blockUntilAvailable());
    assertTrue(Bytes.equals(secondTracker.getData(false), dataTwo));
    assertTrue(thread.hasData);
    assertTrue(Bytes.equals(thread.tracker.getData(false), dataTwo));
    LOG.info("Successfully got data two on all trackers and threads");

    // Change the data back to data one
    zkconn.setData(node, dataOne, -1);

    // Wait for zk event to be processed
    zkListener.waitForDataChange();

    // All trackers should have the node available with data one
    assertNotNull(localTracker.getData(false));
    assertNotNull(localTracker.blockUntilAvailable());
    assertTrue(Bytes.equals(localTracker.getData(false), dataOne));
    assertNotNull(secondTracker.getData(false));
    assertNotNull(secondTracker.blockUntilAvailable());
    assertTrue(Bytes.equals(secondTracker.getData(false), dataOne));
    assertTrue(thread.hasData);
    assertTrue(Bytes.equals(thread.tracker.getData(false), dataOne));
    LOG.info("Successfully got data one following a data change on all trackers and threads");
  }

  public static class WaitToGetDataThread extends Thread {

    TestTracker tracker;
    boolean hasData;

    public WaitToGetDataThread(ZooKeeperWatcher zk, String node) {
      tracker = new TestTracker(zk, node, null);
      tracker.start();
      zk.registerListener(tracker);
      hasData = false;
    }

    public WaitToGetDataThread(ZooKeeperWatcher zk, String node,
        TestTracker tracker) {
      this.tracker = tracker;
      hasData = false;
    }

    @Override
    public void run() {
      LOG.info("Waiting for data to be available in WaitToGetDataThread");
      try {
        tracker.blockUntilAvailable();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      LOG.info("Data now available in tracker from WaitToGetDataThread");
      hasData = true;
    }
  }

  public static class TestTracker extends ZooKeeperNodeTracker {
    public TestTracker(ZooKeeperWatcher watcher, String node,
        Abortable abortable) {
      super(watcher, node, abortable);
    }
  }

  public static class TestingZKListener extends ZooKeeperListener {
    private static final Log LOG = LogFactory.getLog(NodeDeletionListener.class);

    private Semaphore deletedLock;
    private Semaphore createdLock;
    private Semaphore changedLock;
    private String node;

    public TestingZKListener(ZooKeeperWatcher watcher, String node) {
      super(watcher);
      deletedLock = new Semaphore(0);
      createdLock = new Semaphore(0);
      changedLock = new Semaphore(0);
      this.node = node;
    }

    @Override
    public void nodeDeleted(String path) {
      if(path.equals(node)) {
        LOG.debug("nodeDeleted(" + path + ")");
        deletedLock.release();
      }
    }

    @Override
    public void nodeCreated(String path) {
      if(path.equals(node)) {
        LOG.debug("nodeCreated(" + path + ")");
        createdLock.release();
      }
    }

    @Override
    public void nodeDataChanged(String path) {
      if(path.equals(node)) {
        LOG.debug("nodeDataChanged(" + path + ")");
        changedLock.release();
      }
    }

    public void waitForDeletion() throws InterruptedException {
      deletedLock.acquire();
    }

    public void waitForCreation() throws InterruptedException {
      createdLock.acquire();
    }

    public void waitForDataChange() throws InterruptedException {
      changedLock.acquire();
    }
  }

  public static class StubAbortable implements Abortable {
    @Override
    public void abort(final String msg, final Throwable t) {}
    
    @Override
    public boolean isAborted() {
      return false;
    }
    
  }

  public static class StubWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {}
  }
}
