/*
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

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 */
public class TestZKLeaderManager {
  private static Log LOG = LogFactory.getLog(TestZKLeaderManager.class);

  private static final String LEADER_ZNODE =
      "/test/" + TestZKLeaderManager.class.getSimpleName();

  private static class MockAbortable implements Abortable {
    private boolean aborted;

    @Override
    public void abort(String why, Throwable e) {
      aborted = true;
      LOG.fatal("Aborting during test: "+why, e);
      fail("Aborted during test: " + why);
    }

    @Override
    public boolean isAborted() {
      return aborted;
    }
  }

  private static class MockLeader extends Thread implements Stoppable {
    private boolean stopped;
    private ZooKeeperWatcher watcher;
    private ZKLeaderManager zkLeader;
    private AtomicBoolean master = new AtomicBoolean(false);
    private int index;

    public MockLeader(ZooKeeperWatcher watcher, int index) {
      setDaemon(true);
      setName("TestZKLeaderManager-leader-" + index);
      this.index = index;
      this.watcher = watcher;
      this.zkLeader = new ZKLeaderManager(watcher, LEADER_ZNODE,
          Bytes.toBytes(index), this);
    }

    public boolean isMaster() {
      return master.get();
    }

    public int getIndex() {
      return index;
    }

    public ZooKeeperWatcher getWatcher() {
      return watcher;
    }

    public void run() {
      while (!stopped) {
        zkLeader.start();
        zkLeader.waitToBecomeLeader();
        master.set(true);

        while (master.get() && !stopped) {
          try {
            Thread.sleep(200);
          } catch (InterruptedException ignored) {}
        }
      }
    }

    public void abdicate() {
      zkLeader.stepDownAsLeader();
      master.set(false);
    }

    @Override
    public void stop(String why) {
      stopped = true;
      abdicate();
      watcher.close();
    }

    @Override
    public boolean isStopped() {
      return stopped;
    }
  }

  private static HBaseTestingUtility TEST_UTIL;
  private static MockLeader[] CANDIDATES;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniZKCluster();
    Configuration conf = TEST_UTIL.getConfiguration();

    // use an abortable to fail the test in the case of any KeeperExceptions
    MockAbortable abortable = new MockAbortable();
    CANDIDATES = new MockLeader[3];
    for (int i = 0; i < 3; i++) {
      ZooKeeperWatcher watcher = newZK(conf, "server"+i, abortable);
      CANDIDATES[i] = new MockLeader(watcher, i);
      CANDIDATES[i].start();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testLeaderSelection() throws Exception {
    MockLeader currentLeader = getCurrentLeader();
    // one leader should have been found
    assertNotNull("Leader should exist", currentLeader);
    LOG.debug("Current leader index is "+currentLeader.getIndex());

    byte[] znodeData = ZKUtil.getData(CANDIDATES[0].getWatcher(), LEADER_ZNODE);
    assertNotNull("Leader znode should contain leader index", znodeData);
    assertTrue("Leader znode should not be empty", znodeData.length > 0);
    int storedIndex = Bytes.toInt(znodeData);
    LOG.debug("Stored leader index in ZK is "+storedIndex);
    assertEquals("Leader znode should match leader index",
        currentLeader.getIndex(), storedIndex);

    // force a leader transition
    currentLeader.abdicate();
    assertFalse(currentLeader.isMaster());

    // check for new leader
    currentLeader = getCurrentLeader();
    // one leader should have been found
    assertNotNull("New leader should exist after abdication", currentLeader);
    LOG.debug("New leader index is "+currentLeader.getIndex());

    znodeData = ZKUtil.getData(CANDIDATES[0].getWatcher(), LEADER_ZNODE);
    assertNotNull("Leader znode should contain leader index", znodeData);
    assertTrue("Leader znode should not be empty", znodeData.length > 0);
    storedIndex = Bytes.toInt(znodeData);
    LOG.debug("Stored leader index in ZK is "+storedIndex);
    assertEquals("Leader znode should match leader index",
        currentLeader.getIndex(), storedIndex);

    // force another transition by stopping the current
    currentLeader.stop("Stopping for test");
    assertFalse(currentLeader.isMaster());

    // check for new leader
    currentLeader = getCurrentLeader();
    // one leader should have been found
    assertNotNull("New leader should exist after stop", currentLeader);
    LOG.debug("New leader index is "+currentLeader.getIndex());

    znodeData = ZKUtil.getData(CANDIDATES[0].getWatcher(), LEADER_ZNODE);
    assertNotNull("Leader znode should contain leader index", znodeData);
    assertTrue("Leader znode should not be empty", znodeData.length > 0);
    storedIndex = Bytes.toInt(znodeData);
    LOG.debug("Stored leader index in ZK is "+storedIndex);
    assertEquals("Leader znode should match leader index",
        currentLeader.getIndex(), storedIndex);

    // with a second stop we can guarantee that a previous leader has resumed leading
    currentLeader.stop("Stopping for test");
    assertFalse(currentLeader.isMaster());

    // check for new
    currentLeader = getCurrentLeader();
    assertNotNull("New leader should exist", currentLeader);
  }

  private MockLeader getCurrentLeader() throws Exception {
    MockLeader currentLeader = null;
    outer:
    // wait up to 2 secs for initial leader
    for (int i = 0; i < 20; i++) {
      for (int j = 0; j < CANDIDATES.length; j++) {
        if (CANDIDATES[j].isMaster()) {
          // should only be one leader
          if (currentLeader != null) {
            fail("Both candidate "+currentLeader.getIndex()+" and "+j+" claim to be leader!");
          }
          currentLeader = CANDIDATES[j];
        }
      }
      if (currentLeader != null) {
        break outer;
      }
      Thread.sleep(100);
    }
    return currentLeader;
  }

  private static ZooKeeperWatcher newZK(Configuration conf, String name,
      Abortable abort) throws Exception {
    Configuration copy = HBaseConfiguration.create(conf);
    ZooKeeperWatcher zk = new ZooKeeperWatcher(copy, name, abort);
    return zk;
  }
}
