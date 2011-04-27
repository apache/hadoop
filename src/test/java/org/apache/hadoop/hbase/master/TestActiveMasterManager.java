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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the {@link ActiveMasterManager}.
 */
public class TestActiveMasterManager {
  private final static Log LOG = LogFactory.getLog(TestActiveMasterManager.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test public void testRestartMaster() throws IOException, KeeperException {
    ZooKeeperWatcher zk = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
      "testActiveMasterManagerFromZK", null);
    ZKUtil.createAndFailSilent(zk, zk.baseZNode);
    try {
      ZKUtil.deleteNode(zk, zk.masterAddressZNode);
    } catch(KeeperException.NoNodeException nne) {}

    // Create the master node with a dummy address
    ServerName master = new ServerName("localhost", 1, System.currentTimeMillis());
    // Should not have a master yet
    DummyMaster dummyMaster = new DummyMaster();
    ActiveMasterManager activeMasterManager = new ActiveMasterManager(zk,
      master, dummyMaster);
    zk.registerListener(activeMasterManager);
    assertFalse(activeMasterManager.clusterHasActiveMaster.get());

    // First test becoming the active master uninterrupted
    activeMasterManager.blockUntilBecomingActiveMaster();
    assertTrue(activeMasterManager.clusterHasActiveMaster.get());
    assertMaster(zk, master);

    // Now pretend master restart
    DummyMaster secondDummyMaster = new DummyMaster();
    ActiveMasterManager secondActiveMasterManager = new ActiveMasterManager(zk,
      master, secondDummyMaster);
    zk.registerListener(secondActiveMasterManager);
    assertFalse(secondActiveMasterManager.clusterHasActiveMaster.get());
    activeMasterManager.blockUntilBecomingActiveMaster();
    assertTrue(activeMasterManager.clusterHasActiveMaster.get());
    assertMaster(zk, master);
  }

  /**
   * Unit tests that uses ZooKeeper but does not use the master-side methods
   * but rather acts directly on ZK.
   * @throws Exception
   */
  @Test
  public void testActiveMasterManagerFromZK() throws Exception {
    ZooKeeperWatcher zk = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
      "testActiveMasterManagerFromZK", null);
    ZKUtil.createAndFailSilent(zk, zk.baseZNode);
    try {
      ZKUtil.deleteNode(zk, zk.masterAddressZNode);
    } catch(KeeperException.NoNodeException nne) {}

    // Create the master node with a dummy address
    ServerName firstMasterAddress =
      new ServerName("localhost", 1, System.currentTimeMillis());
    ServerName secondMasterAddress =
      new ServerName("localhost", 2, System.currentTimeMillis());

    // Should not have a master yet
    DummyMaster ms1 = new DummyMaster();
    ActiveMasterManager activeMasterManager = new ActiveMasterManager(zk,
      firstMasterAddress, ms1);
    zk.registerListener(activeMasterManager);
    assertFalse(activeMasterManager.clusterHasActiveMaster.get());

    // First test becoming the active master uninterrupted
    activeMasterManager.blockUntilBecomingActiveMaster();
    assertTrue(activeMasterManager.clusterHasActiveMaster.get());
    assertMaster(zk, firstMasterAddress);

    // New manager will now try to become the active master in another thread
    WaitToBeMasterThread t = new WaitToBeMasterThread(zk, secondMasterAddress);
    zk.registerListener(t.manager);
    t.start();
    // Wait for this guy to figure out there is another active master
    // Wait for 1 second at most
    int sleeps = 0;
    while(!t.manager.clusterHasActiveMaster.get() && sleeps < 100) {
      Thread.sleep(10);
      sleeps++;
    }

    // Both should see that there is an active master
    assertTrue(activeMasterManager.clusterHasActiveMaster.get());
    assertTrue(t.manager.clusterHasActiveMaster.get());
    // But secondary one should not be the active master
    assertFalse(t.isActiveMaster);

    // Close the first server and delete it's master node
    ms1.stop("stopping first server");

    // Use a listener to capture when the node is actually deleted
    NodeDeletionListener listener = new NodeDeletionListener(zk, zk.masterAddressZNode);
    zk.registerListener(listener);

    LOG.info("Deleting master node");
    ZKUtil.deleteNode(zk, zk.masterAddressZNode);

    // Wait for the node to be deleted
    LOG.info("Waiting for active master manager to be notified");
    listener.waitForDeletion();
    LOG.info("Master node deleted");

    // Now we expect the secondary manager to have and be the active master
    // Wait for 1 second at most
    sleeps = 0;
    while(!t.isActiveMaster && sleeps < 100) {
      Thread.sleep(10);
      sleeps++;
    }
    LOG.debug("Slept " + sleeps + " times");

    assertTrue(t.manager.clusterHasActiveMaster.get());
    assertTrue(t.isActiveMaster);

    LOG.info("Deleting master node");
    ZKUtil.deleteNode(zk, zk.masterAddressZNode);
  }

  /**
   * Assert there is an active master and that it has the specified address.
   * @param zk
   * @param thisMasterAddress
   * @throws KeeperException
   */
  private void assertMaster(ZooKeeperWatcher zk,
      ServerName expectedAddress)
  throws KeeperException {
    ServerName readAddress =
      new ServerName(Bytes.toString(ZKUtil.getData(zk, zk.masterAddressZNode)));
    assertNotNull(readAddress);
    assertTrue(expectedAddress.equals(readAddress));
  }

  public static class WaitToBeMasterThread extends Thread {

    ActiveMasterManager manager;
    boolean isActiveMaster;

    public WaitToBeMasterThread(ZooKeeperWatcher zk, ServerName address) {
      this.manager = new ActiveMasterManager(zk, address,
          new DummyMaster());
      isActiveMaster = false;
    }

    @Override
    public void run() {
      manager.blockUntilBecomingActiveMaster();
      LOG.info("Second master has become the active master!");
      isActiveMaster = true;
    }
  }

  public static class NodeDeletionListener extends ZooKeeperListener {
    private static final Log LOG = LogFactory.getLog(NodeDeletionListener.class);

    private Semaphore lock;
    private String node;

    public NodeDeletionListener(ZooKeeperWatcher watcher, String node) {
      super(watcher);
      lock = new Semaphore(0);
      this.node = node;
    }

    @Override
    public void nodeDeleted(String path) {
      if(path.equals(node)) {
        LOG.debug("nodeDeleted(" + path + ")");
        lock.release();
      }
    }

    public void waitForDeletion() throws InterruptedException {
      lock.acquire();
    }
  }

  /**
   * Dummy Master Implementation.
   */
  public static class DummyMaster implements Server {
    private volatile boolean stopped;

    @Override
    public void abort(final String msg, final Throwable t) {}

    @Override
    public Configuration getConfiguration() {
      return null;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return null;
    }

    @Override
    public boolean isStopped() {
      return this.stopped;
    }

    @Override
    public void stop(String why) {
      this.stopped = true;
    }

    @Override
    public CatalogTracker getCatalogTracker() {
      return null;
    }
  }
}
