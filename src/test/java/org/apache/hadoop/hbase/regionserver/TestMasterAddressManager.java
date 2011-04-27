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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MasterAddressTracker;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMasterAddressManager {
  private static final Log LOG = LogFactory.getLog(TestMasterAddressManager.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }
  /**
   * Unit tests that uses ZooKeeper but does not use the master-side methods
   * but rather acts directly on ZK.
   * @throws Exception
   */
  @Test
  public void testMasterAddressManagerFromZK() throws Exception {

    ZooKeeperWatcher zk = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "testMasterAddressManagerFromZK", null);
    ZKUtil.createAndFailSilent(zk, zk.baseZNode);

    // Should not have a master yet
    MasterAddressTracker addressManager = new MasterAddressTracker(zk, null);
    addressManager.start();
    assertFalse(addressManager.hasMaster());
    zk.registerListener(addressManager);

    // Use a listener to capture when the node is actually created
    NodeCreationListener listener = new NodeCreationListener(zk, zk.masterAddressZNode);
    zk.registerListener(listener);

    // Create the master node with a dummy address
    String host = "localhost";
    int port = 1234;
    ServerName sn = new ServerName(host, port, System.currentTimeMillis());
    LOG.info("Creating master node");
    ZKUtil.createEphemeralNodeAndWatch(zk, zk.masterAddressZNode, sn.getBytes());

    // Wait for the node to be created
    LOG.info("Waiting for master address manager to be notified");
    listener.waitForCreation();
    LOG.info("Master node created");
    assertTrue(addressManager.hasMaster());
    ServerName pulledAddress = addressManager.getMasterAddress();
    assertTrue(pulledAddress.equals(sn));

  }

  public static class NodeCreationListener extends ZooKeeperListener {
    private static final Log LOG = LogFactory.getLog(NodeCreationListener.class);

    private Semaphore lock;
    private String node;

    public NodeCreationListener(ZooKeeperWatcher watcher, String node) {
      super(watcher);
      lock = new Semaphore(0);
      this.node = node;
    }

    @Override
    public void nodeCreated(String path) {
      if(path.equals(node)) {
        LOG.debug("nodeCreated(" + path + ")");
        lock.release();
      }
    }

    public void waitForCreation() throws InterruptedException {
      lock.acquire();
    }
  }
}
