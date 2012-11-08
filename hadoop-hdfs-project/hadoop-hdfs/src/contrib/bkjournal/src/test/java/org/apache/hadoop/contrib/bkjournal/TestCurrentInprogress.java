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
package org.apache.hadoop.contrib.bkjournal;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that read, update, clear api from CurrentInprogress
 */
public class TestCurrentInprogress {
  private static final Log LOG = LogFactory.getLog(TestCurrentInprogress.class);
  private static final String CURRENT_NODE_PATH = "/test";
  private static final String HOSTPORT = "127.0.0.1:2181";
  private static final int CONNECTION_TIMEOUT = 30000;
  private static NIOServerCnxnFactory serverFactory;
  private static ZooKeeperServer zks;
  private static ZooKeeper zkc;
  private static int ZooKeeperDefaultPort = 2181;
  private static File zkTmpDir;

  private static ZooKeeper connectZooKeeper(String ensemble)
      throws IOException, KeeperException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);

    ZooKeeper zkc = new ZooKeeper(HOSTPORT, 3600, new Watcher() {
      public void process(WatchedEvent event) {
        if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
          latch.countDown();
        }
      }
    });
    if (!latch.await(10, TimeUnit.SECONDS)) {
      throw new IOException("Zookeeper took too long to connect");
    }
    return zkc;
  }

  @BeforeClass
  public static void setupZooKeeper() throws Exception {
    LOG.info("Starting ZK server");
    zkTmpDir = File.createTempFile("zookeeper", "test");
    zkTmpDir.delete();
    zkTmpDir.mkdir();
    try {
      zks = new ZooKeeperServer(zkTmpDir, zkTmpDir, ZooKeeperDefaultPort);
      serverFactory = new NIOServerCnxnFactory();
      serverFactory.configure(new InetSocketAddress(ZooKeeperDefaultPort), 10);
      serverFactory.startup(zks);
    } catch (Exception e) {
      LOG.error("Exception while instantiating ZooKeeper", e);
    }
    boolean b = LocalBookKeeper.waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT);
    LOG.debug("ZooKeeper server up: " + b);
  }

  @AfterClass
  public static void shutDownServer() {
    if (null != zks) {
      zks.shutdown();
    }
    zkTmpDir.delete();
  }

  @Before
  public void setup() throws Exception {
    zkc = connectZooKeeper(HOSTPORT);
  }

  @After
  public void teardown() throws Exception {
    if (null != zkc) {
      zkc.close();
    }

  }

  /**
   * Tests that read should be able to read the data which updated with update
   * api
   */
  @Test
  public void testReadShouldReturnTheZnodePathAfterUpdate() throws Exception {
    String data = "inprogressNode";
    CurrentInprogress ci = new CurrentInprogress(zkc, CURRENT_NODE_PATH);
    ci.init();
    ci.update(data);
    String inprogressNodePath = ci.read();
    assertEquals("Not returning inprogressZnode", "inprogressNode",
        inprogressNodePath);
  }

  /**
   * Tests that read should return null if we clear the updated data in
   * CurrentInprogress node
   */
  @Test
  public void testReadShouldReturnNullAfterClear() throws Exception {
    CurrentInprogress ci = new CurrentInprogress(zkc, CURRENT_NODE_PATH);
    ci.init();
    ci.update("myInprogressZnode");
    ci.read();
    ci.clear();
    String inprogressNodePath = ci.read();
    assertEquals("Expecting null to be return", null, inprogressNodePath);
  }

  /**
   * Tests that update should throw IOE, if version number modifies between read
   * and update
   */
  @Test(expected = IOException.class)
  public void testUpdateShouldFailWithIOEIfVersionNumberChangedAfterRead()
      throws Exception {
    CurrentInprogress ci = new CurrentInprogress(zkc, CURRENT_NODE_PATH);
    ci.init();
    ci.update("myInprogressZnode");
    assertEquals("Not returning myInprogressZnode", "myInprogressZnode", ci
        .read());
    // Updating data in-between to change the data to change the version number
    ci.update("YourInprogressZnode");
    ci.update("myInprogressZnode");
  }

}
