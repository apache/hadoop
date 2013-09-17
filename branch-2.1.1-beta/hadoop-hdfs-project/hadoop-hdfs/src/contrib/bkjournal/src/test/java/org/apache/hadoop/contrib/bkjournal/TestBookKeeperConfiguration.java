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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Random;

import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

public class TestBookKeeperConfiguration {
  private static final Log LOG = LogFactory
      .getLog(TestBookKeeperConfiguration.class);
  private static final int ZK_SESSION_TIMEOUT = 5000;
  private static final String HOSTPORT = "127.0.0.1:2181";
  private static final int CONNECTION_TIMEOUT = 30000;
  private static NIOServerCnxnFactory serverFactory;
  private static ZooKeeperServer zks;
  private static ZooKeeper zkc;
  private static int ZooKeeperDefaultPort = 2181;
  private static File ZkTmpDir;
  private BookKeeperJournalManager bkjm;
  private static final String BK_ROOT_PATH = "/ledgers";

  private static ZooKeeper connectZooKeeper(String ensemble)
      throws IOException, KeeperException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);

    ZooKeeper zkc = new ZooKeeper(HOSTPORT, ZK_SESSION_TIMEOUT, new Watcher() {
      public void process(WatchedEvent event) {
        if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
          latch.countDown();
        }
      }
    });
    if (!latch.await(ZK_SESSION_TIMEOUT, TimeUnit.MILLISECONDS)) {
      throw new IOException("Zookeeper took too long to connect");
    }
    return zkc;
  }

  private NamespaceInfo newNSInfo() {
    Random r = new Random();
    return new NamespaceInfo(r.nextInt(), "testCluster", "TestBPID", -1);
  }

  @BeforeClass
  public static void setupZooKeeper() throws Exception {
    // create a ZooKeeper server(dataDir, dataLogDir, port)
    LOG.info("Starting ZK server");
    ZkTmpDir = File.createTempFile("zookeeper", "test");
    ZkTmpDir.delete();
    ZkTmpDir.mkdir();

    try {
      zks = new ZooKeeperServer(ZkTmpDir, ZkTmpDir, ZooKeeperDefaultPort);
      serverFactory = new NIOServerCnxnFactory();
      serverFactory.configure(new InetSocketAddress(ZooKeeperDefaultPort), 10);
      serverFactory.startup(zks);
    } catch (Exception e) {
      LOG.error("Exception while instantiating ZooKeeper", e);
    }

    boolean b = LocalBookKeeper.waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT);
    LOG.debug("ZooKeeper server up: " + b);
  }

  @Before
  public void setup() throws Exception {
    zkc = connectZooKeeper(HOSTPORT);
    try {
      ZKUtil.deleteRecursive(zkc, BK_ROOT_PATH);
    } catch (KeeperException.NoNodeException e) {
      LOG.debug("Ignoring no node exception on cleanup", e);
    } catch (Exception e) {
      LOG.error("Exception when deleting bookie root path in zk", e);
    }
  }

  @After
  public void teardown() throws Exception {
    if (null != zkc) {
      zkc.close();
    }
    if (null != bkjm) {
      bkjm.close();
    }
  }

  @AfterClass
  public static void teardownZooKeeper() throws Exception {
    if (null != zkc) {
      zkc.close();
    }
  }

  /**
   * Verify the BKJM is creating the bookie available path configured in
   * 'dfs.namenode.bookkeeperjournal.zk.availablebookies'
   */
  @Test
  public void testWithConfiguringBKAvailablePath() throws Exception {
    // set Bookie available path in the configuration
    String bkAvailablePath 
      = BookKeeperJournalManager.BKJM_ZK_LEDGERS_AVAILABLE_PATH_DEFAULT;
    Configuration conf = new Configuration();
    conf.setStrings(BookKeeperJournalManager.BKJM_ZK_LEDGERS_AVAILABLE_PATH,
        bkAvailablePath);
    Assert.assertNull(bkAvailablePath + " already exists", zkc.exists(
        bkAvailablePath, false));
    NamespaceInfo nsi = newNSInfo();
    bkjm = new BookKeeperJournalManager(conf,
        URI.create("bookkeeper://" + HOSTPORT + "/hdfsjournal-WithBKPath"),
        nsi);
    bkjm.format(nsi);
    Assert.assertNotNull("Bookie available path : " + bkAvailablePath
        + " doesn't exists", zkc.exists(bkAvailablePath, false));
  }

  /**
   * Verify the BKJM is creating the bookie available default path, when there
   * is no 'dfs.namenode.bookkeeperjournal.zk.availablebookies' configured
   */
  @Test
  public void testDefaultBKAvailablePath() throws Exception {
    Configuration conf = new Configuration();
    Assert.assertNull(BK_ROOT_PATH + " already exists", zkc.exists(
        BK_ROOT_PATH, false));
    NamespaceInfo nsi = newNSInfo();
    bkjm = new BookKeeperJournalManager(conf,
        URI.create("bookkeeper://" + HOSTPORT + "/hdfsjournal-DefaultBKPath"),
        nsi);
    bkjm.format(nsi);
    Assert.assertNotNull("Bookie available path : " + BK_ROOT_PATH
        + " doesn't exists", zkc.exists(BK_ROOT_PATH, false));
  }
}
