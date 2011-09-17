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
package org.apache.hadoop.hbase.catalog;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.Progressable;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

/**
 * Test {@link CatalogTracker}
 */
public class TestCatalogTracker {
  private static final Log LOG = LogFactory.getLog(TestCatalogTracker.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final ServerName SN =
    new ServerName("example.org", 1234, System.currentTimeMillis());
  private ZooKeeperWatcher watcher;
  private Abortable abortable;

  @BeforeClass public static void beforeClass() throws Exception {
    UTIL.startMiniZKCluster();
  }

  @AfterClass public static void afterClass() throws IOException {
    UTIL.getZkCluster().shutdown();
  }

  @Before public void before() throws IOException {
    this.abortable = new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        LOG.info(why, e);
      }
      
      @Override
      public boolean isAborted()  {
        return false;
      }
    };
    this.watcher = new ZooKeeperWatcher(UTIL.getConfiguration(),
      this.getClass().getSimpleName(), this.abortable, true);
  }

  @After public void after() {
    this.watcher.close();
  }

  private CatalogTracker constructAndStartCatalogTracker()
  throws IOException, InterruptedException {
    return constructAndStartCatalogTracker(null);
  }

  private CatalogTracker constructAndStartCatalogTracker(final HConnection c)
  throws IOException, InterruptedException {
    CatalogTracker ct = new CatalogTracker(this.watcher, null, c,
        this.abortable, 0);
    ct.start();
    return ct;
  }

  /**
   * Test that we get notification if .META. moves.
   * @throws IOException 
   * @throws InterruptedException 
   * @throws KeeperException 
   */
  @Test public void testThatIfMETAMovesWeAreNotified()
  throws IOException, InterruptedException, KeeperException {
    HConnection connection = Mockito.mock(HConnection.class);
    constructAndStartCatalogTracker(connection);
    try {
      RootLocationEditor.setRootLocation(this.watcher,
        new ServerName("example.com", 1234, System.currentTimeMillis()));
    } finally {
      // Clean out root location or later tests will be confused... they presume
      // start fresh in zk.
      RootLocationEditor.deleteRootLocation(this.watcher);
    }
  }

  /**
   * Test interruptable while blocking wait on root and meta.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test public void testInterruptWaitOnMetaAndRoot()
  throws IOException, InterruptedException {
    final CatalogTracker ct = constructAndStartCatalogTracker();
    ServerName hsa = ct.getRootLocation();
    Assert.assertNull(hsa);
    ServerName meta = ct.getMetaLocation();
    Assert.assertNull(meta);
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          ct.waitForMeta();
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted", e);
        }
      }
    };
    t.start();
    while (!t.isAlive()) Threads.sleep(1);
    Threads.sleep(1);
    assertTrue(t.isAlive());
    ct.stop();
    // Join the thread... should exit shortly.
    t.join();
  }

  @Test public void testGetMetaServerConnectionFails()
  throws IOException, InterruptedException, KeeperException {
    HConnection connection = Mockito.mock(HConnection.class);
    ConnectException connectException =
      new ConnectException("Connection refused");
    final HRegionInterface implementation =
      Mockito.mock(HRegionInterface.class);
    Mockito.when(implementation.get((byte [])Mockito.any(), (Get)Mockito.any())).
      thenThrow(connectException);
    Mockito.when(connection.getHRegionConnection((HServerAddress)Matchers.anyObject(), Matchers.anyBoolean())).
      thenReturn(implementation);
    Assert.assertNotNull(connection.getHRegionConnection(new HServerAddress(), false));
    final CatalogTracker ct = constructAndStartCatalogTracker(connection);
    try {
      RootLocationEditor.setRootLocation(this.watcher,
        new ServerName("example.com", 1234, System.currentTimeMillis()));
      Assert.assertFalse(ct.verifyMetaRegionLocation(100));
    } finally {
      // Clean out root location or later tests will be confused... they presume
      // start fresh in zk.
      RootLocationEditor.deleteRootLocation(this.watcher);
    }
  }

  /**
   * Test get of root region fails properly if nothing to connect to.
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  @Test
  public void testVerifyRootRegionLocationFails()
  throws IOException, InterruptedException, KeeperException {
    HConnection connection = Mockito.mock(HConnection.class);
    ConnectException connectException =
      new ConnectException("Connection refused");
    final HRegionInterface implementation =
      Mockito.mock(HRegionInterface.class);
    Mockito.when(implementation.getRegionInfo((byte [])Mockito.any())).
      thenThrow(connectException);
    Mockito.when(connection.getHRegionConnection((HServerAddress)Matchers.anyObject(), Matchers.anyBoolean())).
      thenReturn(implementation);
    Assert.assertNotNull(connection.getHRegionConnection(new HServerAddress(), false));
    final CatalogTracker ct = constructAndStartCatalogTracker(connection);
    try {
      RootLocationEditor.setRootLocation(this.watcher,
        new ServerName("example.com", 1234, System.currentTimeMillis()));
      Assert.assertFalse(ct.verifyRootRegionLocation(100));
    } finally {
      // Clean out root location or later tests will be confused... they presume
      // start fresh in zk.
      RootLocationEditor.deleteRootLocation(this.watcher);
    }
  }

  @Test (expected = NotAllMetaRegionsOnlineException.class)
  public void testTimeoutWaitForRoot()
  throws IOException, InterruptedException {
    final CatalogTracker ct = constructAndStartCatalogTracker();
    ct.waitForRoot(100);
  }

  @Test (expected = NotAllMetaRegionsOnlineException.class)
  public void testTimeoutWaitForMeta()
  throws IOException, InterruptedException {
    final CatalogTracker ct = constructAndStartCatalogTracker();
    ct.waitForMeta(100);
  }

  /**
   * Test waiting on root w/ no timeout specified.
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  @Test public void testNoTimeoutWaitForRoot()
  throws IOException, InterruptedException, KeeperException {
    final CatalogTracker ct = constructAndStartCatalogTracker();
    ServerName hsa = ct.getRootLocation();
    Assert.assertNull(hsa);

    // Now test waiting on root location getting set.
    Thread t = new WaitOnMetaThread(ct);
    startWaitAliveThenWaitItLives(t, 1000);
    // Set a root location.
    hsa = setRootLocation();
    // Join the thread... should exit shortly.
    t.join();
    // Now root is available.
    Assert.assertTrue(ct.getRootLocation().equals(hsa));
  }

  private ServerName setRootLocation() throws KeeperException {
    RootLocationEditor.setRootLocation(this.watcher, SN);
    return SN;
  }

  /**
   * Test waiting on meta w/ no timeout specified.
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  @Test public void testNoTimeoutWaitForMeta()
  throws IOException, InterruptedException, KeeperException {
    // Mock an HConnection and a HRegionInterface implementation.  Have the
    // HConnection return the HRI.  Have the HRI return a few mocked up responses
    // to make our test work.
    HConnection connection = Mockito.mock(HConnection.class);
    HRegionInterface  mockHRI = Mockito.mock(HRegionInterface.class);
    // Make the HRI return an answer no matter how Get is called.  Same for
    // getHRegionInfo.  Thats enough for this test.
    Mockito.when(connection.getHRegionConnection((String)Mockito.any(),
      Matchers.anyInt())).thenReturn(mockHRI);

    final CatalogTracker ct = constructAndStartCatalogTracker(connection);
    ServerName hsa = ct.getMetaLocation();
    Assert.assertNull(hsa);

    // Now test waiting on meta location getting set.
    Thread t = new WaitOnMetaThread(ct) {
      @Override
      void doWaiting() throws InterruptedException {
        this.ct.waitForMeta();
      }
    };
    startWaitAliveThenWaitItLives(t, 1000);

    // Now the ct is up... set into the mocks some answers that make it look
    // like things have been getting assigned.  Make it so we'll return a
    // location (no matter what the Get is).  Same for getHRegionInfo -- always
    // just return the meta region.
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY,
      HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
      Bytes.toBytes(SN.getHostAndPort())));
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY,
      HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
      Bytes.toBytes(SN.getStartcode())));
    final Result result = new Result(kvs);
    Mockito.when(mockHRI.get((byte [])Mockito.any(), (Get)Mockito.any())).
      thenReturn(result);
    Mockito.when(mockHRI.getRegionInfo((byte [])Mockito.any())).
      thenReturn(HRegionInfo.FIRST_META_REGIONINFO);
    // This should trigger wake up of meta wait (Its the removal of the meta
    // region unassigned node that triggers catalogtrackers that a meta has
    // been assigned.
    String node = ct.getMetaNodeTracker().getNode();
    ZKUtil.createAndFailSilent(this.watcher, node);
    MetaEditor.updateMetaLocation(ct, HRegionInfo.FIRST_META_REGIONINFO, SN);
    ZKUtil.deleteNode(this.watcher, node);
    // Join the thread... should exit shortly.
    t.join();
    // Now meta is available.
    Assert.assertTrue(ct.getMetaLocation().equals(SN));
  }

  private void startWaitAliveThenWaitItLives(final Thread t, final int ms) {
    t.start();
    while(!t.isAlive()) {
      // Wait
    }
    // Wait one second.
    Threads.sleep(ms);
    Assert.assertTrue("Assert " + t.getName() + " still waiting", t.isAlive());
  }

  class CountingProgressable implements Progressable {
    final AtomicInteger counter = new AtomicInteger(0);
    @Override
    public void progress() {
      this.counter.incrementAndGet();
    }
  }

  /**
   * Wait on META.
   * Default is wait on -ROOT-.
   */
  class WaitOnMetaThread extends Thread {
    final CatalogTracker ct;

    WaitOnMetaThread(final CatalogTracker ct) {
      super("WaitOnMeta");
      this.ct = ct;
    }

    @Override
    public void run() {
      try {
        doWaiting();
      } catch (InterruptedException e) {
        throw new RuntimeException("Failed wait", e);
      }
      LOG.info("Exiting " + getName());
    }

    void doWaiting() throws InterruptedException {
      this.ct.waitForRoot();
    }
  }
}
