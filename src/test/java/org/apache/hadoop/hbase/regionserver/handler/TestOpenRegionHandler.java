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
package org.apache.hadoop.hbase.regionserver.handler;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test of the {@link OpenRegionHandler}.
 */
public class TestOpenRegionHandler {
  static final Log LOG = LogFactory.getLog(TestOpenRegionHandler.class);
  private final static HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final HTableDescriptor TEST_HTD =
    new HTableDescriptor("TestOpenRegionHandler.java");
  private HRegionInfo TEST_HRI;

  private int testIndex = 0;

  @BeforeClass public static void before() throws Exception {
    HTU.startMiniZKCluster();
  }

  @AfterClass public static void after() throws IOException {
    HTU.shutdownMiniZKCluster();
  }

  /**
   * Before each test, use a different HRI, so the different tests
   * don't interfere with each other. This allows us to use just
   * a single ZK cluster for the whole suite.
   */
  @Before
  public void setupHRI() {
    TEST_HRI = new HRegionInfo(TEST_HTD.getName(),
      Bytes.toBytes(testIndex),
      Bytes.toBytes(testIndex + 1));
    testIndex++;
  }

  /**
   * Test the openregionhandler can deal with its znode being yanked out from
   * under it.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-3627">HBASE-3627</a>
   * @throws IOException
   * @throws NodeExistsException
   * @throws KeeperException
   */
  @Test public void testYankingRegionFromUnderIt()
  throws IOException, NodeExistsException, KeeperException {
    final Server server = new MockServer(HTU);
    final RegionServerServices rss = new MockRegionServerServices();

    HTableDescriptor htd = TEST_HTD;
    final HRegionInfo hri = TEST_HRI;
    HRegion region =
         HRegion.createHRegion(hri, HBaseTestingUtility.getTestDir(), HTU
            .getConfiguration(), htd);
    assertNotNull(region);
    OpenRegionHandler handler = new OpenRegionHandler(server, rss, hri, htd) {
      HRegion openRegion() {
        // Open region first, then remove znode as though it'd been hijacked.
        HRegion region = super.openRegion();
        
        // Don't actually open region BUT remove the znode as though it'd
        // been hijacked on us.
        ZooKeeperWatcher zkw = this.server.getZooKeeper();
        String node = ZKAssign.getNodeName(zkw, hri.getEncodedName());
        try {
          ZKUtil.deleteNodeFailSilent(zkw, node);
        } catch (KeeperException e) {
          throw new RuntimeException("Ugh failed delete of " + node, e);
        }
        return region;
      }
    };
    // Call process without first creating OFFLINE region in zk, see if
    // exception or just quiet return (expected).
    handler.process();
    ZKAssign.createNodeOffline(server.getZooKeeper(), hri, server.getServerName());
    // Call process again but this time yank the zk znode out from under it
    // post OPENING; again will expect it to come back w/o NPE or exception.
    handler.process();
  }
  
  @Test
  public void testFailedOpenRegion() throws Exception {
    Server server = new MockServer(HTU);
    RegionServerServices rsServices = Mockito.mock(RegionServerServices.class);

    // Create it OFFLINE, which is what it expects
    ZKAssign.createNodeOffline(server.getZooKeeper(), TEST_HRI, server.getServerName());

    // Create the handler
    OpenRegionHandler handler =
      new OpenRegionHandler(server, rsServices, TEST_HRI, TEST_HTD) {
        @Override
        HRegion openRegion() {
          // Fake failure of opening a region due to an IOE, which is caught
          return null;
        }
    };
    handler.process();

    // Handler should have transitioned it to FAILED_OPEN
    RegionTransitionData data =
      ZKAssign.getData(server.getZooKeeper(), TEST_HRI.getEncodedName());
    assertEquals(EventType.RS_ZK_REGION_FAILED_OPEN, data.getEventType());
  }
  
  @Test
  public void testFailedUpdateMeta() throws Exception {
    Server server = new MockServer(HTU);
    RegionServerServices rsServices = Mockito.mock(RegionServerServices.class);

    // Create it OFFLINE, which is what it expects
    ZKAssign.createNodeOffline(server.getZooKeeper(), TEST_HRI, server.getServerName());

    // Create the handler
    OpenRegionHandler handler =
      new OpenRegionHandler(server, rsServices, TEST_HRI, TEST_HTD) {
        @Override
        boolean updateMeta(final HRegion r) {
          // Fake failure of updating META
          return false;
        }
    };
    handler.process();

    // Handler should have transitioned it to FAILED_OPEN
    RegionTransitionData data =
      ZKAssign.getData(server.getZooKeeper(), TEST_HRI.getEncodedName());
    assertEquals(EventType.RS_ZK_REGION_FAILED_OPEN, data.getEventType());
  }
  
}
