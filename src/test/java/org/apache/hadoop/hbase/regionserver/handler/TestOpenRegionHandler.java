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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test of the {@link OpenRegionHandler}.
 */
public class TestOpenRegionHandler {
  static final Log LOG = LogFactory.getLog(TestOpenRegionHandler.class);
  private final static HBaseTestingUtility HTU = new HBaseTestingUtility();

  @BeforeClass public static void before() throws Exception {
    HTU.startMiniZKCluster();
  }

  @AfterClass public static void after() throws IOException {
    HTU.shutdownMiniZKCluster();
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

    HTableDescriptor htd = new HTableDescriptor("TestOpenRegionHandler.java");
    final HRegionInfo hri =
      new HRegionInfo(htd.getName(), HConstants.EMPTY_END_ROW,
          HConstants.EMPTY_END_ROW);
    HRegion region =
         HRegion.createHRegion(hri, HBaseTestingUtility.getTestDir(), HTU
            .getConfiguration(), htd);
    assertNotNull(region);
    OpenRegionHandler handler = new OpenRegionHandler(server, rss, hri, htd) {
      HRegion openRegion() {
        // Open region first, then remove znode as though it'd been hijacked.
        //HRegion region = super.openRegion();
        HRegion region = super.openRegion(HBaseTestingUtility.getTestDir());

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
}