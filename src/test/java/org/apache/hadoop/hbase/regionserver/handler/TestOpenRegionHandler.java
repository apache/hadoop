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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.ipc.HBaseRpcMetrics;
import org.apache.hadoop.hbase.regionserver.CompactionRequestor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
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
  private static final Log LOG = LogFactory.getLog(TestOpenRegionHandler.class);
  private final static HBaseTestingUtility HTU = new HBaseTestingUtility();

  @BeforeClass public static void before() throws Exception {
    HTU.startMiniZKCluster();
  }

  @AfterClass public static void after() throws IOException {
    HTU.shutdownMiniZKCluster();
  }

  /**
   * Basic mock Server
   */
  static class MockServer implements Server {
    boolean stopped = false;
    final static String NAME = "MockServer";
    final ZooKeeperWatcher zk;

    MockServer() throws ZooKeeperConnectionException, IOException {
      this.zk =  new ZooKeeperWatcher(HTU.getConfiguration(), NAME, this);
    }

    @Override
    public void abort(String why, Throwable e) {
      LOG.fatal("Abort why=" + why, e);
      this.stopped = true;
    }

    @Override
    public void stop(String why) {
      LOG.debug("Stop why=" + why);
      this.stopped = true;
    }

    @Override
    public boolean isStopped() {
      return this.stopped;
    }

    @Override
    public Configuration getConfiguration() {
      return HTU.getConfiguration();
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return this.zk;
    }

    @Override
    public CatalogTracker getCatalogTracker() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getServerName() {
      return NAME;
    }
  }

  /**
   * Basic mock region server services.
   */
  static class MockRegionServerServices implements RegionServerServices {
    final Map<String, HRegion> regions = new HashMap<String, HRegion>();
    boolean stopping = false;

    @Override
    public boolean removeFromOnlineRegions(String encodedRegionName) {
      return this.regions.remove(encodedRegionName) != null;
    }
    
    @Override
    public HRegion getFromOnlineRegions(String encodedRegionName) {
      return this.regions.get(encodedRegionName);
    }
    
    @Override
    public void addToOnlineRegions(HRegion r) {
      this.regions.put(r.getRegionInfo().getEncodedName(), r);
    }
    
    @Override
    public void postOpenDeployTasks(HRegion r, CatalogTracker ct, boolean daughter)
        throws KeeperException, IOException {
    }
    
    @Override
    public boolean isStopping() {
      return this.stopping;
    }
    
    @Override
    public HLog getWAL() {
      return null;
    }
    
    @Override
    public HServerInfo getServerInfo() {
      return null;
    }
    
    @Override
    public HBaseRpcMetrics getRpcMetrics() {
      return null;
    }
    
    @Override
    public FlushRequester getFlushRequester() {
      return null;
    }
    
    @Override
    public CompactionRequestor getCompactionRequester() {
      return null;
    }

    @Override
    public CatalogTracker getCatalogTracker() {
      return null;
    }

    @Override
    public ZooKeeperWatcher getZooKeeperWatcher() {
      return null;
    }
  };

  /**
   * Test the openregionhandler can deal with its znode being yanked out from
   * under it.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-3627">HBASE-3627</a>
   * @throws IOException
   * @throws NodeExistsException
   * @throws KeeperException
   */
  @Test public void testOpenRegionHandlerYankingRegionFromUnderIt()
  throws IOException, NodeExistsException, KeeperException {
    final Server server = new MockServer();
    final RegionServerServices rss = new MockRegionServerServices();

    HTableDescriptor htd =
      new HTableDescriptor("testOpenRegionHandlerYankingRegionFromUnderIt");
    final HRegionInfo hri =
      new HRegionInfo(htd, HConstants.EMPTY_END_ROW, HConstants.EMPTY_END_ROW);
    OpenRegionHandler handler = new OpenRegionHandler(server, rss, hri) {
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
}