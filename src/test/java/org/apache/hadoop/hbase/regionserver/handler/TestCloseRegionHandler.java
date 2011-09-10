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
package org.apache.hadoop.hbase.regionserver.handler;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test of the {@link CloseRegionHandler}.
 */
public class TestCloseRegionHandler {
  static final Log LOG = LogFactory.getLog(TestCloseRegionHandler.class);
  private final static HBaseTestingUtility HTU = new HBaseTestingUtility();

  /**
   * Test that if we fail a flush, abort gets set on close.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-4270">HBASE-4270</a>
   * @throws IOException
   * @throws NodeExistsException
   * @throws KeeperException
   */
  @Test public void testFailedFlushAborts()
  throws IOException, NodeExistsException, KeeperException {
    final Server server = new MockServer(HTU, false);
    final RegionServerServices rss = new MockRegionServerServices();
    HTableDescriptor htd = new HTableDescriptor("testFailedFlushAborts");
    final HRegionInfo hri =
      new HRegionInfo(htd.getName(), HConstants.EMPTY_END_ROW,
        HConstants.EMPTY_END_ROW);
    HRegion region =
      HRegion.createHRegion(hri, HBaseTestingUtility.getTestDir(),
        HTU.getConfiguration(), htd);
    assertNotNull(region);
    // Spy on the region so can throw exception when close is called.
    HRegion spy = Mockito.spy(region);
    final boolean abort = false;
    Mockito.when(spy.close(abort)).
      thenThrow(new RuntimeException("Mocked failed close!"));
    // The CloseRegionHandler will try to get an HRegion that corresponds
    // to the passed hri -- so insert the region into the online region Set.
    rss.addToOnlineRegions(spy);
    // Assert the Server is NOT stopped before we call close region.
    assertFalse(server.isStopped());
    CloseRegionHandler handler =
      new CloseRegionHandler(server, rss, hri, false, false);
    boolean throwable = false;
    try {
      handler.process();
    } catch (Throwable t) {
      throwable = true;
    } finally {
      assertTrue(throwable);
      // Abort calls stop so stopped flag should be set.
      assertTrue(server.isStopped());
    }
  }
}