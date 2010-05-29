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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMasterWrongRS {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void afterAllTests() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test when region servers start reporting with the wrong address
   * or start code. Currently the decision is to shut them down.
   * See HBASE-2613
   * @throws Exception
   */
  @Test
  public void testRsReportsWrongServerName() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    MiniHBaseClusterRegionServer firstServer =
      (MiniHBaseClusterRegionServer)cluster.getRegionServer(0);
    HRegionServer secondServer = cluster.getRegionServer(1);
    HServerInfo hsi = firstServer.getServerInfo();
    firstServer.setHServerInfo(new HServerInfo(hsi.getServerAddress(),
      hsi.getInfoPort(), hsi.getHostname()));
    // Sleep while the region server pings back
    Thread.sleep(2000);
    assertTrue(firstServer.isOnline());
    assertEquals(2, cluster.getLiveRegionServerThreads().size());

    secondServer.getHServerInfo().setServerAddress(new HServerAddress("0.0.0.0", 60010));
    Thread.sleep(2000);
    assertTrue(secondServer.isOnline());
    assertEquals(1, cluster.getLiveRegionServerThreads().size());
  }
}
