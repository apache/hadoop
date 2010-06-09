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
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestKillingServersFromMaster {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.startMiniCluster(2);
    cluster = TEST_UTIL.getHBaseCluster();
  }

  @AfterClass
  public static void afterAllTests() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    TEST_UTIL.ensureSomeRegionServersAvailable(2);
  }

  /**
   * Test that a region server that reports with the wrong start code
   * gets shut down
   * See HBASE-2613
   * @throws Exception
   */
  @Test (timeout=180000)
  public void testRsReportsWrongStartCode() throws Exception {
    MiniHBaseClusterRegionServer firstServer =
      (MiniHBaseClusterRegionServer)cluster.getRegionServer(0);
    HServerInfo hsi = firstServer.getServerInfo();
    // This constructor creates a new startcode
    firstServer.setHServerInfo(new HServerInfo(hsi.getServerAddress(),
      hsi.getInfoPort(), hsi.getHostname()));
    cluster.waitOnRegionServer(0);
    assertEquals(1, cluster.getLiveRegionServerThreads().size());
  }

  /**
   * Test that a region server that reports with the wrong address
   * gets shut down
   * See HBASE-2613
   * @throws Exception
   */
  @Test (timeout=180000)
  public void testRsReportsWrongAddress() throws Exception {
    MiniHBaseClusterRegionServer firstServer =
      (MiniHBaseClusterRegionServer)cluster.getRegionServer(0);
    firstServer.getHServerInfo().setServerAddress(
      new HServerAddress("0.0.0.0", 60010));
    cluster.waitOnRegionServer(0);
    assertEquals(1, cluster.getLiveRegionServerThreads().size());
  }

  /**
   * Send a YouAreDeadException to the region server and expect it to shut down
   * See HBASE-2691
   * @throws Exception
   */
  @Test (timeout=180000)
  public void testSendYouAreDead() throws Exception {
    cluster.addExceptionToSendRegionServer(0, new YouAreDeadException("bam!"));
    cluster.waitOnRegionServer(0);
    assertEquals(1, cluster.getLiveRegionServerThreads().size());
  }
}
