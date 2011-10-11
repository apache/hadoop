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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TestEndToEndSplitTransaction {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void afterAllTests() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }
  
  @Test
  public void testMasterOpsWhileSplitting() throws Exception {
    byte[] tableName = Bytes.toBytes("TestSplit");
    byte[] familyName = Bytes.toBytes("fam");
    TEST_UTIL.createTable(tableName, familyName);
    TEST_UTIL.loadTable(new HTable(TEST_UTIL.getConfiguration(), tableName),
        familyName);
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    byte []firstRow = Bytes.toBytes("aaa");
    byte []splitRow = Bytes.toBytes("lll");
    byte []lastRow = Bytes.toBytes("zzz");
    HConnection con = HConnectionManager
        .getConnection(TEST_UTIL.getConfiguration());
    // this will also cache the region
    byte[] regionName = con.locateRegion(tableName, splitRow).getRegionInfo()
        .getRegionName();
    HRegion region = server.getRegion(regionName);
    SplitTransaction split = new SplitTransaction(region, splitRow);
    split.prepare();

    // 1. phase I
    PairOfSameType<HRegion> regions = split.createDaughters(server, server);
    assertFalse(test(con, tableName, firstRow, server));
    assertFalse(test(con, tableName, lastRow, server));

    // passing null as services prevents final step
    // 2, most of phase II
    split.openDaughters(server, null, regions.getFirst(), regions.getSecond());
    assertFalse(test(con, tableName, firstRow, server));
    assertFalse(test(con, tableName, lastRow, server));

    // 3. finish phase II
    // note that this replicates some code from SplitTransaction
    // 2nd daughter first
    server.postOpenDeployTasks(regions.getSecond(), server.getCatalogTracker(), true);
    // THIS is the crucial point:
    // the 2nd daughter was added, so querying before the split key should fail.
    assertFalse(test(con, tableName, firstRow, server));
    // past splitkey is ok.
    assertTrue(test(con, tableName, lastRow, server));

    // first daughter second
    server.postOpenDeployTasks(regions.getFirst(), server.getCatalogTracker(), true);
    assertTrue(test(con, tableName, firstRow, server));
    assertTrue(test(con, tableName, lastRow, server));

    // 4. phase III
    split.transitionZKNode(server, regions.getFirst(), regions.getSecond());
    assertTrue(test(con, tableName, firstRow, server));
    assertTrue(test(con, tableName, lastRow, server));
  }

  /**
   * attempt to locate the region and perform a get and scan
   * @return True if successful, False otherwise.
   */
  private boolean test(HConnection con, byte[] tableName, byte[] row,
      HRegionServer server) {
    // not using HTable to avoid timeouts and retries
    try {
      byte[] regionName = con.relocateRegion(tableName, row).getRegionInfo()
          .getRegionName();
      // get and scan should now succeed without exception
      server.get(regionName, new Get(row));
      server.openScanner(regionName, new Scan(row));
    } catch (IOException x) {
      return false;
    }
    return true;
  }
}
