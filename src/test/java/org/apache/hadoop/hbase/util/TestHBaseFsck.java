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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHBaseFsck {

  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private final static Configuration conf = TEST_UTIL.getConfiguration();
  private final static byte[] TABLE = Bytes.toBytes("table");
  private final static byte[] FAM = Bytes.toBytes("fam");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  private List doFsck(boolean fix) throws Exception {
    HBaseFsck fsck = new HBaseFsck(conf);
    fsck.displayFullReport();  // i.e. -details
    fsck.setTimeLag(0);
    fsck.setFixErrors(fix);
    fsck.doWork();
    return fsck.getErrors().getErrorList();
  }

  private void assertNoErrors(List errs) throws Exception {
    assertEquals(0, errs.size());
  }

  private void assertErrors(List errs, ERROR_CODE[] expectedErrors) {
    assertEquals(Arrays.asList(expectedErrors), errs);
  }

  @Test
  public void testHBaseFsck() throws Exception {
    assertNoErrors(doFsck(false));

    TEST_UTIL.createTable(TABLE, FAM);

    // We created 1 table, should be fine
    assertNoErrors(doFsck(false));

    // Now let's mess it up and change the assignment in .META. to
    // point to a different region server
    HTable meta = new HTable(conf, HTableDescriptor.META_TABLEDESC.getName());
    ResultScanner scanner = meta.getScanner(new Scan());

    resforloop:
    for (Result res : scanner) {
      long startCode = Bytes.toLong(res.getValue(HConstants.CATALOG_FAMILY,
          HConstants.STARTCODE_QUALIFIER));

      for (JVMClusterUtil.RegionServerThread rs :
          TEST_UTIL.getHBaseCluster().getRegionServerThreads()) {

        ServerName sn = rs.getRegionServer().getServerName();

        // When we find a diff RS, change the assignment and break
        if (startCode != sn.getStartcode()) {
          Put put = new Put(res.getRow());
          put.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
            Bytes.toBytes(sn.getHostAndPort()));
          put.add(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
            Bytes.toBytes(sn.getStartcode()));
          meta.put(put);
          break resforloop;
        }
      }
    }

    // Try to fix the data
    assertErrors(doFsck(true), new ERROR_CODE[]{
        ERROR_CODE.SERVER_DOES_NOT_MATCH_META});
    Thread.sleep(15000);

    // Should be fixed now
    assertNoErrors(doFsck(false));

    // comment needed - what is the purpose of this line
    new HTable(conf, TABLE).getScanner(new Scan());
  }

  private HRegionInfo createRegion(Configuration conf, final HTableDescriptor
      htd, byte[] startKey, byte[] endKey)
      throws IOException {
    HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
    HRegionInfo hri = new HRegionInfo(htd.getName(), startKey, endKey);
    Put put = new Put(hri.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(hri));
    meta.put(put);
    return hri;
  }

  @Test
  /**
   * Tests for inconsistencies in the META data (duplicate start keys, or holes)
   */
  public void testHBaseFsckMeta() throws Exception {
    assertNoErrors(doFsck(false));

    HTable tbl = TEST_UTIL.createTable(Bytes.toBytes("table2"), FAM);

    Map<HRegionInfo, HServerAddress> hris = tbl.getRegionsInfo();
    HRegionInfo hriOrig = hris.keySet().iterator().next();
    Map<HRegionInfo, ServerName> locations = tbl.getRegionLocations();
    ServerName rsAddressOrig = locations.get(hriOrig);

    byte[][] startKeys = new byte[][]{
        HConstants.EMPTY_BYTE_ARRAY,
        Bytes.toBytes("A"),
        Bytes.toBytes("B"),
        Bytes.toBytes("C")
    };
    TEST_UTIL.createMultiRegions(conf, tbl, FAM, startKeys);
    Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = rootDir.getFileSystem(conf);
    Path p = new Path(rootDir + "/table2", hriOrig.getEncodedName());
    fs.delete(p, true);

    Thread.sleep(1 * 1000);
    ArrayList servers = new ArrayList();
    servers.add(rsAddressOrig);
    try {
      HBaseFsckRepair.fixDupeAssignment(TEST_UTIL.getHBaseAdmin(), hriOrig, servers);
    } catch (IOException ex) {
      ex = RemoteExceptionHandler.checkIOException(ex);
      if (!(ex instanceof UnknownRegionException)) {
        fail("Unexpected exception: " + ex);
      }
    }

    // We created 1 table, should be fine
    assertNoErrors(doFsck(false));

    // Now let's mess it up, by adding a region with a duplicate startkey
    HRegionInfo hriDupe = createRegion(conf, tbl.getTableDescriptor(),
        Bytes.toBytes("A"), Bytes.toBytes("A2"));
    TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriDupe);
    TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
        .waitForAssignment(hriDupe);
    assertErrors(doFsck(false), new ERROR_CODE[]{ERROR_CODE.DUPE_STARTKEYS});

    // Mess it up by creating an overlap in the metadata
    HRegionInfo hriOverlap = createRegion(conf, tbl.getTableDescriptor(),
        Bytes.toBytes("A2"), Bytes.toBytes("B2"));
    TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriOverlap);
    TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
        .waitForAssignment(hriOverlap);
    assertErrors(doFsck(false), new ERROR_CODE[]{
        ERROR_CODE.DUPE_STARTKEYS, ERROR_CODE.OVERLAP_IN_REGION_CHAIN,
        ERROR_CODE.OVERLAP_IN_REGION_CHAIN});

    // Mess it up by leaving a hole in the meta data
    HRegionInfo hriHole = createRegion(conf, tbl.getTableDescriptor(),
        Bytes.toBytes("D"), Bytes.toBytes("E"));
    TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriHole);
    TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
        .waitForAssignment(hriHole);
//    assertError(doFsck(false), ERROR_CODE.OVERLAP_IN_REGION_CHAIN);
    assertErrors(doFsck(false), new ERROR_CODE[]{ ERROR_CODE.DUPE_STARTKEYS,
        ERROR_CODE.OVERLAP_IN_REGION_CHAIN, ERROR_CODE.OVERLAP_IN_REGION_CHAIN,
        ERROR_CODE.HOLE_IN_REGION_CHAIN });

  }

}
