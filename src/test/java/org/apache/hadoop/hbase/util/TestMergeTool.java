/**
 * Copyright 2008 The Apache Software Foundation
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.ToolRunner;

/** Test stand alone merge tool that can merge arbitrary regions */
public class TestMergeTool extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestMergeTool.class);
  HBaseTestingUtility TEST_UTIL;
//  static final byte [] COLUMN_NAME = Bytes.toBytes("contents:");
  static final byte [] FAMILY = Bytes.toBytes("contents");
  static final byte [] QUALIFIER = Bytes.toBytes("dc");

  private final HRegionInfo[] sourceRegions = new HRegionInfo[5];
  private final HRegion[] regions = new HRegion[5];
  private HTableDescriptor desc;
  private byte [][][] rows;
  private MiniDFSCluster dfsCluster = null;

  @Override
  public void setUp() throws Exception {
    // Set the timeout down else this test will take a while to complete.
    this.conf.setLong("hbase.zookeeper.recoverable.waittime", 1000);
    // Make it so we try and connect to a zk that is not there (else we might
    // find a zk ensemble put up by another concurrent test and this will
    // mess up this test.  Choose unlikely port. Default test port is 21818.
    // Default zk port is 2181.
    this.conf.setInt("hbase.zookeeper.property.clientPort", 10001);

    this.conf.set("hbase.hstore.compactionThreshold", "2");

    // Create table description
    this.desc = new HTableDescriptor("TestMergeTool");
    this.desc.addFamily(new HColumnDescriptor(FAMILY));

    /*
     * Create the HRegionInfos for the regions.
     */
    // Region 0 will contain the key range [row_0200,row_0300)
    sourceRegions[0] = new HRegionInfo(this.desc.getName(),
        Bytes.toBytes("row_0200"),
      Bytes.toBytes("row_0300"));

    // Region 1 will contain the key range [row_0250,row_0400) and overlaps
    // with Region 0
    sourceRegions[1] =
      new HRegionInfo(this.desc.getName(),
          Bytes.toBytes("row_0250"),
          Bytes.toBytes("row_0400"));

    // Region 2 will contain the key range [row_0100,row_0200) and is adjacent
    // to Region 0 or the region resulting from the merge of Regions 0 and 1
    sourceRegions[2] =
      new HRegionInfo(this.desc.getName(),
          Bytes.toBytes("row_0100"),
          Bytes.toBytes("row_0200"));

    // Region 3 will contain the key range [row_0500,row_0600) and is not
    // adjacent to any of Regions 0, 1, 2 or the merged result of any or all
    // of those regions
    sourceRegions[3] =
      new HRegionInfo(this.desc.getName(),
          Bytes.toBytes("row_0500"),
          Bytes.toBytes("row_0600"));

    // Region 4 will have empty start and end keys and overlaps all regions.
    sourceRegions[4] =
      new HRegionInfo(this.desc.getName(),
          HConstants.EMPTY_BYTE_ARRAY,
          HConstants.EMPTY_BYTE_ARRAY);

    /*
     * Now create some row keys
     */
    this.rows = new byte [5][][];
    this.rows[0] = Bytes.toByteArrays(new String[] { "row_0210", "row_0280" });
    this.rows[1] = Bytes.toByteArrays(new String[] { "row_0260", "row_0350",
        "row_035" });
    this.rows[2] = Bytes.toByteArrays(new String[] { "row_0110", "row_0175",
        "row_0175", "row_0175"});
    this.rows[3] = Bytes.toByteArrays(new String[] { "row_0525", "row_0560",
        "row_0560", "row_0560", "row_0560"});
    this.rows[4] = Bytes.toByteArrays(new String[] { "row_0050", "row_1000",
        "row_1000", "row_1000", "row_1000", "row_1000" });

    // Start up dfs
    TEST_UTIL = new HBaseTestingUtility(conf);
    this.dfsCluster = TEST_UTIL.startMiniDFSCluster(2);
    this.fs = this.dfsCluster.getFileSystem();
    System.out.println("fs=" + this.fs);
    this.conf.set("fs.defaultFS", fs.getUri().toString());
    Path parentdir = fs.getHomeDirectory();
    conf.set(HConstants.HBASE_DIR, parentdir.toString());
    fs.mkdirs(parentdir);
    FSUtils.setVersion(fs, parentdir);

    // Note: we must call super.setUp after starting the mini cluster or
    // we will end up with a local file system

    super.setUp();
    try {
      // Create root and meta regions
      createRootAndMetaRegions();
      FSUtils.createTableDescriptor(this.fs, this.testDir, this.desc);
      /*
       * Create the regions we will merge
       */
      for (int i = 0; i < sourceRegions.length; i++) {
        regions[i] =
          HRegion.createHRegion(this.sourceRegions[i], this.testDir, this.conf,
              this.desc);
        /*
         * Insert data
         */
        for (int j = 0; j < rows[i].length; j++) {
          byte [] row = rows[i][j];
          Put put = new Put(row);
          put.add(FAMILY, QUALIFIER, row);
          regions[i].put(put);
        }
        HRegion.addRegionToMETA(meta, regions[i]);
      }
      // Close root and meta regions
      closeRootAndMeta();

    } catch (Exception e) {
      TEST_UTIL.shutdownMiniCluster();
      throw e;
    }
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    TEST_UTIL.shutdownMiniCluster();
  }

  /*
   * @param msg Message that describes this merge
   * @param regionName1
   * @param regionName2
   * @param log Log to use merging.
   * @param upperbound Verifying, how high up in this.rows to go.
   * @return Merged region.
   * @throws Exception
   */
  private HRegion mergeAndVerify(final String msg, final String regionName1,
    final String regionName2, final HLog log, final int upperbound)
  throws Exception {
    Merge merger = new Merge(this.conf);
    LOG.info(msg);
    System.out.println("fs2=" + this.conf.get("fs.defaultFS"));
    int errCode = ToolRunner.run(this.conf, merger,
      new String[] {this.desc.getNameAsString(), regionName1, regionName2}
    );
    assertTrue("'" + msg + "' failed with errCode " + errCode, errCode == 0);
    HRegionInfo mergedInfo = merger.getMergedHRegionInfo();

    // Now verify that we can read all the rows from regions 0, 1
    // in the new merged region.
    HRegion merged = HRegion.openHRegion(mergedInfo, this.desc, log, this.conf);
    verifyMerge(merged, upperbound);
    merged.close();
    LOG.info("Verified " + msg);
    return merged;
  }

  private void verifyMerge(final HRegion merged, final int upperbound)
  throws IOException {
    //Test
    Scan scan = new Scan();
    scan.addFamily(FAMILY);
    InternalScanner scanner = merged.getScanner(scan);
    try {
    List<KeyValue> testRes = null;
      while (true) {
        testRes = new ArrayList<KeyValue>();
        boolean hasNext = scanner.next(testRes);
        if (!hasNext) {
          break;
        }
      }
    } finally {
      scanner.close();
    }

    //!Test

    for (int i = 0; i < upperbound; i++) {
      for (int j = 0; j < rows[i].length; j++) {
        Get get = new Get(rows[i][j]);
        get.addFamily(FAMILY);
        Result result = merged.get(get, null);
        assertEquals(1, result.size());
        byte [] bytes = result.raw()[0].getValue();
        assertNotNull(Bytes.toStringBinary(rows[i][j]), bytes);
        assertTrue(Bytes.equals(bytes, rows[i][j]));
      }
    }
  }

  /**
   * Test merge tool.
   * @throws Exception
   */
  public void testMergeTool() throws Exception {
    // First verify we can read the rows from the source regions and that they
    // contain the right data.
    for (int i = 0; i < regions.length; i++) {
      for (int j = 0; j < rows[i].length; j++) {
        Get get = new Get(rows[i][j]);
        get.addFamily(FAMILY);
        Result result = regions[i].get(get, null);
        byte [] bytes = result.raw()[0].getValue();
        assertNotNull(bytes);
        assertTrue(Bytes.equals(bytes, rows[i][j]));
      }
      // Close the region and delete the log
      regions[i].close();
      regions[i].getLog().closeAndDelete();
    }

    // Create a log that we can reuse when we need to open regions
    Path logPath = new Path("/tmp", HConstants.HREGION_LOGDIR_NAME + "_" +
      System.currentTimeMillis());
    LOG.info("Creating log " + logPath.toString());
    Path oldLogDir = new Path("/tmp", HConstants.HREGION_OLDLOGDIR_NAME);
    HLog log = new HLog(this.fs, logPath, oldLogDir, this.conf);
    try {
       // Merge Region 0 and Region 1
      HRegion merged = mergeAndVerify("merging regions 0 and 1",
        this.sourceRegions[0].getRegionNameAsString(),
        this.sourceRegions[1].getRegionNameAsString(), log, 2);

      // Merge the result of merging regions 0 and 1 with region 2
      merged = mergeAndVerify("merging regions 0+1 and 2",
        merged.getRegionInfo().getRegionNameAsString(),
        this.sourceRegions[2].getRegionNameAsString(), log, 3);

      // Merge the result of merging regions 0, 1 and 2 with region 3
      merged = mergeAndVerify("merging regions 0+1+2 and 3",
        merged.getRegionInfo().getRegionNameAsString(),
        this.sourceRegions[3].getRegionNameAsString(), log, 4);

      // Merge the result of merging regions 0, 1, 2 and 3 with region 4
      merged = mergeAndVerify("merging regions 0+1+2+3 and 4",
        merged.getRegionInfo().getRegionNameAsString(),
        this.sourceRegions[4].getRegionNameAsString(), log, rows.length);
    } finally {
      log.closeAndDelete();
    }
  }
}
