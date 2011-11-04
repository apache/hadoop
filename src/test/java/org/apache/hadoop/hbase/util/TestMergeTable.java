/**
 * Copyright 2007 The Apache Software Foundation
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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.junit.Test;

/**
 * Tests merging a normal table's regions
 */
public class TestMergeTable {
  private static final Log LOG = LogFactory.getLog(TestMergeTable.class);
  private final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte [] COLUMN_NAME = Bytes.toBytes("contents");
  private static final byte [] VALUE;
  static {
    // We will use the same value for the rows as that is not really important here
    String partialValue = String.valueOf(System.currentTimeMillis());
    StringBuilder val = new StringBuilder();
    while (val.length() < 1024) {
      val.append(partialValue);
    }
    VALUE = Bytes.toBytes(val.toString());
  }

  /**
   * Test merge.
   * Hand-makes regions of a mergeable size and adds the hand-made regions to
   * hand-made meta.  The hand-made regions are created offline.  We then start
   * up mini cluster, disables the hand-made table and starts in on merging.
   * @throws Exception 
   */
  @Test (timeout=300000) public void testMergeTable() throws Exception {
    // Table we are manually creating offline.
    HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes("test"));
    desc.addFamily(new HColumnDescriptor(COLUMN_NAME));

    // Set maximum regionsize down.
    UTIL.getConfiguration().setLong("hbase.hregion.max.filesize", 64L * 1024L * 1024L);
    // Make it so we don't split.
    UTIL.getConfiguration().setInt("hbase.regionserver.regionSplitLimit", 0);
    // Startup hdfs.  Its in here we'll be putting our manually made regions.
    UTIL.startMiniDFSCluster(1);
    // Create hdfs hbase rootdir.
    Path rootdir = UTIL.createRootDir();
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    if (fs.exists(rootdir)) {
      if (fs.delete(rootdir, true)) {
        LOG.info("Cleaned up existing " + rootdir);
      }
    }

    // Now create three data regions: The first is too large to merge since it
    // will be > 64 MB in size. The second two will be smaller and will be
    // selected for merging.

    // To ensure that the first region is larger than 64MB we need to write at
    // least 65536 rows. We will make certain by writing 70000
    byte [] row_70001 = Bytes.toBytes("row_70001");
    byte [] row_80001 = Bytes.toBytes("row_80001");

    // Create regions and populate them at same time.  Create the tabledir
    // for them first.
    FSTableDescriptors.createTableDescriptor(fs, rootdir, desc);
    HRegion [] regions = {
      createRegion(desc, null, row_70001, 1, 70000, rootdir),
      createRegion(desc, row_70001, row_80001, 70001, 10000, rootdir),
      createRegion(desc, row_80001, null, 80001, 11000, rootdir)
    };

    // Now create the root and meta regions and insert the data regions
    // created above into .META.
    setupROOTAndMeta(rootdir, regions);
    try {
      LOG.info("Starting mini zk cluster");
      UTIL.startMiniZKCluster();
      LOG.info("Starting mini hbase cluster");
      UTIL.startMiniHBaseCluster(1, 1);
      Configuration c = new Configuration(UTIL.getConfiguration());
      CatalogTracker ct = new CatalogTracker(c);
      ct.start();
      List<HRegionInfo> originalTableRegions =
        MetaReader.getTableRegions(ct, desc.getName());
      LOG.info("originalTableRegions size=" + originalTableRegions.size() +
        "; " + originalTableRegions);
      HBaseAdmin admin = new HBaseAdmin(new Configuration(c));
      admin.disableTable(desc.getName());
      HMerge.merge(c, FileSystem.get(c), desc.getName());
      List<HRegionInfo> postMergeTableRegions =
        MetaReader.getTableRegions(ct, desc.getName());
      LOG.info("postMergeTableRegions size=" + postMergeTableRegions.size() +
        "; " + postMergeTableRegions);
      assertTrue("originalTableRegions=" + originalTableRegions.size() +
        ", postMergeTableRegions=" + postMergeTableRegions.size(),
        postMergeTableRegions.size() < originalTableRegions.size());
      LOG.info("Done with merge");
    } finally {
      UTIL.shutdownMiniCluster();
      LOG.info("After cluster shutdown");
    }
  }

  private HRegion createRegion(final HTableDescriptor desc,
      byte [] startKey, byte [] endKey, int firstRow, int nrows, Path rootdir)
  throws IOException {
    HRegionInfo hri = new HRegionInfo(desc.getName(), startKey, endKey);
    HRegion region = HRegion.createHRegion(hri, rootdir, UTIL.getConfiguration(), desc);
    LOG.info("Created region " + region.getRegionNameAsString());
    for(int i = firstRow; i < firstRow + nrows; i++) {
      Put put = new Put(Bytes.toBytes("row_" + String.format("%1$05d", i)));
      put.setWriteToWAL(false);
      put.add(COLUMN_NAME, null,  VALUE);
      region.put(put);
      if (i % 10000 == 0) {
        LOG.info("Flushing write #" + i);
        region.flushcache();
      }
    }
    region.close();
    region.getLog().closeAndDelete();
    return region;
  }

  protected void setupROOTAndMeta(Path rootdir, final HRegion [] regions)
  throws IOException {
    HRegion root =
      HRegion.createHRegion(HRegionInfo.ROOT_REGIONINFO, rootdir,
          UTIL.getConfiguration(), HTableDescriptor.ROOT_TABLEDESC);
    HRegion meta =
      HRegion.createHRegion(HRegionInfo.FIRST_META_REGIONINFO, rootdir,
      UTIL.getConfiguration(), HTableDescriptor.META_TABLEDESC);
    HRegion.addRegionToMETA(root, meta);
    for (HRegion r: regions) {
      HRegion.addRegionToMETA(meta, r);
    }
    meta.close();
    meta.getLog().closeAndDelete();
    root.close();
    root.getLog().closeAndDelete();
  }
}
