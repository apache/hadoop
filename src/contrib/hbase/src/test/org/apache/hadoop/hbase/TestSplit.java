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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * {@Link TestHRegion} does a split but this TestCase adds testing of fast
 * split and manufactures odd-ball split scenarios.
 */
public class TestSplit extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestSplit.class.getName());
  
  /** constructor */
  public TestSplit() {
    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger(this.getClass().getPackage().getName()).
      setLevel(Level.DEBUG);
  }

  /** {@inheritDoc} */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // This size should make it so we always split using the addContent
    // below.  After adding all data, the first region is 1.3M
    conf.setLong("hbase.hregion.max.filesize", 1024 * 128);
  }
  
  /**
   * Splits twice and verifies getting from each of the split regions.
   * @throws Exception
   */
  public void testBasicSplit() throws Exception {
    HRegion region = null;
    HLog hlog = new HLog(this.localFs, this.testDir, this.conf);
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      HRegionInfo hri = new HRegionInfo(1, htd, null, null);
      region = new HRegion(testDir, hlog, this.localFs, this.conf, hri, null);
      basicSplit(region);
    } finally {
      if (region != null) {
        region.close();
      }
      hlog.closeAndDelete();
    }
  }
  
  private void basicSplit(final HRegion region) throws Exception {
    addContent(region, COLFAMILY_NAME3);
    region.internalFlushcache();
    Text midkey = new Text();
    assertTrue(region.needsSplit(midkey));
    HRegion [] regions = split(region);
    // Assert can get rows out of new regions.  Should be able to get first
    // row from first region and the midkey from second region.
    byte [] b = new byte [] {FIRST_CHAR, FIRST_CHAR, FIRST_CHAR};
    assertGet(regions[0], COLFAMILY_NAME3, new Text(b));
    assertGet(regions[1], COLFAMILY_NAME3, midkey);
    // Test I can get scanner and that it starts at right place.
    assertScan(regions[0], COLFAMILY_NAME3, new Text(b));
    assertScan(regions[1], COLFAMILY_NAME3, midkey);
    // Now prove can't split regions that have references.
    Text [] midkeys = new Text[regions.length];
    for (int i = 0; i < regions.length; i++) {
      midkeys[i] = new Text();
      // Even after above splits, still needs split but after splits its
      // unsplitable because biggest store file is reference.  References
      // make the store unsplittable, until something bigger comes along.
      assertFalse(regions[i].needsSplit(midkeys[i]));
      // Add so much data to this region, we create a store file that is > than
      // one of our unsplitable references.
      // it will.
      for (int j = 0; j < 2; j++) {
        addContent(regions[i], COLFAMILY_NAME3);
      }
      addContent(regions[i], COLFAMILY_NAME2);
      addContent(regions[i], COLFAMILY_NAME1);
      regions[i].internalFlushcache();
    }
    
    // Assert that even if one store file is larger than a reference, the
    // region is still deemed unsplitable (Can't split region if references
    // presen).
    for (int i = 0; i < regions.length; i++) {
      midkeys[i] = new Text();
      // Even after above splits, still needs split but after splits its
      // unsplitable because biggest store file is reference.  References
      // make the store unsplittable, until something bigger comes along.
      assertFalse(regions[i].needsSplit(midkeys[i]));
    }
    
    // To make regions splitable force compaction.
    for (int i = 0; i < regions.length; i++) {
      assertTrue(regions[i].compactStores());
    }

    TreeMap<String, HRegion> sortedMap = new TreeMap<String, HRegion>();
    // Split these two daughter regions so then I'll have 4 regions.  Will
    // split because added data above.
    for (int i = 0; i < regions.length; i++) {
      HRegion [] rs = split(regions[i]);
      for (int j = 0; j < rs.length; j++) {
        sortedMap.put(rs[j].getRegionName().toString(), rs[j]);
      }
    }
    LOG.info("Made 4 regions");
    // The splits should have been even.  Test I can get some arbitrary row out
    // of each.
    int interval = (LAST_CHAR - FIRST_CHAR) / 3;
    for (HRegion r: sortedMap.values()) {
      assertGet(r, COLFAMILY_NAME3, new Text(new String(b)));
      b[0] += interval;
    }
  }
  
  /**
   * Test that a region is cleaned up after its daughter splits release all
   * references.
   * @throws Exception
   */
  public void testSplitRegionIsDeleted() throws Exception {
    final int retries = 10; 
    // Start up a hbase cluster
    MiniHBaseCluster cluster = new MiniHBaseCluster(conf, 1, true);
    Path d = cluster.regionThreads.get(0).getRegionServer().rootDir;
    FileSystem fs = (cluster.getDFSCluster() == null)?
      this.localFs:
      cluster.getDFSCluster().getFileSystem();
    HTable meta = null;
    HTable t = null;
    try {
      // Create a table.
      HBaseAdmin admin = new HBaseAdmin(this.conf);
      admin.createTable(createTableDescriptor(getName()));
      // Get connection on the meta table and get count of rows.
      meta = new HTable(this.conf, HConstants.META_TABLE_NAME);
      int count = count(meta, HConstants.COLUMN_FAMILY_STR);
      t = new HTable(this.conf, new Text(getName()));
      addContent(new HTableLoader(t), COLFAMILY_NAME3);
      // All is running in the one JVM so I should be able to get the
      // region instance and bring on a split.
      HRegionInfo hri =
        t.getRegionLocation(HConstants.EMPTY_START_ROW).getRegionInfo();
      HRegion r =
        cluster.regionThreads.get(0).getRegionServer().onlineRegions.get(
            hri.getRegionName());
      // Flush will provoke a split next time the split-checker thread runs.
      r.flushcache(false);
      // Now, wait until split makes it into the meta table.
      for (int i = 0; i < retries &&
      (count(meta, HConstants.COLUMN_FAMILY_STR) <= count); i++) {
        Thread.sleep(5000);
      }
      int oldCount = count;
      count = count(meta, HConstants.COLUMN_FAMILY_STR);
      if (count <= oldCount) {
        throw new IOException("Failed waiting on splits to show up");
      }
      HRegionInfo parent = getSplitParent(meta);
      assertTrue(parent.isOffline());
      Path parentDir = HRegion.getRegionDir(d, parent.getRegionName());
      assertTrue(fs.exists(parentDir));
      LOG.info("Split happened and parent " + parent.getRegionName() + " is " +
      "offline");
      for (int i = 0; i < retries; i++) {
        // Now open a scanner on the table. This will force HTable to recalibrate
        // and in doing so, will force us to wait until the new child regions
        // come on-line (since they are no longer automatically served by the 
        // HRegionServer that was serving the parent. In this test they will
        // end up on the same server (since there is only one), but we have to
        // wait until the master assigns them.
        try {
          HScannerInterface s =
            t.obtainScanner(new Text[] {new Text(COLFAMILY_NAME3)},
                HConstants.EMPTY_START_ROW);
          try {
            HStoreKey key = new HStoreKey();
            TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
            s.next(key, results);
            break;

          } finally {
            s.close();
          }
        } catch (NotServingRegionException x) {
          Thread.sleep(5000);
        }
      }
      // Now, force a compaction.  This will rewrite references and make it
      // so the parent region becomes deletable.
      LOG.info("Starting compaction");
      for (MiniHBaseCluster.RegionServerThread thread: cluster.regionThreads) {
        SortedMap<Text, HRegion> regions =
          thread.getRegionServer().onlineRegions;
        // Retry if ConcurrentModification... alternative of sync'ing is not
        // worth it for sake of unit test.
        for (int i = 0; i < 10; i++) {
          try {
            for (HRegion online: regions.values()) {
              if (online.getTableDesc().getName().toString().equals(getName())) {
                online.compactStores();
              }
            }
            break;
          } catch (ConcurrentModificationException e) {
            LOG.warn("Retrying because ..." + e.toString() + " -- one or " +
            "two should be fine");
            continue;
          }
        }
      }

      // Now wait until parent disappears.
      LOG.info("Waiting on parent " + parent.getRegionName() +
      " to disappear");
      for (int i = 0; i < retries && getSplitParent(meta) != null; i++) {
        Thread.sleep(5000);
      }
      assertTrue(getSplitParent(meta) == null);
      // Assert cleaned up.
      for (int i = 0; i < retries && fs.exists(parentDir); i++) {
        Thread.sleep(5000);
      }
      assertFalse(fs.exists(parentDir));
    } finally {
      cluster.shutdown();
    }
  }
  
  private void assertGet(final HRegion r, final String family, final Text k)
  throws IOException {
    // Now I have k, get values out and assert they are as expected.
    byte [][] results = r.get(k, new Text(family),
      Integer.MAX_VALUE);
    for (int j = 0; j < results.length; j++) {
      Text tmp = new Text(results[j]);
      // Row should be equal to value every time.
      assertEquals(k.toString(), tmp.toString());
    }
  }
  
  private HRegionInfo getSplitParent(final HTable t)
  throws IOException {
    HRegionInfo result = null;
    HScannerInterface s = t.obtainScanner(HConstants.COL_REGIONINFO_ARRAY,
      HConstants.EMPTY_START_ROW, System.currentTimeMillis(), null);
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
      while(s.next(curKey, curVals)) {
        byte[] bytes = curVals.get(HConstants.COL_REGIONINFO);
        if (bytes == null || bytes.length == 0) {
          continue;
        }
        HRegionInfo hri =
          (HRegionInfo) Writables.getWritable(bytes, new HRegionInfo());
        
        // Assert that if region is a split region, that it is also offline.
        // Otherwise, if not a split region, assert that it is online.
        if (hri.isSplit() && hri.isOffline()) {
          result = hri;
          break;
        }
      }
      return result;
    } finally {
      s.close();
    }   
  }
  
  /*
   * Count of rows in table for given column. 
   * @param t
   * @param column
   * @return
   * @throws IOException
   */
  private int count(final HTable t, final String column)
  throws IOException {
    int size = 0;
    Text [] cols = new Text[] {new Text(column)};
    HScannerInterface s = t.obtainScanner(cols, HConstants.EMPTY_START_ROW,
      System.currentTimeMillis(), null);
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
      while(s.next(curKey, curVals)) {
        size++;
      }
      return size;
    } finally {
      s.close();
    }
  }
  
  /*
   * Assert first value in the passed region is <code>firstValue</code>.
   * @param r
   * @param column
   * @param firstValue
   * @throws IOException
   */
  private void assertScan(final HRegion r, final String column,
      final Text firstValue)
  throws IOException {
    Text [] cols = new Text[] {new Text(column)};
    HInternalScannerInterface s = r.getScanner(cols,
      HConstants.EMPTY_START_ROW, System.currentTimeMillis(), null);
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
      boolean first = true;
      OUTER_LOOP: while(s.next(curKey, curVals)) {
        for(Text col: curVals.keySet()) {
          byte [] val = curVals.get(col);
          Text curval = new Text(val);
          if (first) {
            first = false;
            assertTrue(curval.compareTo(firstValue) == 0);
          } else {
            // Not asserting anything.  Might as well break.
            break OUTER_LOOP;
          }
        }
      }
    } finally {
      s.close();
    }
  }
  
  private HRegion [] split(final HRegion r) throws IOException {
    Text midKey = new Text();
    assertTrue(r.needsSplit(midKey));
    // Assert can get mid key from passed region.
    assertGet(r, COLFAMILY_NAME3, midKey);
    HRegion [] regions = r.closeAndSplit(midKey, null);
    assertEquals(regions.length, 2);
    return regions;
  }
}
