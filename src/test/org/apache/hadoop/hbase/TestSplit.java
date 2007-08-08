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
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;

/**
 * {@Link TestHRegion} does a split but this TestCase adds testing of fast
 * split and manufactures odd-ball split scenarios.
 */
public class TestSplit extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestSplit.class);
  private final static String COLFAMILY_NAME1 = "colfamily1:";
  private final static String COLFAMILY_NAME2 = "colfamily2:";
  private final static String COLFAMILY_NAME3 = "colfamily3:";
  private Path testDir = null;
  private FileSystem fs = null;
  private static final char FIRST_CHAR = 'a';
  private static final char LAST_CHAR = 'z';
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.testDir = getUnitTestdir(getName());
    this.fs = FileSystem.getLocal(this.conf);
    if (fs.exists(testDir)) {
      fs.delete(testDir);
    }
    // This size should make it so we always split using the addContent
    // below.  After adding all data, the first region is 1.3M
    conf.setLong("hbase.hregion.max.filesize", 1024 * 128);
  }
  
  @Override
  public void tearDown() throws Exception {
    try {
      if (this.fs.exists(testDir)) {
        this.fs.delete(testDir);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    super.tearDown();
  }
  
  /**
   * Splits twice and verifies getting from each of the split regions.
   * @throws Exception
   */
  public void testBasicSplit() throws Exception {
    HRegion region = null;
    HLog hlog = new HLog(this.fs, this.testDir, this.conf);
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      HRegionInfo hri = new HRegionInfo(1, htd, null, null);
      region = new HRegion(testDir, hlog, fs, this.conf, hri, null);
      basicSplit(region);
    } finally {
      if (region != null) {
        region.close();
      }
      hlog.closeAndDelete();
    }
  }
  
  private HTableDescriptor createTableDescriptor(final String name) {
    HTableDescriptor htd = new HTableDescriptor(name);
    htd.addFamily(new HColumnDescriptor(COLFAMILY_NAME1));
    htd.addFamily(new HColumnDescriptor(COLFAMILY_NAME2));
    htd.addFamily(new HColumnDescriptor(COLFAMILY_NAME3));
    return htd;
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
    final int timeout = 60;
    // Start up a hbase cluster
    this.conf.set(HConstants.HBASE_DIR, this.testDir.toString());
    MiniHBaseCluster.MasterThread masterThread =
      MiniHBaseCluster.startMaster(this.conf);
    List<MiniHBaseCluster.RegionServerThread> regionServerThreads =
      MiniHBaseCluster.startRegionServers(this.conf, 1);
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
      addContent(t, COLFAMILY_NAME3);
      // All is running in the one JVM so I should be able to get the
      // region instance and bring on a split.
      HRegionInfo hri =
        t.getRegionLocation(HConstants.EMPTY_START_ROW).getRegionInfo();
      HRegion r = null;
      synchronized(regionServerThreads) {
        r = regionServerThreads.get(0).getRegionServer().onlineRegions.
          get(hri.getRegionName());
      }
      // Flush will provoke a split next time the split-checker thread runs.
      r.flushcache(false);
      // Now, wait until split makes it into the meta table.
      for (int i = 0; i < timeout &&
        (count(meta, HConstants.COLUMN_FAMILY_STR) <= count); i++) {
        Thread.sleep(1000);
      }
      int oldCount = count;
      count = count(meta, HConstants.COLUMN_FAMILY_STR);
      if (count <= oldCount) {
        throw new IOException("Failed waiting on splits to show up");
      }
      HRegionInfo parent = getSplitParent(meta);
      assertTrue(parent.isOffline());
      Path parentDir =
        HRegion.getRegionDir(this.testDir, parent.getRegionName());
      assertTrue(this.fs.exists(parentDir));
      LOG.info("Split happened and parent " + parent.getRegionName() + " is " +
        "offline");
      // Now, force a compaction.  This will rewrite references and make it
      // so the parent region becomes deletable.
      LOG.info("Starting compaction");
      synchronized(regionServerThreads) {
        for (MiniHBaseCluster.RegionServerThread thread: regionServerThreads) {
          SortedMap<Text, HRegion> regions =
            thread.getRegionServer().onlineRegions;
          // Retry if ConcurrentModification... alternative of sync'ing is not
          // worth it for sake of unit test.
          for (int i = 0; i < 10; i++) {
            try {
              for (HRegion online: regions.values()) {
                if (online.getRegionName().toString().startsWith(getName())) {
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
      }
      
      // Now wait until parent disappears.
      LOG.info("Waiting on parent " + parent.getRegionName() +
        " to disappear");
      for (int i = 0; i < timeout && getSplitParent(meta) != null; i++) {
        Thread.sleep(1000);
      }
      assertTrue(getSplitParent(meta) == null);
      // Assert cleaned up.
      assertFalse(this.fs.exists(parentDir));
    } finally {
      MiniHBaseCluster.shutdown(masterThread, regionServerThreads);
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
        HRegionInfo hri = (HRegionInfo)Writables.
          getWritable(curVals.get(HConstants.COL_REGIONINFO), new HRegionInfo());
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
  
  private void addContent(final HRegion r, final String column)
  throws IOException {
    Text startKey = r.getRegionInfo().getStartKey();
    Text endKey = r.getRegionInfo().getEndKey();
    byte [] startKeyBytes = startKey.getBytes();
    if (startKeyBytes == null || startKeyBytes.length == 0) {
      startKeyBytes = new byte [] {FIRST_CHAR, FIRST_CHAR, FIRST_CHAR};
    }
    // Add rows of three characters.  The first character starts with the
    // 'a' character and runs up to 'z'.  Per first character, we run the
    // second character over same range.  And same for the third so rows
    // (and values) look like this: 'aaa', 'aab', 'aac', etc.
    char secondCharStart = (char)startKeyBytes[1];
    char thirdCharStart = (char)startKeyBytes[2];
    EXIT_ALL_LOOPS: for (char c = (char)startKeyBytes[0]; c <= LAST_CHAR; c++) {
      for (char d = secondCharStart; d <= LAST_CHAR; d++) {
        for (char e = thirdCharStart; e <= LAST_CHAR; e++) {
          byte [] bytes = new byte [] {(byte)c, (byte)d, (byte)e};
          Text t = new Text(new String(bytes));
          if (endKey != null && endKey.getLength() > 0
              && endKey.compareTo(t) <= 0) {
            break EXIT_ALL_LOOPS;
          }
          long lockid = r.startUpdate(t);
          try {
            r.put(lockid, new Text(column), bytes);
            r.commit(lockid, System.currentTimeMillis());
            lockid = -1;
          } finally {
            if (lockid != -1) {
              r.abort(lockid);
            }
          }
        }
        // Set start character back to FIRST_CHAR after we've done first loop.
        thirdCharStart = FIRST_CHAR;
      }
      secondCharStart = FIRST_CHAR;
    }
  }
  
  // TODO: Have HTable and HRegion implement interface that has in it
  // startUpdate, put, delete, commit, abort, etc.
  private void addContent(final HTable table, final String column)
  throws IOException {
    byte [] startKeyBytes = new byte [] {FIRST_CHAR, FIRST_CHAR, FIRST_CHAR};
    // Add rows of three characters.  The first character starts with the
    // 'a' character and runs up to 'z'.  Per first character, we run the
    // second character over same range.  And same for the third so rows
    // (and values) look like this: 'aaa', 'aab', 'aac', etc.
    char secondCharStart = (char)startKeyBytes[1];
    char thirdCharStart = (char)startKeyBytes[2];
    for (char c = (char)startKeyBytes[0]; c <= LAST_CHAR; c++) {
      for (char d = secondCharStart; d <= LAST_CHAR; d++) {
        for (char e = thirdCharStart; e <= LAST_CHAR; e++) {
          byte [] bytes = new byte [] {(byte)c, (byte)d, (byte)e};
          Text t = new Text(new String(bytes));
          long lockid = table.startBatchUpdate(t);
          try {
            table.put(lockid, new Text(column), bytes);
            table.commit(lockid, System.currentTimeMillis());
            lockid = -1;
          } finally {
            if (lockid != -1) {
              table.abort(lockid);
            }
          }
        }
        // Set start character back to FIRST_CHAR after we've done first loop.
        thirdCharStart = FIRST_CHAR;
      }
      secondCharStart = FIRST_CHAR;
    }
  }
}