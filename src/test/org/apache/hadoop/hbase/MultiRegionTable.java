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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;

/**
 * Utility class to build a table of multiple regions.
 */
public class MultiRegionTable extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(MultiRegionTable.class.getName());

  /**
   * Make a multi-region table.  Presumption is that table already exists.
   * Makes it multi-region by filling with data and provoking splits.
   * Asserts parent region is cleaned up after its daughter splits release all
   * references.
   * @param conf
   * @param cluster
   * @param localFs
   * @param tableName
   * @param columnName
   * @throws IOException
   */
  public static void makeMultiRegionTable(Configuration conf,
      MiniHBaseCluster cluster, FileSystem localFs, String tableName,
      String columnName)
  throws IOException {  
    final int retries = 10; 
    final long waitTime =
      conf.getLong("hbase.master.meta.thread.rescanfrequency", 10L * 1000L);
    
    // This size should make it so we always split using the addContent
    // below.  After adding all data, the first region is 1.3M. Should
    // set max filesize to be <= 1M.
    assertTrue(conf.getLong("hbase.hregion.max.filesize",
      HConstants.DEFAULT_MAX_FILE_SIZE) <= 1024 * 1024);

    FileSystem fs = (cluster.getDFSCluster() == null) ?
      localFs : cluster.getDFSCluster().getFileSystem();
    assertNotNull(fs);
    Path d = fs.makeQualified(new Path(conf.get(HConstants.HBASE_DIR)));

    // Get connection on the meta table and get count of rows.
    HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
    int count = count(meta, tableName);
    HTable t = new HTable(conf, new Text(tableName));
    addContent(new HTableIncommon(t), columnName);
    
    // All is running in the one JVM so I should be able to get the single
    // region instance and bring on a split.
    HRegionInfo hri =
      t.getRegionLocation(HConstants.EMPTY_START_ROW).getRegionInfo();
    HRegion r = cluster.regionThreads.get(0).getRegionServer().
      onlineRegions.get(hri.getRegionName());
    
    // Flush will provoke a split next time the split-checker thread runs.
    r.flushcache(false);
    
    // Now, wait until split makes it into the meta table.
    int oldCount = count;
    for (int i = 0; i < retries;  i++) {
      count = count(meta, tableName);
      if (count > oldCount) {
        break;
      }
      try {
        Thread.sleep(waitTime);
      } catch (InterruptedException e) {
        // continue
      }
    }
    if (count <= oldCount) {
      throw new IOException("Failed waiting on splits to show up");
    }
    
    // Get info on the parent from the meta table.  Pass in 'hri'. Its the
    // region we have been dealing with up to this. Its the parent of the
    // region split.
    Map<Text, byte []> data = getSplitParentInfo(meta, hri);
    HRegionInfo parent =
      Writables.getHRegionInfoOrNull(data.get(HConstants.COL_REGIONINFO));
    assertTrue(parent.isOffline());
    assertTrue(parent.isSplit());
    HRegionInfo splitA =
      Writables.getHRegionInfoOrNull(data.get(HConstants.COL_SPLITA));
    HRegionInfo splitB =
      Writables.getHRegionInfoOrNull(data.get(HConstants.COL_SPLITB));
    Path parentDir = HRegion.getRegionDir(d, parent.getRegionName());
    assertTrue(fs.exists(parentDir));
    LOG.info("Split happened. Parent is " + parent.getRegionName() +
        " and daughters are " + splitA.getRegionName() + ", " +
        splitB.getRegionName());
    
    // Recalibrate will cause us to wait on new regions' deployment
    
    recalibrate(t, new Text(columnName), retries, waitTime);
    
    // Compact a region at a time so we can test case where one region has
    // no references but the other still has some
    
    compact(cluster, splitA);
    
    // Wait till the parent only has reference to remaining split, one that
    // still has references.
    
    while (getSplitParentInfo(meta, parent).size() == 3) {
      try {
        Thread.sleep(waitTime);
      } catch (InterruptedException e) {
        // continue
      }
    }
    LOG.info("Parent split returned " +
        getSplitParentInfo(meta, parent).keySet().toString());
    
    // Call second split.
    
    compact(cluster, splitB);
    
    // Now wait until parent disappears.
    
    LOG.info("Waiting on parent " + parent.getRegionName() + " to disappear");
    for (int i = 0; i < retries; i++) {
      if (getSplitParentInfo(meta, parent) == null) {
        break;
      }
      
      try {
        Thread.sleep(waitTime);
      } catch (InterruptedException e) {
        // continue
      }
    }
    assertNull(getSplitParentInfo(meta, parent));
    
    // Assert cleaned up.
    
    for (int i = 0; i < retries; i++) {
      if (!fs.exists(parentDir)) {
        break;
      }
      try {
        Thread.sleep(waitTime);
      } catch (InterruptedException e) {
        // continue
      }
    }
    assertFalse(fs.exists(parentDir));
  }

  /*
   * Count of regions in passed meta table.
   * @param t
   * @param column
   * @return
   * @throws IOException
   */
  private static int count(final HTable t, final String tableName)
    throws IOException {
    
    int size = 0;
    Text [] cols = new Text[] {HConstants.COLUMN_FAMILY};
    HScannerInterface s = t.obtainScanner(cols, HConstants.EMPTY_START_ROW,
      System.currentTimeMillis(), null);
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
      while(s.next(curKey, curVals)) {
        HRegionInfo hri = Writables.
          getHRegionInfoOrNull(curVals.get(HConstants.COL_REGIONINFO));
        if (hri.getTableDesc().getName().toString().equals(tableName)) {
          size++;
        }
      }
      return size;
    } finally {
      s.close();
    }
  }

  /*
   * @return Return row info for passed in region or null if not found in scan.
   */
  private static Map<Text, byte []> getSplitParentInfo(final HTable t,
      final HRegionInfo parent)
  throws IOException {  
    HScannerInterface s = t.obtainScanner(HConstants.COLUMN_FAMILY_ARRAY,
        HConstants.EMPTY_START_ROW, System.currentTimeMillis(), null);
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
      while(s.next(curKey, curVals)) {
        HRegionInfo hri = Writables.
          getHRegionInfoOrNull(curVals.get(HConstants.COL_REGIONINFO));
        if (hri == null) {
          continue;
        }
        // Make sure I get the parent.
        if (hri.getRegionName().toString().
            equals(parent.getRegionName().toString())) {
          return curVals;
        }
      }
      return null;
    } finally {
      s.close();
    }   
  }

  /*
   * Recalibrate passed in HTable.  Run after change in region geography.
   * Open a scanner on the table. This will force HTable to recalibrate
   * and in doing so, will force us to wait until the new child regions
   * come on-line (since they are no longer automatically served by the 
   * HRegionServer that was serving the parent. In this test they will
   * end up on the same server (since there is only one), but we have to
   * wait until the master assigns them. 
   * @param t
   * @param retries
   */
  private static void recalibrate(final HTable t, final Text column,
      final int retries, final long waitTime) throws IOException {
    
    for (int i = 0; i < retries; i++) {
      try {
        HScannerInterface s =
          t.obtainScanner(new Text[] {column}, HConstants.EMPTY_START_ROW);
        try {
          HStoreKey key = new HStoreKey();
          TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
          s.next(key, results);
          break;
        } finally {
          s.close();
        }
      } catch (NotServingRegionException x) {
        System.out.println("it's alright");
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
  }

  /*
   * Compact the passed in region <code>r</code>. 
   * @param cluster
   * @param r
   * @throws IOException
   */
  private static void compact(final MiniHBaseCluster cluster,
      final HRegionInfo r) throws IOException {
    
    LOG.info("Starting compaction");
    for (MiniHBaseCluster.RegionServerThread thread: cluster.regionThreads) {
      SortedMap<Text, HRegion> regions = thread.getRegionServer().onlineRegions;
      
      // Retry if ConcurrentModification... alternative of sync'ing is not
      // worth it for sake of unit test.
      
      for (int i = 0; i < 10; i++) {
        try {
          for (HRegion online: regions.values()) {
            if (online.getRegionName().toString().
                equals(r.getRegionName().toString())) {
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
}