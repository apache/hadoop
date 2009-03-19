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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * {@Link TestHRegion} does a split but this TestCase adds testing of fast
 * split and manufactures odd-ball split scenarios.
 */
public class TestSplit extends HBaseClusterTestCase {
  static final Log LOG = LogFactory.getLog(TestSplit.class.getName());
  
  /** constructor */
  public TestSplit() {
    super();

    // Always compact if there is more than one store file.
    conf.setInt("hbase.hstore.compactionThreshold", 2);
    
    // Make lease timeout longer, lease checks less frequent
    conf.setInt("hbase.master.lease.period", 10 * 1000);
    conf.setInt("hbase.master.lease.thread.wakefrequency", 5 * 1000);
    
    conf.setInt("hbase.regionserver.lease.period", 10 * 1000);
    
    // Increase the amount of time between client retries
    conf.setLong("hbase.client.pause", 15 * 1000);

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
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      htd.addFamily(new HColumnDescriptor(COLFAMILY_NAME3));
      region = createNewHRegion(htd, null, null);
      basicSplit(region);
    } finally {
      if (region != null) {
        region.close();
        region.getLog().closeAndDelete();
      }
    }
  }
  
  /**
   * Test for HBASE-810
   * @throws Exception
   */
  public void testScanSplitOnRegion() throws Exception {
    HRegion region = null;
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      htd.addFamily(new HColumnDescriptor(COLFAMILY_NAME3));
      region = createNewHRegion(htd, null, null);
      addContent(region, COLFAMILY_NAME3);
      region.flushcache();
      final byte [] midkey = region.compactStores();
      assertNotNull(midkey);
      byte [][] cols = {COLFAMILY_NAME3};
      final InternalScanner s = region.getScanner(cols,
        HConstants.EMPTY_START_ROW, System.currentTimeMillis(), null);
      final HRegion regionForThread = region;
      
      Thread splitThread = new Thread() {
        @Override
        public void run() {
          try {
            split(regionForThread, midkey);
          } catch (IOException e) {
            fail("Unexpected exception " + e);
          } 
        }
      };
      splitThread.start();
      HRegionServer server = cluster.getRegionThreads().get(0).getRegionServer();
      long id = server.addScanner(s);
      for(int i = 0; i < 6; i++) {
        try {
          BatchUpdate update = 
            new BatchUpdate(region.getRegionInfo().getStartKey());
          update.put(COLFAMILY_NAME3, Bytes.toBytes("val"));
          region.batchUpdate(update);
          Thread.sleep(1000);
        }
        catch (InterruptedException e) {
          fail("Unexpected exception " + e);
        }
      }
      server.next(id);
      server.close(id);
    } catch(UnknownScannerException ex) {
      ex.printStackTrace();
      fail("Got the " + ex);
    } 
  }
  
  private void basicSplit(final HRegion region) throws Exception {
    LOG.info("" + addContent(region, COLFAMILY_NAME3));
    region.flushcache();
    byte [] splitRow = region.compactStores();
    assertNotNull(splitRow);
    LOG.info("SplitRow: " + Bytes.toString(splitRow));
    HRegion [] regions = split(region, splitRow);
    try {
      // Need to open the regions.
      // TODO: Add an 'open' to HRegion... don't do open by constructing
      // instance.
      for (int i = 0; i < regions.length; i++) {
        regions[i] = openClosedRegion(regions[i]);
      }
      // Assert can get rows out of new regions. Should be able to get first
      // row from first region and the midkey from second region.
      assertGet(regions[0], COLFAMILY_NAME3, Bytes.toBytes(START_KEY));
      assertGet(regions[1], COLFAMILY_NAME3, splitRow);
      // Test I can get scanner and that it starts at right place.
      assertScan(regions[0], COLFAMILY_NAME3,
          Bytes.toBytes(START_KEY));
      assertScan(regions[1], COLFAMILY_NAME3, splitRow);
      // Now prove can't split regions that have references.
      for (int i = 0; i < regions.length; i++) {
        // Add so much data to this region, we create a store file that is >
        // than one of our unsplitable references. it will.
        for (int j = 0; j < 2; j++) {
          addContent(regions[i], COLFAMILY_NAME3);
        }
        addContent(regions[i], COLFAMILY_NAME2);
        addContent(regions[i], COLFAMILY_NAME1);
        regions[i].flushcache();
      }

      byte [][] midkeys = new byte [regions.length][];
      // To make regions splitable force compaction.
      for (int i = 0; i < regions.length; i++) {
        midkeys[i] = regions[i].compactStores();
      }

      TreeMap<String, HRegion> sortedMap = new TreeMap<String, HRegion>();
      // Split these two daughter regions so then I'll have 4 regions. Will
      // split because added data above.
      for (int i = 0; i < regions.length; i++) {
        HRegion[] rs = null;
        if (midkeys[i] != null) {
          rs = split(regions[i], midkeys[i]);
          for (int j = 0; j < rs.length; j++) {
            sortedMap.put(Bytes.toString(rs[j].getRegionName()),
              openClosedRegion(rs[j]));
          }
        }
      }
      LOG.info("Made 4 regions");
      // The splits should have been even. Test I can get some arbitrary row out
      // of each.
      int interval = (LAST_CHAR - FIRST_CHAR) / 3;
      byte[] b = Bytes.toBytes(START_KEY);
      for (HRegion r : sortedMap.values()) {
        assertGet(r, COLFAMILY_NAME3, b);
        b[0] += interval;
      }
    } finally {
      for (int i = 0; i < regions.length; i++) {
        try {
          regions[i].close();
        } catch (IOException e) {
          // Ignore.
        }
      }
    }
  }
  
  private void assertGet(final HRegion r, final byte [] family, final byte [] k)
  throws IOException {
    // Now I have k, get values out and assert they are as expected.
    Cell[] results = r.get(k, family, -1, Integer.MAX_VALUE);
    for (int j = 0; j < results.length; j++) {
      byte [] tmp = results[j].getValue();
      // Row should be equal to value every time.
      assertTrue(Bytes.equals(k, tmp));
    }
  }
  
  /*
   * Assert first value in the passed region is <code>firstValue</code>.
   * @param r
   * @param column
   * @param firstValue
   * @throws IOException
   */
  private void assertScan(final HRegion r, final byte [] column,
      final byte [] firstValue)
  throws IOException {
    byte [][] cols = {column};
    InternalScanner s = r.getScanner(cols,
      HConstants.EMPTY_START_ROW, System.currentTimeMillis(), null);
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<byte [], Cell> curVals =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      boolean first = true;
      OUTER_LOOP: while(s.next(curKey, curVals)) {
        for (Map.Entry<byte[], Cell> entry : curVals.entrySet()) {
          byte [] val = entry.getValue().getValue();
          byte [] curval = val;
          if (first) {
            first = false;
            assertTrue(Bytes.compareTo(curval, firstValue) == 0);
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
  
  protected HRegion [] split(final HRegion r, final byte [] splitRow)
  throws IOException {
    // Assert can get mid key from passed region.
    assertGet(r, COLFAMILY_NAME3, splitRow);
    HRegion [] regions = r.splitRegion(splitRow);
    assertEquals(regions.length, 2);
    return regions;
  }
}