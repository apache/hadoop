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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Basic stand-alone testing of HRegion.
 * 
 * A lot of the meta information for an HRegion now lives inside other
 * HRegions or in the HBaseMaster, so only basic testing is possible.
 */
public class TestHRegion extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestHRegion.class);
  
  /**
   * Since all the "tests" depend on the results of the previous test, they are
   * not Junit tests that can stand alone. Consequently we have a single Junit
   * test that runs the "sub-tests" as private methods.
   * @throws IOException 
   */
  public void testHRegion() throws IOException {
    try {
      init();
      locks();
      badPuts();
      basic();
      scan();
      splitAndMerge();
      read();
    } finally {
      shutdownDfs(cluster);
    }
  }
  
  
  private static final int FIRST_ROW = 1;
  private static final int NUM_VALS = 1000;
  private static final byte [] CONTENTS_BASIC = Bytes.toBytes("contents:basic");
  private static final String CONTENTSTR = "contentstr";
  private static final String ANCHORNUM = "anchor:anchornum-";
  private static final String ANCHORSTR = "anchorstr";
  private static final byte [] CONTENTS_FIRSTCOL = Bytes.toBytes("contents:firstcol");
  private static final byte [] ANCHOR_SECONDCOL = Bytes.toBytes("anchor:secondcol");
  
  private MiniDFSCluster cluster = null;
  private HTableDescriptor desc = null;
  HRegion r = null;
  HRegionIncommon region = null;
  
  private static int numInserted = 0;
  
  @Override
  public void setUp() throws Exception {
    this.conf.set("hbase.hstore.compactionThreshold", "2");

    conf.setLong("hbase.hregion.max.filesize", 65536);

    cluster = new MiniDFSCluster(conf, 2, true, (String[])null);
    fs = cluster.getFileSystem();
    
    // Set the hbase.rootdir to be the home directory in mini dfs.
    this.conf.set(HConstants.HBASE_DIR,
      this.cluster.getFileSystem().getHomeDirectory().toString());
    
    super.setUp();
  }

  // Create directories, start mini cluster, etc.
  
  private void init() throws IOException {
    desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor("contents:"));
    desc.addFamily(new HColumnDescriptor("anchor:"));
    r = createNewHRegion(desc, null, null);
    region = new HRegionIncommon(r);
    LOG.info("setup completed.");
  }

  // Test basic functionality. Writes to contents:basic and anchor:anchornum-*

  private void basic() throws IOException {
    long startTime = System.currentTimeMillis();

    // Write out a bunch of values

    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      BatchUpdate batchUpdate = 
        new BatchUpdate(Bytes.toBytes("row_" + k), System.currentTimeMillis());
      batchUpdate.put(CONTENTS_BASIC,
          (CONTENTSTR + k).getBytes(HConstants.UTF8_ENCODING));
      batchUpdate.put(Bytes.toBytes(ANCHORNUM + k),
          (ANCHORSTR + k).getBytes(HConstants.UTF8_ENCODING));
      region.commit(batchUpdate);
    }
    LOG.info("Write " + NUM_VALS + " rows. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // Flush cache

    startTime = System.currentTimeMillis();

    region.flushcache();

    LOG.info("Cache flush elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // Read them back in

    startTime = System.currentTimeMillis();

    byte [] collabel = null;
    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      byte [] rowlabel = Bytes.toBytes("row_" + k);
      if (k % 100 == 0) LOG.info(Bytes.toString(rowlabel));
      byte [] bodydata = region.get(rowlabel, CONTENTS_BASIC).getValue();
      assertNotNull(bodydata);
      String bodystr = new String(bodydata, HConstants.UTF8_ENCODING).trim();
      String teststr = CONTENTSTR + k;
      assertEquals("Incorrect value for key: (" + rowlabel + "," + CONTENTS_BASIC
          + "), expected: '" + teststr + "' got: '" + bodystr + "'",
          bodystr, teststr);
      collabel = Bytes.toBytes(ANCHORNUM + k);
      bodydata = region.get(rowlabel, collabel).getValue();
      bodystr = new String(bodydata, HConstants.UTF8_ENCODING).trim();
      teststr = ANCHORSTR + k;
      assertEquals("Incorrect value for key: (" + rowlabel + "," + collabel
          + "), expected: '" + teststr + "' got: '" + bodystr + "'",
          bodystr, teststr);
    }

    LOG.info("Read " + NUM_VALS + " rows. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    LOG.info("basic completed.");
  }
  
  private void badPuts() {
    // Try column name not registered in the table.    
    boolean exceptionThrown = false;
    exceptionThrown = false;
    try {
      BatchUpdate batchUpdate = new BatchUpdate(Bytes.toBytes("Some old key"));
      String unregisteredColName = "FamilyGroup:FamilyLabel";
      batchUpdate.put(Bytes.toBytes(unregisteredColName),
        unregisteredColName.getBytes(HConstants.UTF8_ENCODING));
      region.commit(batchUpdate);
    } catch (IOException e) {
      exceptionThrown = true;
    } finally {
    }
    assertTrue("Bad family", exceptionThrown);
    LOG.info("badPuts completed.");
  }
  
  /**
   * Test getting and releasing locks.
   */
  private void locks() {
    final int threadCount = 10;
    final int lockCount = 10;
    
    List<Thread>threads = new ArrayList<Thread>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      threads.add(new Thread(Integer.toString(i)) {
        @Override
        public void run() {
          Integer [] lockids = new Integer[lockCount];
          // Get locks.
          for (int i = 0; i < lockCount; i++) {
            try {
              byte [] rowid = Bytes.toBytes(Integer.toString(i));
              lockids[i] = r.obtainRowLock(rowid);
              rowid.equals(r.getRowFromLock(lockids[i]));
              LOG.debug(getName() + " locked " + rowid.toString());
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
          LOG.debug(getName() + " set " +
              Integer.toString(lockCount) + " locks");
          
          // Abort outstanding locks.
          for (int i = lockCount - 1; i >= 0; i--) {
            r.releaseRowLock(lockids[i]);
            LOG.debug(getName() + " unlocked " + i);
          }
          LOG.debug(getName() + " released " +
              Integer.toString(lockCount) + " locks");
        }
      });
    }
    
    // Startup all our threads.
    for (Thread t : threads) {
      t.start();
    }
    
    // Now wait around till all are done.
    for (Thread t: threads) {
      while (t.isAlive()) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          // Go around again.
        }
      }
    }
    LOG.info("locks completed.");
  }

  // Test scanners. Writes contents:firstcol and anchor:secondcol
  
  private void scan() throws IOException {
    byte [] cols[] = {
        CONTENTS_FIRSTCOL,
        ANCHOR_SECONDCOL
    };

    // Test the Scanner!!!
    String[] vals1 = new String[1000];
    for(int k = 0; k < vals1.length; k++) {
      vals1[k] = Integer.toString(k);
    }

    // 1.  Insert a bunch of values
    
    long startTime = System.currentTimeMillis();

    for(int k = 0; k < vals1.length / 2; k++) {
      String kLabel = String.format("%1$03d", k);

      BatchUpdate batchUpdate = 
        new BatchUpdate(Bytes.toBytes("row_vals1_" + kLabel), 
          System.currentTimeMillis());
      batchUpdate.put(cols[0], vals1[k].getBytes(HConstants.UTF8_ENCODING));
      batchUpdate.put(cols[1], vals1[k].getBytes(HConstants.UTF8_ENCODING));
      region.commit(batchUpdate);
      numInserted += 2;
    }

    LOG.info("Write " + (vals1.length / 2) + " elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // 2.  Scan from cache
    
    startTime = System.currentTimeMillis();

    InternalScanner s =
      r.getScanner(cols, HConstants.EMPTY_START_ROW, System.currentTimeMillis(), null);
    int numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<byte [], Cell> curVals =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<byte []> it = curVals.keySet().iterator(); it.hasNext(); ) {
          byte [] col = it.next();
          byte [] val = curVals.get(col).getValue();
          int curval =
            Integer.parseInt(new String(val, HConstants.UTF8_ENCODING).trim());
          for(int j = 0; j < cols.length; j++) {
            if (Bytes.compareTo(col, cols[j]) == 0) {
              assertEquals("Error at:" + curKey.getRow() + "/"
                  + curKey.getTimestamp()
                  + ", Value for " + col + " should be: " + k
                  + ", but was fetched as: " + curval, k, curval);
              numFetched++;
            }
          }
        }
        curVals.clear();
        k++;
      }
    } finally {
      s.close();
    }
    assertEquals("Inserted " + numInserted + " values, but fetched " + numFetched, numInserted, numFetched);

    LOG.info("Scanned " + (vals1.length / 2)
        + " rows from cache. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // 3.  Flush to disk
    
    startTime = System.currentTimeMillis();
    
    region.flushcache();

    LOG.info("Cache flush elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // 4.  Scan from disk
    
    startTime = System.currentTimeMillis();
    
    s = r.getScanner(cols, HConstants.EMPTY_START_ROW,
      System.currentTimeMillis(), null);
    numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<byte [], Cell> curVals =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<byte []> it = curVals.keySet().iterator(); it.hasNext(); ) {
          byte [] col = it.next();
          byte [] val = curVals.get(col).getValue();
          int curval =
            Integer.parseInt(new String(val, HConstants.UTF8_ENCODING).trim());
          for(int j = 0; j < cols.length; j++) {
            if (Bytes.compareTo(col, cols[j]) == 0) {
              assertEquals("Error at:" + curKey.getRow() + "/"
                  + curKey.getTimestamp()
                  + ", Value for " + col + " should be: " + k
                  + ", but was fetched as: " + curval, k, curval);
              numFetched++;
            }
          }
        }
        curVals.clear();
        k++;
      }
    } finally {
      s.close();
    }
    assertEquals("Inserted " + numInserted + " values, but fetched " + numFetched, numInserted, numFetched);

    LOG.info("Scanned " + (vals1.length / 2)
        + " rows from disk. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // 5.  Insert more values
    
    startTime = System.currentTimeMillis();

    for(int k = vals1.length/2; k < vals1.length; k++) {
      String kLabel = String.format("%1$03d", k);
      
      BatchUpdate batchUpdate = 
        new BatchUpdate(Bytes.toBytes("row_vals1_" + kLabel), 
          System.currentTimeMillis());
      batchUpdate.put(cols[0], vals1[k].getBytes(HConstants.UTF8_ENCODING));
      batchUpdate.put(cols[1], vals1[k].getBytes(HConstants.UTF8_ENCODING));
      region.commit(batchUpdate);
      numInserted += 2;
    }

    LOG.info("Write " + (vals1.length / 2) + " rows (second half). Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // 6.  Scan from cache and disk
    
    startTime = System.currentTimeMillis();

    s = r.getScanner(cols, HConstants.EMPTY_START_ROW,
        System.currentTimeMillis(), null);
    numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<byte [], Cell> curVals =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<byte []> it = curVals.keySet().iterator(); it.hasNext(); ) {
          byte [] col = it.next();
          byte [] val = curVals.get(col).getValue();
          int curval =
            Integer.parseInt(new String(val, HConstants.UTF8_ENCODING).trim());
          for(int j = 0; j < cols.length; j++) {
            if(Bytes.compareTo(col, cols[j]) == 0) {
              assertEquals("Error at:" + curKey.getRow() + "/"
                  + curKey.getTimestamp()
                  + ", Value for " + col + " should be: " + k
                  + ", but was fetched as: " + curval, k, curval);
              numFetched++;
            }
          }
        }
        curVals.clear();
        k++;
      }
    } finally {
      s.close();
    }
    assertEquals("Inserted " + numInserted + " values, but fetched " + numFetched, numInserted, numFetched);

    LOG.info("Scanned " + vals1.length
        + " rows from cache and disk. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));
    
    // 7.  Flush to disk
    
    startTime = System.currentTimeMillis();
    
    region.flushcache();

    LOG.info("Cache flush elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));
    
    // 8.  Scan from disk
    
    startTime = System.currentTimeMillis();
    
    s = r.getScanner(cols, HConstants.EMPTY_START_ROW, System.currentTimeMillis(), null);
    numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<byte [], Cell> curVals =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<byte []> it = curVals.keySet().iterator(); it.hasNext(); ) {
          byte [] col = it.next();
          byte [] val = curVals.get(col).getValue();
          int curval =
            Integer.parseInt(new String(val, HConstants.UTF8_ENCODING).trim());
          for (int j = 0; j < cols.length; j++) {
            if (Bytes.compareTo(col, cols[j]) == 0) {
              assertEquals("Value for " + col + " should be: " + k
                  + ", but was fetched as: " + curval, curval, k);
              numFetched++;
            }
          }
        }
        curVals.clear();
        k++;
      }
    } finally {
      s.close();
    }
    assertEquals("Inserted " + numInserted + " values, but fetched " + numFetched,
      numInserted, numFetched);
    LOG.info("Scanned " + vals1.length
        + " rows from disk. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // 9. Scan with a starting point
    startTime = System.currentTimeMillis();
    s = r.getScanner(cols, Bytes.toBytes("row_vals1_500"),
        System.currentTimeMillis(), null);
    numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<byte [], Cell> curVals =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      int k = 500;
      while(s.next(curKey, curVals)) {
        for(Iterator<byte []> it = curVals.keySet().iterator(); it.hasNext(); ) {
          byte [] col = it.next();
          byte [] val = curVals.get(col).getValue();
          int curval =
            Integer.parseInt(new String(val, HConstants.UTF8_ENCODING).trim());
          for (int j = 0; j < cols.length; j++) {
            if (Bytes.compareTo(col, cols[j]) == 0) {
              assertEquals("Value for " + col + " should be: " + k
                  + ", but was fetched as: " + curval, curval, k);
              numFetched++;
            }
          }
        }
        curVals.clear();
        k++;
      }
    } finally {
      s.close();
    }
    assertEquals("Should have fetched " + (numInserted / 2) +
      " values, but fetched " + numFetched, (numInserted / 2), numFetched);
    
    LOG.info("Scanned " + (numFetched / 2)
        + " rows from disk with specified start point. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    LOG.info("scan completed.");
  }
  
  // NOTE: This test depends on testBatchWrite succeeding
  private void splitAndMerge() throws IOException {
    Path oldRegionPath = r.getRegionDir();
    byte [] splitRow = r.compactStores();
    assertNotNull(splitRow);
    long startTime = System.currentTimeMillis();
    HRegion subregions[] = r.splitRegion(splitRow);
    if (subregions != null) {
      LOG.info("Split region elapsed time: "
          + ((System.currentTimeMillis() - startTime) / 1000.0));
      assertEquals("Number of subregions", subregions.length, 2);
      for (int i = 0; i < subregions.length; i++) {
        subregions[i] = openClosedRegion(subregions[i]);
        subregions[i].compactStores();
      }
      
      // Now merge it back together
      Path oldRegion1 = subregions[0].getRegionDir();
      Path oldRegion2 = subregions[1].getRegionDir();
      startTime = System.currentTimeMillis();
      r = HRegion.mergeAdjacent(subregions[0], subregions[1]);
      region = new HRegionIncommon(r);
      LOG.info("Merge regions elapsed time: " +
        ((System.currentTimeMillis() - startTime) / 1000.0));
      fs.delete(oldRegion1, true);
      fs.delete(oldRegion2, true);
      fs.delete(oldRegionPath, true);
    }
    LOG.info("splitAndMerge completed.");
  }

  // This test verifies that everything is still there after splitting and merging
  
  private void read() throws IOException {

    // First verify the data written by testBasic()

    byte [][] cols = {
        Bytes.toBytes(ANCHORNUM + "[0-9]+"),
        CONTENTS_BASIC
    };
    
    long startTime = System.currentTimeMillis();
    
    InternalScanner s =
      r.getScanner(cols, HConstants.EMPTY_START_ROW,
          System.currentTimeMillis(), null);

    try {

      int contentsFetched = 0;
      int anchorFetched = 0;
      HStoreKey curKey = new HStoreKey();
      TreeMap<byte [], Cell> curVals =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<byte []> it = curVals.keySet().iterator(); it.hasNext(); ) {
          byte [] col = it.next();
          byte [] val = curVals.get(col).getValue();
          String curval = Bytes.toString(val);
          if(Bytes.compareTo(col, CONTENTS_BASIC) == 0) {
            assertTrue("Error at:" + curKey.getRow() + "/" + curKey.getTimestamp()
                + ", Value for " + col + " should start with: " + CONTENTSTR
                + ", but was fetched as: " + curval,
                curval.startsWith(CONTENTSTR));
            contentsFetched++;
            
          } else if (Bytes.toString(col).startsWith(ANCHORNUM)) {
            assertTrue("Error at:" + curKey.getRow() + "/" + curKey.getTimestamp()
                + ", Value for " + Bytes.toString(col) +
                " should start with: " + ANCHORSTR
                + ", but was fetched as: " + curval,
                curval.startsWith(ANCHORSTR));
            anchorFetched++;
            
          } else {
            LOG.info("UNEXPECTED COLUMN " + col);
          }
        }
        curVals.clear();
        k++;
      }
      assertEquals("Expected " + NUM_VALS + " " + Bytes.toString(CONTENTS_BASIC) +
        " values, but fetched " + contentsFetched, NUM_VALS, contentsFetched);
      assertEquals("Expected " + NUM_VALS + " " + ANCHORNUM +
        " values, but fetched " + anchorFetched, NUM_VALS, anchorFetched);

      LOG.info("Scanned " + NUM_VALS
          + " rows from disk. Elapsed time: "
          + ((System.currentTimeMillis() - startTime) / 1000.0));
      
    } finally {
      s.close();
    }
    
    // Verify testScan data
    
    cols = new byte [][] {CONTENTS_FIRSTCOL, ANCHOR_SECONDCOL};
    
    startTime = System.currentTimeMillis();

    s = r.getScanner(cols, HConstants.EMPTY_START_ROW,
      System.currentTimeMillis(), null);
    try {
      int numFetched = 0;
      HStoreKey curKey = new HStoreKey();
      TreeMap<byte [], Cell> curVals =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<byte []> it = curVals.keySet().iterator(); it.hasNext(); ) {
          byte [] col = it.next();
          byte [] val = curVals.get(col).getValue();
          int curval =
            Integer.parseInt(new String(val, HConstants.UTF8_ENCODING).trim());

          for (int j = 0; j < cols.length; j++) {
            if (Bytes.compareTo(col, cols[j]) == 0) {
              assertEquals("Value for " + col + " should be: " + k
                  + ", but was fetched as: " + curval, curval, k);
              numFetched++;
            }
          }
        }
        curVals.clear();
        k++;
      }
      assertEquals("Inserted " + numInserted + " values, but fetched " + numFetched, numInserted, numFetched);

      LOG.info("Scanned " + (numFetched / 2)
          + " rows from disk. Elapsed time: "
          + ((System.currentTimeMillis() - startTime) / 1000.0));
      
    } finally {
      s.close();
    }
    
    // Test a scanner which only specifies the column family name
    
    cols = new byte [][] {
        Bytes.toBytes("anchor:")
    };
    
    startTime = System.currentTimeMillis();
    
    s = r.getScanner(cols, HConstants.EMPTY_START_ROW, System.currentTimeMillis(), null);

    try {
      int fetched = 0;
      HStoreKey curKey = new HStoreKey();
      TreeMap<byte [], Cell> curVals =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      while(s.next(curKey, curVals)) {
        for(Iterator<byte []> it = curVals.keySet().iterator(); it.hasNext(); ) {
          it.next();
          fetched++;
        }
        curVals.clear();
      }
      assertEquals("Inserted " + (NUM_VALS + numInserted/2) +
        " values, but fetched " + fetched, (NUM_VALS + numInserted/2), fetched);
      LOG.info("Scanned " + fetched
          + " rows from disk. Elapsed time: "
          + ((System.currentTimeMillis() - startTime) / 1000.0));
      
    } finally {
      s.close();
    }
    LOG.info("read completed.");
  }
  
}
