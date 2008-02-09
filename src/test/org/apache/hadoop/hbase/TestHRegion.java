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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

/**
 * Basic stand-alone testing of HRegion.
 * 
 * A lot of the meta information for an HRegion now lives inside other
 * HRegions or in the HBaseMaster, so only basic testing is possible.
 */
public class TestHRegion extends HBaseTestCase
implements RegionUnavailableListener {
  static final Logger LOG =
    Logger.getLogger(TestHRegion.class.getName());
  
  /**
   * Since all the "tests" depend on the results of the previous test, they are
   * not Junit tests that can stand alone. Consequently we have a single Junit
   * test that runs the "sub-tests" as private methods.
   * @throws IOException 
   */
  public void testHRegion() throws IOException {
    try {
      setup();
      locks();
      badPuts();
      basic();
      scan();
      batchWrite();
      splitAndMerge();
      read();
      cleanup();
    } finally {
      if (r != null) {
        r.close();
      }
      if (log != null) {
        log.closeAndDelete();
      }
      StaticTestEnvironment.shutdownDfs(cluster);
    }
  }
  
  
  private static final int FIRST_ROW = 1;
  private static final int N_ROWS = 1000000;
  private static final int NUM_VALS = 1000;
  private static final Text CONTENTS_BASIC = new Text("contents:basic");
  private static final String CONTENTSTR = "contentstr";
  private static final String ANCHORNUM = "anchor:anchornum-";
  private static final String ANCHORSTR = "anchorstr";
  private static final Text CONTENTS_BODY = new Text("contents:body");
  private static final Text CONTENTS_FIRSTCOL = new Text("contents:firstcol");
  private static final Text ANCHOR_SECONDCOL = new Text("anchor:secondcol");
  
  private MiniDFSCluster cluster = null;
  private HLog log = null;
  private HTableDescriptor desc = null;
  HRegion r = null;
  HRegionIncommon region = null;
  
  private static int numInserted = 0;

  // Create directories, start mini cluster, etc.
  
  private void setup() throws IOException {

    cluster = new MiniDFSCluster(conf, 2, true, (String[])null);
    // Set the hbase.rootdir to be the home directory in mini dfs.
    this.conf.set(HConstants.HBASE_DIR,
      this.cluster.getFileSystem().getHomeDirectory().toString());

    desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor("contents:"));
    desc.addFamily(new HColumnDescriptor("anchor:"));
    r = createNewHRegion(desc, null, null);
    log = r.getLog();
    region = new HRegionIncommon(r);
  }

  // Test basic functionality. Writes to contents:basic and anchor:anchornum-*

  private void basic() throws IOException {
    long startTime = System.currentTimeMillis();

    // Write out a bunch of values

    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      long writeid = region.startUpdate(new Text("row_" + k));
      region.put(writeid, CONTENTS_BASIC,
          (CONTENTSTR + k).getBytes(HConstants.UTF8_ENCODING));
      region.put(writeid, new Text(ANCHORNUM + k),
          (ANCHORSTR + k).getBytes(HConstants.UTF8_ENCODING));
      region.commit(writeid, System.currentTimeMillis());
    }
    System.out.println("Write " + NUM_VALS + " rows. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // Flush cache

    startTime = System.currentTimeMillis();

    region.flushcache();

    System.out.println("Cache flush elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // Read them back in

    startTime = System.currentTimeMillis();

    Text collabel = null;
    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      Text rowlabel = new Text("row_" + k);

      byte [] bodydata = region.get(rowlabel, CONTENTS_BASIC);
      assertNotNull(bodydata);
      String bodystr = new String(bodydata, HConstants.UTF8_ENCODING).trim();
      String teststr = CONTENTSTR + k;
      assertEquals("Incorrect value for key: (" + rowlabel + "," + CONTENTS_BASIC
          + "), expected: '" + teststr + "' got: '" + bodystr + "'",
          bodystr, teststr);
      collabel = new Text(ANCHORNUM + k);
      bodydata = region.get(rowlabel, collabel);
      bodystr = new String(bodydata, HConstants.UTF8_ENCODING).trim();
      teststr = ANCHORSTR + k;
      assertEquals("Incorrect value for key: (" + rowlabel + "," + collabel
          + "), expected: '" + teststr + "' got: '" + bodystr + "'",
          bodystr, teststr);
    }

    System.out.println("Read " + NUM_VALS + " rows. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));
  }
  
  private void badPuts() {
    
    // Try put with bad lockid.
    boolean exceptionThrown = false;
    try {
      region.put(-1, CONTENTS_BASIC,
          "bad input".getBytes(HConstants.UTF8_ENCODING));
    } catch (Exception e) {
      exceptionThrown = true;
    }
    assertTrue("Bad lock id", exceptionThrown);

    // Try column name not registered in the table.
    exceptionThrown = false;
    long lockid = -1;
    try {
      lockid = region.startUpdate(new Text("Some old key"));
      String unregisteredColName = "FamilyGroup:FamilyLabel";
      region.put(lockid, new Text(unregisteredColName),
        unregisteredColName.getBytes(HConstants.UTF8_ENCODING));
      region.commit(lockid);
    } catch (IOException e) {
      exceptionThrown = true;
    } finally {
      if (lockid != -1) {
        region.abort(lockid);
      }
    }
    assertTrue("Bad family", exceptionThrown);
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
          long [] lockids = new long[lockCount];
          // Get locks.
          for (int i = 0; i < lockCount; i++) {
            try {
              Text rowid = new Text(Integer.toString(i));
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
            r.releaseRowLock(r.getRowFromLock(lockids[i]));
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
  }

  // Test scanners. Writes contents:firstcol and anchor:secondcol
  
  private void scan() throws IOException {
    Text cols[] = new Text[] {
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

      long lockid = region.startUpdate(new Text("row_vals1_" + kLabel));
      region.put(lockid, cols[0], vals1[k].getBytes(HConstants.UTF8_ENCODING));
      region.put(lockid, cols[1], vals1[k].getBytes(HConstants.UTF8_ENCODING));
      region.commit(lockid, System.currentTimeMillis());
      numInserted += 2;
    }

    System.out.println("Write " + (vals1.length / 2) + " elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // 2.  Scan from cache
    
    startTime = System.currentTimeMillis();

    HScannerInterface s =
      r.getScanner(cols, new Text(), System.currentTimeMillis(), null);
    int numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          Text col = it.next();
          byte [] val = curVals.get(col);
          int curval =
            Integer.parseInt(new String(val, HConstants.UTF8_ENCODING).trim());
          for(int j = 0; j < cols.length; j++) {
            if(col.compareTo(cols[j]) == 0) {
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

    System.out.println("Scanned " + (vals1.length / 2)
        + " rows from cache. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // 3.  Flush to disk
    
    startTime = System.currentTimeMillis();
    
    region.flushcache();

    System.out.println("Cache flush elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // 4.  Scan from disk
    
    startTime = System.currentTimeMillis();
    
    s = r.getScanner(cols, new Text(), System.currentTimeMillis(), null);
    numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          Text col = it.next();
          byte [] val = curVals.get(col);
          int curval =
            Integer.parseInt(new String(val, HConstants.UTF8_ENCODING).trim());
          for(int j = 0; j < cols.length; j++) {
            if(col.compareTo(cols[j]) == 0) {
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

    System.out.println("Scanned " + (vals1.length / 2)
        + " rows from disk. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // 5.  Insert more values
    
    startTime = System.currentTimeMillis();

    for(int k = vals1.length/2; k < vals1.length; k++) {
      String kLabel = String.format("%1$03d", k);
      
      long lockid = region.startUpdate(new Text("row_vals1_" + kLabel));
      region.put(lockid, cols[0], vals1[k].getBytes(HConstants.UTF8_ENCODING));
      region.put(lockid, cols[1], vals1[k].getBytes(HConstants.UTF8_ENCODING));
      region.commit(lockid, System.currentTimeMillis());
      numInserted += 2;
    }

    System.out.println("Write " + (vals1.length / 2) + " rows. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // 6.  Scan from cache and disk
    
    startTime = System.currentTimeMillis();

    s = r.getScanner(cols, new Text(), System.currentTimeMillis(), null);
    numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          Text col = it.next();
          byte [] val = curVals.get(col);
          int curval =
            Integer.parseInt(new String(val, HConstants.UTF8_ENCODING).trim());
          for(int j = 0; j < cols.length; j++) {
            if(col.compareTo(cols[j]) == 0) {
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

    System.out.println("Scanned " + vals1.length
        + " rows from cache and disk. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));
    
    // 7.  Flush to disk
    
    startTime = System.currentTimeMillis();
    
    region.flushcache();

    System.out.println("Cache flush elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));
    
    // 8.  Scan from disk
    
    startTime = System.currentTimeMillis();
    
    s = r.getScanner(cols, new Text(), System.currentTimeMillis(), null);
    numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          Text col = it.next();
          byte [] val = curVals.get(col);
          int curval =
            Integer.parseInt(new String(val, HConstants.UTF8_ENCODING).trim());
          for (int j = 0; j < cols.length; j++) {
            if (col.compareTo(cols[j]) == 0) {
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
    assertEquals("Inserted " + numInserted + " values, but fetched " + numFetched, numInserted, numFetched);
    
    System.out.println("Scanned " + vals1.length
        + " rows from disk. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // 9. Scan with a starting point

    startTime = System.currentTimeMillis();
    
    s = r.getScanner(cols, new Text("row_vals1_500"),
        System.currentTimeMillis(), null);
    
    numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
      int k = 500;
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          Text col = it.next();
          byte [] val = curVals.get(col);
          int curval =
            Integer.parseInt(new String(val, HConstants.UTF8_ENCODING).trim());
          for (int j = 0; j < cols.length; j++) {
            if (col.compareTo(cols[j]) == 0) {
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
    assertEquals("Should have fetched " + (numInserted / 2) + " values, but fetched " + numFetched, (numInserted / 2), numFetched);
    
    System.out.println("Scanned " + (numFetched / 2)
        + " rows from disk with specified start point. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));
  }
  
  // Do a large number of writes. Disabled if not debugging because it takes a 
  // long time to run.
  // Creates contents:body
  
  private void batchWrite() throws IOException {
    if(! StaticTestEnvironment.debugging) {
      return;
    }

    long totalFlush = 0;
    long totalCompact = 0;
    long totalLog = 0;
    long startTime = System.currentTimeMillis();

    // 1M writes

    int valsize = 1000;
    for (int k = FIRST_ROW; k <= N_ROWS; k++) {
      // Come up with a random 1000-byte string
      String randstr1 = "" + System.currentTimeMillis();
      StringBuffer buf1 = new StringBuffer("val_" + k + "__");
      while (buf1.length() < valsize) {
        buf1.append(randstr1);
      }

      // Write to the HRegion
      long writeid = region.startUpdate(new Text("row_" + k));
      region.put(writeid, CONTENTS_BODY,
          buf1.toString().getBytes(HConstants.UTF8_ENCODING));
      region.commit(writeid, System.currentTimeMillis());
      if (k > 0 && k % (N_ROWS / 100) == 0) {
        System.out.println("Flushing write #" + k);

        long flushStart = System.currentTimeMillis();
        region.flushcache();
        long flushEnd = System.currentTimeMillis();
        totalFlush += (flushEnd - flushStart);

        if (k % (N_ROWS / 10) == 0) {
          System.out.print("Rolling log...");
          long logStart = System.currentTimeMillis();
          log.rollWriter();
          long logEnd = System.currentTimeMillis();
          totalLog += (logEnd - logStart);
          System.out.println("  elapsed time: " + ((logEnd - logStart) / 1000.0));
        }
      }
    }
    long startCompact = System.currentTimeMillis();
    if(r.compactIfNeeded()) {
      totalCompact = System.currentTimeMillis() - startCompact;
      System.out.println("Region compacted - elapsedTime: " + (totalCompact / 1000.0));

    } else {
      System.out.println("No compaction required.");
    }
    long endTime = System.currentTimeMillis();

    long totalElapsed = (endTime - startTime);
    System.out.println();
    System.out.println("Batch-write complete.");
    System.out.println("Wrote " + N_ROWS + " rows, each of ~" + valsize + " bytes");
    System.out.println("Total flush-time: " + (totalFlush / 1000.0));
    System.out.println("Total compact-time: " + (totalCompact / 1000.0));
    System.out.println("Total log-time: " + (totalLog / 1000.0));
    System.out.println("Total time elapsed: " + (totalElapsed / 1000.0));
    System.out.println("Total time, rows/second: " + (N_ROWS / (totalElapsed / 1000.0)));
    System.out.println("Adjusted time (not including flush, compact, or log): " + ((totalElapsed - totalFlush - totalCompact - totalLog) / 1000.0));
    System.out.println("Adjusted time, rows/second: " + (N_ROWS / ((totalElapsed - totalFlush - totalCompact - totalLog) / 1000.0)));
    System.out.println();

  }

  // NOTE: This test depends on testBatchWrite succeeding
  private void splitAndMerge() throws IOException {
    Path oldRegionPath = r.getRegionDir();
    long startTime = System.currentTimeMillis();
    HRegion subregions[] = r.splitRegion(this);
    if (subregions != null) {
      System.out.println("Split region elapsed time: "
          + ((System.currentTimeMillis() - startTime) / 1000.0));

      assertEquals("Number of subregions", subregions.length, 2);

      // Now merge it back together

      Path oldRegion1 = subregions[0].getRegionDir();
      Path oldRegion2 = subregions[1].getRegionDir();
      startTime = System.currentTimeMillis();
      r = HRegion.closeAndMerge(subregions[0], subregions[1]);
      region = new HRegionIncommon(r);
      System.out.println("Merge regions elapsed time: "
          + ((System.currentTimeMillis() - startTime) / 1000.0));
      fs.delete(oldRegion1);
      fs.delete(oldRegion2);
      fs.delete(oldRegionPath);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void closing(@SuppressWarnings("unused") final Text regionName) {
    // We don't use this here. It is only for the HRegionServer
  }
  
  /**
   * {@inheritDoc}
   */
  public void closed(@SuppressWarnings("unused") final Text regionName) {
    // We don't use this here. It is only for the HRegionServer
  }
  
  // This test verifies that everything is still there after splitting and merging
  
  private void read() throws IOException {

    // First verify the data written by testBasic()

    Text[] cols = new Text[] {
        new Text(ANCHORNUM + "[0-9]+"),
        new Text(CONTENTS_BASIC)
    };
    
    long startTime = System.currentTimeMillis();
    
    HScannerInterface s =
      r.getScanner(cols, new Text(), System.currentTimeMillis(), null);

    try {

      int contentsFetched = 0;
      int anchorFetched = 0;
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          Text col = it.next();
          byte [] val = curVals.get(col);
          String curval = new String(val, HConstants.UTF8_ENCODING).trim();

          if(col.compareTo(CONTENTS_BASIC) == 0) {
            assertTrue("Error at:" + curKey.getRow() + "/" + curKey.getTimestamp()
                + ", Value for " + col + " should start with: " + CONTENTSTR
                + ", but was fetched as: " + curval,
                curval.startsWith(CONTENTSTR));
            contentsFetched++;
            
          } else if(col.toString().startsWith(ANCHORNUM)) {
            assertTrue("Error at:" + curKey.getRow() + "/" + curKey.getTimestamp()
                + ", Value for " + col + " should start with: " + ANCHORSTR
                + ", but was fetched as: " + curval,
                curval.startsWith(ANCHORSTR));
            anchorFetched++;
            
          } else {
            System.out.println("UNEXPECTED COLUMN " + col);
          }
        }
        curVals.clear();
        k++;
      }
      assertEquals("Expected " + NUM_VALS + " " + CONTENTS_BASIC + " values, but fetched " + contentsFetched, NUM_VALS, contentsFetched);
      assertEquals("Expected " + NUM_VALS + " " + ANCHORNUM + " values, but fetched " + anchorFetched, NUM_VALS, anchorFetched);

      System.out.println("Scanned " + NUM_VALS
          + " rows from disk. Elapsed time: "
          + ((System.currentTimeMillis() - startTime) / 1000.0));
      
    } finally {
      s.close();
    }
    
    // Verify testScan data
    
    cols = new Text[] {
        CONTENTS_FIRSTCOL,
        ANCHOR_SECONDCOL
    };
    
    startTime = System.currentTimeMillis();

    s = r.getScanner(cols, new Text(), System.currentTimeMillis(), null);
    try {
      int numFetched = 0;
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          Text col = it.next();
          byte [] val = curVals.get(col);
          int curval =
            Integer.parseInt(new String(val, HConstants.UTF8_ENCODING).trim());

          for (int j = 0; j < cols.length; j++) {
            if (col.compareTo(cols[j]) == 0) {
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

      System.out.println("Scanned " + (numFetched / 2)
          + " rows from disk. Elapsed time: "
          + ((System.currentTimeMillis() - startTime) / 1000.0));
      
    } finally {
      s.close();
    }
    
    // Verify testBatchWrite data

    if(StaticTestEnvironment.debugging) {
      startTime = System.currentTimeMillis();
      s = r.getScanner(new Text[] { CONTENTS_BODY }, new Text(),
          System.currentTimeMillis(), null);
      
      try {
        int numFetched = 0;
        HStoreKey curKey = new HStoreKey();
        TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
        int k = 0;
        while(s.next(curKey, curVals)) {
          for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
            Text col = it.next();
            byte [] val = curVals.get(col);
            assertTrue(col.compareTo(CONTENTS_BODY) == 0);
            assertNotNull(val);
            numFetched++;
          }
          curVals.clear();
          k++;
        }
        assertEquals("Inserted " + N_ROWS + " values, but fetched " + numFetched, N_ROWS, numFetched);

        System.out.println("Scanned " + N_ROWS
            + " rows from disk. Elapsed time: "
            + ((System.currentTimeMillis() - startTime) / 1000.0));
        
      } finally {
        s.close();
      }
    }
    
    // Test a scanner which only specifies the column family name
    
    cols = new Text[] {
        new Text("anchor:")
    };
    
    startTime = System.currentTimeMillis();
    
    s = r.getScanner(cols, new Text(), System.currentTimeMillis(), null);

    try {
      int fetched = 0;
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          it.next();
          fetched++;
        }
        curVals.clear();
      }
      assertEquals("Inserted " + (NUM_VALS + numInserted/2) + " values, but fetched " + fetched, (NUM_VALS + numInserted/2), fetched);

      System.out.println("Scanned " + fetched
          + " rows from disk. Elapsed time: "
          + ((System.currentTimeMillis() - startTime) / 1000.0));
      
    } finally {
      s.close();
    }
  }
  
  private static void deleteFile(File f) {
    if(f.isDirectory()) {
      File[] children = f.listFiles();
      for(int i = 0; i < children.length; i++) {
        deleteFile(children[i]);
      }
    }
    f.delete();
  }
  
  private void cleanup() {
    try {
      r.close();
      r = null;
      log.closeAndDelete();
      log = null;
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Delete all the DFS files

    deleteFile(new File(System.getProperty("test.build.data"), "dfs"));
  }
}
