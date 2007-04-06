/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;

/**
 * Basic stand-alone testing of HRegion.
 * 
 * A lot of the meta information for an HRegion now lives inside other
 * HRegions or in the HBaseMaster, so only basic testing is possible.
 */
public class TestHRegion extends TestCase {
  
  /** Constructor */
  public TestHRegion(String name) {
    super(name);
  }
  
  /** Test suite so that all tests get run */
  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTest(new TestHRegion("testSetup"));
    suite.addTest(new TestHRegion("testBasic"));
    suite.addTest(new TestHRegion("testScan"));
    suite.addTest(new TestHRegion("testBatchWrite"));
    suite.addTest(new TestHRegion("testSplitAndMerge"));
    suite.addTest(new TestHRegion("testRead"));
    suite.addTest(new TestHRegion("testCleanup"));
    return suite;
  }
  
  
  private static final int FIRST_ROW = 0;
  private static final int N_ROWS = 1000000;
  private static final int NUM_VALS = 1000;
  private static final Text CONTENTS_BASIC = new Text("contents:basic");
  private static final String CONTENTSTR = "contentstr";
  private static final String ANCHORNUM = "anchor:anchornum-";
  private static final String ANCHORSTR = "anchorstr";
  private static final Text CONTENTS_BODY = new Text("contents:body");
  private static final Text CONTENTS_FIRSTCOL = new Text("contents:firstcol");
  private static final Text ANCHOR_SECONDCOL = new Text("anchor:secondcol");
  
  private static boolean initialized = false;
  private static boolean failures = false;
  private static Configuration conf = null;
  private static MiniDFSCluster cluster = null;
  private static FileSystem fs = null;
  private static Path parentdir = null;
  private static Path newlogdir = null;
  private static Path oldlogfile = null;
  private static HLog log = null;
  private static HTableDescriptor desc = null;
  private static HRegion region = null;
  
  private static int numInserted = 0;

  // Set up environment, start mini cluster, etc.
  
  public void testSetup() throws IOException {
    try {
      if(System.getProperty("test.build.data") == null) {
        String dir = new File(new File("").getAbsolutePath(), "build/contrib/hbase/test").getAbsolutePath();
        System.out.println(dir);
        System.setProperty("test.build.data", dir);
      }
      conf = new Configuration();
      
      Environment.getenv();
      if(Environment.debugging) {
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        
        PatternLayout consoleLayout
            = (PatternLayout)rootLogger.getAppender("console").getLayout();
        consoleLayout.setConversionPattern("%d %-5p [%t] %l: %m%n");
      
        Logger.getLogger("org.apache.hadoop.hbase").setLevel(Environment.logLevel);
      }
      
      cluster = new MiniDFSCluster(conf, 2, true, null);
      fs = cluster.getFileSystem();
      parentdir = new Path("/hbase");
      fs.mkdirs(parentdir);
      newlogdir = new Path(parentdir, "log");
      oldlogfile = new Path(parentdir, "oldlogfile");

      log = new HLog(fs, newlogdir, conf);
      desc = new HTableDescriptor("test", 3);
      desc.addFamily(new Text("contents"));
      desc.addFamily(new Text("anchor"));
      region = new HRegion(parentdir, log, fs, conf, 
          new HRegionInfo(1, desc, null, null), null, oldlogfile);
      
    } catch(IOException e) {
      failures = true;
      throw e;
    }
    initialized = true;
  }

  // Test basic functionality. Writes to contents:basic and anchor:anchornum-*

  public void testBasic() throws IOException {
    if(!initialized) {
      throw new IllegalStateException();
    }

    try {
      
      // Write out a bunch of values
      
      for (int k = 0; k < NUM_VALS; k++) {
        long writeid = region.startUpdate(new Text("row_" + k));
        region.put(writeid, CONTENTS_BASIC, (CONTENTSTR + k).getBytes());
        region.put(writeid, new Text(ANCHORNUM + k), (ANCHORSTR + k).getBytes());
        region.commit(writeid);
      }
      region.flushcache(false);

      // Read them back in

      Text collabel = null;
      for (int k = 0; k < NUM_VALS; k++) {
        Text rowlabel = new Text("row_" + k);

        byte bodydata[] = region.get(rowlabel, CONTENTS_BASIC);
        assertNotNull(bodydata);
        String bodystr = new String(bodydata).toString().trim();
        String teststr = CONTENTSTR + k;
        assertEquals("Incorrect value for key: (" + rowlabel + "," + CONTENTS_BASIC
            + "), expected: '" + teststr + "' got: '" + bodystr + "'",
            bodystr, teststr);
        collabel = new Text(ANCHORNUM + k);
        bodydata = region.get(rowlabel, collabel);
        bodystr = new String(bodydata).toString().trim();
        teststr = ANCHORSTR + k;
        assertEquals("Incorrect value for key: (" + rowlabel + "," + collabel
            + "), expected: '" + teststr + "' got: '" + bodystr + "'",
            bodystr, teststr);
/*
        // Check to make sure that null values are actually null
        for (int j = 0; j < Math.min(15, NUM_VALS); j++) {
          if (k != j) {
            collabel = new Text(ANCHORNUM + j);
            byte results[] = region.get(rowlabel, collabel);
            if (results != null) {
              throw new IOException("Found incorrect value at [" + rowlabel + ", " + collabel + "] == " + new String(results).toString().trim());
            }
          }
        }
*/
      }
    } catch(IOException e) {
      failures = true;
      throw e;
    }
  }

  // Test scanners. Writes contents:firstcol and anchor:secondcol
  
  public void testScan() throws IOException {
    if(!initialized) {
      throw new IllegalStateException();
    }

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
    for(int k = 0; k < vals1.length / 2; k++) {
      String kLabel = String.format("%1$03d", k);

      long lockid = region.startUpdate(new Text("row_vals1_" + kLabel));
      region.put(lockid, cols[0], vals1[k].getBytes());
      region.put(lockid, cols[1], vals1[k].getBytes());
      region.commit(lockid);
      numInserted += 2;
    }

    // 2.  Scan
    HScannerInterface s = region.getScanner(cols, new Text());
    int numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte[]> curVals = new TreeMap<Text, byte[]>();
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          Text col = it.next();
          byte val[] = curVals.get(col);
          int curval = Integer.parseInt(new String(val).trim());

          for(int j = 0; j < cols.length; j++) {
            if(col.compareTo(cols[j]) == 0) {
              assertEquals("Error at:" + curKey.getRow() + "/" + curKey.getTimestamp()
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

    // 3.  Flush to disk
    region.flushcache(false);

    // 4.  Scan
    s = region.getScanner(cols, new Text());
    numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte[]> curVals = new TreeMap<Text, byte[]>();
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          Text col = it.next();
          byte val[] = curVals.get(col);
          int curval = Integer.parseInt(new String(val).trim());

          for(int j = 0; j < cols.length; j++) {
            if(col.compareTo(cols[j]) == 0) {
              assertEquals("Error at:" + curKey.getRow() + "/" + curKey.getTimestamp()
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

    // 5.  Insert more values
    for(int k = vals1.length/2; k < vals1.length; k++) {
      String kLabel = String.format("%1$03d", k);
      
      long lockid = region.startUpdate(new Text("row_vals1_" + kLabel));
      region.put(lockid, cols[0], vals1[k].getBytes());
      region.put(lockid, cols[1], vals1[k].getBytes());
      region.commit(lockid);
      numInserted += 2;
    }

    // 6.  Scan
    s = region.getScanner(cols, new Text());
    numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte[]> curVals = new TreeMap<Text, byte[]>();
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          Text col = it.next();
          byte val[] = curVals.get(col);
          int curval = Integer.parseInt(new String(val).trim());

          for(int j = 0; j < cols.length; j++) {
            if(col.compareTo(cols[j]) == 0) {
              assertEquals("Error at:" + curKey.getRow() + "/" + curKey.getTimestamp()
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

    // 7.  Flush to disk
    region.flushcache(false);

    // 8.  Scan
    s = region.getScanner(cols, new Text());
    numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte[]> curVals = new TreeMap<Text, byte[]>();
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          Text col = it.next();
          byte val[] = curVals.get(col);
          int curval = Integer.parseInt(new String(val).trim());

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
    
    // 9. Scan with a starting point

    s = region.getScanner(cols, new Text("row_vals1_500"));
    numFetched = 0;
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte[]> curVals = new TreeMap<Text, byte[]>();
      int k = 500;
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          Text col = it.next();
          byte val[] = curVals.get(col);
          int curval = Integer.parseInt(new String(val).trim());

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
    
  }
  
  // Do a large number of writes. Disabled if not debugging because it takes a 
  // long time to run.
  // Creates contents:body
  
  public void testBatchWrite() throws IOException {
    if(!initialized || failures) {
      throw new IllegalStateException();
    }
    if(! Environment.debugging) {
      return;
    }

    try {
      long totalFlush = 0;
      long totalCompact = 0;
      long totalLog = 0;
      long startTime = System.currentTimeMillis();

      // 1M writes

      int valsize = 1000;
      for (int k = FIRST_ROW; k < N_ROWS; k++) {
        // Come up with a random 1000-byte string
        String randstr1 = "" + System.currentTimeMillis();
        StringBuffer buf1 = new StringBuffer("val_" + k + "__");
        while (buf1.length() < valsize) {
          buf1.append(randstr1);
        }

        // Write to the HRegion
        long writeid = region.startUpdate(new Text("row_" + k));
        region.put(writeid, CONTENTS_BODY, buf1.toString().getBytes());
        region.commit(writeid);
        if (k > 0 && k % (N_ROWS / 100) == 0) {
          System.out.println("Flushing write #" + k);

          long flushStart = System.currentTimeMillis();
          region.flushcache(false);
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
      if(region.compactStores()) {
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

    } catch(IOException e) {
      failures = true;
      throw e;
    }
  }

  // NOTE: This test depends on testBatchWrite succeeding
  
  public void testSplitAndMerge() throws IOException {
    if(!initialized || failures) {
      throw new IllegalStateException();
    }
    
    try {
      Text midKey = new Text();
      
      if(region.needsSplit(midKey)) {
        System.out.println("Needs split");
      }
      
      // Split it anyway

      Text midkey = new Text("row_" + (Environment.debugging ? (N_ROWS / 2) : (NUM_VALS/2)));
      Path oldRegionPath = region.getRegionDir();
      HRegion subregions[] = region.closeAndSplit(midkey);
      assertEquals("Number of subregions", subregions.length, 2);

      // Now merge it back together

      Path oldRegion1 = subregions[0].getRegionDir();
      Path oldRegion2 = subregions[1].getRegionDir();
      region = HRegion.closeAndMerge(subregions[0], subregions[1]);

      fs.delete(oldRegionPath);
      fs.delete(oldRegion1);
      fs.delete(oldRegion2);
      
    } catch(IOException e) {
      failures = true;
      throw e;
    }
  }

  // This test verifies that everything is still there after splitting and merging
  
  public void testRead() throws IOException {
    if(!initialized || failures) {
      throw new IllegalStateException();
    }

    // First verify the data written by testBasic()

    Text[] cols = new Text[] {
        new Text(ANCHORNUM + "[0-9]+"),
        new Text(CONTENTS_BASIC)
    };
    
    HScannerInterface s = region.getScanner(cols, new Text());

    try {

      int contentsFetched = 0;
      int anchorFetched = 0;
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte[]> curVals = new TreeMap<Text, byte[]>();
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          Text col = it.next();
          byte val[] = curVals.get(col);
          String curval = new String(val).trim();

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
            System.out.println(col);
          }
        }
        curVals.clear();
        k++;
      }
      assertEquals("Expected " + NUM_VALS + " " + CONTENTS_BASIC + " values, but fetched " + contentsFetched, NUM_VALS, contentsFetched);
      assertEquals("Expected " + NUM_VALS + " " + ANCHORNUM + " values, but fetched " + anchorFetched, NUM_VALS, anchorFetched);

    } finally {
      s.close();
    }
    
    // Verify testScan data
    
    cols = new Text[] {
        CONTENTS_FIRSTCOL,
        ANCHOR_SECONDCOL
    };

    s = region.getScanner(cols, new Text());
    try {
      int numFetched = 0;
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte[]> curVals = new TreeMap<Text, byte[]>();
      int k = 0;
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          Text col = it.next();
          byte val[] = curVals.get(col);
          int curval = Integer.parseInt(new String(val).trim());

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

    } finally {
      s.close();
    }
    
    // Verify testBatchWrite data

    if(Environment.debugging) {
      s = region.getScanner(new Text[] { CONTENTS_BODY }, new Text());
      try {
        int numFetched = 0;
        HStoreKey curKey = new HStoreKey();
        TreeMap<Text, byte[]> curVals = new TreeMap<Text, byte[]>();
        int k = 0;
        while(s.next(curKey, curVals)) {
          for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
            Text col = it.next();
            byte val[] = curVals.get(col);

            assertTrue(col.compareTo(CONTENTS_BODY) == 0);
            assertNotNull(val);
            numFetched++;
          }
          curVals.clear();
          k++;
        }
        assertEquals("Inserted " + N_ROWS + " values, but fetched " + numFetched, N_ROWS, numFetched);

      } finally {
        s.close();
      }
    }
    
    // Test a scanner which only specifies the column family name
    
    cols = new Text[] {
        new Text("anchor:")
    };
    
    s = region.getScanner(cols, new Text());

    try {
      int fetched = 0;
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte[]> curVals = new TreeMap<Text, byte[]>();
      while(s.next(curKey, curVals)) {
        for(Iterator<Text> it = curVals.keySet().iterator(); it.hasNext(); ) {
          it.next();
          fetched++;
        }
        curVals.clear();
      }
      assertEquals("Inserted " + (NUM_VALS + numInserted/2) + " values, but fetched " + fetched, (NUM_VALS + numInserted/2), fetched);

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
  
  public void testCleanup() throws IOException {
    if(!initialized) {
      throw new IllegalStateException();
    }

    // Shut down the mini cluster
    
    cluster.shutdown();
    
    // Delete all the DFS files
    
    deleteFile(new File(System.getProperty("test.build.data"), "dfs"));
    
    }
}
