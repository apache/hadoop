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

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;

/**
 * Test HBase Master and Region servers, client API 
 */
public class TestHBaseCluster extends TestCase {

  /** constructor */
  public TestHBaseCluster(String name) {
    super(name);
  }

  /** Test suite so that all tests get run */
  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTest(new TestHBaseCluster("testSetup"));
    suite.addTest(new TestHBaseCluster("testBasic"));
    suite.addTest(new TestHBaseCluster("testScanner"));
    suite.addTest(new TestHBaseCluster("testCleanup"));
    return suite;
  }

  private static final int FIRST_ROW = 1;
  private static final int NUM_VALS = 1000;
  private static final Text CONTENTS = new Text("contents:");
  private static final Text CONTENTS_BASIC = new Text("contents:basic");
  private static final String CONTENTSTR = "contentstr";
  private static final Text ANCHOR = new Text("anchor:");
  private static final String ANCHORNUM = "anchor:anchornum-";
  private static final String ANCHORSTR = "anchorstr";

  private static Configuration conf = null;
  private static boolean failures = false;
  private static boolean initialized = false;
  private static MiniHBaseCluster cluster = null;
  private static HTableDescriptor desc = null;
  private static HClient client = null;

  // Set up environment, start mini cluster, etc.
  
  @SuppressWarnings("unchecked")
  public void testSetup() throws Exception {
    try {
      if(System.getProperty("test.build.data") == null) {
        String dir = new File(new File("").getAbsolutePath(), "build/contrib/hbase/test").getAbsolutePath();
        System.out.println(dir);
        System.setProperty("test.build.data", dir);
      }
      conf = new HBaseConfiguration();
      
      Environment.getenv();
      if(Environment.debugging) {
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.WARN);

        ConsoleAppender consoleAppender = null;
        for(Enumeration<Appender> e = (Enumeration<Appender>)rootLogger.getAllAppenders();
            e.hasMoreElements();) {
        
          Appender a = e.nextElement();
          if(a instanceof ConsoleAppender) {
            consoleAppender = (ConsoleAppender)a;
            break;
          }
        }
        if(consoleAppender != null) {
          Layout layout = consoleAppender.getLayout();
          if(layout instanceof PatternLayout) {
            PatternLayout consoleLayout = (PatternLayout)layout;
            consoleLayout.setConversionPattern("%d %-5p [%t] %l: %m%n");
          }
        }
        Logger.getLogger("org.apache.hadoop.hbase").setLevel(Environment.logLevel);
      }
      cluster = new MiniHBaseCluster(conf, 1);
      client = new HClient(conf);

      desc = new HTableDescriptor("test", 3);
      desc.addFamily(new Text(CONTENTS));
      desc.addFamily(new Text(ANCHOR));
      client.createTable(desc);
      
    } catch(Exception e) {
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
      long startTime = System.currentTimeMillis();
      
      client.openTable(desc.getName());
      
      // Write out a bunch of values
      
      for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
        long writeid = client.startUpdate(new Text("row_" + k));
        client.put(writeid, CONTENTS_BASIC, (CONTENTSTR + k).getBytes());
        client.put(writeid, new Text(ANCHORNUM + k), (ANCHORSTR + k).getBytes());
        client.commit(writeid);
      }
      System.out.println("Write " + NUM_VALS + " rows. Elapsed time: "
          + ((System.currentTimeMillis() - startTime) / 1000.0));

      // Read them back in

      startTime = System.currentTimeMillis();
      
      Text collabel = null;
      for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
        Text rowlabel = new Text("row_" + k);

        byte bodydata[] = client.get(rowlabel, CONTENTS_BASIC);
        assertNotNull(bodydata);
        String bodystr = new String(bodydata).toString().trim();
        String teststr = CONTENTSTR + k;
        assertEquals("Incorrect value for key: (" + rowlabel + "," + CONTENTS_BASIC
            + "), expected: '" + teststr + "' got: '" + bodystr + "'",
            bodystr, teststr);
        collabel = new Text(ANCHORNUM + k);
        bodydata = client.get(rowlabel, collabel);
        bodystr = new String(bodydata).toString().trim();
        teststr = ANCHORSTR + k;
        assertEquals("Incorrect value for key: (" + rowlabel + "," + collabel
            + "), expected: '" + teststr + "' got: '" + bodystr + "'",
            bodystr, teststr);
      }
      
      System.out.println("Read " + NUM_VALS + " rows. Elapsed time: "
          + ((System.currentTimeMillis() - startTime) / 1000.0));

    } catch(IOException e) {
      failures = true;
      throw e;
    }
  }
  
  public void testScanner() throws IOException {
    if(!initialized || failures) {
      throw new IllegalStateException();
    }

    Text[] cols = new Text[] {
        new Text(ANCHORNUM + "[0-9]+"),
        new Text(CONTENTS_BASIC)
    };
    
    long startTime = System.currentTimeMillis();
    
    HScannerInterface s = client.obtainScanner(cols, new Text());
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

      System.out.println("Scanned " + NUM_VALS
          + " rows. Elapsed time: "
          + ((System.currentTimeMillis() - startTime) / 1000.0));

    } catch(IOException e) {
      failures = true;
      throw e;
      
    } finally {
      s.close();
    }
  }

  public void testListTables() throws IOException {
    if(!initialized || failures) {
      throw new IllegalStateException();
    }
    
    try {
      HTableDescriptor[] tables = client.listTables();
      assertEquals(1, tables.length);
      assertEquals(desc.getName(), tables[0].getName());
      TreeSet<Text> families = tables[0].families();
      assertEquals(2, families.size());
      assertTrue(families.contains(new Text(CONTENTS)));
      assertTrue(families.contains(new Text(ANCHOR)));
      
    } catch(IOException e) {
      failures = true;
      throw e;
    }
  }
  
  public void testCleanup() throws IOException {
    if(!initialized) {
      throw new IllegalStateException();
    }
    
    try {
      if(!failures) {
        // Delete the table we created

        client.deleteTable(desc.getName());
        try {
          Thread.sleep(60000);                  // Wait for table to be deleted
          
        } catch(InterruptedException e) {
        }
      }
      
    } finally {
      // Shut down the cluster
    
      cluster.shutdown();
      client.close();
    }
  }
}
