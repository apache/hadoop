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
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;

/**
 * Test HBase Master and Region servers, client API 
 */
public class TestHBaseCluster extends HBaseClusterTestCase {

  private HTableDescriptor desc;
  private HBaseAdmin admin;
  private HTable table;

  /** constructor */
  public TestHBaseCluster() {
    super(true);
    this.desc = null;
    this.admin = null;
    this.table = null;

    // Make the thread wake frequency a little slower so other threads
    // can run
    conf.setInt("hbase.server.thread.wakefrequency", 2000);
    
    // Make lease timeout longer, lease checks less frequent
    conf.setInt("hbase.master.lease.period", 10 * 1000);
    conf.setInt("hbase.master.lease.thread.wakefrequency", 5 * 1000);
    
    // Increase the amount of time between client retries
    conf.setLong("hbase.client.pause", 15 * 1000);
  }

  /**
   * Since all the "tests" depend on the results of the previous test, they are
   * not Junit tests that can stand alone. Consequently we have a single Junit
   * test that runs the "sub-tests" as private methods.
   * @throws IOException 
   */
  public void testHBaseCluster() throws IOException {
    setup();
    basic();
    scanner();
    listTables();
    cleanup();
  }

  private static final int FIRST_ROW = 1;
  private static final int NUM_VALS = 1000;
  private static final Text CONTENTS = new Text("contents:");
  private static final Text CONTENTS_BASIC = new Text("contents:basic");
  private static final String CONTENTSTR = "contentstr";
  private static final Text ANCHOR = new Text("anchor:");
  private static final String ANCHORNUM = "anchor:anchornum-";
  private static final String ANCHORSTR = "anchorstr";

  private void setup() throws IOException {
    desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor(CONTENTS.toString()));
    desc.addFamily(new HColumnDescriptor(ANCHOR.toString()));
    admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    table = new HTable(conf, desc.getName());
  }
      
  // Test basic functionality. Writes to contents:basic and anchor:anchornum-*

  private void basic() throws IOException {
    long startTime = System.currentTimeMillis();

    // Write out a bunch of values

    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      long writeid = table.startUpdate(new Text("row_" + k));
      table.put(writeid, CONTENTS_BASIC,
          (CONTENTSTR + k).getBytes(HConstants.UTF8_ENCODING));
      table.put(writeid, new Text(ANCHORNUM + k),
          (ANCHORSTR + k).getBytes(HConstants.UTF8_ENCODING));
      table.commit(writeid);
    }
    System.out.println("Write " + NUM_VALS + " rows. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // Read them back in

    startTime = System.currentTimeMillis();

    Text collabel = null;
    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      Text rowlabel = new Text("row_" + k);

      byte bodydata[] = table.get(rowlabel, CONTENTS_BASIC);
      assertNotNull("no data for row " + rowlabel + "/" + CONTENTS_BASIC,
          bodydata);
      String bodystr = new String(bodydata, HConstants.UTF8_ENCODING);
      String teststr = CONTENTSTR + k;
      assertTrue("Incorrect value for key: (" + rowlabel + "/" +
          CONTENTS_BASIC + "), expected: '" + teststr + "' got: '" +
          bodystr + "'", teststr.compareTo(bodystr) == 0);
      
      collabel = new Text(ANCHORNUM + k);
      bodydata = table.get(rowlabel, collabel);
      assertNotNull("no data for row " + rowlabel + "/" + collabel, bodydata);
      bodystr = new String(bodydata, HConstants.UTF8_ENCODING);
      teststr = ANCHORSTR + k;
      assertTrue("Incorrect value for key: (" + rowlabel + "/" + collabel +
          "), expected: '" + teststr + "' got: '" + bodystr + "'",
          teststr.compareTo(bodystr) == 0);
    }

    System.out.println("Read " + NUM_VALS + " rows. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));
  }
  
  private void scanner() throws IOException {
    Text[] cols = new Text[] {
        new Text(ANCHORNUM + "[0-9]+"),
        new Text(CONTENTS_BASIC)
    };
    
    long startTime = System.currentTimeMillis();
    
    HScannerInterface s = table.obtainScanner(cols, new Text());
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

    } finally {
      s.close();
    }
  }

  private void listTables() throws IOException {
    HTableDescriptor[] tables = admin.listTables();
    assertEquals(1, tables.length);
    assertEquals(desc.getName(), tables[0].getName());
    Set<Text> families = tables[0].families().keySet();
    assertEquals(2, families.size());
    assertTrue(families.contains(new Text(CONTENTS)));
    assertTrue(families.contains(new Text(ANCHOR)));
  }
  
  private void cleanup() throws IOException {

    // Delete the table we created

    admin.deleteTable(desc.getName());
  }
}
