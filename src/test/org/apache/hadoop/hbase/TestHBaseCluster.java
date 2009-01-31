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
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test HBase Master and Region servers, client API 
 */
public class TestHBaseCluster extends HBaseClusterTestCase {
  private static final Log LOG = LogFactory.getLog(TestHBaseCluster.class);

  private HTableDescriptor desc;
  private HBaseAdmin admin;
  private HTable table;

  /** constructor */
  public TestHBaseCluster() {
    super();
    this.desc = null;
    this.admin = null;
    this.table = null;

    // Make the thread wake frequency a little slower so other threads
    // can run
    conf.setInt("hbase.server.thread.wakefrequency", 2000);
    
    // Make lease timeout longer, lease checks less frequent
    conf.setInt("hbase.master.lease.period", 10 * 1000);
    
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
  }

  private static final int FIRST_ROW = 1;
  private static final int NUM_VALS = 1000;
  private static final byte [] CONTENTS = Bytes.toBytes("contents:");
  private static final byte [] CONTENTS_MINUS_COLON = Bytes.toBytes("contents");
  private static final byte [] CONTENTS_BASIC = Bytes.toBytes("contents:basic");
  private static final String CONTENTSTR = "contentstr";
  private static final byte [] ANCHOR = Bytes.toBytes("anchor:");
  private static final byte [] ANCHOR_MINUS_COLON = Bytes.toBytes("anchor");
  private static final String ANCHORNUM = "anchor:anchornum-";
  private static final String ANCHORSTR = "anchorstr";

  private void setup() throws IOException {
    desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor(CONTENTS));
    desc.addFamily(new HColumnDescriptor(ANCHOR));
    admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    table = new HTable(conf, desc.getName());
  }
      
  // Test basic functionality. Writes to contents:basic and anchor:anchornum-*

  private void basic() throws IOException {
    long startTime = System.currentTimeMillis();

    // Write out a bunch of values

    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      BatchUpdate b = new BatchUpdate("row_" + k);
      b.put(CONTENTS_BASIC, Bytes.toBytes(CONTENTSTR + k));
      b.put(ANCHORNUM + k, Bytes.toBytes(ANCHORSTR + k));
      table.commit(b);
    }
    LOG.info("Write " + NUM_VALS + " rows. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // Read them back in

    startTime = System.currentTimeMillis();

    byte [] collabel = null;
    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      byte [] rowlabel = Bytes.toBytes("row_" + k);

      byte bodydata[] = table.get(rowlabel, CONTENTS_BASIC).getValue();
      assertNotNull("no data for row " + rowlabel + "/" + CONTENTS_BASIC,
          bodydata);
      String bodystr = new String(bodydata, HConstants.UTF8_ENCODING);
      String teststr = CONTENTSTR + k;
      assertTrue("Incorrect value for key: (" + rowlabel + "/" +
          CONTENTS_BASIC + "), expected: '" + teststr + "' got: '" +
          bodystr + "'", teststr.compareTo(bodystr) == 0);
      
      collabel = Bytes.toBytes(ANCHORNUM + k);
      bodydata = table.get(rowlabel, collabel).getValue();
      assertNotNull("no data for row " + rowlabel + "/" + collabel, bodydata);
      bodystr = new String(bodydata, HConstants.UTF8_ENCODING);
      teststr = ANCHORSTR + k;
      assertTrue("Incorrect value for key: (" + rowlabel + "/" + collabel +
          "), expected: '" + teststr + "' got: '" + bodystr + "'",
          teststr.compareTo(bodystr) == 0);
    }

    LOG.info("Read " + NUM_VALS + " rows. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));
  }
  
  private void scanner() throws IOException {
    byte [][] cols = new byte [][] {Bytes.toBytes(ANCHORNUM + "[0-9]+"),
      CONTENTS_BASIC};
    
    long startTime = System.currentTimeMillis();
    
    Scanner s = table.getScanner(cols, HConstants.EMPTY_BYTE_ARRAY);
    try {

      int contentsFetched = 0;
      int anchorFetched = 0;
      int k = 0;
      for (RowResult curVals : s) {
        for (Iterator<byte []> it = curVals.keySet().iterator(); it.hasNext(); ) {
          byte [] col = it.next();
          byte val[] = curVals.get(col).getValue();
          String curval = Bytes.toString(val);
          if (Bytes.compareTo(col, CONTENTS_BASIC) == 0) {
            assertTrue("Error at:" + curVals.getRow() 
                + ", Value for " + col + " should start with: " + CONTENTSTR
                + ", but was fetched as: " + curval,
                curval.startsWith(CONTENTSTR));
            contentsFetched++;
            
          } else if (Bytes.toString(col).startsWith(ANCHORNUM)) {
            assertTrue("Error at:" + curVals.getRow()
                + ", Value for " + col + " should start with: " + ANCHORSTR
                + ", but was fetched as: " + curval,
                curval.startsWith(ANCHORSTR));
            anchorFetched++;
            
          } else {
            LOG.info(Bytes.toString(col));
          }
        }
        k++;
      }
      assertEquals("Expected " + NUM_VALS + " " +
        Bytes.toString(CONTENTS_BASIC) + " values, but fetched " +
        contentsFetched,
        NUM_VALS, contentsFetched);
      assertEquals("Expected " + NUM_VALS + " " + ANCHORNUM +
        " values, but fetched " + anchorFetched,
        NUM_VALS, anchorFetched);

      LOG.info("Scanned " + NUM_VALS
          + " rows. Elapsed time: "
          + ((System.currentTimeMillis() - startTime) / 1000.0));

    } finally {
      s.close();
    }
  }

  private void listTables() throws IOException {
    HTableDescriptor[] tables = admin.listTables();
    assertEquals(1, tables.length);
    assertTrue(Bytes.equals(desc.getName(), tables[0].getName()));
    Collection<HColumnDescriptor> families = tables[0].getFamilies();
    assertEquals(2, families.size());
    assertTrue(tables[0].hasFamily(CONTENTS));
    assertTrue(tables[0].hasFamily(ANCHOR));
  }
}