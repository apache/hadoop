/**
 * Copyright 2009 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
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
  private static final byte [] CONTENTS_CF = Bytes.toBytes("contents");
  private static final String CONTENTS_CQ_STR = "basic";
  private static final byte [] CONTENTS_CQ = Bytes.toBytes(CONTENTS_CQ_STR);
  private static final String CONTENTSTR = "contentstr";
  //
  private static final byte [] ANCHOR_CF = Bytes.toBytes("anchor");
  private static final String ANCHORNUM_CQ = "anchornum-";
  private static final String ANCHORSTR_VALUE = "anchorstr";

  private void setup() throws IOException {
    desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor(CONTENTS_CF));
    desc.addFamily(new HColumnDescriptor(ANCHOR_CF));
    admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    table = new HTable(conf, desc.getName());
  }
      
  // Test basic functionality. Writes to contents:basic and anchor:anchornum-*

  private void basic() throws IOException {
    long startTime = System.currentTimeMillis();

    // Write out a bunch of values

    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      Put put = new Put(Bytes.toBytes("row_" + k));
      put.add(CONTENTS_CF, CONTENTS_CQ, Bytes.toBytes(CONTENTSTR + k));
      put.add(ANCHOR_CF, Bytes.toBytes(ANCHORNUM_CQ + k), Bytes.toBytes(ANCHORSTR_VALUE + k));
      table.put(put);
    }
    LOG.info("Write " + NUM_VALS + " rows. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));

    // Read them back in

    startTime = System.currentTimeMillis();

    byte [] collabel = null;
    for (int k = FIRST_ROW; k <= NUM_VALS; k++) {
      String rowlabelStr = "row_" + k;
      byte [] rowlabel = Bytes.toBytes(rowlabelStr);

      Get get = new Get(rowlabel);
      get.addColumn(CONTENTS_CF, CONTENTS_CQ);
      byte [] bodydata = table.get(get).getValue(CONTENTS_CF, CONTENTS_CQ);
      assertNotNull("no data for row " + rowlabelStr + "/" + CONTENTS_CQ_STR,
          bodydata);
      String bodystr = new String(bodydata, HConstants.UTF8_ENCODING);
      String teststr = CONTENTSTR + k;
      assertTrue("Incorrect value for key: (" + rowlabelStr + "/" +
          CONTENTS_CQ_STR + "), expected: '" + teststr + "' got: '" +
          bodystr + "'", teststr.compareTo(bodystr) == 0);
      
      String collabelStr = ANCHORNUM_CQ + k;
      collabel = Bytes.toBytes(collabelStr);
      
      get = new Get(rowlabel);
      get.addColumn(ANCHOR_CF, collabel);
      
      bodydata = table.get(get).getValue(ANCHOR_CF, collabel);
      assertNotNull("no data for row " + rowlabelStr + "/" + collabelStr, bodydata);
      bodystr = new String(bodydata, HConstants.UTF8_ENCODING);
      teststr = ANCHORSTR_VALUE + k;
      assertTrue("Incorrect value for key: (" + rowlabelStr + "/" + collabelStr +
          "), expected: '" + teststr + "' got: '" + bodystr + "'",
          teststr.compareTo(bodystr) == 0);
    }

    LOG.info("Read " + NUM_VALS + " rows. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));
  }
  
  private void scanner() throws IOException {
    
    long startTime = System.currentTimeMillis();
    
    Scan scan = new Scan();
    scan.addFamily(ANCHOR_CF);
    scan.addColumn(CONTENTS_CF, CONTENTS_CQ);
    ResultScanner s = table.getScanner(scan);
    try {

      int contentsFetched = 0;
      int anchorFetched = 0;
      int k = 0;
      for (Result curVals : s) {
        for(KeyValue kv : curVals.raw()) {
          byte [] family = kv.getFamily();
          byte [] qualifier = kv.getQualifier();
          String strValue = new String(kv.getValue());
          if(Bytes.equals(family, CONTENTS_CF)) {
            assertTrue("Error at:" + Bytes.toString(curVals.getRow()) 
                + ", Value for " + Bytes.toString(qualifier) + " should start with: " + CONTENTSTR
                + ", but was fetched as: " + strValue,
                strValue.startsWith(CONTENTSTR));
            contentsFetched++;
            
          } else if(Bytes.equals(family, ANCHOR_CF)) {
            assertTrue("Error at:" + Bytes.toString(curVals.getRow()) 
                + ", Value for " + Bytes.toString(qualifier) + " should start with: " + ANCHORSTR_VALUE
                + ", but was fetched as: " + strValue,
                strValue.startsWith(ANCHORSTR_VALUE));
            anchorFetched++;
            
          } else {
            LOG.info("Family: " + Bytes.toString(family) + ", Qualifier: " + Bytes.toString(qualifier));
          }
        }
        k++;
      }
      assertEquals("Expected " + NUM_VALS + " " +
        Bytes.toString(CONTENTS_CQ) + " values, but fetched " +
        contentsFetched,
        NUM_VALS, contentsFetched);
      assertEquals("Expected " + NUM_VALS + " " + ANCHORNUM_CQ +
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
    assertTrue(tables[0].hasFamily(CONTENTS_CF));
    assertTrue(tables[0].hasFamily(ANCHOR_CF));
  }
}