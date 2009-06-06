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

package org.apache.hadoop.hbase.client;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 */
public class TestOldAPIGetRowVersions extends HBaseClusterTestCase {
  private static final Log LOG = LogFactory.getLog(TestGetRowVersions.class);
  private static final String TABLE_NAME = "test";
  private static final String CONTENTS_STR = "contents:";
  private static final String ROW = "row";
  private static final String COLUMN = "contents:contents";
  private static final long TIMESTAMP = System.currentTimeMillis();
  private static final String VALUE1 = "value1";
  private static final String VALUE2 = "value2";
  private HBaseAdmin admin = null;
  private HTable table = null;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(CONTENTS_STR));
    this.admin = new HBaseAdmin(conf);
    this.admin.createTable(desc);
    this.table = new HTable(conf, TABLE_NAME);
  }

  /** @throws Exception */
  public void testGetRowMultipleVersions() throws Exception {
    BatchUpdate b = new BatchUpdate(ROW, TIMESTAMP);
    b.put(COLUMN, Bytes.toBytes(VALUE1));
    this.table.commit(b);
    /* Taking out this recycle of the mini cluster -- it don't work well
     * Debug it if fails in TestGetRowVersion, not this old api version.
    // Shut down and restart the HBase cluster
    this.cluster.shutdown();
    this.zooKeeperCluster.shutdown();
    LOG.debug("HBase cluster shut down -- restarting");
    this.hBaseClusterSetup();
    */
    // Make a new connection
    this.table = new HTable(conf, TABLE_NAME);
    // Overwrite previous value
    b = new BatchUpdate(ROW, TIMESTAMP);
    b.put(COLUMN, Bytes.toBytes(VALUE2));
    this.table.commit(b);
    // Now verify that getRow(row, column, latest) works
    RowResult r = table.getRow(ROW);
    assertNotNull(r);
    assertTrue(r.size() != 0);
    Cell c = r.get(COLUMN);
    assertNotNull(c);
    assertTrue(c.getValue().length != 0);
    String value = Bytes.toString(c.getValue());
    assertTrue(value.compareTo(VALUE2) == 0);
    // Now check getRow with multiple versions
    r = table.getRow(ROW, HConstants.ALL_VERSIONS);
    for (Map.Entry<byte[], Cell> e: r.entrySet()) {
      // Column name
//      System.err.print("  " + Bytes.toString(e.getKey()));
      c = e.getValue();
      
      // Need to iterate since there may be multiple versions
      for (Iterator<Map.Entry<Long, byte[]>> it = c.iterator();
            it.hasNext(); ) {
        Map.Entry<Long, byte[]> v = it.next();
        value = Bytes.toString(v.getValue());
//        System.err.println(" = " + value);
        assertTrue(VALUE2.compareTo(Bytes.toString(v.getValue())) == 0);
      }
    }
  }
}