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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests forced splitting of HTable
 */
public class TestForceSplit extends HBaseClusterTestCase {
  private static final byte[] tableName = Bytes.toBytes("test");
  private static final byte[] columnName = Bytes.toBytes("a:");
  private static final byte[] key_mmi = Bytes.toBytes("mmi");

  protected void setUp() throws Exception {
    super.setUp();
    this.conf.setInt("hbase.io.index.interval", 32);
  }

  /**
   * the test
   * @throws IOException
   */
  public void testHTable() throws Exception {
    // create the test table
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(columnName));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(htd);
    HTable table = new HTable(conf, tableName);
    byte[] k = new byte[3];
    for (byte b1 = 'a'; b1 < 'z'; b1++) {
      for (byte b2 = 'a'; b2 < 'z'; b2++) {
        for (byte b3 = 'a'; b3 < 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          BatchUpdate update = new BatchUpdate(k);
          update.put(columnName, k);
          table.commit(update);
        }
      }
    }

    // get the initial layout (should just be one region)
    Map<HRegionInfo,HServerAddress> m = table.getRegionsInfo();
    System.out.println("Initial regions (" + m.size() + "): " + m);
    assertTrue(m.size() == 1);

    // tell the master to split the table
    admin.modifyTable(tableName, HConstants.MODIFY_TABLE_SPLIT);

    // give some time for the split to happen
    Thread.sleep(15 * 1000);

    // check again
    table = new HTable(conf, tableName);
    m = table.getRegionsInfo();
    System.out.println("Regions after split (" + m.size() + "): " + m);
    // should have two regions now
    assertTrue(m.size() == 2);
    // and "mmi" should be the midpoint
    for (HRegionInfo hri: m.keySet()) {
      byte[] start = hri.getStartKey();
      byte[] end = hri.getEndKey();
      if (Bytes.equals(start, HConstants.EMPTY_BYTE_ARRAY))
        assertTrue(Bytes.equals(end, key_mmi));
      if (Bytes.equals(end, key_mmi))
        assertTrue(Bytes.equals(start, HConstants.EMPTY_BYTE_ARRAY));
    }
  }
}
