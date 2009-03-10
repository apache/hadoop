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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Bytes;

public class TestAtomicIncrement extends HBaseClusterTestCase {
  static final Log LOG = LogFactory.getLog(TestAtomicIncrement.class);

  private static final byte [] CONTENTS = Bytes.toBytes("contents:");

  public void testIncrement() throws IOException {
    try {
      HTable table = null;

      // Setup

      HTableDescriptor desc = new HTableDescriptor(getName());
      desc.addFamily(
          new HColumnDescriptor(CONTENTS,               // Column name
              1,                                        // Max versions
              HColumnDescriptor.DEFAULT_COMPRESSION,   // no compression
              HColumnDescriptor.DEFAULT_IN_MEMORY,      // not in memory
              HColumnDescriptor.DEFAULT_BLOCKCACHE,
              HColumnDescriptor.DEFAULT_LENGTH,
              HColumnDescriptor.DEFAULT_TTL,
              false
          )
      );

      // Create the table

      HBaseAdmin admin = new HBaseAdmin(conf);
      admin.createTable(desc);

      try {
        // Give cache flusher and log roller a chance to run
        // Otherwise we'll never hit the bloom filter, just the memcache
        Thread.sleep(conf.getLong(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000) * 10);
        
      } catch (InterruptedException e) {
        // ignore
      }
      // Open table

      table = new HTable(conf, desc.getName());
      
      byte [] row = Bytes.toBytes("foo");
      byte [] column = "contents:1".getBytes(HConstants.UTF8_ENCODING);
      // increment by 1:
      assertEquals(1L, table.incrementColumnValue(row, column, 1));
      
      // set a weird value, then increment:
      row = Bytes.toBytes("foo2");
      byte [] value = {0,0,1};
      BatchUpdate bu = new BatchUpdate(row);
      bu.put(column, value);
      table.commit(bu);
      
      assertEquals(2L, table.incrementColumnValue(row, column, 1));

      assertEquals(-2L, table.incrementColumnValue(row, column, -4));

      row = Bytes.toBytes("foo3");
      byte[] value2 = {1,2,3,4,5,6,7,8,9};
      bu = new BatchUpdate(row);
      bu.put(column, value2);
      table.commit(bu);
      
      try {
        table.incrementColumnValue(row, column, 1);
        fail();
      } catch (IOException e) {
        System.out.println("Expected exception: " + e);
        // expected exception.
      }
      

    } catch (Exception e) {
      e.printStackTrace();
      if (e instanceof IOException) {
        IOException i = (IOException) e;
        throw i;
      }
      fail();
    }

  }

}
