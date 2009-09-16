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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/** Tests table creation restrictions*/
public class TestTable extends HBaseClusterTestCase {
  /**
   * the test
   * @throws IOException
   */
  public void testCreateTable() throws IOException {
    final HBaseAdmin admin = new HBaseAdmin(conf);
    String msg = null;
    try {
      admin.createTable(HTableDescriptor.ROOT_TABLEDESC);
    } catch (IllegalArgumentException e) {
      msg = e.toString();
    }
    assertTrue("Unexcepted exception message " + msg, msg != null &&
      msg.startsWith(IllegalArgumentException.class.getName()) &&
      msg.contains(HTableDescriptor.ROOT_TABLEDESC.getNameAsString()));
    
    msg = null;
    try {
      admin.createTable(HTableDescriptor.META_TABLEDESC);
    } catch(IllegalArgumentException e) {
      msg = e.toString();
    }
    assertTrue("Unexcepted exception message " + msg, msg != null &&
      msg.startsWith(IllegalArgumentException.class.getName()) &&
      msg.contains(HTableDescriptor.META_TABLEDESC.getNameAsString()));
    
    // Try doing a duplicate database create.
    msg = null;
    HTableDescriptor desc = new HTableDescriptor(getName());
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc);
    assertTrue("First table creation completed", admin.listTables().length == 1);
    boolean gotException = false;
    try {
      admin.createTable(desc);
    } catch (TableExistsException e) {
      gotException = true;
      msg = e.getMessage();
    }
    assertTrue("Didn't get a TableExistsException!", gotException);
    assertTrue("Unexpected exception message " + msg, msg != null &&
      msg.contains(getName()));
    
    // Now try and do concurrent creation with a bunch of threads.
    final HTableDescriptor threadDesc =
      new HTableDescriptor("threaded_" + getName());
    threadDesc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    int count = 10;
    Thread [] threads = new Thread [count];
    final AtomicInteger successes = new AtomicInteger(0);
    final AtomicInteger failures = new AtomicInteger(0);
    for (int i = 0; i < count; i++) {
      threads[i] = new Thread(Integer.toString(i)) {
        @Override
        public void run() {
          try {
            admin.createTable(threadDesc);
            successes.incrementAndGet();
          } catch (TableExistsException e) {
            failures.incrementAndGet();
          } catch (IOException e) {
            System.out.println("Got an IOException... " + e);
            fail();
          }
        }
      };
    }
    for (int i = 0; i < count; i++) {
      threads[i].start();
    }
    for (int i = 0; i < count; i++) {
      while(threads[i].isAlive()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
    // All threads are now dead.  Count up how many tables were created and
    // how many failed w/ appropriate exception.
    assertEquals(1, successes.get());
    assertEquals(count - 1, failures.get());
  }
  
  /**
   * Test for hadoop-1581 'HBASE: Unopenable tablename bug'.
   * @throws Exception
   */
  public void testTableNameClash() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(new HTableDescriptor(getName() + "SOMEUPPERCASE"));
    admin.createTable(new HTableDescriptor(getName()));
    // Before fix, below would fail throwing a NoServerForRegionException.
    @SuppressWarnings("unused")
    HTable table = new HTable(conf, getName());
  }

  /**
   * Test read only tables
   * @throws Exception
   */
  public void testReadOnlyTable() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor desc = new HTableDescriptor(getName());
    byte[] colName = Bytes.toBytes("test");
    desc.addFamily(new HColumnDescriptor(colName));
    desc.setReadOnly(true);
    admin.createTable(desc);
    HTable table = new HTable(conf, getName());
    try {
      byte[] value = Bytes.toBytes("somedata");
      // This used to use an empty row... That must have been a bug
      Put put = new Put(value);
      byte [][] famAndQf = KeyValue.parseColumn(colName);
      put.add(famAndQf[0], famAndQf[1], value);
      table.put(put);
      fail("Put on read only table succeeded");  
    } catch (Exception e) {
      // expected
    }
  }
  
  /**
   * Test that user table names can contain '-' and '.' so long as they do not
   * start with same. HBASE-771
   */
  public void testTableNames() {
    byte[][] illegalNames = new byte[][] {
        Bytes.toBytes("-bad"),
        Bytes.toBytes(".bad"),
        HConstants.ROOT_TABLE_NAME,
        HConstants.META_TABLE_NAME
    };
    for (int i = 0; i < illegalNames.length; i++) {
      try {
        new HTableDescriptor(illegalNames[i]);
        fail("Did not detect '" + Bytes.toString(illegalNames[i]) +
            "' as an illegal user table name");
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
    byte[] legalName = Bytes.toBytes("g-oo.d");
    try {
      new HTableDescriptor(legalName);
    } catch (IllegalArgumentException e) {
      fail("Legal user table name: '" + Bytes.toString(legalName) +
          "' caused IllegalArgumentException: " + e.getMessage());
    }
  }
}