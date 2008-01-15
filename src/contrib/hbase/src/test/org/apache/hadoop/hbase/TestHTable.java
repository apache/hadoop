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
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Tests HTable
 */
public class TestHTable extends HBaseClusterTestCase implements HConstants {
  private static final Log LOG = LogFactory.getLog(TestHTable.class);
  private static final HColumnDescriptor column =
    new HColumnDescriptor(COLUMN_FAMILY.toString());

  private static final Text nosuchTable = new Text("nosuchTable");
  private static final Text tableAname = new Text("tableA");
  private static final Text tableBname = new Text("tableB");
  
  private static final Text row = new Text("row");
 
  /**
   * the test
   * @throws IOException
   */
  public void testHTable() throws IOException {
    byte[] value = "value".getBytes(UTF8_ENCODING);
    
    try {
      new HTable(conf, nosuchTable);
      
    } catch (TableNotFoundException e) {
      // expected

    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
    
    HTableDescriptor tableAdesc = new HTableDescriptor(tableAname.toString());
    tableAdesc.addFamily(column);
    
    HTableDescriptor tableBdesc = new HTableDescriptor(tableBname.toString());
    tableBdesc.addFamily(column);

    // create a couple of tables
    
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(tableAdesc);
    admin.createTable(tableBdesc);
    
    // put some data into table A
    
    HTable a = new HTable(conf, tableAname);
    
    // Assert the metadata is good.
    HTableDescriptor meta = a.getMetadata();
    assertTrue(meta.equals(tableAdesc));
    
    long lockid = a.startUpdate(row);
    a.put(lockid, COLUMN_FAMILY, value);
    a.commit(lockid);
    
    // open a new connection to A and a connection to b
    
    HTable newA = new HTable(conf, tableAname);
    HTable b = new HTable(conf, tableBname);

    // copy data from A to B
    
    HScannerInterface s =
      newA.obtainScanner(COLUMN_FAMILY_ARRAY, EMPTY_START_ROW);
    
    try {
      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
      while(s.next(key, results)) {
        lockid = b.startUpdate(key.getRow());
        for(Map.Entry<Text, byte[]> e: results.entrySet()) {
          b.put(lockid, e.getKey(), e.getValue());
        }
        b.commit(lockid);
        b.abort(lockid);
      }
    } finally {
      s.close();
    }
    
    // Close table A and note how A becomes inaccessable
    
    a.close();
    
    try {
      a.get(row, COLUMN_FAMILY);
      fail();
    } catch (IllegalStateException e) {
      // expected
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    
    // Opening a new connection to A will cause the tables to be reloaded

    try {
      HTable anotherA = new HTable(conf, tableAname);
      anotherA.get(row, COLUMN_FAMILY);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    
    // We can still access A through newA because it has the table information
    // cached. And if it needs to recalibrate, that will cause the information
    // to be reloaded.
    
  }
  
  /**
    * For HADOOP-2579
    */
  public void testTableNotFoundExceptionWithoutAnyTables() {
    try {
      new HTable(conf, new Text("notATable"));
      fail("Should have thrown a TableNotFoundException");
    } catch (TableNotFoundException e) {
      // expected
    } catch (IOException e) {
      e.printStackTrace();
      fail("Should have thrown a TableNotFoundException instead of a " +
        e.getClass());
    }
  }
  
  /**
    * For HADOOP-2579
    */
  public void testTableNotFoundExceptionWithATable() {
    try {
      HColumnDescriptor column =
        new HColumnDescriptor(COLUMN_FAMILY.toString());
      HBaseAdmin admin = new HBaseAdmin(conf);
      HTableDescriptor testTableADesc =
        new HTableDescriptor("table");
      testTableADesc.addFamily(column);
      admin.createTable(testTableADesc);

      // This should throw a TableNotFoundException, it has not been created
      new HTable(conf, new Text("notATable"));
      
      fail("Should have thrown a TableNotFoundException");
    } catch (TableNotFoundException e) {
      // expected
    } catch (IOException e) {
      e.printStackTrace();
      fail("Should have thrown a TableNotFoundException instead of a " +
        e.getClass());
    }
  }
  
}
