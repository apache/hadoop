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

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests HTable
 */
public class TestHTable extends HBaseClusterTestCase implements HConstants {
  private static final HColumnDescriptor family1 =
    new HColumnDescriptor(CATALOG_FAMILY);

  private static final byte [] nosuchTable = Bytes.toBytes("nosuchTable");
  private static final byte [] tableAname = Bytes.toBytes("tableA");
  private static final byte [] tableBname = Bytes.toBytes("tableB");
  
  private static final byte [] row = Bytes.toBytes("row");
 
  private static final byte [] attrName = Bytes.toBytes("TESTATTR");
  private static final byte [] attrValue = Bytes.toBytes("somevalue");


  
  
  
  public void testGet() throws IOException {
    HTable table = null;
    try {
      HColumnDescriptor family2 =
        new HColumnDescriptor(Bytes.toBytes("info2"));
      HBaseAdmin admin = new HBaseAdmin(conf);
      HTableDescriptor testTableADesc =
        new HTableDescriptor(tableAname);
      testTableADesc.addFamily(family1);
      testTableADesc.addFamily(family2);
      admin.createTable(testTableADesc);
      
      table = new HTable(conf, tableAname);
      System.out.println("Adding row to table");
      Put put = new Put(row);
      
      for(int i = 0; i < 5; i++) {
        put.add(CATALOG_FAMILY, Bytes.toBytes(Integer.toString(i)), 
            Bytes.toBytes(i));
      }
      
      table.put(put);
      
//      Get get = new Get(row);
//      get.addColumn(CATALOG_FAMILY,Bytes.toBytes(2));
//      
//      System.out.println("Getting data from table");
//      Result res = table.get(get);
//      System.out.println("Got data from table");
//      System.out.println(res);
      
      

//      assertTrue(table.exists(row));
//      for(int i = 0; i < 5; i++)
//        assertTrue(table.exists(row, Bytes.toBytes(CATALOG_FAMILY_STR + i)));

      Get get = null;
      Result result = null;
      
      get = new Get(row);
      get.addFamily(CATALOG_FAMILY);
//      get.addColumn(CATALOG_FAMILY, Bytes.toBytes(Integer.toString(1)));
      System.out.println("Getting row");
      long start = System.nanoTime();
      result = table.get(get);
      long stop = System.nanoTime();
      System.out.println("timer " +(stop-start));
      System.out.println("result " +result);
      for(int i = 0; i < 5; i++)
        assertTrue(result.containsColumn(CATALOG_FAMILY, 
            Bytes.toBytes(Integer.toString(i))));

//      get = new Get(row);
//      result = table.get(get);
//      for(int i = 0; i < 5; i++)
//        assertTrue(result.containsColumn(CATALOG_FAMILY, 
//            Bytes.toBytes(Integer.toString(i))));
//
//      byte [] family = Bytes.toBytes("info2");
//      byte [] qf = Bytes.toBytes("a");
//      
//      put = new Put(row);
//      put.add(family, qf, qf);
//      table.put(put);
//      
//      get = new Get(row);
//      get.addFamily(CATALOG_FAMILY);
//      get.addColumn(family, qf);
//      result = table.get(get);
//      for(int i = 0; i < 5; i++)
//        assertTrue(result.containsColumn(CATALOG_FAMILY, 
//            Bytes.toBytes(Integer.toString(i))));
//      assertTrue(result.containsColumn(family, qf));
    } catch (IOException e) {
      e.printStackTrace();
      fail("Should not have any exception " +
        e.getClass());
    }    
  }

  

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
    
    HTableDescriptor tableAdesc = new HTableDescriptor(tableAname);
    tableAdesc.addFamily(family1);
    
    HTableDescriptor tableBdesc = new HTableDescriptor(tableBname);
    tableBdesc.addFamily(family1);

    // create a couple of tables
    
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(tableAdesc);
    admin.createTable(tableBdesc);
    
    // put some data into table A
    
    HTable a = new HTable(conf, tableAname);
    
    // Assert the metadata is good.
    HTableDescriptor meta =
      a.getConnection().getHTableDescriptor(tableAdesc.getName());
    assertTrue(meta.equals(tableAdesc));
    
    Put put = new Put(row);
    put.add(CATALOG_FAMILY, null, value);
    a.put(put);
    
    // open a new connection to A and a connection to b
    
    HTable newA = new HTable(conf, tableAname);
    HTable b = new HTable(conf, tableBname);

    // copy data from A to B
    
    Scan scan = new Scan();
    scan.addFamily(HConstants.CATALOG_FAMILY);

    ResultScanner s = newA.getScanner(scan);
    
    try {
      for (Result r : s) {
        put = new Put(r.getRow());
        for(KeyValue kv : r.sorted()) {
          put.add(kv);
        }
        b.put(put);
      }
    } finally {
      s.close();
    }
    
    // Opening a new connection to A will cause the tables to be reloaded

    try {
      HTable anotherA = new HTable(conf, tableAname);
      Get get = new Get(row);
      get.addFamily(CATALOG_FAMILY);
      anotherA.get(get);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    
    // We can still access A through newA because it has the table information
    // cached. And if it needs to recalibrate, that will cause the information
    // to be reloaded.

    // Test user metadata

    try {
      // make a modifiable descriptor
      HTableDescriptor desc = new HTableDescriptor(a.getTableDescriptor());
      // offline the table
      admin.disableTable(tableAname);
      // add a user attribute to HTD
      desc.setValue(attrName, attrValue);
      // add a user attribute to HCD
      for (HColumnDescriptor c: desc.getFamilies())
        c.setValue(attrName, attrValue);
      // update metadata for all regions of this table
      admin.modifyTable(tableAname, HConstants.Modify.TABLE_SET_HTD, desc);
      // enable the table
      admin.enableTable(tableAname);

      // test that attribute changes were applied
      desc = a.getTableDescriptor();
      if (Bytes.compareTo(desc.getName(), tableAname) != 0)
        fail("wrong table descriptor returned");
      // check HTD attribute
      value = desc.getValue(attrName);
      if (value == null)
        fail("missing HTD attribute value");
      if (Bytes.compareTo(value, attrValue) != 0)
        fail("HTD attribute value is incorrect");
      // check HCD attribute
      for (HColumnDescriptor c: desc.getFamilies()) {
        value = c.getValue(attrName);
        if (value == null)
          fail("missing HCD attribute value");
        if (Bytes.compareTo(value, attrValue) != 0)
          fail("HCD attribute value is incorrect");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * For HADOOP-2579
   */
  public void testTableNotFoundExceptionWithoutAnyTables() {
    try {
      new HTable(conf, "notATable");
      fail("Should have thrown a TableNotFoundException");
    } catch (TableNotFoundException e) {
      // expected
    } catch (IOException e) {
      e.printStackTrace();
      fail("Should have thrown a TableNotFoundException instead of a " +
        e.getClass());
    }
  }

  public void testGetClosestRowBefore() throws IOException {
    HColumnDescriptor family2 =
      new HColumnDescriptor(Bytes.toBytes("info2"));
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor testTableADesc =
      new HTableDescriptor(tableAname);
    testTableADesc.addFamily(family1);
    testTableADesc.addFamily(family2);
    admin.createTable(testTableADesc);
    
    byte[] firstRow = Bytes.toBytes("ro");
    byte[] beforeFirstRow = Bytes.toBytes("rn");
    byte[] beforeSecondRow = Bytes.toBytes("rov");
    
    HTable table = new HTable(conf, tableAname);
    Put put = new Put(firstRow);
    Put put2 = new Put(row);
    byte[] zero = new byte[]{0};
    byte[] one = new byte[]{1};
    
    put.add(CATALOG_FAMILY, null, zero);
    put2.add(CATALOG_FAMILY, null, one);
    
    table.put(put);
    table.put(put2);
    
    Result result = null;
    
    // Test before first that null is returned
    result = table.getRowOrBefore(beforeFirstRow, CATALOG_FAMILY);
    assertTrue(result == null);
    
    // Test at first that first is returned
    result = table.getRowOrBefore(firstRow, CATALOG_FAMILY);
    assertTrue(result.containsColumn(CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getValue(CATALOG_FAMILY, null), zero));
    
    // Test in between first and second that first is returned
    result = table.getRowOrBefore(beforeSecondRow, CATALOG_FAMILY);
    assertTrue(result.containsColumn(CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getValue(CATALOG_FAMILY, null), zero));
    
    // Test at second make sure second is returned
    result = table.getRowOrBefore(row, CATALOG_FAMILY);
    assertTrue(result.containsColumn(CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getValue(CATALOG_FAMILY, null), one));
    
    // Test after second, make sure second is returned
    result = table.getRowOrBefore(Bytes.add(row,one), CATALOG_FAMILY);
    assertTrue(result.containsColumn(CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getValue(CATALOG_FAMILY, null), one));
  }

  /**
   * For HADOOP-2579
   */
  public void testTableNotFoundExceptionWithATable() {
   try {
     HBaseAdmin admin = new HBaseAdmin(conf);
     HTableDescriptor testTableADesc =
       new HTableDescriptor("table");
     testTableADesc.addFamily(family1);
     admin.createTable(testTableADesc);

     // This should throw a TableNotFoundException, it has not been created
     new HTable(conf, "notATable");
     
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
