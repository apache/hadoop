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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests HTable
 */
public class TestHTable extends HBaseClusterTestCase implements HConstants {
  private static final HColumnDescriptor column =
    new HColumnDescriptor(COLUMN_FAMILY);

  private static final byte [] nosuchTable = Bytes.toBytes("nosuchTable");
  private static final byte [] tableAname = Bytes.toBytes("tableA");
  private static final byte [] tableBname = Bytes.toBytes("tableB");
  
  private static final byte [] row = Bytes.toBytes("row");
 
  private static final byte [] attrName = Bytes.toBytes("TESTATTR");
  private static final byte [] attrValue = Bytes.toBytes("somevalue");

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
    tableAdesc.addFamily(column);
    
    HTableDescriptor tableBdesc = new HTableDescriptor(tableBname);
    tableBdesc.addFamily(column);

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
    
    BatchUpdate batchUpdate = new BatchUpdate(row);
    batchUpdate.put(COLUMN_FAMILY, value);
    a.commit(batchUpdate);
    
    // open a new connection to A and a connection to b
    
    HTable newA = new HTable(conf, tableAname);
    HTable b = new HTable(conf, tableBname);

    // copy data from A to B
    
    Scanner s =
      newA.getScanner(COLUMN_FAMILY_ARRAY, EMPTY_START_ROW);
    
    try {
      for (RowResult r : s) {
        batchUpdate = new BatchUpdate(r.getRow());
        for(Map.Entry<byte [], Cell> e: r.entrySet()) {
          batchUpdate.put(e.getKey(), e.getValue().getValue());
        }
        b.commit(batchUpdate);
      }
    } finally {
      s.close();
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
      admin.modifyTable(tableAname, HConstants.MODIFY_TABLE_SET_HTD, desc);
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
  
  /**
    * For HADOOP-2579
    */
  public void testTableNotFoundExceptionWithATable() {
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      HTableDescriptor testTableADesc =
        new HTableDescriptor("table");
      testTableADesc.addFamily(column);
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
  
  public void testGetRow() {
    HTable table = null;
    try {
      HColumnDescriptor column2 =
        new HColumnDescriptor(Bytes.toBytes("info2:"));
      HBaseAdmin admin = new HBaseAdmin(conf);
      HTableDescriptor testTableADesc =
        new HTableDescriptor(tableAname);
      testTableADesc.addFamily(column);
      testTableADesc.addFamily(column2);
      admin.createTable(testTableADesc);
      
      table = new HTable(conf, tableAname);
      BatchUpdate batchUpdate = new BatchUpdate(row);
      
      for(int i = 0; i < 5; i++)
        batchUpdate.put(COLUMN_FAMILY_STR+i, Bytes.toBytes(i));
      
      table.commit(batchUpdate);

      assertTrue(table.exists(row));
      for(int i = 0; i < 5; i++)
        assertTrue(table.exists(row, Bytes.toBytes(COLUMN_FAMILY_STR+i)));

      RowResult result = null;
      result = table.getRow(row,  new byte[][] {COLUMN_FAMILY});
      for(int i = 0; i < 5; i++)
        assertTrue(result.containsKey(Bytes.toBytes(COLUMN_FAMILY_STR+i)));
      
      result = table.getRow(row);
      for(int i = 0; i < 5; i++)
        assertTrue(result.containsKey(Bytes.toBytes(COLUMN_FAMILY_STR+i)));

      batchUpdate = new BatchUpdate(row);
      batchUpdate.put("info2:a", Bytes.toBytes("a"));
      table.commit(batchUpdate);
      
      result = table.getRow(row, new byte[][] { COLUMN_FAMILY,
          Bytes.toBytes("info2:a") });
      for(int i = 0; i < 5; i++)
        assertTrue(result.containsKey(Bytes.toBytes(COLUMN_FAMILY_STR+i)));
      assertTrue(result.containsKey(Bytes.toBytes("info2:a")));
   
    } catch (IOException e) {
      e.printStackTrace();
      fail("Should not have any exception " +
        e.getClass());
    }
  }
  
  public void testGetClosestRowBefore() throws IOException {
    HColumnDescriptor column2 =
      new HColumnDescriptor(Bytes.toBytes("info2:"));
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor testTableADesc =
      new HTableDescriptor(tableAname);
    testTableADesc.addFamily(column);
    testTableADesc.addFamily(column2);
    admin.createTable(testTableADesc);
    
    byte[] firstRow = Bytes.toBytes("ro");
    byte[] beforeFirstRow = Bytes.toBytes("rn");
    byte[] beforeSecondRow = Bytes.toBytes("rov");
    
    HTable table = new HTable(conf, tableAname);
    BatchUpdate batchUpdate = new BatchUpdate(firstRow);
    BatchUpdate batchUpdate2 = new BatchUpdate(row);
    byte[] zero = new byte[]{0};
    byte[] one = new byte[]{1};
    byte[] columnFamilyBytes = Bytes.toBytes(COLUMN_FAMILY_STR);
    
    batchUpdate.put(COLUMN_FAMILY_STR,zero);
    batchUpdate2.put(COLUMN_FAMILY_STR,one);
    
    table.commit(batchUpdate);
    table.commit(batchUpdate2);
    
    RowResult result = null;
    
    // Test before first that null is returned
    result = table.getClosestRowBefore(beforeFirstRow, columnFamilyBytes);
    assertTrue(result == null);
    
    // Test at first that first is returned
    result = table.getClosestRowBefore(firstRow, columnFamilyBytes);
    assertTrue(result.containsKey(COLUMN_FAMILY_STR));
    assertTrue(Bytes.equals(result.get(COLUMN_FAMILY_STR).getValue(), zero));
    
    // Test inbetween first and second that first is returned
    result = table.getClosestRowBefore(beforeSecondRow, columnFamilyBytes);
    assertTrue(result.containsKey(COLUMN_FAMILY_STR));
    assertTrue(Bytes.equals(result.get(COLUMN_FAMILY_STR).getValue(), zero));
    
    // Test at second make sure second is returned
    result = table.getClosestRowBefore(row, columnFamilyBytes);
    assertTrue(result.containsKey(COLUMN_FAMILY_STR));
    assertTrue(Bytes.equals(result.get(COLUMN_FAMILY_STR).getValue(), one));
    
    // Test after second, make sure second is returned
    result = table.getClosestRowBefore(Bytes.add(row,one), columnFamilyBytes);
    assertTrue(result.containsKey(COLUMN_FAMILY_STR));
    assertTrue(Bytes.equals(result.get(COLUMN_FAMILY_STR).getValue(), one));
  }
}
