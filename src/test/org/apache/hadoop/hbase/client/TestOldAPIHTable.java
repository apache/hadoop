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
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests HTable
 */
public class TestOldAPIHTable extends HBaseClusterTestCase implements HConstants {
  private static final String COLUMN_FAMILY_STR = "contents:";
  private static final byte [] COLUMN_FAMILY = Bytes.toBytes(COLUMN_FAMILY_STR);
  private static final byte [][] COLUMN_FAMILY_ARRAY = {COLUMN_FAMILY};
  
  private static final HColumnDescriptor column =
    new HColumnDescriptor(COLUMN_FAMILY);

  private static final byte [] nosuchTable = Bytes.toBytes("nosuchTable");
  private static final byte [] tableAname = Bytes.toBytes("tableA");
  private static final byte [] tableBname = Bytes.toBytes("tableB");
  
  private static final byte [] row = Bytes.toBytes("row");
 
  private static final byte [] attrName = Bytes.toBytes("TESTATTR");
  private static final byte [] attrValue = Bytes.toBytes("somevalue");


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

//  public void testCheckAndSave() throws IOException {
//    HTable table = null;
//    HColumnDescriptor column2 =
//      new HColumnDescriptor(Bytes.toBytes("info2:"));
//    HBaseAdmin admin = new HBaseAdmin(conf);
//    HTableDescriptor testTableADesc =
//      new HTableDescriptor(tableAname);
//    testTableADesc.addFamily(column);
//    testTableADesc.addFamily(column2);
//    admin.createTable(testTableADesc);
//    
//    table = new HTable(conf, tableAname);
//    BatchUpdate batchUpdate = new BatchUpdate(row);
//    BatchUpdate batchUpdate2 = new BatchUpdate(row);
//    BatchUpdate batchUpdate3 = new BatchUpdate(row);
//
//    // this row doesn't exist when checkAndSave is invoked
//    byte [] row1 = Bytes.toBytes("row1");
//    BatchUpdate batchUpdate4 = new BatchUpdate(row1);
//    
//    // to be used for a checkAndSave for expected empty columns
//    BatchUpdate batchUpdate5 = new BatchUpdate(row);
//
//    HbaseMapWritable<byte[],byte[]> expectedValues =
//      new HbaseMapWritable<byte[],byte[]>();
//    HbaseMapWritable<byte[],byte[]> badExpectedValues =
//      new HbaseMapWritable<byte[],byte[]>();
//    HbaseMapWritable<byte[],byte[]> expectedNoValues =
//      new HbaseMapWritable<byte[],byte[]>();
//    // the columns used here must not be updated on batchupate
//    HbaseMapWritable<byte[],byte[]> expectedNoValues1 =
//      new HbaseMapWritable<byte[],byte[]>();
//
//    for(int i = 0; i < 5; i++) {
//      // This batchupdate is our initial batch update,
//      // As such we also set our expected values to the same values
//      // since we will be comparing the two
//      batchUpdate.put(COLUMN_FAMILY_STR+i, Bytes.toBytes(i));
//      expectedValues.put(Bytes.toBytes(COLUMN_FAMILY_STR+i), Bytes.toBytes(i));
//      
//      badExpectedValues.put(Bytes.toBytes(COLUMN_FAMILY_STR+i),
//        Bytes.toBytes(500));
//
//      expectedNoValues.put(Bytes.toBytes(COLUMN_FAMILY_STR+i), new byte[] {});
//      // the columns used here must not be updated on batchupate
//      expectedNoValues1.put(Bytes.toBytes(COLUMN_FAMILY_STR+i+","+i), new byte[] {});
//
//
//      // This is our second batchupdate that we will use to update the initial
//      // batchupdate
//      batchUpdate2.put(COLUMN_FAMILY_STR+i, Bytes.toBytes(i+1));
//      
//      // This final batch update is to check that our expected values (which
//      // are now wrong)
//      batchUpdate3.put(COLUMN_FAMILY_STR+i, Bytes.toBytes(i+2));
//
//      // Batch update that will not happen because it is to happen with some 
//      // expected values, but the row doesn't exist
//      batchUpdate4.put(COLUMN_FAMILY_STR+i, Bytes.toBytes(i));
//
//      // Batch update will happen: the row exists, but the expected columns don't,
//      // just as the condition
//      batchUpdate5.put(COLUMN_FAMILY_STR+i, Bytes.toBytes(i+3));
//    }
//    
//    // Initialize rows
//    table.commit(batchUpdate);
//    
//    // check if incorrect values are returned false
//    assertFalse(table.checkAndSave(batchUpdate2,badExpectedValues,null));
//    
//    // make sure first expected values are correct
//    assertTrue(table.checkAndSave(batchUpdate2, expectedValues,null));
//        
//    // make sure check and save truly saves the data after checking the expected
//    // values
//    RowResult r = table.getRow(row);
//    byte[][] columns = batchUpdate2.getColumns();
//    for(int i = 0;i < columns.length;i++) {
//      assertTrue(Bytes.equals(r.get(columns[i]).getValue(),batchUpdate2.get(columns[i])));
//    }
//    
//    // make sure that the old expected values fail
//    assertFalse(table.checkAndSave(batchUpdate3, expectedValues,null));
//
//    // row doesn't exist, so doesn't matter the expected 
//    // values (unless they are empty) 
//    assertFalse(table.checkAndSave(batchUpdate4, badExpectedValues, null));
//
//    assertTrue(table.checkAndSave(batchUpdate4, expectedNoValues, null));
//    // make sure check and save saves the data when expected values were empty and the row
//    // didn't exist
//    r = table.getRow(row1);
//    columns = batchUpdate4.getColumns();
//    for(int i = 0; i < columns.length;i++) {
//      assertTrue(Bytes.equals(r.get(columns[i]).getValue(),batchUpdate4.get(columns[i])));
//    }  
//
//    // since the row isn't empty anymore, those expected (empty) values 
//    // are not valid anymore, so check and save method doesn't save. 
//    assertFalse(table.checkAndSave(batchUpdate4, expectedNoValues, null));
//    
//    // the row exists, but the columns don't. since the expected values are 
//    // for columns without value, checkAndSave must be successful. 
//    assertTrue(table.checkAndSave(batchUpdate5, expectedNoValues1, null));
//    // make sure checkAndSave saved values for batchUpdate5.
//    r = table.getRow(row);
//    columns = batchUpdate5.getColumns();
//    for(int i = 0; i < columns.length;i++) {
//      assertTrue(Bytes.equals(r.get(columns[i]).getValue(),batchUpdate5.get(columns[i])));
//    }  
//
//    // since the condition wasn't changed, the following checkAndSave 
//    // must also be successful.
//    assertTrue(table.checkAndSave(batchUpdate, expectedNoValues1, null));
//    // make sure checkAndSave saved values for batchUpdate1
//    r = table.getRow(row);
//    columns = batchUpdate.getColumns();
//    for(int i = 0; i < columns.length;i++) {
//      assertTrue(Bytes.equals(r.get(columns[i]).getValue(),batchUpdate.get(columns[i])));
//    }
//
//    // one failing condition must make the following checkAndSave fail
//    // the failing condition is a column to be empty, however, it has a value.
//    HbaseMapWritable<byte[],byte[]> expectedValues1 =
//      new HbaseMapWritable<byte[],byte[]>();
//    expectedValues1.put(Bytes.toBytes(COLUMN_FAMILY_STR+0), new byte[] {});
//    expectedValues1.put(Bytes.toBytes(COLUMN_FAMILY_STR+"EMPTY+ROW"), new byte[] {});
//    assertFalse(table.checkAndSave(batchUpdate5, expectedValues1, null));
//
//    // assure the values on the row remain the same
//    r = table.getRow(row);
//    columns = batchUpdate.getColumns();
//    for(int i = 0; i < columns.length;i++) {
//      assertTrue(Bytes.equals(r.get(columns[i]).getValue(),batchUpdate.get(columns[i])));
//    }    
//  }

}
