/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.thrift;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.NotFound;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Unit testing for ThriftServer.HBaseHandler, a part of the 
 * org.apache.hadoop.hbase.thrift package.  
 */
public class DisabledTestThriftServer extends HBaseClusterTestCase {

  // Static names for tables, columns, rows, and values
  private static byte[] tableAname = Bytes.toBytes("tableA");
  private static byte[] tableBname = Bytes.toBytes("tableB");
  private static byte[] columnAname = Bytes.toBytes("columnA:");
  private static byte[] columnBname = Bytes.toBytes("columnB:");
  private static byte[] badColumnName = Bytes.toBytes("forgotColon");
  private static byte[] rowAname = Bytes.toBytes("rowA");
  private static byte[] rowBname = Bytes.toBytes("rowB");
  private static byte[] valueAname = Bytes.toBytes("valueA");
  private static byte[] valueBname = Bytes.toBytes("valueB");
  private static byte[] valueCname = Bytes.toBytes("valueC");
  private static byte[] valueDname = Bytes.toBytes("valueD");

  /**
   * Runs all of the tests under a single JUnit test method.  We 
   * consolidate all testing to one method because HBaseClusterTestCase
   * is prone to OutOfMemoryExceptions when there are three or more 
   * JUnit test methods.
   * 
   * @throws Exception
   */
  public void testAll() throws Exception {
    // Run all tests
    doTestTableCreateDrop();
    doTestTableMutations();
    doTestTableTimestampsAndColumns();
    doTestTableScanners();
  }

  /**
   * Tests for creating, enabling, disabling, and deleting tables.  Also 
   * tests that creating a table with an invalid column name yields an 
   * IllegalArgument exception.
   * 
   * @throws Exception
   */
  public void doTestTableCreateDrop() throws Exception {
    ThriftServer.HBaseHandler handler = new ThriftServer.HBaseHandler();

    // Create/enable/disable/delete tables, ensure methods act correctly
    assertEquals(handler.getTableNames().size(), 0);
    handler.createTable(tableAname, getColumnDescriptors());
    assertEquals(handler.getTableNames().size(), 1);
    assertEquals(handler.getColumnDescriptors(tableAname).size(), 2);
    assertTrue(handler.isTableEnabled(tableAname));
    handler.createTable(tableBname, new ArrayList<ColumnDescriptor>());
    assertEquals(handler.getTableNames().size(), 2);
    handler.disableTable(tableBname);
    assertFalse(handler.isTableEnabled(tableBname));
    handler.deleteTable(tableBname);
    assertEquals(handler.getTableNames().size(), 1);
    handler.disableTable(tableAname);
    assertFalse(handler.isTableEnabled(tableAname));
    handler.enableTable(tableAname);
    assertTrue(handler.isTableEnabled(tableAname));
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);

    // Make sure that trying to create a table with a bad column name creates 
    // an IllegalArgument exception.
    List<ColumnDescriptor> cDescriptors = new ArrayList<ColumnDescriptor>();
    ColumnDescriptor badDescriptor = new ColumnDescriptor();
    badDescriptor.name = badColumnName;
    cDescriptors.add(badDescriptor);
    String message = null;
    try {
      handler.createTable(tableBname, cDescriptors);
    } catch (IllegalArgument ia) {
      message = ia.message;
    }
    assertEquals("Family names must end in a colon: " + new String(badColumnName), message);
  }

  /**
   * Tests adding a series of Mutations and BatchMutations, including a 
   * delete mutation.  Also tests data retrieval, and getting back multiple 
   * versions.  
   * 
   * @throws Exception
   */
  public void doTestTableMutations() throws Exception {
    // Setup
    ThriftServer.HBaseHandler handler = new ThriftServer.HBaseHandler();
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply a few Mutations to rowA
    handler.mutateRow(tableAname, rowAname, getMutations());

    // Assert that the changes were made
    assertTrue(Bytes.equals(valueAname, handler.get(tableAname, rowAname, columnAname).value));
    TRowResult rowResult1 = handler.getRow(tableAname, rowAname);
    assertTrue(Bytes.equals(rowAname, rowResult1.row));
    assertTrue(Bytes.equals(valueBname, rowResult1.columns.get(columnBname).value));

    // Apply a few BatchMutations for rowA and rowB
    handler.mutateRows(tableAname, getBatchMutations());

    // Assert that changes were made to rowA
    boolean failed1 = false;
    try {
      handler.get(tableAname, rowAname, columnAname);
    } catch (NotFound nf) {
      failed1 = true;
    }
    assertTrue(failed1);
    assertTrue(Bytes.equals(valueCname, handler.get(tableAname, rowAname, columnBname).value));
    List<TCell> versions = handler.getVer(tableAname, rowAname, columnBname, MAXVERSIONS);
    assertTrue(Bytes.equals(valueCname, versions.get(0).value));
    assertTrue(Bytes.equals(valueBname, versions.get(1).value));

    // Assert that changes were made to rowB
    TRowResult rowResult2 = handler.getRow(tableAname, rowBname);
    assertTrue(Bytes.equals(rowBname, rowResult2.row));
    assertTrue(Bytes.equals(valueCname, rowResult2.columns.get(columnAname).value));
	  assertTrue(Bytes.equals(valueDname, rowResult2.columns.get(columnBname).value));

    // Apply some deletes
    handler.deleteAll(tableAname, rowAname, columnBname);
    handler.deleteAllRow(tableAname, rowBname);

    // Assert that the deletes were applied
    boolean failed2 = false;
    try {
      handler.get(tableAname, rowAname, columnBname);
    } catch (NotFound nf) {
      failed2 = true;
    }
    assertTrue(failed2);
    boolean failed3 = false;
    try {
      handler.getRow(tableAname, rowBname);
    } catch (NotFound nf) {
      failed3 = true;
    }
    assertTrue(failed3);

    // Teardown
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
  }

  /**
   * Similar to testTableMutations(), except Mutations are applied with 
   * specific timestamps and data retrieval uses these timestamps to 
   * extract specific versions of data.  
   * 
   * @throws Exception
   */
  public void doTestTableTimestampsAndColumns() throws Exception {
    // Setup
    ThriftServer.HBaseHandler handler = new ThriftServer.HBaseHandler();
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply timestamped Mutations to rowA
    long time1 = System.currentTimeMillis();
    handler.mutateRowTs(tableAname, rowAname, getMutations(), time1);

    // Sleep to assure that 'time1' and 'time2' will be different even with a
    // coarse grained system timer.
    Thread.sleep(1000);

    // Apply timestamped BatchMutations for rowA and rowB
    long time2 = System.currentTimeMillis();
    handler.mutateRowsTs(tableAname, getBatchMutations(), time2);

    // Apply an overlapping timestamped mutation to rowB
    handler.mutateRowTs(tableAname, rowBname, getMutations(), time2);

    // Assert that the timestamp-related methods retrieve the correct data
    assertEquals(handler.getVerTs(tableAname, rowAname, columnBname, time2, MAXVERSIONS).size(), 2);
    assertEquals(handler.getVerTs(tableAname, rowAname, columnBname, time1, MAXVERSIONS).size(), 1);

    TRowResult rowResult1 = handler.getRowTs(tableAname, rowAname, time1);
    TRowResult rowResult2 = handler.getRowTs(tableAname, rowAname, time2);
    assertTrue(Bytes.equals(rowResult1.columns.get(columnAname).value, valueAname));
    assertTrue(Bytes.equals(rowResult1.columns.get(columnBname).value, valueBname));
    assertTrue(Bytes.equals(rowResult2.columns.get(columnBname).value, valueCname));
    
    assertFalse(rowResult2.columns.containsKey(columnAname));
    
    List<byte[]> columns = new ArrayList<byte[]>();
    columns.add(columnBname);

    rowResult1 = handler.getRowWithColumns(tableAname, rowAname, columns);
    assertTrue(Bytes.equals(rowResult1.columns.get(columnBname).value, valueCname));
    assertFalse(rowResult1.columns.containsKey(columnAname));

    rowResult1 = handler.getRowWithColumnsTs(tableAname, rowAname, columns, time1);
    assertTrue(Bytes.equals(rowResult1.columns.get(columnBname).value, valueBname));
    assertFalse(rowResult1.columns.containsKey(columnAname));
    
    // Apply some timestamped deletes
    handler.deleteAllTs(tableAname, rowAname, columnBname, time1);
    handler.deleteAllRowTs(tableAname, rowBname, time2);

    // Assert that the timestamp-related methods retrieve the correct data
    boolean failed = false;
    try {
      handler.getVerTs(tableAname, rowAname, columnBname, time1, MAXVERSIONS);
    } catch (NotFound nf) {
      failed = true;
    }
    assertTrue(failed);
    assertTrue(Bytes.equals(handler.get(tableAname, rowAname, columnBname).value, valueCname));
    boolean failed2 = false;
    try {
      handler.getRow(tableAname, rowBname);
    } catch (NotFound nf) {
      failed2 = true;
    }
    assertTrue(failed2);

    // Teardown
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
  }

  /**
   * Tests the four different scanner-opening methods (with and without 
   * a stoprow, with and without a timestamp).  
   * 
   * @throws Exception
   */
  public void doTestTableScanners() throws Exception {
    // Setup
    ThriftServer.HBaseHandler handler = new ThriftServer.HBaseHandler();
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply timestamped Mutations to rowA
    long time1 = System.currentTimeMillis();
    handler.mutateRowTs(tableAname, rowAname, getMutations(), time1);

    // Sleep to assure that 'time1' and 'time2' will be different even with a
    // coarse grained system timer.
    Thread.sleep(1000);

    // Apply timestamped BatchMutations for rowA and rowB
    long time2 = System.currentTimeMillis();
    handler.mutateRowsTs(tableAname, getBatchMutations(), time2);

    // Test a scanner on all rows and all columns, no timestamp
    int scanner1 = handler.scannerOpen(tableAname, rowAname, getColumnList(true, true));
    TRowResult rowResult1a = handler.scannerGet(scanner1);
    assertTrue(Bytes.equals(rowResult1a.row, rowAname));
    assertEquals(rowResult1a.columns.size(), 1);
    assertTrue(Bytes.equals(rowResult1a.columns.get(columnBname).value, valueCname));
    TRowResult rowResult1b = handler.scannerGet(scanner1);
    assertTrue(Bytes.equals(rowResult1b.row, rowBname));
    assertEquals(rowResult1b.columns.size(), 2);
    assertTrue(Bytes.equals(rowResult1b.columns.get(columnAname).value, valueCname));
    assertTrue(Bytes.equals(rowResult1b.columns.get(columnBname).value, valueDname));
    closeScanner(scanner1, handler);

    // Test a scanner on all rows and all columns, with timestamp
    int scanner2 = handler.scannerOpenTs(tableAname, rowAname, getColumnList(true, true), time1);
    TRowResult rowResult2a = handler.scannerGet(scanner2);
    assertEquals(rowResult2a.columns.size(), 2);
    assertTrue(Bytes.equals(rowResult2a.columns.get(columnAname).value, valueAname));
    assertTrue(Bytes.equals(rowResult2a.columns.get(columnBname).value, valueBname));
    closeScanner(scanner2, handler);

    // Test a scanner on the first row and first column only, no timestamp
    int scanner3 = handler.scannerOpenWithStop(tableAname, rowAname, rowBname, 
        getColumnList(true, false));
    closeScanner(scanner3, handler);

    // Test a scanner on the first row and second column only, with timestamp
    int scanner4 = handler.scannerOpenWithStopTs(tableAname, rowAname, rowBname, 
        getColumnList(false, true), time1);
    TRowResult rowResult4a = handler.scannerGet(scanner4);
    assertEquals(rowResult4a.columns.size(), 1);
    assertTrue(Bytes.equals(rowResult4a.columns.get(columnBname).value, valueBname));

    // Teardown
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
  }

  /**
   * 
   * @return a List of ColumnDescriptors for use in creating a table.  Has one 
   * default ColumnDescriptor and one ColumnDescriptor with fewer versions
   */
  private List<ColumnDescriptor> getColumnDescriptors() {
    ArrayList<ColumnDescriptor> cDescriptors = new ArrayList<ColumnDescriptor>();

    // A default ColumnDescriptor
    ColumnDescriptor cDescA = new ColumnDescriptor();
    cDescA.name = columnAname;
    cDescriptors.add(cDescA);

    // A slightly customized ColumnDescriptor (only 2 versions)
    ColumnDescriptor cDescB = new ColumnDescriptor(columnBname, 2, "NONE", 
        false, 2147483647, "NONE", 0, 0, false, -1);
    cDescriptors.add(cDescB);

    return cDescriptors;
  }

  /**
   * 
   * @param includeA whether or not to include columnA
   * @param includeB whether or not to include columnB
   * @return a List of column names for use in retrieving a scanner
   */
  private List<byte[]> getColumnList(boolean includeA, boolean includeB) {
    List<byte[]> columnList = new ArrayList<byte[]>();
    if (includeA) columnList.add(columnAname);
    if (includeB) columnList.add(columnBname);
    return columnList;
  }

  /**
   * 
   * @return a List of Mutations for a row, with columnA having valueA 
   * and columnB having valueB
   */
  private List<Mutation> getMutations() {
    List<Mutation> mutations = new ArrayList<Mutation>();
    mutations.add(new Mutation(false, columnAname, valueAname));
    mutations.add(new Mutation(false, columnBname, valueBname));
    return mutations;
  }

  /**
   * 
   * @return a List of BatchMutations with the following effects:
   * (rowA, columnA): delete
   * (rowA, columnB): place valueC
   * (rowB, columnA): place valueC
   * (rowB, columnB): place valueD  
   */
  private List<BatchMutation> getBatchMutations() {
    List<BatchMutation> batchMutations = new ArrayList<BatchMutation>();
    // Mutations to rowA
    List<Mutation> rowAmutations = new ArrayList<Mutation>();
    rowAmutations.add(new Mutation(true, columnAname, null));
    rowAmutations.add(new Mutation(false, columnBname, valueCname));
    batchMutations.add(new BatchMutation(rowAname, rowAmutations));
    // Mutations to rowB
    List<Mutation> rowBmutations = new ArrayList<Mutation>();
    rowBmutations.add(new Mutation(false, columnAname, valueCname));
    rowBmutations.add(new Mutation(false, columnBname, valueDname));
    batchMutations.add(new BatchMutation(rowBname, rowBmutations));
    return batchMutations;
  }

  /**
   * Asserts that the passed scanner is exhausted, and then closes 
   * the scanner.
   * 
   * @param scannerId the scanner to close
   * @param handler the HBaseHandler interfacing to HBase
   * @throws Exception
   */
  private void closeScanner(int scannerId, ThriftServer.HBaseHandler handler) throws Exception {
    boolean failed = false;
    try {
      handler.scannerGet(scannerId);
    } catch (NotFound nf) {
      failed = true;
    }
    assertTrue(failed);
    handler.scannerClose(scannerId);
  }
}
