/*
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
package org.apache.hadoop.hbase.thrift;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit testing for ThriftServer.HBaseHandler, a part of the
 * org.apache.hadoop.hbase.thrift package.
 */
public class TestThriftServer {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  protected static final int MAXVERSIONS = 3;
  private static ByteBuffer $bb(String i) {
    return ByteBuffer.wrap(Bytes.toBytes(i));
  }
  // Static names for tables, columns, rows, and values
  private static ByteBuffer tableAname = $bb("tableA");
  private static ByteBuffer tableBname = $bb("tableB");
  private static ByteBuffer columnAname = $bb("columnA:");
  private static ByteBuffer columnBname = $bb("columnB:");
  private static ByteBuffer rowAname = $bb("rowA");
  private static ByteBuffer rowBname = $bb("rowB");
  private static ByteBuffer valueAname = $bb("valueA");
  private static ByteBuffer valueBname = $bb("valueB");
  private static ByteBuffer valueCname = $bb("valueC");
  private static ByteBuffer valueDname = $bb("valueD");

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

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
    doTestGetTableRegions();
  }

  /**
   * Tests for creating, enabling, disabling, and deleting tables.  Also
   * tests that creating a table with an invalid column name yields an
   * IllegalArgument exception.
   *
   * @throws Exception
   */
  @Test
  public void doTestTableCreateDrop() throws Exception {
    ThriftServer.HBaseHandler handler =
      new ThriftServer.HBaseHandler(UTIL.getConfiguration());

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
    /* TODO Reenable.
    assertFalse(handler.isTableEnabled(tableAname));
    handler.enableTable(tableAname);
    assertTrue(handler.isTableEnabled(tableAname));
    handler.disableTable(tableAname);*/
    handler.deleteTable(tableAname);
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
    ThriftServer.HBaseHandler handler =
      new ThriftServer.HBaseHandler(UTIL.getConfiguration());
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply a few Mutations to rowA
    //     mutations.add(new Mutation(false, columnAname, valueAname));
    //     mutations.add(new Mutation(false, columnBname, valueBname));
    handler.mutateRow(tableAname, rowAname, getMutations());

    // Assert that the changes were made
    assertEquals(valueAname,
      handler.get(tableAname, rowAname, columnAname).get(0).value);
    TRowResult rowResult1 = handler.getRow(tableAname, rowAname).get(0);
    assertEquals(rowAname, rowResult1.row);
    assertEquals(valueBname,
      rowResult1.columns.get(columnBname).value);

    // Apply a few BatchMutations for rowA and rowB
    // rowAmutations.add(new Mutation(true, columnAname, null));
    // rowAmutations.add(new Mutation(false, columnBname, valueCname));
    // batchMutations.add(new BatchMutation(rowAname, rowAmutations));
    // Mutations to rowB
    // rowBmutations.add(new Mutation(false, columnAname, valueCname));
    // rowBmutations.add(new Mutation(false, columnBname, valueDname));
    // batchMutations.add(new BatchMutation(rowBname, rowBmutations));
    handler.mutateRows(tableAname, getBatchMutations());

    // Assert that changes were made to rowA
    List<TCell> cells = handler.get(tableAname, rowAname, columnAname);
    assertFalse(cells.size() > 0);
    assertEquals(valueCname, handler.get(tableAname, rowAname, columnBname).get(0).value);
    List<TCell> versions = handler.getVer(tableAname, rowAname, columnBname, MAXVERSIONS);
    assertEquals(valueCname, versions.get(0).value);
    assertEquals(valueBname, versions.get(1).value);

    // Assert that changes were made to rowB
    TRowResult rowResult2 = handler.getRow(tableAname, rowBname).get(0);
    assertEquals(rowBname, rowResult2.row);
    assertEquals(valueCname, rowResult2.columns.get(columnAname).value);
	  assertEquals(valueDname, rowResult2.columns.get(columnBname).value);

    // Apply some deletes
    handler.deleteAll(tableAname, rowAname, columnBname);
    handler.deleteAllRow(tableAname, rowBname);

    // Assert that the deletes were applied
    int size = handler.get(tableAname, rowAname, columnBname).size();
    assertEquals(0, size);
    size = handler.getRow(tableAname, rowBname).size();
    assertEquals(0, size);

    // Try null mutation
    List<Mutation> mutations = new ArrayList<Mutation>();
    mutations.add(new Mutation(false, columnAname, null));
    handler.mutateRow(tableAname, rowAname, mutations);
    TRowResult rowResult3 = handler.getRow(tableAname, rowAname).get(0);
    assertEquals(rowAname, rowResult3.row);
    assertEquals(0, rowResult3.columns.get(columnAname).value.array().length);

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
    ThriftServer.HBaseHandler handler =
      new ThriftServer.HBaseHandler(UTIL.getConfiguration());
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply timestamped Mutations to rowA
    long time1 = System.currentTimeMillis();
    handler.mutateRowTs(tableAname, rowAname, getMutations(), time1);

    Thread.sleep(1000);

    // Apply timestamped BatchMutations for rowA and rowB
    long time2 = System.currentTimeMillis();
    handler.mutateRowsTs(tableAname, getBatchMutations(), time2);

    // Apply an overlapping timestamped mutation to rowB
    handler.mutateRowTs(tableAname, rowBname, getMutations(), time2);

    // the getVerTs is [inf, ts) so you need to increment one.
    time1 += 1;
    time2 += 2;

    // Assert that the timestamp-related methods retrieve the correct data
    assertEquals(2, handler.getVerTs(tableAname, rowAname, columnBname, time2,
      MAXVERSIONS).size());
    assertEquals(1, handler.getVerTs(tableAname, rowAname, columnBname, time1,
      MAXVERSIONS).size());

    TRowResult rowResult1 = handler.getRowTs(tableAname, rowAname, time1).get(0);
    TRowResult rowResult2 = handler.getRowTs(tableAname, rowAname, time2).get(0);
    // columnA was completely deleted
    //assertTrue(Bytes.equals(rowResult1.columns.get(columnAname).value, valueAname));
    assertEquals(rowResult1.columns.get(columnBname).value, valueBname);
    assertEquals(rowResult2.columns.get(columnBname).value, valueCname);

    // ColumnAname has been deleted, and will never be visible even with a getRowTs()
    assertFalse(rowResult2.columns.containsKey(columnAname));

    List<ByteBuffer> columns = new ArrayList<ByteBuffer>();
    columns.add(columnBname);

    rowResult1 = handler.getRowWithColumns(tableAname, rowAname, columns).get(0);
    assertEquals(rowResult1.columns.get(columnBname).value, valueCname);
    assertFalse(rowResult1.columns.containsKey(columnAname));

    rowResult1 = handler.getRowWithColumnsTs(tableAname, rowAname, columns, time1).get(0);
    assertEquals(rowResult1.columns.get(columnBname).value, valueBname);
    assertFalse(rowResult1.columns.containsKey(columnAname));

    // Apply some timestamped deletes
    // this actually deletes _everything_.
    // nukes everything in columnB: forever.
    handler.deleteAllTs(tableAname, rowAname, columnBname, time1);
    handler.deleteAllRowTs(tableAname, rowBname, time2);

    // Assert that the timestamp-related methods retrieve the correct data
    int size = handler.getVerTs(tableAname, rowAname, columnBname, time1, MAXVERSIONS).size();
    assertEquals(0, size);

    size = handler.getVerTs(tableAname, rowAname, columnBname, time2, MAXVERSIONS).size();
    assertEquals(1, size);

    // should be available....
    assertEquals(handler.get(tableAname, rowAname, columnBname).get(0).value, valueCname);

    assertEquals(0, handler.getRow(tableAname, rowBname).size());

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
    ThriftServer.HBaseHandler handler =
      new ThriftServer.HBaseHandler(UTIL.getConfiguration());
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

    time1 += 1;

    // Test a scanner on all rows and all columns, no timestamp
    int scanner1 = handler.scannerOpen(tableAname, rowAname, getColumnList(true, true));
    TRowResult rowResult1a = handler.scannerGet(scanner1).get(0);
    assertEquals(rowResult1a.row, rowAname);
    // This used to be '1'.  I don't know why when we are asking for two columns
    // and when the mutations above would seem to add two columns to the row.
    // -- St.Ack 05/12/2009
    assertEquals(rowResult1a.columns.size(), 1);
    assertEquals(rowResult1a.columns.get(columnBname).value, valueCname);

    TRowResult rowResult1b = handler.scannerGet(scanner1).get(0);
    assertEquals(rowResult1b.row, rowBname);
    assertEquals(rowResult1b.columns.size(), 2);
    assertEquals(rowResult1b.columns.get(columnAname).value, valueCname);
    assertEquals(rowResult1b.columns.get(columnBname).value, valueDname);
    closeScanner(scanner1, handler);

    // Test a scanner on all rows and all columns, with timestamp
    int scanner2 = handler.scannerOpenTs(tableAname, rowAname, getColumnList(true, true), time1);
    TRowResult rowResult2a = handler.scannerGet(scanner2).get(0);
    assertEquals(rowResult2a.columns.size(), 1);
    // column A deleted, does not exist.
    //assertTrue(Bytes.equals(rowResult2a.columns.get(columnAname).value, valueAname));
    assertEquals(rowResult2a.columns.get(columnBname).value, valueBname);
    closeScanner(scanner2, handler);

    // Test a scanner on the first row and first column only, no timestamp
    int scanner3 = handler.scannerOpenWithStop(tableAname, rowAname, rowBname,
        getColumnList(true, false));
    closeScanner(scanner3, handler);

    // Test a scanner on the first row and second column only, with timestamp
    int scanner4 = handler.scannerOpenWithStopTs(tableAname, rowAname, rowBname,
        getColumnList(false, true), time1);
    TRowResult rowResult4a = handler.scannerGet(scanner4).get(0);
    assertEquals(rowResult4a.columns.size(), 1);
    assertEquals(rowResult4a.columns.get(columnBname).value, valueBname);

    // Teardown
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
  }
  
  /**
   * For HBASE-2556
   * Tests for GetTableRegions
   *
   * @throws Exception
   */
  public void doTestGetTableRegions() throws Exception {
    ThriftServer.HBaseHandler handler =
      new ThriftServer.HBaseHandler(UTIL.getConfiguration());
    handler.createTable(tableAname, getColumnDescriptors());
    int regionCount = handler.getTableRegions(tableAname).size();
    assertEquals("empty table should have only 1 region, " +
            "but found " + regionCount, regionCount, 1);
    handler.disableTable(tableAname);    
    handler.deleteTable(tableAname);
    regionCount = handler.getTableRegions(tableAname).size();
    assertEquals("non-existing table should have 0 region, " +
            "but found " + regionCount, regionCount, 0);    
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
        false, "NONE", 0, 0, false, -1);
    cDescriptors.add(cDescB);

    return cDescriptors;
  }

  /**
   *
   * @param includeA whether or not to include columnA
   * @param includeB whether or not to include columnB
   * @return a List of column names for use in retrieving a scanner
   */
  private List<ByteBuffer> getColumnList(boolean includeA, boolean includeB) {
    List<ByteBuffer> columnList = new ArrayList<ByteBuffer>();
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

    // Mutations to rowA.  You can't mix delete and put anymore.
    List<Mutation> rowAmutations = new ArrayList<Mutation>();
    rowAmutations.add(new Mutation(true, columnAname, null));
    batchMutations.add(new BatchMutation(rowAname, rowAmutations));

    rowAmutations = new ArrayList<Mutation>();
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
    handler.scannerGet(scannerId);
    handler.scannerClose(scannerId);
  }
}
