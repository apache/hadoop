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

package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class TestClient extends HBaseClusterTestCase {

  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");
  
  private static byte [] EMPTY = new byte[0];
  
  /**
   * Constructor does nothing special, start cluster.
   */
  public TestClient() {
    super();
  }
  
  public void XtestSuperSimple() throws Exception {
    byte [] TABLE = Bytes.toBytes("testSuperSimple");
    HTable ht = createTable(TABLE, FAMILY);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    Scan scan = new Scan();
    scan.addColumn(FAMILY, TABLE);
    ResultScanner scanner = ht.getScanner(scan);
    Result result = scanner.next();
    assertTrue("Expected null result", result == null);
    scanner.close();
    System.out.println("Done.");
  }
  
  public void testFilters() throws Exception {
    byte [] TABLE = Bytes.toBytes("testFilters");
    HTable ht = createTable(TABLE, FAMILY);
    byte [][] ROWS = makeN(ROW, 10);
    byte [][] QUALIFIERS = {
        Bytes.toBytes("col0-<d2v1>-<d3v2>"), Bytes.toBytes("col1-<d2v1>-<d3v2>"), 
        Bytes.toBytes("col2-<d2v1>-<d3v2>"), Bytes.toBytes("col3-<d2v1>-<d3v2>"), 
        Bytes.toBytes("col4-<d2v1>-<d3v2>"), Bytes.toBytes("col5-<d2v1>-<d3v2>"), 
        Bytes.toBytes("col6-<d2v1>-<d3v2>"), Bytes.toBytes("col7-<d2v1>-<d3v2>"), 
        Bytes.toBytes("col8-<d2v1>-<d3v2>"), Bytes.toBytes("col9-<d2v1>-<d3v2>")
    };
    for(int i=0;i<10;i++) {
      Put put = new Put(ROWS[i]);
      put.add(FAMILY, QUALIFIERS[i], VALUE);
      ht.put(put);
    }
    Scan scan = new Scan();
    scan.addFamily(FAMILY);
    Filter filter = new QualifierFilter(CompareOp.EQUAL,
        new RegexStringComparator("col[1-5]"));
    scan.setFilter(filter);
    ResultScanner scanner = ht.getScanner(scan);
    int expectedIndex = 1;
    for(Result result : ht.getScanner(scan)) {
      assertEquals(result.size(), 1);
      assertTrue(Bytes.equals(result.raw()[0].getRow(), ROWS[expectedIndex]));
      assertTrue(Bytes.equals(result.raw()[0].getQualifier(), 
          QUALIFIERS[expectedIndex]));
      expectedIndex++;
    }
    assertEquals(expectedIndex, 6);
    scanner.close();
  }
  
  /**
   * Test simple table and non-existent row cases.
   */
  public void testSimpleMissing() throws Exception {
    
    byte [] TABLE = Bytes.toBytes("testSimpleMissing");
    
    HTable ht = createTable(TABLE, FAMILY);
    
    byte [][] ROWS = makeN(ROW, 4);
    
    // Try to get a row on an empty table
    
    Get get = new Get(ROWS[0]);
    Result result = ht.get(get);
    assertEmptyResult(result);
    
    get = new Get(ROWS[0]);
    get.addFamily(FAMILY);
    result = ht.get(get);
    assertEmptyResult(result);
    
    get = new Get(ROWS[0]);
    get.addColumn(FAMILY, QUALIFIER);
    result = ht.get(get);
    assertEmptyResult(result);
    
    Scan scan = new Scan();
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    
    scan = new Scan(ROWS[0]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    scan = new Scan(ROWS[0],ROWS[1]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    scan = new Scan();
    scan.addFamily(FAMILY);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    scan = new Scan();
    scan.addColumn(FAMILY, QUALIFIER);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    // Insert a row
    
    Put put = new Put(ROWS[2]);
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    
    // Try to get empty rows around it
    
    get = new Get(ROWS[1]);
    result = ht.get(get);
    assertEmptyResult(result);
    
    get = new Get(ROWS[0]);
    get.addFamily(FAMILY);
    result = ht.get(get);
    assertEmptyResult(result);
    
    get = new Get(ROWS[3]);
    get.addColumn(FAMILY, QUALIFIER);
    result = ht.get(get);
    assertEmptyResult(result);
    
    // Try to scan empty rows around it
    
    scan = new Scan(ROWS[3]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    scan = new Scan(ROWS[0],ROWS[2]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    // Make sure we can actually get the row
    
    get = new Get(ROWS[2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);
    
    get = new Get(ROWS[2]);
    get.addFamily(FAMILY);
    result = ht.get(get);
    assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);
    
    get = new Get(ROWS[2]);
    get.addColumn(FAMILY, QUALIFIER);
    result = ht.get(get);
    assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);
    
    // Make sure we can scan the row
    
    scan = new Scan();
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);
    
    scan = new Scan(ROWS[0],ROWS[3]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);
    
    scan = new Scan(ROWS[2],ROWS[3]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);
  }
  
  /**
   * Test basic puts, gets, scans, and deletes for a single row
   * in a multiple family table.
   */
  public void testSingleRowMultipleFamily() throws Exception {
    
    byte [] TABLE = Bytes.toBytes("testSingleRowMultipleFamily");

    byte [][] ROWS = makeN(ROW, 3);
    byte [][] FAMILIES = makeN(FAMILY, 10);
    byte [][] QUALIFIERS = makeN(QUALIFIER, 10);
    byte [][] VALUES = makeN(VALUE, 10);
   
    HTable ht = createTable(TABLE, FAMILIES);
    
    Get get;
    Scan scan;
    Delete delete;
    Put put;
    Result result;
    
    ////////////////////////////////////////////////////////////////////////////
    // Insert one column to one family
    ////////////////////////////////////////////////////////////////////////////
    
    put = new Put(ROWS[0]);
    put.add(FAMILIES[4], QUALIFIERS[0], VALUES[0]);
    ht.put(put);
    
    // Get the single column
    getVerifySingleColumn(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0, VALUES, 0);
    
    // Scan the single column
    scanVerifySingleColumn(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0, VALUES, 0);
    
    // Get empty results around inserted column
    getVerifySingleEmpty(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0);
    
    // Scan empty results around inserted column
    scanVerifySingleEmpty(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0);
    
    ////////////////////////////////////////////////////////////////////////////
    // Flush memstore and run same tests from storefiles
    ////////////////////////////////////////////////////////////////////////////
    
    flushMemStore(TABLE);
    
    // Redo get and scan tests from storefile
    
    getVerifySingleColumn(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0, VALUES, 0);
    scanVerifySingleColumn(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0, VALUES, 0);
    getVerifySingleEmpty(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0);
    scanVerifySingleEmpty(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0);
    
    ////////////////////////////////////////////////////////////////////////////
    // Now, Test reading from memstore and storefiles at once
    ////////////////////////////////////////////////////////////////////////////
    
    // Insert multiple columns to two other families
    
    put = new Put(ROWS[0]);
    put.add(FAMILIES[2], QUALIFIERS[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIERS[4], VALUES[4]);
    put.add(FAMILIES[4], QUALIFIERS[4], VALUES[4]);
    put.add(FAMILIES[6], QUALIFIERS[6], VALUES[6]);
    put.add(FAMILIES[6], QUALIFIERS[7], VALUES[7]);
    put.add(FAMILIES[7], QUALIFIERS[7], VALUES[7]);
    put.add(FAMILIES[9], QUALIFIERS[0], VALUES[0]);
    ht.put(put);
    
    // Get multiple columns across multiple families and get empties around it
    singleRowGetTest(ht, ROWS, FAMILIES, QUALIFIERS, VALUES);
 
    // Scan multiple columns across multiple families and scan empties around it
    singleRowScanTest(ht, ROWS, FAMILIES, QUALIFIERS, VALUES);

    ////////////////////////////////////////////////////////////////////////////
    // Flush the table again
    ////////////////////////////////////////////////////////////////////////////
    
    flushMemStore(TABLE);
    
    // Redo tests again
    
    singleRowGetTest(ht, ROWS, FAMILIES, QUALIFIERS, VALUES);
    singleRowScanTest(ht, ROWS, FAMILIES, QUALIFIERS, VALUES);
    
    // Insert more data to memstore
    
    put = new Put(ROWS[0]);
    put.add(FAMILIES[6], QUALIFIERS[5], VALUES[5]);
    put.add(FAMILIES[6], QUALIFIERS[8], VALUES[8]);
    put.add(FAMILIES[6], QUALIFIERS[9], VALUES[9]);
    put.add(FAMILIES[4], QUALIFIERS[3], VALUES[3]);
    ht.put(put);
    
    ////////////////////////////////////////////////////////////////////////////
    // Delete a storefile column
    ////////////////////////////////////////////////////////////////////////////
    delete = new Delete(ROWS[0]);
    delete.deleteColumns(FAMILIES[6], QUALIFIERS[7]);
    ht.delete(delete);
    
    // Try to get deleted column
    
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[7]);
    result = ht.get(get);
    assertEmptyResult(result);
    
    // Try to scan deleted column
    
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[7]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    // Make sure we can still get a column before it and after it
    
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[6]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);
    
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[8]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[8], VALUES[8]);
    
    // Make sure we can still scan a column before it and after it
    
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);
    
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[8]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[8], VALUES[8]);
    
    ////////////////////////////////////////////////////////////////////////////
    // Delete a memstore column
    ////////////////////////////////////////////////////////////////////////////
    delete = new Delete(ROWS[0]);
    delete.deleteColumns(FAMILIES[6], QUALIFIERS[8]);
    ht.delete(delete);
    
    // Try to get deleted column
    
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[8]);
    result = ht.get(get);
    assertEmptyResult(result);
    
    // Try to scan deleted column
    
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[8]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    // Make sure we can still get a column before it and after it
    
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[6]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);
    
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[9]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);
    
    // Make sure we can still scan a column before it and after it
    
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);
    
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[9]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);
    
    ////////////////////////////////////////////////////////////////////////////
    // Delete joint storefile/memstore family
    ////////////////////////////////////////////////////////////////////////////
    
    delete = new Delete(ROWS[0]);
    delete.deleteFamily(FAMILIES[4]);
    ht.delete(delete);
    
    // Try to get storefile column in deleted family
    
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = ht.get(get);
    assertEmptyResult(result);
    
    // Try to get memstore column in deleted family
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[3]);
    result = ht.get(get);
    assertEmptyResult(result);
    
    // Try to get deleted family
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[4]);
    result = ht.get(get);
    assertEmptyResult(result);
    
    // Try to scan storefile column in deleted family
    
    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    // Try to scan memstore column in deleted family
    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[3]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    // Try to scan deleted family
    scan = new Scan();
    scan.addFamily(FAMILIES[4]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    // Make sure we can still get another family
    
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[2], QUALIFIERS[2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[2], QUALIFIERS[2], VALUES[2]);
    
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[9]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);
    
    // Make sure we can still scan another family
    
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);
    
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[9]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);
    
    ////////////////////////////////////////////////////////////////////////////
    // Flush everything and rerun delete tests
    ////////////////////////////////////////////////////////////////////////////
    
    flushMemStore(TABLE);
    
    // Try to get storefile column in deleted family
    
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = ht.get(get);
    assertEmptyResult(result);
    
    // Try to get memstore column in deleted family
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[3]);
    result = ht.get(get);
    assertEmptyResult(result);
    
    // Try to get deleted family
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[4]);
    result = ht.get(get);
    assertEmptyResult(result);
    
    // Try to scan storefile column in deleted family
    
    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    // Try to scan memstore column in deleted family
    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[3]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    // Try to scan deleted family
    scan = new Scan();
    scan.addFamily(FAMILIES[4]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    // Make sure we can still get another family
    
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[2], QUALIFIERS[2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[2], QUALIFIERS[2], VALUES[2]);
    
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[9]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);
    
    // Make sure we can still scan another family
    
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);
    
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[9]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);
    
  }

  @SuppressWarnings("unused")
  public void testNull() throws Exception {
    
    byte [] TABLE = Bytes.toBytes("testNull");
    
    // Null table name (should NOT work)
    try {
      HTable htFail = createTable(null, FAMILY);
      assertTrue("Creating a table with null name passed, should have failed",
          false);
    } catch(Exception e) {}
    
    // Null family (should NOT work)
    try {
      HTable htFail = createTable(TABLE, (byte[])null);
      assertTrue("Creating a table with a null family passed, should fail",
          false);
    } catch(Exception e) {}
    
    HTable ht = createTable(TABLE, FAMILY);
    
    // Null row (should NOT work)
    try {
      Put put = new Put((byte[])null);
      put.add(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      assertTrue("Inserting a null row worked, should throw exception",
          false);
    } catch(Exception e) {}
    
    // Null qualifier (should work)
    try {
      
      Put put = new Put(ROW);
      put.add(FAMILY, null, VALUE);
      ht.put(put);
      
      getTestNull(ht, ROW, FAMILY, VALUE);
      
      scanTestNull(ht, ROW, FAMILY, VALUE);
      
      Delete delete = new Delete(ROW);
      delete.deleteColumns(FAMILY, null);
      ht.delete(delete);
      
      Get get = new Get(ROW);
      Result result = ht.get(get);
      assertEmptyResult(result);
      
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue("Using a row with null qualifier threw exception, should "
          + "pass", false);
    }
    
    // Use a new table
    
    byte [] TABLE2 = Bytes.toBytes("testNull2");
    ht = createTable(TABLE2, FAMILY);
    
    // Empty qualifier, byte[0] instead of null (should work)
    try {
      
      Put put = new Put(ROW);
      put.add(FAMILY, EMPTY, VALUE);
      ht.put(put);
      
      getTestNull(ht, ROW, FAMILY, VALUE);
      
      scanTestNull(ht, ROW, FAMILY, VALUE);
      
      // Flush and try again
      
      flushMemStore(TABLE2);
      
      getTestNull(ht, ROW, FAMILY, VALUE);
      
      scanTestNull(ht, ROW, FAMILY, VALUE);
      
      Delete delete = new Delete(ROW);
      delete.deleteColumns(FAMILY, EMPTY);
      ht.delete(delete);
      
      Get get = new Get(ROW);
      Result result = ht.get(get);
      assertEmptyResult(result);
      
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue("Using a row with null qualifier threw exception, should "
          + "pass", false);
    }
    
    // Null value
    try {
      
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, null);
      ht.put(put);
      
      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      Result result = ht.get(get);
      assertSingleResult(result, ROW, FAMILY, QUALIFIER, null);
      
      Scan scan = new Scan();
      scan.addColumn(FAMILY, QUALIFIER);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROW, FAMILY, QUALIFIER, null);
      
      Delete delete = new Delete(ROW);
      delete.deleteColumns(FAMILY, QUALIFIER);
      ht.delete(delete);
      
      get = new Get(ROW);
      result = ht.get(get);
      assertEmptyResult(result);
    
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue("Null values should be allowed, but threw exception", 
          false);
    }
    
  }
  
  public void testVersions() throws Exception {
    
    byte [] TABLE = Bytes.toBytes("testSimpleVersions");
    
    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);
    
    HTable ht = createTable(TABLE, FAMILY, 10);
    
    // Insert 4 versions of same column
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);
    
    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    
    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    
    // Ensure maxVersions in query is respected
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);
    
    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);
    
    // Flush and redo

    flushMemStore(TABLE);
    
    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    
    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    
    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);
    
    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);
    
    
    // Add some memstore and retest

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    put.add(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    put.add(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
    ht.put(put);
    
    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);
    
    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);
    
    get = new Get(ROW);
    get.setMaxVersions();
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);
    
    scan = new Scan(ROW);
    scan.setMaxVersions();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);
    
    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    
    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
    
    // Ensure maxVersions of table is respected

    flushMemStore(TABLE);

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
    put.add(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
    put.add(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
    put.add(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
    ht.put(put);
    
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);
    
    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);
    
    // Delete a version in the memstore and a version in a storefile
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[11]);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[7]);
    ht.delete(delete);
    
    // Test that it's gone
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);
    
    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);
    
  }
  
  public void testVersionLimits() throws Exception {
    byte [] TABLE = Bytes.toBytes("testVersionLimits");
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    int [] LIMITS = {1,3,5};
    long [] STAMPS = makeStamps(10);
    byte [][] VALUES = makeNAscii(VALUE, 10);
    HTable ht = createTable(TABLE, FAMILIES, LIMITS); 
    
    // Insert limit + 1 on each family
    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[5], VALUES[5]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[6], VALUES[6]);
    ht.put(put);
    
    // Verify we only get the right number out of each

    // Family0
    
    Get get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER, 
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);
    
    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER, 
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);
    
    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER, 
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);
    
    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER, 
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);
    
    // Family1
    
    get = new Get(ROW);
    get.addColumn(FAMILIES[1], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER, 
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);
    
    get = new Get(ROW);
    get.addFamily(FAMILIES[1]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER, 
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);
    
    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[1], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER, 
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);
    
    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[1]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER, 
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);
    
    // Family2
    
    get = new Get(ROW);
    get.addColumn(FAMILIES[2], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER, 
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);
    
    get = new Get(ROW);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER, 
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);
    
    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[2], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER, 
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);
    
    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER, 
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);
    
    // Try all families

    get = new Get(ROW);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);
    
    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);
    
    get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.addColumn(FAMILIES[1], QUALIFIER);
    get.addColumn(FAMILIES[2], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);
    
    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);
    
    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.addFamily(FAMILIES[0]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);
    
    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.addColumn(FAMILIES[1], QUALIFIER);
    scan.addColumn(FAMILIES[2], QUALIFIER);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);
    
  }
  
  public void testDeletes() throws Exception {
    
    byte [] TABLE = Bytes.toBytes("testDeletes");
    
    byte [][] ROWS = makeNAscii(ROW, 6);
    byte [][] FAMILIES = makeN(FAMILY, 3);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};
    
    HTable ht = createTable(TABLE, FAMILIES);
    
    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, ts[1], VALUES[1]);
    ht.put(put);
    
    Delete delete = new Delete(ROW);
    delete.deleteFamily(FAMILIES[0], ts[0]);
    ht.delete(delete);
    
    Get get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER, 
        new long [] {ts[1]},
        new byte[][] {VALUES[1]},
        0, 0);
    
    Scan scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER, 
        new long [] {ts[1]},
        new byte[][] {VALUES[1]},
        0, 0);
	  
    // Test delete latest version
    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
    put.add(FAMILIES[0], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[0], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);
    
    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], QUALIFIER);
    ht.delete(delete);
    
    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER, 
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);
    
    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER, 
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);
    
    // Expected client behavior might be that you can re-put deleted values
    // But alas, this is not to be.  We can't put them back in either case.
    
    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
    ht.put(put);
    
    // The Get returns the latest value but then does not return the
    // oldest, which was never deleted, ts[1]. 
    
    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER, 
        new long [] {ts[2], ts[3], ts[4]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4]},
        0, 2);
    
    // The Scanner returns the previous values, the expected-unexpected behavior
    
    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER, 
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);
    
    // Test deleting an entire family from one row but not the other various ways
    
    put = new Put(ROWS[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);
    
    put = new Put(ROWS[1]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);
    
    put = new Put(ROWS[2]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);
    
    delete = new Delete(ROWS[0]);
    delete.deleteFamily(FAMILIES[2]);
    ht.delete(delete);
    
    delete = new Delete(ROWS[1]);
    delete.deleteColumns(FAMILIES[1], QUALIFIER);
    ht.delete(delete);
    
    delete = new Delete(ROWS[2]);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[2], QUALIFIER);
    ht.delete(delete);
    
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER, 
        new long [] {ts[0], ts[1]},
        new byte[][] {VALUES[0], VALUES[1]},
        0, 1);
    
    scan = new Scan(ROWS[0]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER, 
        new long [] {ts[0], ts[1]},
        new byte[][] {VALUES[0], VALUES[1]},
        0, 1);
    
    get = new Get(ROWS[1]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    scan = new Scan(ROWS[1]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    
    get = new Get(ROWS[2]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);
    assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER, 
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    scan = new Scan(ROWS[2]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);
    assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER, 
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);
    
    // Test if we delete the family first in one row (HBASE-1541)
    
    delete = new Delete(ROWS[3]);
    delete.deleteFamily(FAMILIES[1]);
    ht.delete(delete);
    
    put = new Put(ROWS[3]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[0]);
    ht.put(put);
    
    put = new Put(ROWS[4]);
    put.add(FAMILIES[1], QUALIFIER, VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[2]);
    ht.put(put);
    
    get = new Get(ROWS[3]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);
    
    get = new Get(ROWS[4]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    scan = new Scan(ROWS[3]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    ResultScanner scanner = ht.getScanner(scan);
    result = scanner.next();
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);
    assertTrue(Bytes.equals(result.sorted()[0].getRow(), ROWS[3]));
    assertTrue(Bytes.equals(result.sorted()[0].getValue(), VALUES[0]));
    result = scanner.next();
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertTrue(Bytes.equals(result.sorted()[0].getRow(), ROWS[4]));
    assertTrue(Bytes.equals(result.sorted()[1].getRow(), ROWS[4]));
    assertTrue(Bytes.equals(result.sorted()[0].getValue(), VALUES[1]));
    assertTrue(Bytes.equals(result.sorted()[1].getValue(), VALUES[2]));
    scanner.close();
  }
  
  /**
   * Baseline "scalability" test.
   * 
   * Tests one hundred families, one million columns, one million versions
   */
  public void XtestMillions() throws Exception {
    
    // 100 families
    
    // millions of columns
    
    // millions of versions
    
  }

  public void XtestMultipleRegionsAndBatchPuts() throws Exception {
    // Two family table
    
    // Insert lots of rows
    
    // Insert to the same row with batched puts
    
    // Insert to multiple rows with batched puts
    
    // Split the table
    
    // Get row from first region
    
    // Get row from second region
    
    // Scan all rows
    
    // Insert to multiple regions with batched puts
    
    // Get row from first region
    
    // Get row from second region
    
    // Scan all rows
    
    
  }
  
  public void XtestMultipleRowMultipleFamily() throws Exception {
    
  }
  
  /**
   * Explicitly test JIRAs related to HBASE-880 / Client API
   */
  public void testJIRAs() throws Exception {
    jiraTest867();
    jiraTest861();
    jiraTest33();
    jiraTest1014();
    jiraTest1182();
    jiraTest52();
  }
  
  //
  // JIRA Testers
  //
  
  /**
   * HBASE-867
   *    If millions of columns in a column family, hbase scanner won't come up
   *    
   *    Test will create numRows rows, each with numColsPerRow columns 
   *    (1 version each), and attempt to scan them all.
   *    
   *    To test at scale, up numColsPerRow to the millions
   *    (have not gotten that to work running as junit though)
   */
  private void jiraTest867() throws Exception {
    
    int numRows = 10;
    int numColsPerRow = 2000;
    
    byte [] TABLE = Bytes.toBytes("jiraTest867");
    
    byte [][] ROWS = makeN(ROW, numRows);
    byte [][] QUALIFIERS = makeN(QUALIFIER, numColsPerRow);
    
    HTable ht = createTable(TABLE, FAMILY);
    
    // Insert rows
    
    for(int i=0;i<numRows;i++) {
      Put put = new Put(ROWS[i]);
      for(int j=0;j<numColsPerRow;j++) {
        put.add(FAMILY, QUALIFIERS[j], QUALIFIERS[j]);
      }
      assertTrue("Put expected to contain " + numColsPerRow + " columns but " +
          "only contains " + put.size(), put.size() == numColsPerRow);
      ht.put(put);
    }
    
    // Get a row
    Get get = new Get(ROWS[numRows-1]);
    Result result = ht.get(get);
    assertNumKeys(result, numColsPerRow);
    KeyValue [] keys = result.sorted();
    for(int i=0;i<result.size();i++) {
      assertKey(keys[i], ROWS[numRows-1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
    }
    
    // Scan the rows
    Scan scan = new Scan();
    ResultScanner scanner = ht.getScanner(scan);
    int rowCount = 0;
    while((result = scanner.next()) != null) {
      assertNumKeys(result, numColsPerRow);
      KeyValue [] kvs = result.sorted();
      for(int i=0;i<numColsPerRow;i++) {
        assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }
      rowCount++;
    }
    scanner.close();
    assertTrue("Expected to scan " + numRows + " rows but actually scanned "
        + rowCount + " rows", rowCount == numRows);
    
    // flush and try again
    
    flushMemStore(TABLE);
    
    // Get a row
    get = new Get(ROWS[numRows-1]);
    result = ht.get(get);
    assertNumKeys(result, numColsPerRow);
    keys = result.sorted();
    for(int i=0;i<result.size();i++) {
      assertKey(keys[i], ROWS[numRows-1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
    }
    
    // Scan the rows
    scan = new Scan();
    scanner = ht.getScanner(scan);
    rowCount = 0;
    while((result = scanner.next()) != null) {
      assertNumKeys(result, numColsPerRow);
      KeyValue [] kvs = result.sorted();
      for(int i=0;i<numColsPerRow;i++) {
        assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }
      rowCount++;
    }
    scanner.close();
    assertTrue("Expected to scan " + numRows + " rows but actually scanned "
        + rowCount + " rows", rowCount == numRows);
    
  }
  
  /**
   * HBASE-861
   *    get with timestamp will return a value if there is a version with an 
   *    earlier timestamp
   */
  private void jiraTest861() throws Exception {
    
    byte [] TABLE = Bytes.toBytes("jiraTest861");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);
    
    HTable ht = createTable(TABLE, FAMILY, 10);
    
    // Insert three versions
    
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    ht.put(put);
    
    // Get the middle value
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    
    // Try to get one version before (expect fail) 
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    
    // Try to get one version after (expect fail)
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
    
    // Try same from storefile
    flushMemStore(TABLE);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
    
    // Insert two more versions surrounding others, into memstore
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    ht.put(put);
    
    // Check we can get everything we should and can't get what we shouldn't
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    
    // Try same from two storefiles
    flushMemStore(TABLE);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    
  }
  
  /**
   * HBASE-33
   *    Add a HTable get/obtainScanner method that retrieves all versions of a 
   *    particular column and row between two timestamps
   */
  private void jiraTest33() throws Exception {

    byte [] TABLE = Bytes.toBytes("jiraTest33");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);
    
    HTable ht = createTable(TABLE, FAMILY, 10);
    
    // Insert lots versions
    
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);
    
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);
    
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    // Try same from storefile
    flushMemStore(TABLE);

    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);
    
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);
    
  }
  
  /**
   * HBASE-1014
   *    commit(BatchUpdate) method should return timestamp
   */
  private void jiraTest1014() throws Exception {

    byte [] TABLE = Bytes.toBytes("jiraTest1014");
    
    HTable ht = createTable(TABLE, FAMILY, 10);
    
    long manualStamp = 12345;
    
    // Insert lots versions
    
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, manualStamp, VALUE);
    ht.put(put);

    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, manualStamp, VALUE);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp-1);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp+1);
    
  }
  
  /**
   * HBASE-1182
   *    Scan for columns > some timestamp 
   */
  private void jiraTest1182() throws Exception {

    byte [] TABLE = Bytes.toBytes("jiraTest1182");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);
    
    HTable ht = createTable(TABLE, FAMILY, 10);
    
    // Insert lots versions
    
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);
    
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    
    // Try same from storefile
    flushMemStore(TABLE);

    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    
    
  }
  
  /**
   * HBASE-52
   *    Add a means of scanning over all versions
   */
  private void jiraTest52() throws Exception {

    byte [] TABLE = Bytes.toBytes("jiraTest52");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);
    
    HTable ht = createTable(TABLE, FAMILY, 10);
    
    // Insert lots versions
    
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);
    
    getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    
    scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    // Try same from storefile
    flushMemStore(TABLE);

    getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    
    scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    
  }

  //
  // Bulk Testers
  //
  
  private void getVersionRangeAndVerifyGreaterThan(HTable ht, byte [] row, 
      byte [] family, byte [] qualifier, long [] stamps, byte [][] values, 
      int start, int end)
  throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setMaxVersions(Integer.MAX_VALUE);
    get.setTimeRange(stamps[start+1], Long.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, row, family, qualifier, stamps, values, start+1, end);
  }
  
  private void getVersionRangeAndVerify(HTable ht, byte [] row, byte [] family,
      byte [] qualifier, long [] stamps, byte [][] values, int start, int end)
  throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setMaxVersions(Integer.MAX_VALUE);
    get.setTimeRange(stamps[start], stamps[end]+1);
    Result result = ht.get(get);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }
  
  private void getAllVersionsAndVerify(HTable ht, byte [] row, byte [] family,
      byte [] qualifier, long [] stamps, byte [][] values, int start, int end)
  throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }
  
  private void scanVersionRangeAndVerifyGreaterThan(HTable ht, byte [] row, 
      byte [] family, byte [] qualifier, long [] stamps, byte [][] values, 
      int start, int end)
  throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.setTimeRange(stamps[start+1], Long.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNResult(result, row, family, qualifier, stamps, values, start+1, end);
  }
  
  private void scanVersionRangeAndVerify(HTable ht, byte [] row, byte [] family,
      byte [] qualifier, long [] stamps, byte [][] values, int start, int end)
  throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.setTimeRange(stamps[start], stamps[end]+1);
    Result result = getSingleScanResult(ht, scan);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  private void scanAllVersionsAndVerify(HTable ht, byte [] row, byte [] family,
      byte [] qualifier, long [] stamps, byte [][] values, int start, int end)
  throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }
  
  private void getVersionAndVerify(HTable ht, byte [] row, byte [] family,
      byte [] qualifier, long stamp, byte [] value)
  throws Exception {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setTimeStamp(stamp);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertSingleResult(result, row, family, qualifier, stamp, value);
  }
  
  private void getVersionAndVerifyMissing(HTable ht, byte [] row, byte [] family,
      byte [] qualifier, long stamp)
  throws Exception {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setTimeStamp(stamp);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertEmptyResult(result);
  }
  
  private void scanVersionAndVerify(HTable ht, byte [] row, byte [] family,
      byte [] qualifier, long stamp, byte [] value)
  throws Exception {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setTimeStamp(stamp);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, qualifier, stamp, value);
  }
  
  private void scanVersionAndVerifyMissing(HTable ht, byte [] row, 
      byte [] family, byte [] qualifier, long stamp)
  throws Exception {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setTimeStamp(stamp);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNullResult(result);
  }
  
  private void getTestNull(HTable ht, byte [] row, byte [] family, 
      byte [] value)
  throws Exception {
      
    Get get = new Get(row);
    get.addColumn(family, null);
    Result result = ht.get(get);
    assertSingleResult(result, row, family, null, value);
    
    get = new Get(row);
    get.addColumn(family, EMPTY);
    result = ht.get(get);
    assertSingleResult(result, row, family, EMPTY, value);
    
    get = new Get(row);
    get.addFamily(family);
    result = ht.get(get);
    assertSingleResult(result, row, family, EMPTY, value);
    
    get = new Get(row);
    result = ht.get(get);
    assertSingleResult(result, row, family, EMPTY, value);
    
  }
  
  private void scanTestNull(HTable ht, byte [] row, byte [] family, 
      byte [] value)
  throws Exception {
    
    Scan scan = new Scan();
    scan.addColumn(family, null);
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, EMPTY, value);
    
    scan = new Scan();
    scan.addColumn(family, EMPTY);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, EMPTY, value);
    
    scan = new Scan();
    scan.addFamily(family);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, EMPTY, value);
    
    scan = new Scan();
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, EMPTY, value);
    
  }
  
  private void singleRowGetTest(HTable ht, byte [][] ROWS, byte [][] FAMILIES, 
      byte [][] QUALIFIERS, byte [][] VALUES)
  throws Exception {
    
    // Single column from memstore
    Get get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[0]);
    Result result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0]);
    
    // Single column from storefile
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[2], QUALIFIERS[2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[2], QUALIFIERS[2], VALUES[2]);
    
    // Single column from storefile, family match
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[7]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[7], QUALIFIERS[7], VALUES[7]);
    
    // Two columns, one from memstore one from storefile, same family,
    // wildcard match
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[4]);
    result = ht.get(get);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
        FAMILIES[4], QUALIFIERS[4], VALUES[4]);
    
    // Two columns, one from memstore one from storefile, same family,
    // explicit match
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = ht.get(get);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
        FAMILIES[4], QUALIFIERS[4], VALUES[4]);
  
    // Three column, one from memstore two from storefile, different families,
    // wildcard match
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[4]);
    get.addFamily(FAMILIES[7]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] { {4, 0, 0}, {4, 4, 4}, {7, 7, 7} });
    
    // Multiple columns from everywhere storefile, many family, wildcard
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[2]);
    get.addFamily(FAMILIES[4]);
    get.addFamily(FAMILIES[6]);
    get.addFamily(FAMILIES[7]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] { 
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
    });
    
    // Multiple columns from everywhere storefile, many family, wildcard
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[2], QUALIFIERS[2]);
    get.addColumn(FAMILIES[2], QUALIFIERS[4]);
    get.addColumn(FAMILIES[4], QUALIFIERS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[4]);
    get.addColumn(FAMILIES[6], QUALIFIERS[6]);
    get.addColumn(FAMILIES[6], QUALIFIERS[7]);
    get.addColumn(FAMILIES[7], QUALIFIERS[7]);
    get.addColumn(FAMILIES[7], QUALIFIERS[8]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] { 
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
    });
    
    // Everything
    get = new Get(ROWS[0]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] { 
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}, {9, 0, 0}
    });
    
    // Get around inserted columns
    
    get = new Get(ROWS[1]);
    result = ht.get(get);
    assertEmptyResult(result);
    
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[3]);
    get.addColumn(FAMILIES[2], QUALIFIERS[3]);
    result = ht.get(get);
    assertEmptyResult(result);
    
  }
  
  private void singleRowScanTest(HTable ht, byte [][] ROWS, byte [][] FAMILIES, 
      byte [][] QUALIFIERS, byte [][] VALUES)
  throws Exception {
  
    // Single column from memstore
    Scan scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[0]);
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0]);
    
    // Single column from storefile
    scan = new Scan();
    scan.addColumn(FAMILIES[2], QUALIFIERS[2]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[2], QUALIFIERS[2], VALUES[2]);
    
    // Single column from storefile, family match
    scan = new Scan();
    scan.addFamily(FAMILIES[7]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[7], QUALIFIERS[7], VALUES[7]);
    
    // Two columns, one from memstore one from storefile, same family,
    // wildcard match
    scan = new Scan();
    scan.addFamily(FAMILIES[4]);
    result = getSingleScanResult(ht, scan);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
        FAMILIES[4], QUALIFIERS[4], VALUES[4]);
    
    // Two columns, one from memstore one from storefile, same family,
    // explicit match
    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[0]);
    scan.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = getSingleScanResult(ht, scan);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
        FAMILIES[4], QUALIFIERS[4], VALUES[4]);
  
    // Three column, one from memstore two from storefile, different families,
    // wildcard match
    scan = new Scan();
    scan.addFamily(FAMILIES[4]);
    scan.addFamily(FAMILIES[7]);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] { {4, 0, 0}, {4, 4, 4}, {7, 7, 7} });
    
    // Multiple columns from everywhere storefile, many family, wildcard
    scan = new Scan();
    scan.addFamily(FAMILIES[2]);
    scan.addFamily(FAMILIES[4]);
    scan.addFamily(FAMILIES[6]);
    scan.addFamily(FAMILIES[7]);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] { 
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
    });
    
    // Multiple columns from everywhere storefile, many family, wildcard
    scan = new Scan();
    scan.addColumn(FAMILIES[2], QUALIFIERS[2]);
    scan.addColumn(FAMILIES[2], QUALIFIERS[4]);
    scan.addColumn(FAMILIES[4], QUALIFIERS[0]);
    scan.addColumn(FAMILIES[4], QUALIFIERS[4]);
    scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
    scan.addColumn(FAMILIES[6], QUALIFIERS[7]);
    scan.addColumn(FAMILIES[7], QUALIFIERS[7]);
    scan.addColumn(FAMILIES[7], QUALIFIERS[8]);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] { 
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
    });
    
    // Everything
    scan = new Scan();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] { 
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}, {9, 0, 0}
    });
    
    // Scan around inserted columns
    
    scan = new Scan(ROWS[1]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[3]);
    scan.addColumn(FAMILIES[2], QUALIFIERS[3]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
  }
  
  
  
  /**
   * Verify a single column using gets.
   * Expects family and qualifier arrays to be valid for at least 
   * the range:  idx-2 < idx < idx+2
   */
  private void getVerifySingleColumn(HTable ht,
      byte [][] ROWS, int ROWIDX,
      byte [][] FAMILIES, int FAMILYIDX,
      byte [][] QUALIFIERS, int QUALIFIERIDX,
      byte [][] VALUES, int VALUEIDX)
  throws Exception {
    
    Get get = new Get(ROWS[ROWIDX]);
    Result result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX], 
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);
    
    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX], 
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);
    
    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[FAMILYIDX-2]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    get.addFamily(FAMILIES[FAMILYIDX+2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX], 
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);
    
    get = new Get(ROWS[ROWIDX]);
    get.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[0]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX], 
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);
    
    get = new Get(ROWS[ROWIDX]);
    get.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[1]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX], 
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);
    
    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    get.addColumn(FAMILIES[FAMILYIDX+1], QUALIFIERS[1]);
    get.addColumn(FAMILIES[FAMILYIDX-2], QUALIFIERS[1]);
    get.addFamily(FAMILIES[FAMILYIDX-1]);
    get.addFamily(FAMILIES[FAMILYIDX+2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX], 
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);
    
  }
  
  
  /**
   * Verify a single column using scanners.
   * Expects family and qualifier arrays to be valid for at least 
   * the range:  idx-2 to idx+2
   * Expects row array to be valid for at least idx to idx+2 
   */
  private void scanVerifySingleColumn(HTable ht,
      byte [][] ROWS, int ROWIDX,
      byte [][] FAMILIES, int FAMILYIDX,
      byte [][] QUALIFIERS, int QUALIFIERIDX,
      byte [][] VALUES, int VALUEIDX)
  throws Exception {

    Scan scan = new Scan();
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX], 
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);
    
    scan = new Scan(ROWS[ROWIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX], 
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);
  
    scan = new Scan(ROWS[ROWIDX], ROWS[ROWIDX+1]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX], 
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);
    
    scan = new Scan(HConstants.EMPTY_START_ROW, ROWS[ROWIDX+1]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX], 
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);
    
    scan = new Scan();
    scan.addFamily(FAMILIES[FAMILYIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX], 
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);
    
    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX], 
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);
    
    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX+1]);
    scan.addFamily(FAMILIES[FAMILYIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX], 
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);
    
    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX-1], QUALIFIERS[QUALIFIERIDX+1]);
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX]);
    scan.addFamily(FAMILIES[FAMILYIDX+1]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX], 
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

  }
  
  /**
   * Verify we do not read any values by accident around a single column
   * Same requirements as getVerifySingleColumn
   */
  private void getVerifySingleEmpty(HTable ht, 
      byte [][] ROWS, int ROWIDX, 
      byte [][] FAMILIES, int FAMILYIDX, 
      byte [][] QUALIFIERS, int QUALIFIERIDX)
  throws Exception {
  
    Get get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[4]);
    get.addColumn(FAMILIES[4], QUALIFIERS[1]);
    Result result = ht.get(get);
    assertEmptyResult(result);
    
    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[4]);
    get.addColumn(FAMILIES[4], QUALIFIERS[2]);
    result = ht.get(get);
    assertEmptyResult(result);
    
    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[3]);
    get.addColumn(FAMILIES[4], QUALIFIERS[2]);
    get.addFamily(FAMILIES[5]);
    result = ht.get(get);
    assertEmptyResult(result);
    
    get = new Get(ROWS[ROWIDX+1]);
    result = ht.get(get);
    assertEmptyResult(result);
    
  }
    
  private void scanVerifySingleEmpty(HTable ht, 
      byte [][] ROWS, int ROWIDX, 
      byte [][] FAMILIES, int FAMILYIDX, 
      byte [][] QUALIFIERS, int QUALIFIERIDX)
  throws Exception {
  
    Scan scan = new Scan(ROWS[ROWIDX+1]); 
    Result result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    scan = new Scan(ROWS[ROWIDX+1],ROWS[ROWIDX+2]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    scan = new Scan(HConstants.EMPTY_START_ROW, ROWS[ROWIDX]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX+1]);
    scan.addFamily(FAMILIES[FAMILYIDX-1]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
    
  }
  
  //
  // Verifiers
  //
  
  private void assertKey(KeyValue key, byte [] row, byte [] family,
      byte [] qualifier, byte [] value)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(key.getRow()) +"]",
        equals(row, key.getRow()));
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(key.getFamily()) + "]",
        equals(family, key.getFamily()));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(key.getQualifier()) + "]",
        equals(qualifier, key.getQualifier()));
    assertTrue("Expected value [" + Bytes.toString(value) + "] " +
        "Got value [" + Bytes.toString(key.getValue()) + "]",
        equals(value, key.getValue()));
  }
  
  private void assertNumKeys(Result result, int n) throws Exception {
    assertTrue("Expected " + n + " keys but got " + result.size(),
        result.size() == n);
  }

  
  private void assertNResult(Result result, byte [] row, 
      byte [][] families, byte [][] qualifiers, byte [][] values,
      int [][] idxs)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    assertTrue("Expected " + idxs.length + " keys but result contains " 
        + result.size(), result.size() == idxs.length);
    
    KeyValue [] keys = result.sorted();
    
    for(int i=0;i<keys.length;i++) {
      byte [] family = families[idxs[i][0]];
      byte [] qualifier = qualifiers[idxs[i][1]];
      byte [] value = values[idxs[i][2]];
      KeyValue key = keys[i];

      assertTrue("(" + i + ") Expected family [" + Bytes.toString(family)
          + "] " + "Got family [" + Bytes.toString(key.getFamily()) + "]",
          equals(family, key.getFamily()));
      assertTrue("(" + i + ") Expected qualifier [" + Bytes.toString(qualifier) 
          + "] " + "Got qualifier [" + Bytes.toString(key.getQualifier()) + "]",
          equals(qualifier, key.getQualifier()));
      assertTrue("(" + i + ") Expected value [" + Bytes.toString(value) + "] "
          + "Got value [" + Bytes.toString(key.getValue()) + "]",
          equals(value, key.getValue()));
    }
  }
  
  private void assertNResult(Result result, byte [] row, 
      byte [] family, byte [] qualifier, long [] stamps, byte [][] values, 
      int start, int end)
  throws IOException {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    int expectedResults = end - start + 1;
    assertTrue("Expected " + expectedResults + " keys but result contains " 
        + result.size(), result.size() == expectedResults);
    
    KeyValue [] keys = result.sorted();
    
    for(int i=0;i<keys.length;i++) {
      byte [] value = values[end-i];
      long ts = stamps[end-i];
      KeyValue key = keys[i];

      assertTrue("(" + i + ") Expected family [" + Bytes.toString(family)
          + "] " + "Got family [" + Bytes.toString(key.getFamily()) + "]",
          equals(family, key.getFamily()));
      assertTrue("(" + i + ") Expected qualifier [" + Bytes.toString(qualifier) 
          + "] " + "Got qualifier [" + Bytes.toString(key.getQualifier()) + "]",
          equals(qualifier, key.getQualifier()));
      assertTrue("Expected ts [" + ts + "] " +
          "Got ts [" + key.getTimestamp() + "]", ts == key.getTimestamp());
      assertTrue("(" + i + ") Expected value [" + Bytes.toString(value) + "] "
          + "Got value [" + Bytes.toString(key.getValue()) + "]",
          equals(value, key.getValue()));
    }
  }
  
  /**
   * Validate that result contains two specified keys, exactly.
   * It is assumed key A sorts before key B.
   */
  private void assertDoubleResult(Result result, byte [] row, 
      byte [] familyA, byte [] qualifierA, byte [] valueA,
      byte [] familyB, byte [] qualifierB, byte [] valueB)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    assertTrue("Expected two keys but result contains " + result.size(),
        result.size() == 2);
    KeyValue [] kv = result.sorted();
    KeyValue kvA = kv[0];
    assertTrue("(A) Expected family [" + Bytes.toString(familyA) + "] " +
        "Got family [" + Bytes.toString(kvA.getFamily()) + "]",
        equals(familyA, kvA.getFamily()));
    assertTrue("(A) Expected qualifier [" + Bytes.toString(qualifierA) + "] " +
        "Got qualifier [" + Bytes.toString(kvA.getQualifier()) + "]",
        equals(qualifierA, kvA.getQualifier()));
    assertTrue("(A) Expected value [" + Bytes.toString(valueA) + "] " +
        "Got value [" + Bytes.toString(kvA.getValue()) + "]",
        equals(valueA, kvA.getValue()));
    KeyValue kvB = kv[1];
    assertTrue("(B) Expected family [" + Bytes.toString(familyB) + "] " +
        "Got family [" + Bytes.toString(kvB.getFamily()) + "]",
        equals(familyB, kvB.getFamily()));
    assertTrue("(B) Expected qualifier [" + Bytes.toString(qualifierB) + "] " +
        "Got qualifier [" + Bytes.toString(kvB.getQualifier()) + "]",
        equals(qualifierB, kvB.getQualifier()));
    assertTrue("(B) Expected value [" + Bytes.toString(valueB) + "] " +
        "Got value [" + Bytes.toString(kvB.getValue()) + "]",
        equals(valueB, kvB.getValue()));
  }
  
  /**
   * 
   */
  private void assertSingleResult(Result result, byte [] row, byte [] family, 
      byte [] qualifier, byte [] value)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    assertTrue("Expected a single key but result contains " + result.size(),
        result.size() == 1);
    KeyValue kv = result.sorted()[0];
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(kv.getFamily()) + "]",
        equals(family, kv.getFamily()));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(kv.getQualifier()) + "]",
        equals(qualifier, kv.getQualifier()));
    assertTrue("Expected value [" + Bytes.toString(value) + "] " +
        "Got value [" + Bytes.toString(kv.getValue()) + "]",
        equals(value, kv.getValue()));
  }
  
  private void assertSingleResult(Result result, byte [] row, byte [] family, 
      byte [] qualifier, long ts, byte [] value)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    assertTrue("Expected a single key but result contains " + result.size(),
        result.size() == 1);
    KeyValue kv = result.sorted()[0];
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(kv.getFamily()) + "]",
        equals(family, kv.getFamily()));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(kv.getQualifier()) + "]",
        equals(qualifier, kv.getQualifier()));
    assertTrue("Expected ts [" + ts + "] " +
        "Got ts [" + kv.getTimestamp() + "]", ts == kv.getTimestamp());
    assertTrue("Expected value [" + Bytes.toString(value) + "] " +
        "Got value [" + Bytes.toString(kv.getValue()) + "]",
        equals(value, kv.getValue()));
  }
  
  private void assertEmptyResult(Result result) throws Exception {
    assertTrue("expected an empty result but result contains " + 
        result.size() + " keys", result.isEmpty());
  }
  
  private void assertNullResult(Result result) throws Exception {
    assertTrue("expected null result but received a non-null result",
        result == null);
  }
  
  //
  // Helpers
  //
  
  private void flushMemStore(byte [] tableName) throws Exception {
    System.out.println("\n\nFlushing table [" + Bytes.toString(tableName) + "]...\n");
//    HBaseAdmin hba = new HBaseAdmin(conf);
//    hba.flush(tableName);
    cluster.flushcache();
    System.out.println("\nTable flushed.\n\n");
  }
  
  private Result getSingleScanResult(HTable ht, Scan scan) throws IOException {
    ResultScanner scanner = ht.getScanner(scan);
    Result result = scanner.next();
    scanner.close();
    return result;
  }
  
  private byte [][] makeNAscii(byte [] base, int n) {
    if(n > 256) {
      return makeNBig(base, n);
    }
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      byte [] tail = Bytes.toBytes(new Integer(i).toString());
      ret[i] = Bytes.add(base, tail);
    }
    return ret;
  }
  
  private byte [][] makeN(byte [] base, int n) {
    if(n > 256) {
      return makeNBig(base, n);
    }
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      ret[i] = Bytes.add(base, new byte[]{(byte)i});
    }
    return ret;
  }
  
  private byte [][] makeNBig(byte [] base, int n) {
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      int byteA = (i % 256);
      int byteB = (i >> 8);
      ret[i] = Bytes.add(base, new byte[]{(byte)byteB,(byte)byteA});
    }
    return ret;
  }
  
  private long [] makeStamps(int n) {
    long [] stamps = new long[n];
    for(int i=0;i<n;i++) stamps[i] = i+1;
    return stamps;
  }
  
  private HTable createTable(byte [] tableName, byte [] family) 
  throws IOException{
    return createTable(tableName, new byte[][]{family});
  }
  
  private HTable createTable(byte [] tableName, byte [][] families) 
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for(byte [] family : families) {
      desc.addFamily(new HColumnDescriptor(family));
    }
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    return new HTable(conf, tableName);
  }
  
  private HTable createTable(byte [] tableName, byte [] family, int numVersions)
  throws IOException {
    return createTable(tableName, new byte[][]{family}, numVersions);
  }
  
  private HTable createTable(byte [] tableName, byte [][] families,
      int numVersions)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for(byte [] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family, numVersions,
          HColumnDescriptor.DEFAULT_COMPRESSION,
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          HColumnDescriptor.DEFAULT_BLOCKCACHE,
          Integer.MAX_VALUE, HColumnDescriptor.DEFAULT_TTL, false);
      desc.addFamily(hcd);
    }
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    return new HTable(conf, tableName);
  }
  
  private HTable createTable(byte [] tableName, byte [][] families,
      int [] numVersions)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    int i = 0;
    for(byte [] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family, numVersions[i],
          HColumnDescriptor.DEFAULT_COMPRESSION,
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          HColumnDescriptor.DEFAULT_BLOCKCACHE,
          Integer.MAX_VALUE, HColumnDescriptor.DEFAULT_TTL, false);
      desc.addFamily(hcd);
      i++;
    }
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    return new HTable(conf, tableName);
  }
  
  private boolean equals(byte [] left, byte [] right) {
    if(left == null && right == null) return true;
    if(left == null && right.length == 0) return true;
    if(right == null && left.length == 0) return true;
    return Bytes.equals(left, right);
  }
  
  
  
  
  
  
  
  

  
  public void XtestDuplicateVersions() throws Exception {
    
    byte [] TABLE = Bytes.toBytes("testDuplicateVersions");
    
    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);
    
    HTable ht = createTable(TABLE, FAMILY, 10);
    
    // Insert 4 versions of same column
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);
    
    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    
    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    
    // Ensure maxVersions in query is respected
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);
    
    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);
    
    // Flush and redo

    flushMemStore(TABLE);
    
    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    
    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    
    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);
    
    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);
    
    
    // Add some memstore and retest

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    put.add(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    put.add(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
    ht.put(put);
    
    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(7);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);
    
    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(7);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);
    
    get = new Get(ROW);
    get.setMaxVersions(7);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);
    
    scan = new Scan(ROW);
    scan.setMaxVersions(7);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);
    
    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    
    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
    
    // Ensure maxVersions of table is respected

    flushMemStore(TABLE);

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
    put.add(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
    put.add(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
    put.add(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
    ht.put(put);
    
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);
    
    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);
    
    // Delete a version in the memstore and a version in a storefile
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[11]);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[7]);
    ht.delete(delete);
    
    // Test that it's gone
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 7);
    
    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER, 
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 7);
    
  }
  
  
  
}
