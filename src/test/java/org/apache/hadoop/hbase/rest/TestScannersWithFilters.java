/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.rest;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.rest.model.ScannerModel;
import org.apache.hadoop.hbase.util.Bytes;

public class TestScannersWithFilters extends HBaseRESTClusterTestBase {

  static final Log LOG = LogFactory.getLog(TestScannersWithFilters.class);

  static final byte [][] ROWS_ONE = {
    Bytes.toBytes("testRowOne-0"), Bytes.toBytes("testRowOne-1"),
    Bytes.toBytes("testRowOne-2"), Bytes.toBytes("testRowOne-3")
  };

  static final byte [][] ROWS_TWO = {
    Bytes.toBytes("testRowTwo-0"), Bytes.toBytes("testRowTwo-1"),
    Bytes.toBytes("testRowTwo-2"), Bytes.toBytes("testRowTwo-3")
  };

  static final byte [][] FAMILIES = {
    Bytes.toBytes("testFamilyOne"), Bytes.toBytes("testFamilyTwo")
  };

  static final byte [][] QUALIFIERS_ONE = {
    Bytes.toBytes("testQualifierOne-0"), Bytes.toBytes("testQualifierOne-1"),
    Bytes.toBytes("testQualifierOne-2"), Bytes.toBytes("testQualifierOne-3")
  };

  static final byte [][] QUALIFIERS_TWO = {
    Bytes.toBytes("testQualifierTwo-0"), Bytes.toBytes("testQualifierTwo-1"),
    Bytes.toBytes("testQualifierTwo-2"), Bytes.toBytes("testQualifierTwo-3")
  };

  static final byte [][] VALUES = {
    Bytes.toBytes("testValueOne"), Bytes.toBytes("testValueTwo")
  };

  Client client;
  JAXBContext context;
  Marshaller marshaller;
  Unmarshaller unmarshaller;
  long numRows = ROWS_ONE.length + ROWS_TWO.length;
  long colsPerRow = FAMILIES.length * QUALIFIERS_ONE.length;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    context = JAXBContext.newInstance(
        CellModel.class,
        CellSetModel.class,
        RowModel.class,
        ScannerModel.class);
    marshaller = context.createMarshaller();
    unmarshaller = context.createUnmarshaller();
    client = new Client(new Cluster().add("localhost", testServletPort));
    HBaseAdmin admin = new HBaseAdmin(conf);
    if (!admin.tableExists(getName())) {
      HTableDescriptor htd = new HTableDescriptor(getName());
      htd.addFamily(new HColumnDescriptor(FAMILIES[0]));
      htd.addFamily(new HColumnDescriptor(FAMILIES[1]));
      admin.createTable(htd);
      HTable table = new HTable(conf, getName());
      // Insert first half
      for(byte [] ROW : ROWS_ONE) {
        Put p = new Put(ROW);
        for(byte [] QUALIFIER : QUALIFIERS_ONE) {
          p.add(FAMILIES[0], QUALIFIER, VALUES[0]);
        }
        table.put(p);
      }
      for(byte [] ROW : ROWS_TWO) {
        Put p = new Put(ROW);
        for(byte [] QUALIFIER : QUALIFIERS_TWO) {
          p.add(FAMILIES[1], QUALIFIER, VALUES[1]);
        }
        table.put(p);
      }
      
      // Insert second half (reverse families)
      for(byte [] ROW : ROWS_ONE) {
        Put p = new Put(ROW);
        for(byte [] QUALIFIER : QUALIFIERS_ONE) {
          p.add(FAMILIES[1], QUALIFIER, VALUES[0]);
        }
        table.put(p);
      }
      for(byte [] ROW : ROWS_TWO) {
        Put p = new Put(ROW);
        for(byte [] QUALIFIER : QUALIFIERS_TWO) {
          p.add(FAMILIES[0], QUALIFIER, VALUES[1]);
        }
        table.put(p);
      }
      
      // Delete the second qualifier from all rows and families
      for(byte [] ROW : ROWS_ONE) {
        Delete d = new Delete(ROW);
        d.deleteColumns(FAMILIES[0], QUALIFIERS_ONE[1]);
        d.deleteColumns(FAMILIES[1], QUALIFIERS_ONE[1]);
        table.delete(d);
      }    
      for(byte [] ROW : ROWS_TWO) {
        Delete d = new Delete(ROW);
        d.deleteColumns(FAMILIES[0], QUALIFIERS_TWO[1]);
        d.deleteColumns(FAMILIES[1], QUALIFIERS_TWO[1]);
        table.delete(d);
      }
      colsPerRow -= 2;
      
      // Delete the second rows from both groups, one column at a time
      for(byte [] QUALIFIER : QUALIFIERS_ONE) {
        Delete d = new Delete(ROWS_ONE[1]);
        d.deleteColumns(FAMILIES[0], QUALIFIER);
        d.deleteColumns(FAMILIES[1], QUALIFIER);
        table.delete(d);
      }
      for(byte [] QUALIFIER : QUALIFIERS_TWO) {
        Delete d = new Delete(ROWS_TWO[1]);
        d.deleteColumns(FAMILIES[0], QUALIFIER);
        d.deleteColumns(FAMILIES[1], QUALIFIER);
        table.delete(d);
      }
      numRows -= 2;
    }
  }

  @Override
  protected void tearDown() throws Exception {
    client.shutdown();
    super.tearDown();
  }

  void verifyScan(Scan s, long expectedRows, long expectedKeys) 
      throws Exception {
    ScannerModel model = ScannerModel.fromScan(s);
    model.setBatch(Integer.MAX_VALUE); // fetch it all at once
    StringWriter writer = new StringWriter();
    marshaller.marshal(model, writer);
    LOG.debug(writer.toString());
    byte[] body = Bytes.toBytes(writer.toString());
    Response response = client.put("/" + getName() + "/scanner", MIMETYPE_XML,
      body);
    assertEquals(response.getCode(), 201);
    String scannerURI = response.getLocation();
    assertNotNull(scannerURI);

    // get a cell set
    response = client.get(scannerURI, MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    CellSetModel cells = (CellSetModel)
      unmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));

    int rows = cells.getRows().size();
    assertTrue("Scanned too many rows! Only expected " + expectedRows + 
        " total but scanned " + rows, expectedRows == rows);
    for (RowModel row: cells.getRows()) {
      int count = row.getCells().size();
      assertEquals("Expected " + expectedKeys + " keys per row but " +
        "returned " + count, expectedKeys, count);
    }

    // delete the scanner
    response = client.delete(scannerURI);
    assertEquals(response.getCode(), 200);
  }

  void verifyScanFull(Scan s, KeyValue [] kvs) throws Exception {
    ScannerModel model = ScannerModel.fromScan(s);
    model.setBatch(Integer.MAX_VALUE); // fetch it all at once
    StringWriter writer = new StringWriter();
    marshaller.marshal(model, writer);
    LOG.debug(writer.toString());
    byte[] body = Bytes.toBytes(writer.toString());
    Response response = client.put("/" + getName() + "/scanner", MIMETYPE_XML,
      body);
    assertEquals(response.getCode(), 201);
    String scannerURI = response.getLocation();
    assertNotNull(scannerURI);

    // get a cell set
    response = client.get(scannerURI, MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    CellSetModel cellSet = (CellSetModel)
      unmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));

    // delete the scanner
    response = client.delete(scannerURI);
    assertEquals(response.getCode(), 200);

    int row = 0;
    int idx = 0;
    Iterator<RowModel> i = cellSet.getRows().iterator();
    for (boolean done = true; done; row++) {
      done = i.hasNext();
      if (!done) break;
      RowModel rowModel = i.next();
      List<CellModel> cells = rowModel.getCells();
      if (cells.isEmpty()) break;
      assertTrue("Scanned too many keys! Only expected " + kvs.length + 
        " total but already scanned " + (cells.size() + idx), 
        kvs.length >= idx + cells.size());
      for (CellModel cell: cells) {
        assertTrue("Row mismatch", 
            Bytes.equals(rowModel.getKey(), kvs[idx].getRow()));
        byte[][] split = KeyValue.parseColumn(cell.getColumn());
        assertTrue("Family mismatch", 
            Bytes.equals(split[0], kvs[idx].getFamily()));
        assertTrue("Qualifier mismatch", 
            Bytes.equals(split[1], kvs[idx].getQualifier()));
        assertTrue("Value mismatch", 
            Bytes.equals(cell.getValue(), kvs[idx].getValue()));
        idx++;
      }
    }
    assertEquals("Expected " + kvs.length + " total keys but scanned " + idx,
      kvs.length, idx);
  }

  void verifyScanNoEarlyOut(Scan s, long expectedRows, long expectedKeys) 
      throws Exception {
    ScannerModel model = ScannerModel.fromScan(s);
    model.setBatch(Integer.MAX_VALUE); // fetch it all at once
    StringWriter writer = new StringWriter();
    marshaller.marshal(model, writer);
    LOG.debug(writer.toString());
    byte[] body = Bytes.toBytes(writer.toString());
    Response response = client.put("/" + getName() + "/scanner", MIMETYPE_XML,
      body);
    assertEquals(response.getCode(), 201);
    String scannerURI = response.getLocation();
    assertNotNull(scannerURI);

    // get a cell set
    response = client.get(scannerURI, MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    CellSetModel cellSet = (CellSetModel)
      unmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));

    // delete the scanner
    response = client.delete(scannerURI);
    assertEquals(response.getCode(), 200);

    Iterator<RowModel> i = cellSet.getRows().iterator();
    int j = 0;
    for (boolean done = true; done; j++) {
      done = i.hasNext();
      if (!done) break;
      RowModel rowModel = i.next();
      List<CellModel> cells = rowModel.getCells();
      if (cells.isEmpty()) break;
      assertTrue("Scanned too many rows! Only expected " + expectedRows + 
        " total but already scanned " + (j+1), expectedRows > j);
      assertEquals("Expected " + expectedKeys + " keys per row but " +
        "returned " + cells.size(), expectedKeys, cells.size());
    }
    assertEquals("Expected " + expectedRows + " rows but scanned " + j +
      " rows", expectedRows, j);
  }

  void doTestNoFilter() throws Exception {
    // No filter
    long expectedRows = this.numRows;
    long expectedKeys = this.colsPerRow;
    
    // Both families
    Scan s = new Scan();
    verifyScan(s, expectedRows, expectedKeys);

    // One family
    s = new Scan();
    s.addFamily(FAMILIES[0]);
    verifyScan(s, expectedRows, expectedKeys/2);
  }

  void doTestPrefixFilter() throws Exception {
    // Grab rows from group one (half of total)
    long expectedRows = this.numRows / 2;
    long expectedKeys = this.colsPerRow;
    Scan s = new Scan();
    s.setFilter(new PrefixFilter(Bytes.toBytes("testRowOne")));
    verifyScan(s, expectedRows, expectedKeys);
  }

  void doTestPageFilter() throws Exception {
    // KVs in first 6 rows
    KeyValue [] expectedKVs = {
      // testRowOne-0
      new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
      // testRowOne-2
      new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
      // testRowOne-3
      new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
      // testRowTwo-0
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
      // testRowTwo-2
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
      // testRowTwo-3
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1])
    };
    
    // Grab all 6 rows
    long expectedRows = 6;
    long expectedKeys = this.colsPerRow;
    Scan s = new Scan();
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);
    s.setFilter(new PageFilter(expectedRows));
    verifyScanFull(s, expectedKVs);
    
    // Grab first 4 rows (6 cols per row)
    expectedRows = 4;
    expectedKeys = this.colsPerRow;
    s = new Scan();
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);
    s.setFilter(new PageFilter(expectedRows));
    verifyScanFull(s, Arrays.copyOf(expectedKVs, 24));
    
    // Grab first 2 rows
    expectedRows = 2;
    expectedKeys = this.colsPerRow;
    s = new Scan();
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);
    s.setFilter(new PageFilter(expectedRows));
    verifyScanFull(s, Arrays.copyOf(expectedKVs, 12));

    // Grab first row
    expectedRows = 1;
    expectedKeys = this.colsPerRow;
    s = new Scan();
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);
    s.setFilter(new PageFilter(expectedRows));
    verifyScanFull(s, Arrays.copyOf(expectedKVs, 6));    
  }

  void doTestInclusiveStopFilter() throws Exception {
    // Grab rows from group one
    
    // If we just use start/stop row, we get total/2 - 1 rows
    long expectedRows = (this.numRows / 2) - 1;
    long expectedKeys = this.colsPerRow;
    Scan s = new Scan(Bytes.toBytes("testRowOne-0"), 
        Bytes.toBytes("testRowOne-3"));
    verifyScan(s, expectedRows, expectedKeys);
    
    // Now use start row with inclusive stop filter
    expectedRows = this.numRows / 2;
    s = new Scan(Bytes.toBytes("testRowOne-0"));
    s.setFilter(new InclusiveStopFilter(Bytes.toBytes("testRowOne-3")));
    verifyScan(s, expectedRows, expectedKeys);

    // Grab rows from group two
    
    // If we just use start/stop row, we get total/2 - 1 rows
    expectedRows = (this.numRows / 2) - 1;
    expectedKeys = this.colsPerRow;
    s = new Scan(Bytes.toBytes("testRowTwo-0"), 
        Bytes.toBytes("testRowTwo-3"));
    verifyScan(s, expectedRows, expectedKeys);
    
    // Now use start row with inclusive stop filter
    expectedRows = this.numRows / 2;
    s = new Scan(Bytes.toBytes("testRowTwo-0"));
    s.setFilter(new InclusiveStopFilter(Bytes.toBytes("testRowTwo-3")));
    verifyScan(s, expectedRows, expectedKeys);

  }
  
  void doTestQualifierFilter() throws Exception {
    // Match two keys (one from each family) in half the rows
    long expectedRows = this.numRows / 2;
    long expectedKeys = 2;
    Filter f = new QualifierFilter(CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes("testQualifierOne-2")));
    Scan s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match keys less than same qualifier
    // Expect only two keys (one from each family) in half the rows
    expectedRows = this.numRows / 2;
    expectedKeys = 2;
    f = new QualifierFilter(CompareOp.LESS,
        new BinaryComparator(Bytes.toBytes("testQualifierOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match keys less than or equal
    // Expect four keys (two from each family) in half the rows
    expectedRows = this.numRows / 2;
    expectedKeys = 4;
    f = new QualifierFilter(CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testQualifierOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match keys not equal
    // Expect four keys (two from each family)
    // Only look in first group of rows
    expectedRows = this.numRows / 2;
    expectedKeys = 4;
    f = new QualifierFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testQualifierOne-2")));
    s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match keys greater or equal
    // Expect four keys (two from each family)
    // Only look in first group of rows
    expectedRows = this.numRows / 2;
    expectedKeys = 4;
    f = new QualifierFilter(CompareOp.GREATER_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testQualifierOne-2")));
    s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match keys greater
    // Expect two keys (one from each family)
    // Only look in first group of rows
    expectedRows = this.numRows / 2;
    expectedKeys = 2;
    f = new QualifierFilter(CompareOp.GREATER,
        new BinaryComparator(Bytes.toBytes("testQualifierOne-2")));
    s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match keys not equal to
    // Look across rows and fully validate the keys and ordering
    // Expect varied numbers of keys, 4 per row in group one, 6 per row in
    // group two
    f = new QualifierFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(QUALIFIERS_ONE[2]));
    s = new Scan();
    s.setFilter(f);
    
    KeyValue [] kvs = {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
    };
    verifyScanFull(s, kvs);
     
    // Test across rows and groups with a regex
    // Filter out "test*-2"
    // Expect 4 keys per row across both groups
    f = new QualifierFilter(CompareOp.NOT_EQUAL,
        new RegexStringComparator("test.+-2"));
    s = new Scan();
    s.setFilter(f);
    
    kvs = new KeyValue [] {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
    };
    verifyScanFull(s, kvs);
  }
  
  void doTestRowFilter() throws Exception {
    // Match a single row, all keys
    long expectedRows = 1;
    long expectedKeys = this.colsPerRow;
    Filter f = new RowFilter(CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    Scan s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match a two rows, one from each group, using regex
    expectedRows = 2;
    expectedKeys = this.colsPerRow;
    f = new RowFilter(CompareOp.EQUAL,
        new RegexStringComparator("testRow.+-2"));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match rows less than
    // Expect all keys in one row
    expectedRows = 1;
    expectedKeys = this.colsPerRow;
    f = new RowFilter(CompareOp.LESS,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match rows less than or equal
    // Expect all keys in two rows
    expectedRows = 2;
    expectedKeys = this.colsPerRow;
    f = new RowFilter(CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match rows not equal
    // Expect all keys in all but one row
    expectedRows = this.numRows - 1;
    expectedKeys = this.colsPerRow;
    f = new RowFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match keys greater or equal
    // Expect all keys in all but one row
    expectedRows = this.numRows - 1;
    expectedKeys = this.colsPerRow;
    f = new RowFilter(CompareOp.GREATER_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match keys greater
    // Expect all keys in all but two rows
    expectedRows = this.numRows - 2;
    expectedKeys = this.colsPerRow;
    f = new RowFilter(CompareOp.GREATER,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match rows not equal to testRowTwo-2
    // Look across rows and fully validate the keys and ordering
    // Should see all keys in all rows but testRowTwo-2
    f = new RowFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    
    KeyValue [] kvs = {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
    };
    verifyScanFull(s, kvs);
    
    // Test across rows and groups with a regex
    // Filter out everything that doesn't match "*-2"
    // Expect all keys in two rows
    f = new RowFilter(CompareOp.EQUAL,
        new RegexStringComparator(".+-2"));
    s = new Scan();
    s.setFilter(f);
    
    kvs = new KeyValue [] {
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[3], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[3], VALUES[0]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1])
    };
    verifyScanFull(s, kvs);
  }
  
  void doTestValueFilter() throws Exception {
    // Match group one rows
    long expectedRows = this.numRows / 2;
    long expectedKeys = this.colsPerRow;
    Filter f = new ValueFilter(CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    Scan s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match group two rows
    expectedRows = this.numRows / 2;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueTwo")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match all values using regex
    expectedRows = this.numRows;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.EQUAL,
        new RegexStringComparator("testValue((One)|(Two))"));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match values less than
    // Expect group one rows
    expectedRows = this.numRows / 2;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.LESS,
        new BinaryComparator(Bytes.toBytes("testValueTwo")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match values less than or equal
    // Expect all rows
    expectedRows = this.numRows;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueTwo")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values less than or equal
    // Expect group one rows
    expectedRows = this.numRows / 2;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match values not equal
    // Expect half the rows
    expectedRows = this.numRows / 2;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match values greater or equal
    // Expect all rows
    expectedRows = this.numRows;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.GREATER_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match values greater
    // Expect half rows
    expectedRows = this.numRows / 2;
    expectedKeys = this.colsPerRow;
    f = new ValueFilter(CompareOp.GREATER,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);
    
    // Match values not equal to testValueOne
    // Look across rows and fully validate the keys and ordering
    // Should see all keys in all group two rows
    f = new ValueFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);
    
    KeyValue [] kvs = {
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
    };
    verifyScanFull(s, kvs);
  }
  
  void doTestSkipFilter() throws Exception {
    // Test for qualifier regex: "testQualifierOne-2"
    // Should only get rows from second group, and all keys
    Filter f = new SkipFilter(new QualifierFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testQualifierOne-2"))));
    Scan s = new Scan();
    s.setFilter(f);
    
    KeyValue [] kvs = {
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[3], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[3], VALUES[1]),
    };
    verifyScanFull(s, kvs);
  }
    
  void doTestFilterList() throws Exception {
    // Test getting a single row, single key using Row, Qualifier, and Value 
    // regular expression and substring filters
    // Use must pass all
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new RowFilter(CompareOp.EQUAL,
      new RegexStringComparator(".+-2")));
    filters.add(new QualifierFilter(CompareOp.EQUAL,
      new RegexStringComparator(".+-2")));
    filters.add(new ValueFilter(CompareOp.EQUAL,
      new SubstringComparator("One")));
    Filter f = new FilterList(Operator.MUST_PASS_ALL, filters);
    Scan s = new Scan();
    s.addFamily(FAMILIES[0]);
    s.setFilter(f);
    KeyValue [] kvs = {
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0])
    };
    verifyScanFull(s, kvs);

    // Test getting everything with a MUST_PASS_ONE filter including row, qf,
    // val, regular expression and substring filters
    filters.clear();
    filters.add(new RowFilter(CompareOp.EQUAL,
      new RegexStringComparator(".+Two.+")));
    filters.add(new QualifierFilter(CompareOp.EQUAL,
      new RegexStringComparator(".+-2")));
    filters.add(new ValueFilter(CompareOp.EQUAL,
      new SubstringComparator("One")));
    f = new FilterList(Operator.MUST_PASS_ONE, filters);
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, this.numRows, this.colsPerRow);    
  }
  
  void doTestFirstKeyOnlyFilter() throws Exception {
    Scan s = new Scan();
    s.setFilter(new FirstKeyOnlyFilter());
    // Expected KVs, the first KV from each of the remaining 6 rows
    KeyValue [] kvs = {
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1])
    };
    verifyScanFull(s, kvs);
  }
  
  public void testScannersWithFilters() throws Exception {
    doTestNoFilter();
    doTestPrefixFilter();
    doTestPageFilter();
    doTestInclusiveStopFilter();
    doTestQualifierFilter();
    doTestRowFilter();
    doTestValueFilter();
    doTestSkipFilter();
    doTestFilterList();
    doTestFirstKeyOnlyFilter();
  }
}
