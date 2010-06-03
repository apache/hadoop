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
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Random;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.httpclient.Header;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.rest.model.ScannerModel;
import org.apache.hadoop.hbase.util.Bytes;

public class TestScannerResource extends HBaseRESTClusterTestBase {
  static final String TABLE = "TestScannerResource";
  static final String CFA = "a";
  static final String CFB = "b";
  static final String COLUMN_1 = CFA + ":1";
  static final String COLUMN_2 = CFB + ":2";

  static int expectedRows1;
  static int expectedRows2;

  Client client;
  JAXBContext context;
  Marshaller marshaller;
  Unmarshaller unmarshaller;
  HBaseAdmin admin;

  int insertData(String tableName, String column, double prob)
      throws IOException {
    Random rng = new Random();
    int count = 0;
    HTable table = new HTable(conf, tableName);
    byte[] k = new byte[3];
    byte [][] famAndQf = KeyValue.parseColumn(Bytes.toBytes(column));
    for (byte b1 = 'a'; b1 < 'z'; b1++) {
      for (byte b2 = 'a'; b2 < 'z'; b2++) {
        for (byte b3 = 'a'; b3 < 'z'; b3++) {
          if (rng.nextDouble() < prob) {
            k[0] = b1;
            k[1] = b2;
            k[2] = b3;
            Put put = new Put(k);
            put.add(famAndQf[0], famAndQf[1], k);
            table.put(put);
            count++;
          }
        }
      }
    }
    table.flushCommits();
    return count;
  }

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
    admin = new HBaseAdmin(conf);
    if (admin.tableExists(TABLE)) {
      return;
    }
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(new HColumnDescriptor(CFA));
    htd.addFamily(new HColumnDescriptor(CFB));
    admin.createTable(htd);
    expectedRows1 = insertData(TABLE, COLUMN_1, 1.0);
    expectedRows2 = insertData(TABLE, COLUMN_2, 0.5);
  }

  @Override
  protected void tearDown() throws Exception {
    client.shutdown();
    super.tearDown();
  }

  int countCellSet(CellSetModel model) {
    int count = 0;
    Iterator<RowModel> rows = model.getRows().iterator();
    while (rows.hasNext()) {
      RowModel row = rows.next();
      Iterator<CellModel> cells = row.getCells().iterator();
      while (cells.hasNext()) {
        cells.next();
        count++;
      }
    }
    return count;
  }

  void doTestSimpleScannerXML() throws IOException, JAXBException {
    final int BATCH_SIZE = 5;
    // new scanner
    ScannerModel model = new ScannerModel();
    model.setBatch(BATCH_SIZE);
    model.addColumn(Bytes.toBytes(COLUMN_1));
    StringWriter writer = new StringWriter();
    marshaller.marshal(model, writer);
    byte[] body = Bytes.toBytes(writer.toString());
    Response response = client.put("/" + TABLE + "/scanner", MIMETYPE_XML,
      body);
    assertEquals(response.getCode(), 201);
    String scannerURI = response.getLocation();
    assertNotNull(scannerURI);

    // get a cell set
    response = client.get(scannerURI, MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    CellSetModel cellSet = (CellSetModel)
      unmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));
    // confirm batch size conformance
    assertEquals(countCellSet(cellSet), BATCH_SIZE);

    // delete the scanner
    response = client.delete(scannerURI);
    assertEquals(response.getCode(), 200);
  }

  void doTestSimpleScannerPB() throws IOException {
    final int BATCH_SIZE = 10;
    // new scanner
    ScannerModel model = new ScannerModel();
    model.setBatch(BATCH_SIZE);
    model.addColumn(Bytes.toBytes(COLUMN_1));
    Response response = client.put("/" + TABLE + "/scanner",
      MIMETYPE_PROTOBUF, model.createProtobufOutput());
    assertEquals(response.getCode(), 201);
    String scannerURI = response.getLocation();
    assertNotNull(scannerURI);

    // get a cell set
    response = client.get(scannerURI, MIMETYPE_PROTOBUF);
    assertEquals(response.getCode(), 200);
    CellSetModel cellSet = new CellSetModel();
    cellSet.getObjectFromMessage(response.getBody());
    // confirm batch size conformance
    assertEquals(countCellSet(cellSet), BATCH_SIZE);

    // delete the scanner
    response = client.delete(scannerURI);
    assertEquals(response.getCode(), 200);
  }

  void doTestSimpleScannerBinary() throws IOException {
    // new scanner
    ScannerModel model = new ScannerModel();
    model.setBatch(1);
    model.addColumn(Bytes.toBytes(COLUMN_1));
    Response response = client.put("/" + TABLE + "/scanner",
      MIMETYPE_PROTOBUF, model.createProtobufOutput());
    assertEquals(response.getCode(), 201);
    String scannerURI = response.getLocation();
    assertNotNull(scannerURI);

    // get a cell
    response = client.get(scannerURI, MIMETYPE_BINARY);
    assertEquals(response.getCode(), 200);
    // verify that data was returned
    assertTrue(response.getBody().length > 0);
    // verify that the expected X-headers are present
    boolean foundRowHeader = false, foundColumnHeader = false,
      foundTimestampHeader = false;
    for (Header header: response.getHeaders()) {
      if (header.getName().equals("X-Row")) {
        foundRowHeader = true;
      } else if (header.getName().equals("X-Column")) {
        foundColumnHeader = true;
      } else if (header.getName().equals("X-Timestamp")) {
        foundTimestampHeader = true;
      }
    }
    assertTrue(foundRowHeader);
    assertTrue(foundColumnHeader);
    assertTrue(foundTimestampHeader);

    // delete the scanner
    response = client.delete(scannerURI);
    assertEquals(response.getCode(), 200);
  }

  int fullTableScan(ScannerModel model) throws IOException {
    model.setBatch(100);
    Response response = client.put("/" + TABLE + "/scanner",
        MIMETYPE_PROTOBUF, model.createProtobufOutput());
    assertEquals(response.getCode(), 201);
    String scannerURI = response.getLocation();
    assertNotNull(scannerURI);
    int count = 0;
    while (true) {
      response = client.get(scannerURI, MIMETYPE_PROTOBUF);
      assertTrue(response.getCode() == 200 || response.getCode() == 204);
      if (response.getCode() == 200) {
        CellSetModel cellSet = new CellSetModel();
        cellSet.getObjectFromMessage(response.getBody());
        Iterator<RowModel> rows = cellSet.getRows().iterator();
        while (rows.hasNext()) {
          RowModel row = rows.next();
          Iterator<CellModel> cells = row.getCells().iterator();
          while (cells.hasNext()) {
            cells.next();
            count++;
          }
        }
      } else {
        break;
      }
    }
    // delete the scanner
    response = client.delete(scannerURI);
    assertEquals(response.getCode(), 200);
    return count;
  }

  void doTestFullTableScan() throws IOException {
    ScannerModel model = new ScannerModel();
    model.addColumn(Bytes.toBytes(COLUMN_1));
    assertEquals(fullTableScan(model), expectedRows1);

    model = new ScannerModel();
    model.addColumn(Bytes.toBytes(COLUMN_2));
    assertEquals(fullTableScan(model), expectedRows2);
  }

  public void testScannerResource() throws Exception {
    doTestSimpleScannerXML();
    doTestSimpleScannerPB();
    doTestSimpleScannerBinary();
    doTestFullTableScan();
  }
}
