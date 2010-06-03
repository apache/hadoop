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
import java.net.URLEncoder;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.httpclient.Header;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.util.Bytes;

public class TestRowResource extends HBaseRESTClusterTestBase {
  static final String TABLE = "TestRowResource";
  static final String CFA = "a";
  static final String CFB = "b";
  static final String COLUMN_1 = CFA + ":1";
  static final String COLUMN_2 = CFB + ":2";
  static final String ROW_1 = "testrow1";
  static final String VALUE_1 = "testvalue1";
  static final String ROW_2 = "testrow2";
  static final String VALUE_2 = "testvalue2";
  static final String ROW_3 = "testrow3";
  static final String VALUE_3 = "testvalue3";
  static final String ROW_4 = "testrow4";
  static final String VALUE_4 = "testvalue4";

  Client client;
  JAXBContext context;
  Marshaller marshaller;
  Unmarshaller unmarshaller;
  HBaseAdmin admin;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    context = JAXBContext.newInstance(
        CellModel.class,
        CellSetModel.class,
        RowModel.class);
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
  }

  @Override
  protected void tearDown() throws Exception {
    client.shutdown();
    super.tearDown();
  }

  Response deleteRow(String table, String row) throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    Response response = client.delete(path.toString());
    Thread.yield();
    return response;
  }

  Response deleteValue(String table, String row, String column)
      throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    Response response = client.delete(path.toString());
    Thread.yield();
    return response;
  }

  Response getValueXML(String table, String row, String column)
      throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    Response response = client.get(path.toString(), MIMETYPE_XML);
    return response;
  }

  Response getValuePB(String table, String row, String column) 
      throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    Response response = client.get(path.toString(), MIMETYPE_PROTOBUF); 
    return response;
  }

  Response putValueXML(String table, String row, String column, String value)
      throws IOException, JAXBException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    RowModel rowModel = new RowModel(row);
    rowModel.addCell(new CellModel(Bytes.toBytes(column),
      Bytes.toBytes(value)));
    CellSetModel cellSetModel = new CellSetModel();
    cellSetModel.addRow(rowModel);
    StringWriter writer = new StringWriter();
    marshaller.marshal(cellSetModel, writer);
    Response response = client.put(path.toString(), MIMETYPE_XML,
      Bytes.toBytes(writer.toString()));
    Thread.yield();
    return response;
  }

  void checkValueXML(String table, String row, String column, String value)
      throws IOException, JAXBException {
    Response response = getValueXML(table, row, column);
    assertEquals(response.getCode(), 200);
    CellSetModel cellSet = (CellSetModel)
      unmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));
    RowModel rowModel = cellSet.getRows().get(0);
    CellModel cell = rowModel.getCells().get(0);
    assertEquals(Bytes.toString(cell.getColumn()), column);
    assertEquals(Bytes.toString(cell.getValue()), value);
  }

  Response putValuePB(String table, String row, String column, String value)
      throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    RowModel rowModel = new RowModel(row);
    rowModel.addCell(new CellModel(Bytes.toBytes(column),
      Bytes.toBytes(value)));
    CellSetModel cellSetModel = new CellSetModel();
    cellSetModel.addRow(rowModel);
    Response response = client.put(path.toString(), MIMETYPE_PROTOBUF,
      cellSetModel.createProtobufOutput());
    Thread.yield();
    return response;
  }

  void checkValuePB(String table, String row, String column, String value)
      throws IOException {
    Response response = getValuePB(table, row, column);
    assertEquals(response.getCode(), 200);
    CellSetModel cellSet = new CellSetModel();
    cellSet.getObjectFromMessage(response.getBody());
    RowModel rowModel = cellSet.getRows().get(0);
    CellModel cell = rowModel.getCells().get(0);
    assertEquals(Bytes.toString(cell.getColumn()), column);
    assertEquals(Bytes.toString(cell.getValue()), value);
  }

  void doTestDelete() throws IOException, JAXBException {
    Response response;
    
    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);
    response = putValueXML(TABLE, ROW_1, COLUMN_2, VALUE_2);
    assertEquals(response.getCode(), 200);
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    checkValueXML(TABLE, ROW_1, COLUMN_2, VALUE_2);

    response = deleteValue(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 200);
    response = getValueXML(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 404);
    checkValueXML(TABLE, ROW_1, COLUMN_2, VALUE_2);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);    
    response = getValueXML(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 404);
    response = getValueXML(TABLE, ROW_1, COLUMN_2);
    assertEquals(response.getCode(), 404);
  }

  void doTestSingleCellGetPutXML() throws IOException, JAXBException {
    Response response = getValueXML(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 404);

    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_2);
    assertEquals(response.getCode(), 200);
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_2);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);    
  }

  void doTestSingleCellGetPutPB() throws IOException, JAXBException {
    Response response = getValuePB(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 404);

    response = putValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);

    response = putValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_2);
    assertEquals(response.getCode(), 200);
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_2);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);    
  }

  void doTestSingleCellGetPutBinary() throws IOException {
    final String path = "/" + TABLE + "/" + ROW_3 + "/" + COLUMN_1;
    final byte[] body = Bytes.toBytes(VALUE_3);
    Response response = client.put(path, MIMETYPE_BINARY, body);
    assertEquals(response.getCode(), 200);
    Thread.yield();

    response = client.get(path, MIMETYPE_BINARY);
    assertEquals(response.getCode(), 200);
    assertTrue(Bytes.equals(response.getBody(), body));
    boolean foundTimestampHeader = false;
    for (Header header: response.getHeaders()) {
      if (header.getName().equals("X-Timestamp")) {
        foundTimestampHeader = true;
        break;
      }
    }
    assertTrue(foundTimestampHeader);

    response = deleteRow(TABLE, ROW_3);
    assertEquals(response.getCode(), 200);
  }

  void doTestSingleCellGetJSON() throws IOException, JAXBException {
    final String path = "/" + TABLE + "/" + ROW_4 + "/" + COLUMN_1;
    Response response = client.put(path, MIMETYPE_BINARY,
      Bytes.toBytes(VALUE_4));
    assertEquals(response.getCode(), 200);
    Thread.yield();
    response = client.get(path, MIMETYPE_JSON);
    assertEquals(response.getCode(), 200);
    response = deleteRow(TABLE, ROW_4);
    assertEquals(response.getCode(), 200);
  }

  void doTestURLEncodedKey() throws IOException, JAXBException {
    String encodedKey = URLEncoder.encode("http://www.google.com/", 
      HConstants.UTF8_ENCODING);
    Response response;
    response = putValueXML(TABLE, encodedKey, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);
    response = putValuePB(TABLE, encodedKey, COLUMN_2, VALUE_2);
    assertEquals(response.getCode(), 200);
    checkValuePB(TABLE, encodedKey, COLUMN_1, VALUE_1);
    checkValueXML(TABLE, encodedKey, COLUMN_2, VALUE_2);
  }

  void doTestMultiCellGetPutXML() throws IOException, JAXBException {
    String path = "/" + TABLE + "/fakerow";  // deliberate nonexistent row

    CellSetModel cellSetModel = new CellSetModel();
    RowModel rowModel = new RowModel(ROW_1);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_1)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2),
      Bytes.toBytes(VALUE_2)));
    cellSetModel.addRow(rowModel);
    rowModel = new RowModel(ROW_2);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_3)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2),
      Bytes.toBytes(VALUE_4)));
    cellSetModel.addRow(rowModel);
    StringWriter writer = new StringWriter();
    marshaller.marshal(cellSetModel, writer);
    Response response = client.put(path, MIMETYPE_XML,
      Bytes.toBytes(writer.toString()));
    Thread.yield();

    // make sure the fake row was not actually created
    response = client.get(path, MIMETYPE_XML);
    assertEquals(response.getCode(), 404);

    // check that all of the values were created
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    checkValueXML(TABLE, ROW_1, COLUMN_2, VALUE_2);
    checkValueXML(TABLE, ROW_2, COLUMN_1, VALUE_3);
    checkValueXML(TABLE, ROW_2, COLUMN_2, VALUE_4);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);    
    response = deleteRow(TABLE, ROW_2);
    assertEquals(response.getCode(), 200);
  }

  void doTestMultiCellGetPutPB() throws IOException {
    String path = "/" + TABLE + "/fakerow";  // deliberate nonexistent row

    CellSetModel cellSetModel = new CellSetModel();
    RowModel rowModel = new RowModel(ROW_1);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_1)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2),
      Bytes.toBytes(VALUE_2)));
    cellSetModel.addRow(rowModel);
    rowModel = new RowModel(ROW_2);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_3)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2),
      Bytes.toBytes(VALUE_4)));
    cellSetModel.addRow(rowModel);
    Response response = client.put(path, MIMETYPE_PROTOBUF,
      cellSetModel.createProtobufOutput());
    Thread.yield();

    // make sure the fake row was not actually created
    response = client.get(path, MIMETYPE_PROTOBUF);
    assertEquals(response.getCode(), 404);

    // check that all of the values were created
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    checkValuePB(TABLE, ROW_1, COLUMN_2, VALUE_2);
    checkValuePB(TABLE, ROW_2, COLUMN_1, VALUE_3);
    checkValuePB(TABLE, ROW_2, COLUMN_2, VALUE_4);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);    
    response = deleteRow(TABLE, ROW_2);
    assertEquals(response.getCode(), 200);
  }

  public void testRowResource() throws Exception {
    doTestDelete();
    doTestSingleCellGetPutXML();
    doTestSingleCellGetPutPB();
    doTestSingleCellGetPutBinary();
    doTestSingleCellGetJSON();
    doTestURLEncodedKey();
    doTestMultiCellGetPutXML();
    doTestMultiCellGetPutPB();
  }
}
