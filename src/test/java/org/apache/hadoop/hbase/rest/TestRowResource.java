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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
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

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRowResource {
  private static final String TABLE = "TestRowResource";
  private static final String CFA = "a";
  private static final String CFB = "b";
  private static final String COLUMN_1 = CFA + ":1";
  private static final String COLUMN_2 = CFB + ":2";
  private static final String ROW_1 = "testrow1";
  private static final String VALUE_1 = "testvalue1";
  private static final String ROW_2 = "testrow2";
  private static final String VALUE_2 = "testvalue2";
  private static final String ROW_3 = "testrow3";
  private static final String VALUE_3 = "testvalue3";
  private static final String ROW_4 = "testrow4";
  private static final String VALUE_4 = "testvalue4";

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL = 
    new HBaseRESTTestingUtility();
  private static Client client;
  private static JAXBContext context;
  private static Marshaller marshaller;
  private static Unmarshaller unmarshaller;
  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniCluster(3);
    REST_TEST_UTIL.startServletContainer(conf);
    context = JAXBContext.newInstance(
        CellModel.class,
        CellSetModel.class,
        RowModel.class);
    marshaller = context.createMarshaller();
    unmarshaller = context.createUnmarshaller();
    client = new Client(new Cluster().add("localhost", 
      REST_TEST_UTIL.getServletPort()));
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(TABLE)) {
      return;
    }
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(new HColumnDescriptor(CFA));
    htd.addFamily(new HColumnDescriptor(CFB));
    admin.createTable(htd);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  private static Response deleteRow(String table, String row) 
      throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    Response response = client.delete(path.toString());
    Thread.yield();
    return response;
  }

  private static Response deleteValue(String table, String row, String column)
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

  private static Response getValueXML(String table, String row, String column)
      throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    return getValueXML(path.toString());
  }

  private static Response getValueXML(String table, String startRow,
      String endRow, String column) throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(startRow);
    path.append(",");
    path.append(endRow);
    path.append('/');
    path.append(column);
    return getValueXML(path.toString());
  }

  private static Response getValueXML(String url) throws IOException {
    Response response = client.get(url, Constants.MIMETYPE_XML);
    return response;
  }

  private static Response getValuePB(String table, String row, String column) 
      throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    return getValuePB(path.toString());
  }

  private static Response getValuePB(String url) throws IOException {
    Response response = client.get(url, Constants.MIMETYPE_PROTOBUF); 
    return response;
  }

  private static Response putValueXML(String table, String row, String column,
      String value) throws IOException, JAXBException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    return putValueXML(path.toString(), table, row, column, value);
  }

  private static Response putValueXML(String url, String table, String row,
      String column, String value) throws IOException, JAXBException {
    RowModel rowModel = new RowModel(row);
    rowModel.addCell(new CellModel(Bytes.toBytes(column),
      Bytes.toBytes(value)));
    CellSetModel cellSetModel = new CellSetModel();
    cellSetModel.addRow(rowModel);
    StringWriter writer = new StringWriter();
    marshaller.marshal(cellSetModel, writer);
    Response response = client.put(url, Constants.MIMETYPE_XML,
      Bytes.toBytes(writer.toString()));
    Thread.yield();
    return response;
  }

  private static void checkValueXML(String table, String row, String column,
      String value) throws IOException, JAXBException {
    Response response = getValueXML(table, row, column);
    assertEquals(response.getCode(), 200);
    CellSetModel cellSet = (CellSetModel)
      unmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));
    RowModel rowModel = cellSet.getRows().get(0);
    CellModel cell = rowModel.getCells().get(0);
    assertEquals(Bytes.toString(cell.getColumn()), column);
    assertEquals(Bytes.toString(cell.getValue()), value);
  }

  private static void checkValueXML(String url, String table, String row,
      String column, String value) throws IOException, JAXBException {
    Response response = getValueXML(url);
    assertEquals(response.getCode(), 200);
    CellSetModel cellSet = (CellSetModel)
      unmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));
    RowModel rowModel = cellSet.getRows().get(0);
    CellModel cell = rowModel.getCells().get(0);
    assertEquals(Bytes.toString(cell.getColumn()), column);
    assertEquals(Bytes.toString(cell.getValue()), value);
  }

  private static Response putValuePB(String table, String row, String column,
      String value) throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    return putValuePB(path.toString(), table, row, column, value);
  }

  private static Response putValuePB(String url, String table, String row,
      String column, String value) throws IOException {
    RowModel rowModel = new RowModel(row);
    rowModel.addCell(new CellModel(Bytes.toBytes(column),
      Bytes.toBytes(value)));
    CellSetModel cellSetModel = new CellSetModel();
    cellSetModel.addRow(rowModel);
    Response response = client.put(url, Constants.MIMETYPE_PROTOBUF,
      cellSetModel.createProtobufOutput());
    Thread.yield();
    return response;
  }

  private static void checkValuePB(String table, String row, String column, 
      String value) throws IOException {
    Response response = getValuePB(table, row, column);
    assertEquals(response.getCode(), 200);
    CellSetModel cellSet = new CellSetModel();
    cellSet.getObjectFromMessage(response.getBody());
    RowModel rowModel = cellSet.getRows().get(0);
    CellModel cell = rowModel.getCells().get(0);
    assertEquals(Bytes.toString(cell.getColumn()), column);
    assertEquals(Bytes.toString(cell.getValue()), value);
  }

  @Test
  public void testDelete() throws IOException, JAXBException {
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

  @Test
  public void testForbidden() throws IOException, JAXBException {
    Response response;

    conf.set("hbase.rest.readonly", "true");

    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 403);
    response = putValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 403);
    response = deleteValue(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 403);
    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 403);

    conf.set("hbase.rest.readonly", "false");

    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);
    response = putValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);
    response = deleteValue(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 200);
    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testSingleCellGetPutXML() throws IOException, JAXBException {
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

  @Test
  public void testSingleCellGetPutPB() throws IOException, JAXBException {
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

  @Test
  public void testSingleCellGetPutBinary() throws IOException {
    final String path = "/" + TABLE + "/" + ROW_3 + "/" + COLUMN_1;
    final byte[] body = Bytes.toBytes(VALUE_3);
    Response response = client.put(path, Constants.MIMETYPE_BINARY, body);
    assertEquals(response.getCode(), 200);
    Thread.yield();

    response = client.get(path, Constants.MIMETYPE_BINARY);
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

  @Test
  public void testSingleCellGetJSON() throws IOException, JAXBException {
    final String path = "/" + TABLE + "/" + ROW_4 + "/" + COLUMN_1;
    Response response = client.put(path, Constants.MIMETYPE_BINARY,
      Bytes.toBytes(VALUE_4));
    assertEquals(response.getCode(), 200);
    Thread.yield();
    response = client.get(path, Constants.MIMETYPE_JSON);
    assertEquals(response.getCode(), 200);
    response = deleteRow(TABLE, ROW_4);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testURLEncodedKey() throws IOException, JAXBException {
    String urlKey = "http://example.com/foo";
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(TABLE);
    path.append('/');
    path.append(URLEncoder.encode(urlKey, HConstants.UTF8_ENCODING));
    path.append('/');
    path.append(COLUMN_1);
    Response response;
    response = putValueXML(path.toString(), TABLE, urlKey, COLUMN_1,
      VALUE_1);
    assertEquals(response.getCode(), 200);
    checkValueXML(path.toString(), TABLE, urlKey, COLUMN_1, VALUE_1);
  }

  @Test
  public void testNoSuchCF() throws IOException, JAXBException {
    final String goodPath = "/" + TABLE + "/" + ROW_1 + "/" + CFA+":";
    final String badPath = "/" + TABLE + "/" + ROW_1 + "/" + "BAD";
    Response response = client.post(goodPath, Constants.MIMETYPE_BINARY,
      Bytes.toBytes(VALUE_1));
    assertEquals(response.getCode(), 200);
    assertEquals(client.get(goodPath, Constants.MIMETYPE_BINARY).getCode(),
      200);
    assertEquals(client.get(badPath, Constants.MIMETYPE_BINARY).getCode(),
      404);
    assertEquals(client.get(goodPath, Constants.MIMETYPE_BINARY).getCode(),
      200);
  }

  @Test
  public void testMultiCellGetPutXML() throws IOException, JAXBException {
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
    Response response = client.put(path, Constants.MIMETYPE_XML,
      Bytes.toBytes(writer.toString()));
    Thread.yield();

    // make sure the fake row was not actually created
    response = client.get(path, Constants.MIMETYPE_XML);
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

  @Test
  public void testMultiCellGetPutPB() throws IOException {
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
    Response response = client.put(path, Constants.MIMETYPE_PROTOBUF,
      cellSetModel.createProtobufOutput());
    Thread.yield();

    // make sure the fake row was not actually created
    response = client.get(path, Constants.MIMETYPE_PROTOBUF);
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

  @Test
  public void testStartEndRowGetPutXML() throws IOException, JAXBException {
    String[] rows = { ROW_1, ROW_2, ROW_3 };
    String[] values = { VALUE_1, VALUE_2, VALUE_3 }; 
    Response response = null;
    for (int i = 0; i < rows.length; i++) {
      response = putValueXML(TABLE, rows[i], COLUMN_1, values[i]);
      assertEquals(200, response.getCode());
      checkValueXML(TABLE, rows[i], COLUMN_1, values[i]);
    }
    response = getValueXML(TABLE, rows[0], rows[2], COLUMN_1);
    assertEquals(200, response.getCode());
    CellSetModel cellSet = (CellSetModel)
      unmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));
    assertEquals(2, cellSet.getRows().size());
    for (int i = 0; i < cellSet.getRows().size()-1; i++) {
      RowModel rowModel = cellSet.getRows().get(i);
      for (CellModel cell: rowModel.getCells()) {
        assertEquals(COLUMN_1, Bytes.toString(cell.getColumn()));
        assertEquals(values[i], Bytes.toString(cell.getValue()));
      }   
    }
    for (String row : rows) {
      response = deleteRow(TABLE, row);
      assertEquals(200, response.getCode());
    }
  }
}
