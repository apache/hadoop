/**
 * Copyright 2011 The Apache Software Foundation
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class TestMultiRowResource {

  private static final String TABLE = "TestRowResource";
  private static final String CFA = "a";
  private static final String CFB = "b";
  private static final String COLUMN_1 = CFA + ":1";
  private static final String COLUMN_2 = CFB + ":2";
  private static final String ROW_1 = "testrow5";
  private static final String VALUE_1 = "testvalue5";
  private static final String ROW_2 = "testrow6";
  private static final String VALUE_2 = "testvalue6";


  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL = new HBaseRESTTestingUtility();

  private static Client client;
  private static JAXBContext context;
  private static Marshaller marshaller;
  private static Unmarshaller unmarshaller;
  private static Configuration conf;


  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(conf);
    context = JAXBContext.newInstance(
            CellModel.class,
            CellSetModel.class,
            RowModel.class);
    marshaller = context.createMarshaller();
    unmarshaller = context.createUnmarshaller();
    client = new Client(new Cluster().add("localhost", REST_TEST_UTIL.getServletPort()));
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


  @Test
  public void testMultiCellGetJSON() throws IOException, JAXBException {
    String row_5_url = "/" + TABLE + "/" + ROW_1 + "/" + COLUMN_1;
    String row_6_url = "/" + TABLE + "/" + ROW_2 + "/" + COLUMN_2;


    StringBuilder path = new StringBuilder();
    path.append("/");
    path.append(TABLE);
    path.append("/multiget/?row=");
    path.append(ROW_1);
    path.append("&row=");
    path.append(ROW_2);

    client.post(row_5_url, Constants.MIMETYPE_BINARY, Bytes.toBytes(VALUE_1));
    client.post(row_6_url, Constants.MIMETYPE_BINARY, Bytes.toBytes(VALUE_2));


    Response response = client.get(path.toString(), Constants.MIMETYPE_JSON);
    assertEquals(response.getCode(), 200);

    client.delete(row_5_url);
    client.delete(row_6_url);

  }

  @Test
  public void testMultiCellGetXML() throws IOException, JAXBException {
    String row_5_url = "/" + TABLE + "/" + ROW_1 + "/" + COLUMN_1;
    String row_6_url = "/" + TABLE + "/" + ROW_2 + "/" + COLUMN_2;


    StringBuilder path = new StringBuilder();
    path.append("/");
    path.append(TABLE);
    path.append("/multiget/?row=");
    path.append(ROW_1);
    path.append("&row=");
    path.append(ROW_2);

    client.post(row_5_url, Constants.MIMETYPE_BINARY, Bytes.toBytes(VALUE_1));
    client.post(row_6_url, Constants.MIMETYPE_BINARY, Bytes.toBytes(VALUE_2));


    Response response = client.get(path.toString(), Constants.MIMETYPE_XML);
    assertEquals(response.getCode(), 200);

    client.delete(row_5_url);
    client.delete(row_6_url);

  }

  @Test
  public void testMultiCellGetJSONNotFound() throws IOException {
    String row_5_url = "/" + TABLE + "/" + ROW_1 + "/" + COLUMN_1;

    StringBuilder path = new StringBuilder();
    path.append("/");
    path.append(TABLE);
    path.append("/multiget/?row=");
    path.append(ROW_1);
    path.append("&row=");
    path.append(ROW_2);

    client.post(row_5_url, Constants.MIMETYPE_BINARY, Bytes.toBytes(VALUE_1));

    Response response = client.get(path.toString(), Constants.MIMETYPE_JSON);

    assertEquals(response.getCode(), 404);

  }
}
