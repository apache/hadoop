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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.ColumnSchemaModel;
import org.apache.hadoop.hbase.rest.model.TableSchemaModel;
import org.apache.hadoop.hbase.rest.model.TestTableSchemaModel;
import org.apache.hadoop.hbase.util.Bytes;

import static org.junit.Assert.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSchemaResource {
  private static String TABLE1 = "TestSchemaResource1";
  private static String TABLE2 = "TestSchemaResource2";

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL =
    new HBaseRESTTestingUtility();
  private static Client client;
  private static JAXBContext context;
  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniCluster(3);
    REST_TEST_UTIL.startServletContainer(conf);
    client = new Client(new Cluster().add("localhost",
      REST_TEST_UTIL.getServletPort()));
    context = JAXBContext.newInstance(
      ColumnSchemaModel.class,
      TableSchemaModel.class);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  private static byte[] toXML(TableSchemaModel model) throws JAXBException {
    StringWriter writer = new StringWriter();
    context.createMarshaller().marshal(model, writer);
    return Bytes.toBytes(writer.toString());
  }

  private static TableSchemaModel fromXML(byte[] content)
      throws JAXBException {
    return (TableSchemaModel) context.createUnmarshaller()
      .unmarshal(new ByteArrayInputStream(content));
  }

  @Test
  public void testTableCreateAndDeleteXML() throws IOException, JAXBException {
    String schemaPath = "/" + TABLE1 + "/schema";
    TableSchemaModel model;
    Response response;

    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    assertFalse(admin.tableExists(TABLE1));

    // create the table
    model = TestTableSchemaModel.buildTestModel(TABLE1);
    TestTableSchemaModel.checkModel(model, TABLE1);
    response = client.put(schemaPath, Constants.MIMETYPE_XML, toXML(model));
    assertEquals(response.getCode(), 201);

    // recall the same put operation but in read-only mode
    conf.set("hbase.rest.readonly", "true");
    response = client.put(schemaPath, Constants.MIMETYPE_XML, toXML(model));
    assertEquals(response.getCode(), 403);

    // retrieve the schema and validate it
    response = client.get(schemaPath, Constants.MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    model = fromXML(response.getBody());
    TestTableSchemaModel.checkModel(model, TABLE1);

    // delete the table
    client.delete(schemaPath);

    // make sure HBase concurs
    assertFalse(admin.tableExists(TABLE1));

    // return read-only setting back to default
    conf.set("hbase.rest.readonly", "false");
  }

  @Test
  public void testTableCreateAndDeletePB() throws IOException, JAXBException {
    String schemaPath = "/" + TABLE2 + "/schema";
    TableSchemaModel model;
    Response response;

    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    assertFalse(admin.tableExists(TABLE2));

    // create the table
    model = TestTableSchemaModel.buildTestModel(TABLE2);
    TestTableSchemaModel.checkModel(model, TABLE2);
    response = client.put(schemaPath, Constants.MIMETYPE_PROTOBUF,
      model.createProtobufOutput());
    assertEquals(response.getCode(), 201);

    // recall the same put operation but in read-only mode
    conf.set("hbase.rest.readonly", "true");
    response = client.put(schemaPath, Constants.MIMETYPE_PROTOBUF,
      model.createProtobufOutput());
    assertEquals(response.getCode(), 403);

    // retrieve the schema and validate it
    response = client.get(schemaPath, Constants.MIMETYPE_PROTOBUF);
    assertEquals(response.getCode(), 200);
    model = new TableSchemaModel();
    model.getObjectFromMessage(response.getBody());
    TestTableSchemaModel.checkModel(model, TABLE2);

    // delete the table
    client.delete(schemaPath);

    // make sure HBase concurs
    assertFalse(admin.tableExists(TABLE2));

    // return read-only setting back to default
    conf.set("hbase.rest.readonly", "false");
  }
}
