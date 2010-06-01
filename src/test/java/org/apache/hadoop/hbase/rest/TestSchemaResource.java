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

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.ColumnSchemaModel;
import org.apache.hadoop.hbase.rest.model.TableSchemaModel;
import org.apache.hadoop.hbase.rest.model.TestTableSchemaModel;
import org.apache.hadoop.hbase.util.Bytes;

public class TestSchemaResource extends HBaseRESTClusterTestBase {
  static String TABLE1 = "TestSchemaResource1";
  static String TABLE2 = "TestSchemaResource2";

  Client client;
  JAXBContext context;
  HBaseAdmin admin;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    context = JAXBContext.newInstance(
        ColumnSchemaModel.class,
        TableSchemaModel.class);
    admin = new HBaseAdmin(conf);
    client = new Client(new Cluster().add("localhost", testServletPort));
  }

  @Override
  protected void tearDown() throws Exception {
    client.shutdown();
    super.tearDown();
  }

  byte[] toXML(TableSchemaModel model) throws JAXBException {
    StringWriter writer = new StringWriter();
    context.createMarshaller().marshal(model, writer);
    return Bytes.toBytes(writer.toString());
  }

  TableSchemaModel fromXML(byte[] content) throws JAXBException {
    return (TableSchemaModel) context.createUnmarshaller()
      .unmarshal(new ByteArrayInputStream(content));
  }

  void doTestTableCreateAndDeleteXML() throws IOException, JAXBException {
    String schemaPath = "/" + TABLE1 + "/schema";
    TableSchemaModel model;
    Response response;

    assertFalse(admin.tableExists(TABLE1));

    // create the table
    model = TestTableSchemaModel.buildTestModel(TABLE1);
    TestTableSchemaModel.checkModel(model, TABLE1);
    response = client.put(schemaPath, MIMETYPE_XML, toXML(model));
    assertEquals(response.getCode(), 201);

    // make sure HBase concurs, and wait for the table to come online
    admin.enableTable(TABLE1);

    // retrieve the schema and validate it
    response = client.get(schemaPath, MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    model = fromXML(response.getBody());
    TestTableSchemaModel.checkModel(model, TABLE1);

    // delete the table
    client.delete(schemaPath);

    // make sure HBase concurs
    assertFalse(admin.tableExists(TABLE1));
  }

  void doTestTableCreateAndDeletePB() throws IOException, JAXBException {
    String schemaPath = "/" + TABLE2 + "/schema";
    TableSchemaModel model;
    Response response;

    assertFalse(admin.tableExists(TABLE2));

    // create the table
    model = TestTableSchemaModel.buildTestModel(TABLE2);
    TestTableSchemaModel.checkModel(model, TABLE2);
    response = client.put(schemaPath, Constants.MIMETYPE_PROTOBUF,
      model.createProtobufOutput());
    assertEquals(response.getCode(), 201);

    // make sure HBase concurs, and wait for the table to come online
    admin.enableTable(TABLE2);

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
  }

  public void testSchemaResource() throws Exception {
    doTestTableCreateAndDeleteXML();
    doTestTableCreateAndDeletePB();
  }
}
