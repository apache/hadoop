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

package org.apache.hadoop.hbase.stargate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.stargate.client.Client;
import org.apache.hadoop.hbase.stargate.client.Cluster;
import org.apache.hadoop.hbase.stargate.client.Response;
import org.apache.hadoop.hbase.stargate.model.TableModel;
import org.apache.hadoop.hbase.stargate.model.TableInfoModel;
import org.apache.hadoop.hbase.stargate.model.TableListModel;
import org.apache.hadoop.hbase.stargate.model.TableRegionModel;
import org.apache.hadoop.hbase.util.Bytes;

public class TestTableResource extends MiniClusterTestCase {
  private static String TABLE = "TestTableResource";
  private static String COLUMN = "test:";

  private Client client;
  private JAXBContext context;
  private HBaseAdmin admin;

  public TestTableResource() throws JAXBException {
    super();
    context = JAXBContext.newInstance(
        TableModel.class,
        TableInfoModel.class,
        TableListModel.class,
        TableRegionModel.class);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    client = new Client(new Cluster().add("localhost", testServletPort));
    admin = new HBaseAdmin(conf);
    if (admin.tableExists(TABLE)) {
      return;
    }
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(new HColumnDescriptor(KeyValue.parseColumn(
        Bytes.toBytes(COLUMN))[0]));
    admin.createTable(htd);
    new HTable(conf, TABLE);
  }

  @Override
  protected void tearDown() throws Exception {
    client.shutdown();
    super.tearDown();
  }

  private void checkTableList(TableListModel model) {
    boolean found = false;
    Iterator<TableModel> tables = model.getTables().iterator();
    assertTrue(tables.hasNext());
    while (tables.hasNext()) {
      TableModel table = tables.next();
      if (table.getName().equals(TABLE)) {
        found = true;
        break;
      }
    }
    assertTrue(found);
  }

  public void testTableListText() throws IOException {
    Response response = client.get("/", MIMETYPE_PLAIN);
    assertEquals(response.getCode(), 200);
  }

  public void testTableListXML() throws IOException, JAXBException {
    Response response = client.get("/", MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    TableListModel model = (TableListModel)
      context.createUnmarshaller()
        .unmarshal(new ByteArrayInputStream(response.getBody()));
    checkTableList(model);
  }

  public void testTableListJSON() throws IOException {
    Response response = client.get("/", MIMETYPE_JSON);
    assertEquals(response.getCode(), 200);
  }

  public void testTableListPB() throws IOException, JAXBException {
    Response response = client.get("/", MIMETYPE_PROTOBUF);
    assertEquals(response.getCode(), 200);
    TableListModel model = new TableListModel();
    model.getObjectFromMessage(response.getBody());
    checkTableList(model);
  }
}
