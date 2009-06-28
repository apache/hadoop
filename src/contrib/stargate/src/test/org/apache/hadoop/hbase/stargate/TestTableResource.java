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

package org.apache.hadoop.hbase.stargate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.stargate.client.Client;
import org.apache.hadoop.hbase.stargate.client.Cluster;
import org.apache.hadoop.hbase.stargate.client.Response;
import org.apache.hadoop.hbase.stargate.model.TableModel;
import org.apache.hadoop.hbase.stargate.model.TableInfoModel;
import org.apache.hadoop.hbase.stargate.model.TableListModel;
import org.apache.hadoop.hbase.stargate.model.TableRegionModel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

public class TestTableResource extends MiniClusterTestCase {
  private static final Log LOG = 
    LogFactory.getLog(TestTableResource.class);

  private static String TABLE = "TestTableResource";
  private static String COLUMN = "test:";
  private static Map<HRegionInfo,HServerAddress> regionMap;

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
    htd.addFamily(new HColumnDescriptor(COLUMN));
    admin.createTable(htd);
    HTable table = new HTable(conf, TABLE);
    byte[] k = new byte[3];
    byte [][] famAndQf = KeyValue.parseColumn(Bytes.toBytes(COLUMN));
    for (byte b1 = 'a'; b1 < 'z'; b1++) {
      for (byte b2 = 'a'; b2 < 'z'; b2++) {
        for (byte b3 = 'a'; b3 < 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          Put put = new Put(k);
          put.add(famAndQf[0], famAndQf[1], k);
          table.put(put);
        }
      }
    }
    table.flushCommits();
    // get the initial layout (should just be one region)
    Map<HRegionInfo,HServerAddress> m = table.getRegionsInfo();
    assertEquals(m.size(), 1);
    // tell the master to split the table
    admin.split(TABLE);
    // give some time for the split to happen
    try {
      Thread.sleep(15 * 1000);
    } catch (InterruptedException e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
    // check again
    m = table.getRegionsInfo();
    // should have two regions now
    assertEquals(m.size(), 2);
    regionMap = m;
    LOG.info("regions: " + regionMap);
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

  public void checkTableInfo(TableInfoModel model) {
    assertEquals(model.getName(), TABLE);
    Iterator<TableRegionModel> regions = model.getRegions().iterator();
    assertTrue(regions.hasNext());
    while (regions.hasNext()) {
      TableRegionModel region = regions.next();
      boolean found = false;
      for (Map.Entry<HRegionInfo,HServerAddress> e: regionMap.entrySet()) {
        HRegionInfo hri = e.getKey();
        if (hri.getRegionNameAsString().equals(region.getName())) {
          found = true;
          byte[] startKey = hri.getStartKey();
          byte[] endKey = hri.getEndKey();
          InetSocketAddress sa = e.getValue().getInetSocketAddress();
          String location = sa.getHostName() + ":" +
            Integer.valueOf(sa.getPort());
          assertEquals(hri.getRegionId(), region.getId());
          assertTrue(Bytes.equals(startKey, region.getStartKey()));
          assertTrue(Bytes.equals(endKey, region.getEndKey()));
          assertEquals(location, region.getLocation());
          break;
        }
      }
      assertTrue(found);
    }
  }

  public void testTableInfoText() throws IOException {
    Response response = client.get("/" + TABLE + "/regions", MIMETYPE_PLAIN);
    assertEquals(response.getCode(), 200);
  }

  public void testTableInfoXML() throws IOException, JAXBException {
    Response response = client.get("/" + TABLE + "/regions", MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    TableInfoModel model = (TableInfoModel)
      context.createUnmarshaller()
        .unmarshal(new ByteArrayInputStream(response.getBody()));
    checkTableInfo(model);
  }

  public void testTableInfoJSON() throws IOException {
    Response response = client.get("/" + TABLE + "/regions", MIMETYPE_JSON);
    assertEquals(response.getCode(), 200);
  }

  public void testTableInfoPB() throws IOException, JAXBException {
    Response response = 
      client.get("/" + TABLE + "/regions", MIMETYPE_PROTOBUF);
    assertEquals(response.getCode(), 200);
    TableInfoModel model = new TableInfoModel();
    model.getObjectFromMessage(response.getBody());
    checkTableInfo(model);
  }
}
