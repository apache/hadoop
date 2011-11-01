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
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.TableModel;
import org.apache.hadoop.hbase.rest.model.TableInfoModel;
import org.apache.hadoop.hbase.rest.model.TableListModel;
import org.apache.hadoop.hbase.rest.model.TableRegionModel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import static org.junit.Assert.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTableResource {
  private static final Log LOG = LogFactory.getLog(TestTableResource.class);

  private static String TABLE = "TestTableResource";
  private static String COLUMN_FAMILY = "test";
  private static String COLUMN = COLUMN_FAMILY + ":qualifier";
  private static Map<HRegionInfo,HServerAddress> regionMap;

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL = 
    new HBaseRESTTestingUtility();
  private static Client client;
  private static JAXBContext context;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    REST_TEST_UTIL.startServletContainer(TEST_UTIL.getConfiguration());
    client = new Client(new Cluster().add("localhost", 
      REST_TEST_UTIL.getServletPort()));
    context = JAXBContext.newInstance(
        TableModel.class,
        TableInfoModel.class,
        TableListModel.class,
        TableRegionModel.class);
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(TABLE)) {
      return;
    }
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    admin.createTable(htd);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), TABLE);
    byte[] k = new byte[3];
    byte [][] famAndQf = KeyValue.parseColumn(Bytes.toBytes(COLUMN));
    for (byte b1 = 'a'; b1 < 'z'; b1++) {
      for (byte b2 = 'a'; b2 < 'z'; b2++) {
        for (byte b3 = 'a'; b3 < 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          Put put = new Put(k);
          put.setWriteToWAL(false);
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

    long timeout = System.currentTimeMillis() + (15 * 1000);
    while (System.currentTimeMillis() < timeout && m.size()!=2){
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        LOG.warn(StringUtils.stringifyException(e));
      }
      // check again
      m = table.getRegionsInfo();
    }

    // should have two regions now
    assertEquals(m.size(), 2);
    regionMap = m;
    LOG.info("regions: " + regionMap);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  private static void checkTableList(TableListModel model) {
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

  void checkTableInfo(TableInfoModel model) {
    assertEquals(model.getName(), TABLE);
    Iterator<TableRegionModel> regions = model.getRegions().iterator();
    assertTrue(regions.hasNext());
    while (regions.hasNext()) {
      TableRegionModel region = regions.next();
      boolean found = false;
      for (Map.Entry<HRegionInfo,HServerAddress> e: regionMap.entrySet()) {
        HRegionInfo hri = e.getKey();
        String hriRegionName = hri.getRegionNameAsString();
        String regionName = region.getName();
        if (hriRegionName.equals(regionName)) {
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

  @Test
  public void testTableListText() throws IOException {
    Response response = client.get("/", Constants.MIMETYPE_TEXT);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testTableListXML() throws IOException, JAXBException {
    Response response = client.get("/", Constants.MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    TableListModel model = (TableListModel)
      context.createUnmarshaller()
        .unmarshal(new ByteArrayInputStream(response.getBody()));
    checkTableList(model);
  }

  @Test
  public void testTableListJSON() throws IOException {
    Response response = client.get("/", Constants.MIMETYPE_JSON);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testTableListPB() throws IOException, JAXBException {
    Response response = client.get("/", Constants.MIMETYPE_PROTOBUF);
    assertEquals(response.getCode(), 200);
    TableListModel model = new TableListModel();
    model.getObjectFromMessage(response.getBody());
    checkTableList(model);
  }

  @Test
  public void testTableInfoText() throws IOException {
    Response response = client.get("/" + TABLE + "/regions",
      Constants.MIMETYPE_TEXT);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testTableInfoXML() throws IOException, JAXBException {
    Response response = client.get("/" + TABLE + "/regions", 
      Constants.MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    TableInfoModel model = (TableInfoModel)
      context.createUnmarshaller()
        .unmarshal(new ByteArrayInputStream(response.getBody()));
    checkTableInfo(model);
  }

  @Test
  public void testTableInfoJSON() throws IOException {
    Response response = client.get("/" + TABLE + "/regions", 
      Constants.MIMETYPE_JSON);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testTableInfoPB() throws IOException, JAXBException {
    Response response = client.get("/" + TABLE + "/regions",
      Constants.MIMETYPE_PROTOBUF);
    assertEquals(response.getCode(), 200);
    TableInfoModel model = new TableInfoModel();
    model.getObjectFromMessage(response.getBody());
    checkTableInfo(model);
  }
}
