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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.StorageClusterVersionModel;
import org.apache.hadoop.hbase.rest.model.VersionModel;
import org.apache.hadoop.hbase.util.Bytes;

import static org.junit.Assert.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.spi.container.servlet.ServletContainer;

public class TestVersionResource {
  private static final Log LOG = LogFactory.getLog(TestVersionResource.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL = 
    new HBaseRESTTestingUtility();
  private static Client client;
  private static JAXBContext context;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(TEST_UTIL.getConfiguration());
    client = new Client(new Cluster().add("localhost", 
      REST_TEST_UTIL.getServletPort()));
    context = JAXBContext.newInstance(
      VersionModel.class,
      StorageClusterVersionModel.class);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  private static void validate(VersionModel model) {
    assertNotNull(model);
    assertNotNull(model.getRESTVersion());
    assertEquals(model.getRESTVersion(), RESTServlet.VERSION_STRING);
    String osVersion = model.getOSVersion(); 
    assertNotNull(osVersion);
    assertTrue(osVersion.contains(System.getProperty("os.name")));
    assertTrue(osVersion.contains(System.getProperty("os.version")));
    assertTrue(osVersion.contains(System.getProperty("os.arch")));
    String jvmVersion = model.getJVMVersion();
    assertNotNull(jvmVersion);
    assertTrue(jvmVersion.contains(System.getProperty("java.vm.vendor")));
    assertTrue(jvmVersion.contains(System.getProperty("java.version")));
    assertTrue(jvmVersion.contains(System.getProperty("java.vm.version")));
    assertNotNull(model.getServerVersion());
    String jerseyVersion = model.getJerseyVersion();
    assertNotNull(jerseyVersion);
    assertEquals(jerseyVersion, ServletContainer.class.getPackage()
      .getImplementationVersion());
  }

  @Test
  public void testGetStargateVersionText() throws IOException {
    Response response = client.get("/version", Constants.MIMETYPE_TEXT);
    assertTrue(response.getCode() == 200);
    String body = Bytes.toString(response.getBody());
    assertTrue(body.length() > 0);
    assertTrue(body.contains(RESTServlet.VERSION_STRING));
    assertTrue(body.contains(System.getProperty("java.vm.vendor")));
    assertTrue(body.contains(System.getProperty("java.version")));
    assertTrue(body.contains(System.getProperty("java.vm.version")));
    assertTrue(body.contains(System.getProperty("os.name")));
    assertTrue(body.contains(System.getProperty("os.version")));
    assertTrue(body.contains(System.getProperty("os.arch")));
    assertTrue(body.contains(ServletContainer.class.getPackage()
      .getImplementationVersion()));
  }

  @Test
  public void testGetStargateVersionXML() throws IOException, JAXBException {
    Response response = client.get("/version", Constants.MIMETYPE_XML);
    assertTrue(response.getCode() == 200);
    VersionModel model = (VersionModel)
      context.createUnmarshaller().unmarshal(
        new ByteArrayInputStream(response.getBody()));
    validate(model);
    LOG.info("success retrieving Stargate version as XML");
  }

  @Test
  public void testGetStargateVersionJSON() throws IOException {
    Response response = client.get("/version", Constants.MIMETYPE_JSON);
    assertTrue(response.getCode() == 200);
  }

  @Test
  public void testGetStargateVersionPB() throws IOException {
    Response response = client.get("/version", Constants.MIMETYPE_PROTOBUF);
    assertTrue(response.getCode() == 200);
    VersionModel model = new VersionModel();
    model.getObjectFromMessage(response.getBody());
    validate(model);
    LOG.info("success retrieving Stargate version as protobuf");
  }

  @Test
  public void testGetStorageClusterVersionText() throws IOException {
    Response response = client.get("/version/cluster", 
      Constants.MIMETYPE_TEXT);
    assertTrue(response.getCode() == 200);
  }

  @Test
  public void testGetStorageClusterVersionXML() throws IOException,
      JAXBException {
    Response response = client.get("/version/cluster",Constants.MIMETYPE_XML);
    assertTrue(response.getCode() == 200);
    StorageClusterVersionModel clusterVersionModel = 
      (StorageClusterVersionModel)
        context.createUnmarshaller().unmarshal(
          new ByteArrayInputStream(response.getBody()));
    assertNotNull(clusterVersionModel);
    assertNotNull(clusterVersionModel.getVersion());
    LOG.info("success retrieving storage cluster version as XML");
  }

  @Test
  public void doTestGetStorageClusterVersionJSON() throws IOException {
    Response response = client.get("/version/cluster", Constants.MIMETYPE_JSON);
    assertTrue(response.getCode() == 200);
  }
}
