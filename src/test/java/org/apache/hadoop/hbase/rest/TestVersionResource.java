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
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.StorageClusterVersionModel;
import org.apache.hadoop.hbase.rest.model.VersionModel;
import org.apache.hadoop.hbase.util.Bytes;

import com.sun.jersey.spi.container.servlet.ServletContainer;

public class TestVersionResource extends HBaseRESTClusterTestBase {
  static final Log LOG = LogFactory.getLog(TestVersionResource.class);

  Client client;
  JAXBContext context;

  public TestVersionResource() throws JAXBException {
    super();
    context = JAXBContext.newInstance(
      VersionModel.class,
      StorageClusterVersionModel.class);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    client = new Client(new Cluster().add("localhost", testServletPort));
  }

  @Override
  protected void tearDown() throws Exception {
    client.shutdown();
    super.tearDown();
  }

  void validate(VersionModel model) {
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

  void doTestGetStargateVersionText() throws IOException {
    Response response = client.get("/version", MIMETYPE_TEXT);
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

  void doTestGetStargateVersionXML() throws IOException, JAXBException {
    Response response = client.get("/version", MIMETYPE_XML);
    assertTrue(response.getCode() == 200);
    VersionModel model = (VersionModel)
      context.createUnmarshaller().unmarshal(
        new ByteArrayInputStream(response.getBody()));
    validate(model);
    LOG.info("success retrieving Stargate version as XML");
  }

  void doTestGetStargateVersionJSON() throws IOException {
    Response response = client.get("/version", MIMETYPE_JSON);
    assertTrue(response.getCode() == 200);
  }

  void doTestGetStargateVersionPB() throws IOException {
    Response response = client.get("/version", MIMETYPE_PROTOBUF);
    assertTrue(response.getCode() == 200);
    VersionModel model = new VersionModel();
    model.getObjectFromMessage(response.getBody());
    validate(model);
    LOG.info("success retrieving Stargate version as protobuf");
  }

  void doTestGetStorageClusterVersionText() throws IOException {
    Response response = client.get("/version/cluster", MIMETYPE_TEXT);
    assertTrue(response.getCode() == 200);
  }

  void doTestGetStorageClusterVersionXML() throws IOException,
      JAXBException {
    Response response = client.get("/version/cluster", MIMETYPE_XML);
    assertTrue(response.getCode() == 200);
    StorageClusterVersionModel clusterVersionModel = 
      (StorageClusterVersionModel)
        context.createUnmarshaller().unmarshal(
          new ByteArrayInputStream(response.getBody()));
    assertNotNull(clusterVersionModel);
    assertNotNull(clusterVersionModel.getVersion());
    LOG.info("success retrieving storage cluster version as XML");
  }

  void doTestGetStorageClusterVersionJSON() throws IOException {
    Response response = client.get("/version/cluster", MIMETYPE_JSON);
    assertTrue(response.getCode() == 200);
  }

  public void testVersionResource() throws Exception {
    doTestGetStargateVersionText();
    doTestGetStargateVersionXML();
    doTestGetStargateVersionJSON();
    doTestGetStargateVersionPB();
    doTestGetStorageClusterVersionText();
    doTestGetStorageClusterVersionXML();
    doTestGetStorageClusterVersionJSON();
  }
}
