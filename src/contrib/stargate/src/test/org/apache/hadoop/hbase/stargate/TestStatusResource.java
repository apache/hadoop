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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.hadoop.hbase.stargate.client.Client;
import org.apache.hadoop.hbase.stargate.client.Cluster;
import org.apache.hadoop.hbase.stargate.client.Response;
import org.apache.hadoop.hbase.stargate.model.StorageClusterStatusModel;
import org.apache.hadoop.hbase.util.Bytes;

public class TestStatusResource extends MiniClusterTestCase {
  private static final byte[] ROOT_REGION_NAME = Bytes.toBytes("-ROOT-,,0");
  private static final byte[] META_REGION_NAME = Bytes.toBytes(".META.,,1");

  private Client client;
  private JAXBContext context;
  
  public TestStatusResource() throws JAXBException {
    super();
    context = JAXBContext.newInstance(
        StorageClusterStatusModel.class);
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

  private void validate(StorageClusterStatusModel model) {
    assertNotNull(model);
    assertTrue(model.getRegions() >= 2);
    assertTrue(model.getRequests() >= 0);
    // assumes minicluster with two regionservers
    assertTrue(model.getAverageLoad() >= 1.0);
    assertNotNull(model.getLiveNodes());
    assertNotNull(model.getDeadNodes());
    assertFalse(model.getLiveNodes().isEmpty());
    boolean foundRoot = false, foundMeta = false;
    for (StorageClusterStatusModel.Node node: model.getLiveNodes()) {
      assertNotNull(node.getName());
      assertTrue(node.getStartCode() > 0L);
      assertTrue(node.getRequests() >= 0);
      assertFalse(node.getRegions().isEmpty());
      for (StorageClusterStatusModel.Node.Region region: node.getRegions()) {
        if (Bytes.equals(region.getName(), ROOT_REGION_NAME)) {
          foundRoot = true;
        } else if (Bytes.equals(region.getName(), META_REGION_NAME)) {
          foundMeta = true;
        }
      }
    }
    assertTrue(foundRoot);
    assertTrue(foundMeta);
  }

  public void testGetClusterStatusXML() throws IOException, JAXBException {
    Response response = client.get(Constants.PATH_STATUS_CLUSTER, MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    StorageClusterStatusModel model = (StorageClusterStatusModel)
      context.createUnmarshaller().unmarshal(
        new ByteArrayInputStream(response.getBody()));
    validate(model);
  }
  
  public void testGetClusterStatusPB() throws IOException {
    Response response =
      client.get(Constants.PATH_STATUS_CLUSTER, MIMETYPE_PROTOBUF);
    assertEquals(response.getCode(), 200);
    StorageClusterStatusModel model = new StorageClusterStatusModel();
    model.getObjectFromMessage(response.getBody());
    validate(model);
  }
}
