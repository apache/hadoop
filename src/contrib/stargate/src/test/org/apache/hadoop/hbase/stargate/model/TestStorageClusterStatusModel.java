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

package org.apache.hadoop.hbase.stargate.model;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;

public class TestStorageClusterStatusModel extends TestCase {

  private static final String AS_XML =
    "<ClusterStatus requests=\"0\" regions=\"2\" averageLoad=\"1.0\">" +
      "<LiveNodes>" +
        "<Node startCode=\"1245219839331\" requests=\"0\" name=\"test1\">" +
          "<Region name=\"LVJPT1QtLCww\"/>" + 
        "</Node>" +
        "<Node startCode=\"1245239331198\" requests=\"0\" name=\"test2\">" +
          "<Region name=\"Lk1FVEEuLCwxMjQ2MDAwMDQzNzI0\"/>" +
        "</Node>" +
      "</LiveNodes>" +
      "<DeadNodes/>" +
    "</ClusterStatus>";

  private static final String AS_PB = 
    "ChsKBXRlc3QxEAAaCS1ST09ULSwsMCDjuovnniQKJwoFdGVzdDIQABoVLk1FVEEuLCwxMjQ2MDAw" +
    "MDQzNzI0IP6SsfCeJBgCIAApAAAAAAAA8D8=";

  private JAXBContext context;

  public TestStorageClusterStatusModel() throws JAXBException {
    super();
    context = JAXBContext.newInstance(StorageClusterStatusModel.class);
  }

  private StorageClusterStatusModel buildTestModel() {
    StorageClusterStatusModel model = new StorageClusterStatusModel();
    model.setRegions(2);
    model.setRequests(0);
    model.setAverageLoad(1.0);
    model.addLiveNode("test1", 1245219839331L)
      .addRegion(Bytes.toBytes("-ROOT-,,0"));
    model.addLiveNode("test2", 1245239331198L)
      .addRegion(Bytes.toBytes(".META.,,1246000043724"));
    return model;
  }

  @SuppressWarnings("unused")
  private String toXML(StorageClusterStatusModel model) throws JAXBException {
    StringWriter writer = new StringWriter();
    context.createMarshaller().marshal(model, writer);
    return writer.toString();
  }

  private StorageClusterStatusModel fromXML(String xml) throws JAXBException {
    return (StorageClusterStatusModel)
      context.createUnmarshaller().unmarshal(new StringReader(xml));
  }

  @SuppressWarnings("unused")
  private byte[] toPB(StorageClusterStatusModel model) {
    return model.createProtobufOutput();
  }

  private StorageClusterStatusModel fromPB(String pb) throws IOException {
    return (StorageClusterStatusModel) 
      new StorageClusterStatusModel().getObjectFromMessage(Base64.decode(AS_PB));
  }

  private void checkModel(StorageClusterStatusModel model) {
    assertEquals(model.getRegions(), 2);
    assertEquals(model.getRequests(), 0);
    assertEquals(model.getAverageLoad(), 1.0);
    Iterator<StorageClusterStatusModel.Node> nodes =
      model.getLiveNodes().iterator();
    StorageClusterStatusModel.Node node = nodes.next();
    assertEquals(node.getName(), "test1");
    assertEquals(node.getStartCode(), 1245219839331L);
    Iterator<StorageClusterStatusModel.Node.Region> regions = 
      node.getRegions().iterator();
    StorageClusterStatusModel.Node.Region region = regions.next();
    assertTrue(Bytes.toString(region.getName()).equals("-ROOT-,,0"));
    assertFalse(regions.hasNext());
    node = nodes.next();
    assertEquals(node.getName(), "test2");
    assertEquals(node.getStartCode(), 1245239331198L);
    regions = node.getRegions().iterator();
    region = regions.next();
    assertEquals(Bytes.toString(region.getName()), ".META.,,1246000043724");
    assertFalse(regions.hasNext());
    assertFalse(nodes.hasNext());
  }

  public void testBuildModel() throws Exception {
    checkModel(buildTestModel());
  }

  public void testFromXML() throws Exception {
    checkModel(fromXML(AS_XML));
  }

  public void testFromPB() throws Exception {
    checkModel(fromPB(AS_PB));
  }
}
