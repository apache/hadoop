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

package org.apache.hadoop.hbase.rest.model;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;

public class TestTableInfoModel extends TestCase {
  private static final String TABLE = "testtable";
  private static final byte[] START_KEY = Bytes.toBytes("abracadbra");
  private static final byte[] END_KEY = Bytes.toBytes("zzyzx");
  private static final long ID = 8731042424L;
  private static final String LOCATION = "testhost:9876";
  
  private static final String AS_XML =
    "<TableInfo name=\"testtable\">" +
      "<Region location=\"testhost:9876\"" +
        " endKey=\"enp5eng=\"" +
        " startKey=\"YWJyYWNhZGJyYQ==\"" +
        " id=\"8731042424\"" +
        " name=\"testtable,abracadbra,8731042424\"/>" +
    "</TableInfo>";

  private static final String AS_PB = 
    "Cgl0ZXN0dGFibGUSSQofdGVzdHRhYmxlLGFicmFjYWRicmEsODczMTA0MjQyNBIKYWJyYWNhZGJy" +
    "YRoFenp5engg+MSkwyAqDXRlc3Rob3N0Ojk4NzY=";

  private JAXBContext context;

  public TestTableInfoModel() throws JAXBException {
    super();
    context = JAXBContext.newInstance(
        TableInfoModel.class,
        TableRegionModel.class);
  }

  private TableInfoModel buildTestModel() {
    TableInfoModel model = new TableInfoModel();
    model.setName(TABLE);
    model.add(new TableRegionModel(TABLE, ID, START_KEY, END_KEY, LOCATION));
    return model;
  }

  @SuppressWarnings("unused")
  private String toXML(TableInfoModel model) throws JAXBException {
    StringWriter writer = new StringWriter();
    context.createMarshaller().marshal(model, writer);
    return writer.toString();
  }

  private TableInfoModel fromXML(String xml) throws JAXBException {
    return (TableInfoModel)
      context.createUnmarshaller().unmarshal(new StringReader(xml));
  }

  @SuppressWarnings("unused")
  private byte[] toPB(TableInfoModel model) {
    return model.createProtobufOutput();
  }

  private TableInfoModel fromPB(String pb) throws IOException {
    return (TableInfoModel) 
      new TableInfoModel().getObjectFromMessage(Base64.decode(AS_PB));
  }

  private void checkModel(TableInfoModel model) {
    assertEquals(model.getName(), TABLE);
    Iterator<TableRegionModel> regions = model.getRegions().iterator();
    TableRegionModel region = regions.next();
    assertTrue(Bytes.equals(region.getStartKey(), START_KEY));
    assertTrue(Bytes.equals(region.getEndKey(), END_KEY));
    assertEquals(region.getId(), ID);
    assertEquals(region.getLocation(), LOCATION);
    assertFalse(regions.hasNext());
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
