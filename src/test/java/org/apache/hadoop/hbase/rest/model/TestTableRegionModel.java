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

import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;

public class TestTableRegionModel extends TestCase {
  private static final String TABLE = "testtable";
  private static final byte[] START_KEY = Bytes.toBytes("abracadbra");
  private static final byte[] END_KEY = Bytes.toBytes("zzyzx");
  private static final long ID = 8731042424L;
  private static final String LOCATION = "testhost:9876";

  private static final String AS_XML =
    "<Region location=\"testhost:9876\"" +
      " endKey=\"enp5eng=\"" +
      " startKey=\"YWJyYWNhZGJyYQ==\"" +
      " id=\"8731042424\"" +
      " name=\"testtable,abracadbra,8731042424\"/>";

  private JAXBContext context;

  public TestTableRegionModel() throws JAXBException {
    super();
    context = JAXBContext.newInstance(TableRegionModel.class);
  }

  private TableRegionModel buildTestModel() {
    TableRegionModel model =
      new TableRegionModel(TABLE, ID, START_KEY, END_KEY, LOCATION);
    return model;
  }

  @SuppressWarnings("unused")
  private String toXML(TableRegionModel model) throws JAXBException {
    StringWriter writer = new StringWriter();
    context.createMarshaller().marshal(model, writer);
    return writer.toString();
  }

  private TableRegionModel fromXML(String xml) throws JAXBException {
    return (TableRegionModel)
      context.createUnmarshaller().unmarshal(new StringReader(xml));
  }

  private void checkModel(TableRegionModel model) {
    assertTrue(Bytes.equals(model.getStartKey(), START_KEY));
    assertTrue(Bytes.equals(model.getEndKey(), END_KEY));
    assertEquals(model.getId(), ID);
    assertEquals(model.getLocation(), LOCATION);
    assertEquals(model.getName(), 
      TABLE + "," + Bytes.toString(START_KEY) + "," + Long.toString(ID) +
      ".ad9860f031282c46ed431d7af8f94aca.");
  }

  public void testBuildModel() throws Exception {
    checkModel(buildTestModel());
  }

  public void testGetName() {
    TableRegionModel model = buildTestModel();
    String modelName = model.getName();
    HRegionInfo hri = new HRegionInfo(new HTableDescriptor(TABLE),
      START_KEY, END_KEY, false, ID);
    assertEquals(modelName, hri.getRegionNameAsString());
  }

  public void testSetName() {
    TableRegionModel model = buildTestModel();
    String name = model.getName();
    model.setName(name);
    assertEquals(name, model.getName());
  }

  public void testFromXML() throws Exception {
    checkModel(fromXML(AS_XML));
  }
}