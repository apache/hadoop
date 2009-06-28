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

import junit.framework.TestCase;

public class TestTableSchemaModel extends TestCase {

  public static final String TABLE_NAME = "testTable";
  private static final boolean IN_MEMORY = false;
  private static final boolean IS_META = false;
  private static final boolean IS_ROOT = false;
  private static final boolean READONLY = false;

  private static final String AS_XML =
    "<TableSchema name=\"testTable\"" +
      " IS_META=\"false\"" +
      " IS_ROOT=\"false\"" +
      " READONLY=\"false\"" +
      " IN_MEMORY=\"false\">" +
      TestColumnSchemaModel.AS_XML + 
    "</TableSchema>";

  private static final String AS_PB = 
    "Cgl0ZXN0VGFibGUSEAoHSVNfTUVUQRIFZmFsc2USEAoHSVNfUk9PVBIFZmFsc2USEQoIUkVBRE9O" +
    "TFkSBWZhbHNlEhIKCUlOX01FTU9SWRIFZmFsc2UamAEKCnRlc3Rjb2x1bW4SEgoJQkxPQ0tTSVpF" +
    "EgUxNjM4NBIUCgtCTE9PTUZJTFRFUhIFZmFsc2USEgoKQkxPQ0tDQUNIRRIEdHJ1ZRIRCgtDT01Q" +
    "UkVTU0lPThICZ3oSDQoIVkVSU0lPTlMSATESDAoDVFRMEgU4NjQwMBISCglJTl9NRU1PUlkSBWZh" +
    "bHNlGICjBSABKgJneiAAKAA=";

  private JAXBContext context;

  public TestTableSchemaModel() throws JAXBException {
    super();
    context = JAXBContext.newInstance(
        ColumnSchemaModel.class,
        TableSchemaModel.class);
  }

  public static TableSchemaModel buildTestModel() {
    return buildTestModel(TABLE_NAME);
  }

  public static TableSchemaModel buildTestModel(String name) {
    TableSchemaModel model = new TableSchemaModel();
    model.setName(name);
    model.__setInMemory(IN_MEMORY);
    model.__setIsMeta(IS_META);
    model.__setIsRoot(IS_ROOT);
    model.__setReadOnly(READONLY);
    model.addColumnFamily(TestColumnSchemaModel.buildTestModel());
    return model;
  }

  @SuppressWarnings("unused")
  private String toXML(TableSchemaModel model) throws JAXBException {
    StringWriter writer = new StringWriter();
    context.createMarshaller().marshal(model, writer);
    return writer.toString();
  }

  private TableSchemaModel fromXML(String xml) throws JAXBException {
    return (TableSchemaModel)
      context.createUnmarshaller().unmarshal(new StringReader(xml));
  }

  @SuppressWarnings("unused")
  private byte[] toPB(TableSchemaModel model) {
    return model.createProtobufOutput();
  }

  private TableSchemaModel fromPB(String pb) throws IOException {
    return (TableSchemaModel) 
      new TableSchemaModel().getObjectFromMessage(Base64.decode(AS_PB));
  }

  public static void checkModel(TableSchemaModel model) {
    checkModel(model, TABLE_NAME);
  }

  public static void checkModel(TableSchemaModel model, String tableName) {
    assertEquals(model.getName(), tableName);
    assertEquals(model.__getInMemory(), IN_MEMORY);
    assertEquals(model.__getIsMeta(), IS_META);
    assertEquals(model.__getIsRoot(), IS_ROOT);
    assertEquals(model.__getReadOnly(), READONLY);
    Iterator<ColumnSchemaModel> families = model.getColumns().iterator();
    assertTrue(families.hasNext());
    ColumnSchemaModel family = families.next();
    TestColumnSchemaModel.checkModel(family);
    assertFalse(families.hasNext());
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
