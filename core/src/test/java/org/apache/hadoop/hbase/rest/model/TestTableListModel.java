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

import junit.framework.TestCase;

public class TestTableListModel extends TestCase {
  private static final String TABLE1 = "table1";
  private static final String TABLE2 = "table2";
  private static final String TABLE3 = "table3";
  
  private static final String AS_XML =
    "<TableList><table name=\"table1\"/><table name=\"table2\"/>" +
      "<table name=\"table3\"/></TableList>";

  private static final String AS_PB = "CgZ0YWJsZTEKBnRhYmxlMgoGdGFibGUz";

  private JAXBContext context;

  public TestTableListModel() throws JAXBException {
    super();
    context = JAXBContext.newInstance(
        TableListModel.class,
        TableModel.class);
  }

  private TableListModel buildTestModel() {
    TableListModel model = new TableListModel();
    model.add(new TableModel(TABLE1));
    model.add(new TableModel(TABLE2));
    model.add(new TableModel(TABLE3));
    return model;
  }

  @SuppressWarnings("unused")
  private String toXML(TableListModel model) throws JAXBException {
    StringWriter writer = new StringWriter();
    context.createMarshaller().marshal(model, writer);
    return writer.toString();
  }

  private TableListModel fromXML(String xml) throws JAXBException {
    return (TableListModel)
      context.createUnmarshaller().unmarshal(new StringReader(xml));
  }

  @SuppressWarnings("unused")
  private byte[] toPB(TableListModel model) {
    return model.createProtobufOutput();
  }

  private TableListModel fromPB(String pb) throws IOException {
    return (TableListModel) 
      new TableListModel().getObjectFromMessage(Base64.decode(AS_PB));
  }

  private void checkModel(TableListModel model) {
    Iterator<TableModel> tables = model.getTables().iterator();
    TableModel table = tables.next();
    assertEquals(table.getName(), TABLE1);
    table = tables.next();
    assertEquals(table.getName(), TABLE2);
    table = tables.next();
    assertEquals(table.getName(), TABLE3);
    assertFalse(tables.hasNext());
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
