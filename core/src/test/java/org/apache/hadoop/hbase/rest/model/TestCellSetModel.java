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

public class TestCellSetModel extends TestCase {

  private static final byte[] ROW1 = Bytes.toBytes("testrow1");
  private static final byte[] COLUMN1 = Bytes.toBytes("testcolumn1");
  private static final byte[] VALUE1 = Bytes.toBytes("testvalue1");
  private static final long TIMESTAMP1 = 1245219839331L;
  private static final byte[] ROW2 = Bytes.toBytes("testrow1");
  private static final byte[] COLUMN2 = Bytes.toBytes("testcolumn2");
  private static final byte[] VALUE2 = Bytes.toBytes("testvalue2");
  private static final long TIMESTAMP2 = 1245239813319L;
  private static final byte[] COLUMN3 = Bytes.toBytes("testcolumn3");
  private static final byte[] VALUE3 = Bytes.toBytes("testvalue3");
  private static final long TIMESTAMP3 = 1245393318192L;

  private static final String AS_XML =
    "<CellSet>" + 
      "<Row key=\"dGVzdHJvdzE=\">" + 
        "<Cell timestamp=\"1245219839331\" column=\"dGVzdGNvbHVtbjE=\">" + 
          "dGVzdHZhbHVlMQ==</Cell>" + 
        "</Row>" + 
      "<Row key=\"dGVzdHJvdzE=\">" + 
        "<Cell timestamp=\"1245239813319\" column=\"dGVzdGNvbHVtbjI=\">" +
          "dGVzdHZhbHVlMg==</Cell>" + 
        "<Cell timestamp=\"1245393318192\" column=\"dGVzdGNvbHVtbjM=\">" + 
          "dGVzdHZhbHVlMw==</Cell>" + 
        "</Row>" +
      "</CellSet>";

  private static final String AS_PB = 
    "CiwKCHRlc3Ryb3cxEiASC3Rlc3Rjb2x1bW4xGOO6i+eeJCIKdGVzdHZhbHVlMQpOCgh0ZXN0cm93" +
    "MRIgEgt0ZXN0Y29sdW1uMhjHyc7wniQiCnRlc3R2YWx1ZTISIBILdGVzdGNvbHVtbjMYsOLnuZ8k" +
    "Igp0ZXN0dmFsdWUz";

  private JAXBContext context;

  public TestCellSetModel() throws JAXBException {
    super();
    context = JAXBContext.newInstance(
        CellModel.class,
        CellSetModel.class,
        RowModel.class);
  }

  private CellSetModel buildTestModel() {
    CellSetModel model = new CellSetModel();
    RowModel row;
    row = new RowModel();
    row.setKey(ROW1);
    row.addCell(new CellModel(COLUMN1, TIMESTAMP1, VALUE1));
    model.addRow(row);
    row = new RowModel();
    row.setKey(ROW2);
    row.addCell(new CellModel(COLUMN2, TIMESTAMP2, VALUE2));
    row.addCell(new CellModel(COLUMN3, TIMESTAMP3, VALUE3));
    model.addRow(row);
    return model;
  }

  @SuppressWarnings("unused")
  private String toXML(CellSetModel model) throws JAXBException {
    StringWriter writer = new StringWriter();
    context.createMarshaller().marshal(model, writer);
    return writer.toString();
  }

  private CellSetModel fromXML(String xml) throws JAXBException {
    return (CellSetModel)
      context.createUnmarshaller().unmarshal(new StringReader(xml));
  }

  @SuppressWarnings("unused")
  private byte[] toPB(CellSetModel model) {
    return model.createProtobufOutput();
  }

  private CellSetModel fromPB(String pb) throws IOException {
    return (CellSetModel) 
      new CellSetModel().getObjectFromMessage(Base64.decode(AS_PB));
  }

  private void checkModel(CellSetModel model) {
    Iterator<RowModel> rows = model.getRows().iterator();
    RowModel row = rows.next();
    assertTrue(Bytes.equals(ROW1, row.getKey()));
    Iterator<CellModel> cells = row.getCells().iterator();
    CellModel cell = cells.next();
    assertTrue(Bytes.equals(COLUMN1, cell.getColumn()));
    assertTrue(Bytes.equals(VALUE1, cell.getValue()));
    assertTrue(cell.hasUserTimestamp());
    assertEquals(cell.getTimestamp(), TIMESTAMP1);
    assertFalse(cells.hasNext());
    row = rows.next();
    assertTrue(Bytes.equals(ROW2, row.getKey()));
    cells = row.getCells().iterator();
    cell = cells.next();
    assertTrue(Bytes.equals(COLUMN2, cell.getColumn()));
    assertTrue(Bytes.equals(VALUE2, cell.getValue()));
    assertTrue(cell.hasUserTimestamp());
    assertEquals(cell.getTimestamp(), TIMESTAMP2);
    cell = cells.next();
    assertTrue(Bytes.equals(COLUMN3, cell.getColumn()));
    assertTrue(Bytes.equals(VALUE3, cell.getValue()));
    assertTrue(cell.hasUserTimestamp());
    assertEquals(cell.getTimestamp(), TIMESTAMP3);
    assertFalse(cells.hasNext());
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
