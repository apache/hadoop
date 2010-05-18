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

import junit.framework.TestCase;

public class TestColumnSchemaModel extends TestCase {

  protected static final String COLUMN_NAME = "testcolumn";
  protected static final boolean BLOCKCACHE = true;
  protected static final int BLOCKSIZE = 16384;
  protected static final boolean BLOOMFILTER = false;
  protected static final String COMPRESSION = "GZ";
  protected static final boolean IN_MEMORY = false;
  protected static final int TTL = 86400;
  protected static final int VERSIONS = 1;

  protected static final String AS_XML =
    "<ColumnSchema name=\"testcolumn\"" +
      " BLOCKSIZE=\"16384\"" +
      " BLOOMFILTER=\"false\"" +
      " BLOCKCACHE=\"true\"" +
      " COMPRESSION=\"GZ\"" +
      " VERSIONS=\"1\"" +
      " TTL=\"86400\"" +
      " IN_MEMORY=\"false\"/>";

  private JAXBContext context;

  public TestColumnSchemaModel() throws JAXBException {
    super();
    context = JAXBContext.newInstance(ColumnSchemaModel.class);
  }

  protected static ColumnSchemaModel buildTestModel() {
    ColumnSchemaModel model = new ColumnSchemaModel();
    model.setName(COLUMN_NAME);
    model.__setBlockcache(BLOCKCACHE);
    model.__setBlocksize(BLOCKSIZE);
    model.__setBloomfilter(BLOOMFILTER);
    model.__setCompression(COMPRESSION);
    model.__setInMemory(IN_MEMORY);
    model.__setTTL(TTL);
    model.__setVersions(VERSIONS);
    return model;
  }

  @SuppressWarnings("unused")
  private String toXML(ColumnSchemaModel model) throws JAXBException {
    StringWriter writer = new StringWriter();
    context.createMarshaller().marshal(model, writer);
    return writer.toString();
  }

  private ColumnSchemaModel fromXML(String xml) throws JAXBException {
    return (ColumnSchemaModel)
      context.createUnmarshaller().unmarshal(new StringReader(xml));
  }

  protected static void checkModel(ColumnSchemaModel model) {
    assertEquals(model.getName(), COLUMN_NAME);
    assertEquals(model.__getBlockcache(), BLOCKCACHE);
    assertEquals(model.__getBlocksize(), BLOCKSIZE);
    assertEquals(model.__getBloomfilter(), BLOOMFILTER);
    assertTrue(model.__getCompression().equalsIgnoreCase(COMPRESSION));
    assertEquals(model.__getInMemory(), IN_MEMORY);
    assertEquals(model.__getTTL(), TTL);
    assertEquals(model.__getVersions(), VERSIONS);
  }

  public void testBuildModel() throws Exception {
    checkModel(buildTestModel());
  }

  public void testFromXML() throws Exception {
    checkModel(fromXML(AS_XML));
  }
}
