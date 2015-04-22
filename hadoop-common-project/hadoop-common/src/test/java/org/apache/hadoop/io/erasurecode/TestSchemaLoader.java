/**
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
package org.apache.hadoop.io.erasurecode;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

import org.junit.Test;

public class TestSchemaLoader {

  final static String TEST_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).getAbsolutePath();

  final static String SCHEMA_FILE = new File(TEST_DIR, "test-ecschema")
      .getAbsolutePath();

  @Test
  public void testLoadSchema() throws Exception {
    PrintWriter out = new PrintWriter(new FileWriter(SCHEMA_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<schemas>");
    out.println("  <schema name=\"RSk6m3\">");
    out.println("    <k>6</k>");
    out.println("    <m>3</m>");
    out.println("    <codec>RS</codec>");
    out.println("  </schema>");
    out.println("  <schema name=\"RSk10m4\">");
    out.println("    <k>10</k>");
    out.println("    <m>4</m>");
    out.println("    <codec>RS</codec>");
    out.println("  </schema>");
    out.println("</schemas>");
    out.close();

    SchemaLoader schemaLoader = new SchemaLoader();
    List<ECSchema> schemas = schemaLoader.loadSchema(SCHEMA_FILE);

    assertEquals(2, schemas.size());

    ECSchema schema1 = schemas.get(0);
    assertEquals("RSk6m3", schema1.getSchemaName());
    assertEquals(0, schema1.getExtraOptions().size());
    assertEquals(6, schema1.getNumDataUnits());
    assertEquals(3, schema1.getNumParityUnits());
    assertEquals("RS", schema1.getCodecName());

    ECSchema schema2 = schemas.get(1);
    assertEquals("RSk10m4", schema2.getSchemaName());
    assertEquals(0, schema2.getExtraOptions().size());
    assertEquals(10, schema2.getNumDataUnits());
    assertEquals(4, schema2.getNumParityUnits());
    assertEquals("RS", schema2.getCodecName());
  }
}
