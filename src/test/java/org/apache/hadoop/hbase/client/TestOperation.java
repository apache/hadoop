/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * Run tests that use the funtionality of the Operation superclass for 
 * Puts, Gets, Deletes, Scans, and MultiPuts.
 */
public class TestOperation {
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");

  private static ObjectMapper mapper = new ObjectMapper();

  /**
   * Test the client Operations' JSON encoding to ensure that produced JSON is 
   * parseable and that the details are present and not corrupted.
   * @throws IOException
   */
  @Test
  public void testOperationJSON()
      throws IOException {
    // produce a Scan Operation
    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    // get its JSON representation, and parse it
    String json = scan.toJSON();
    Map<String, Object> parsedJSON = mapper.readValue(json, HashMap.class);
    // check for the row
    assertEquals("startRow incorrect in Scan.toJSON()",
        Bytes.toStringBinary(ROW), parsedJSON.get("startRow"));
    // check for the family and the qualifier.
    List familyInfo = (List) ((Map) parsedJSON.get("families")).get(
        Bytes.toStringBinary(FAMILY));
    assertNotNull("Family absent in Scan.toJSON()", familyInfo);
    assertEquals("Qualifier absent in Scan.toJSON()", 1, familyInfo.size());
    assertEquals("Qualifier incorrect in Scan.toJSON()",
        Bytes.toStringBinary(QUALIFIER),
        familyInfo.get(0));

    // produce a Get Operation
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    // get its JSON representation, and parse it
    json = get.toJSON();
    parsedJSON = mapper.readValue(json, HashMap.class);
    // check for the row
    assertEquals("row incorrect in Get.toJSON()",
        Bytes.toStringBinary(ROW), parsedJSON.get("row"));
    // check for the family and the qualifier.
    familyInfo = (List) ((Map) parsedJSON.get("families")).get(
        Bytes.toStringBinary(FAMILY));
    assertNotNull("Family absent in Get.toJSON()", familyInfo);
    assertEquals("Qualifier absent in Get.toJSON()", 1, familyInfo.size());
    assertEquals("Qualifier incorrect in Get.toJSON()",
        Bytes.toStringBinary(QUALIFIER),
        familyInfo.get(0));

    // produce a Put operation
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    // get its JSON representation, and parse it
    json = put.toJSON();
    parsedJSON = mapper.readValue(json, HashMap.class);
    // check for the row
    assertEquals("row absent in Put.toJSON()",
        Bytes.toStringBinary(ROW), parsedJSON.get("row"));
    // check for the family and the qualifier.
    familyInfo = (List) ((Map) parsedJSON.get("families")).get(
        Bytes.toStringBinary(FAMILY));
    assertNotNull("Family absent in Put.toJSON()", familyInfo);
    assertEquals("KeyValue absent in Put.toJSON()", 1, familyInfo.size());
    Map kvMap = (Map) familyInfo.get(0);
    assertEquals("Qualifier incorrect in Put.toJSON()",
        Bytes.toStringBinary(QUALIFIER),
        kvMap.get("qualifier"));
    assertEquals("Value length incorrect in Put.toJSON()", 
        VALUE.length, kvMap.get("vlen"));

    // produce a Delete operation
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER);
    // get its JSON representation, and parse it
    json = delete.toJSON();
    parsedJSON = mapper.readValue(json, HashMap.class);
    // check for the row
    assertEquals("row absent in Delete.toJSON()",
        Bytes.toStringBinary(ROW), parsedJSON.get("row"));
    // check for the family and the qualifier.
    familyInfo = (List) ((Map) parsedJSON.get("families")).get(
        Bytes.toStringBinary(FAMILY));
    assertNotNull("Family absent in Delete.toJSON()", familyInfo);
    assertEquals("KeyValue absent in Delete.toJSON()", 1, familyInfo.size());
    kvMap = (Map) familyInfo.get(0);
    assertEquals("Qualifier incorrect in Delete.toJSON()", 
        Bytes.toStringBinary(QUALIFIER), kvMap.get("qualifier"));
  }
}
