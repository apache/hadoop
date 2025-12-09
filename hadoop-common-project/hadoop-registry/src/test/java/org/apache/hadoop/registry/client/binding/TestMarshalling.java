/*
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

package org.apache.hadoop.registry.client.binding;

import org.apache.hadoop.registry.RegistryTestHelper;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test record marshalling
 */
@Timeout(10)
public class TestMarshalling extends RegistryTestHelper {
  private static final Logger
      LOG = LoggerFactory.getLogger(TestMarshalling.class);

  private static RegistryUtils.ServiceRecordMarshal marshal;

  @BeforeAll
  public static void setupClass() {
    marshal = new RegistryUtils.ServiceRecordMarshal();
  }

  @Test
  public void testRoundTrip() throws Throwable {
    String persistence = PersistencePolicies.PERMANENT;
    ServiceRecord record = createRecord(persistence);
    record.set("customkey", "customvalue");
    record.set("customkey2", "customvalue2");
    RegistryTypeUtils.validateServiceRecord("", record);
    LOG.info(marshal.toJson(record));
    byte[] bytes = marshal.toBytes(record);
    ServiceRecord r2 = marshal.fromBytes("", bytes);
    assertMatches(record, r2);
    RegistryTypeUtils.validateServiceRecord("", r2);
  }


  @Test
  public void testUnmarshallNoData() throws Throwable {
    assertThrows(NoRecordException.class, () -> {
      marshal.fromBytes("src", new byte[]{});
    });
  }

  @Test
  public void testUnmarshallNotEnoughData() throws Throwable {
    // this is nominally JSON -but without the service record header
    assertThrows(NoRecordException.class, () -> {
      marshal.fromBytes("src", new byte[]{'{', '}'}, ServiceRecord.RECORD_TYPE);
    });
  }

  @Test
  public void testUnmarshallNoBody() throws Throwable {
    assertThrows(InvalidRecordException.class, () -> {
      byte[] bytes = "this is not valid JSON at all and should fail".getBytes();
      marshal.fromBytes("src", bytes);
    });
  }

  @Test
  public void testUnmarshallWrongType() throws Throwable {
    assertThrows(InvalidRecordException.class, () -> {
      byte[] bytes = "{'type':''}".getBytes();
      ServiceRecord serviceRecord = marshal.fromBytes("marshalling", bytes);
      RegistryTypeUtils.validateServiceRecord("validating", serviceRecord);
    });
  }

  @Test
  public void testUnmarshallWrongLongType() throws Throwable {
    assertThrows(NoRecordException.class, () -> {
      ServiceRecord record = new ServiceRecord();
      record.type = "ThisRecordHasALongButNonMatchingType";
      byte[] bytes = marshal.toBytes(record);
      ServiceRecord serviceRecord = marshal.fromBytes("marshalling",
          bytes, ServiceRecord.RECORD_TYPE);
    });
  }

  @Test
  public void testUnmarshallNoType() throws Throwable {
    assertThrows(NoRecordException.class, () -> {
      ServiceRecord record = new ServiceRecord();
      record.type = "NoRecord";
      byte[] bytes = marshal.toBytes(record);
      ServiceRecord serviceRecord = marshal.fromBytes("marshalling",
          bytes, ServiceRecord.RECORD_TYPE);
    });
  }

  @Test
  public void testRecordValidationWrongType() throws Throwable {
    assertThrows(InvalidRecordException.class, () -> {
      ServiceRecord record = new ServiceRecord();
      record.type = "NotAServiceRecordType";
      RegistryTypeUtils.validateServiceRecord("validating", record);
    });
  }

  @Test
  public void testUnknownFieldsRoundTrip() throws Throwable {
    ServiceRecord record =
        createRecord(PersistencePolicies.APPLICATION_ATTEMPT);
    record.set("key", "value");
    record.set("intval", "2");
    assertEquals("value", record.get("key"));
    assertEquals("2", record.get("intval"));
    assertNull(record.get("null"));
    assertEquals("defval", record.get("null", "defval"));
    byte[] bytes = marshal.toBytes(record);
    ServiceRecord r2 = marshal.fromBytes("", bytes);
    assertEquals("value", r2.get("key"));
    assertEquals("2", r2.get("intval"));
  }

  @Test
  public void testFieldPropagationInCopy() throws Throwable {
    ServiceRecord record =
        createRecord(PersistencePolicies.APPLICATION_ATTEMPT);
    record.set("key", "value");
    record.set("intval", "2");
    ServiceRecord that = new ServiceRecord(record);
    assertMatches(record, that);
  }

}
