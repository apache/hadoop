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

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Test;

/**
 * Unit tests for key converters for various tables' row keys.
 *
 */
public class TestKeyConverters {

  @Test
  public void testAppIdKeyConverter() {
    AppIdKeyConverter appIdKeyConverter = new AppIdKeyConverter();
    long currentTs = System.currentTimeMillis();
    ApplicationId appId1 = ApplicationId.newInstance(currentTs, 1);
    ApplicationId appId2 = ApplicationId.newInstance(currentTs, 2);
    ApplicationId appId3 = ApplicationId.newInstance(currentTs + 300, 1);
    String appIdStr1 = appId1.toString();
    String appIdStr2 = appId2.toString();
    String appIdStr3 = appId3.toString();
    byte[] appIdBytes1 = appIdKeyConverter.encode(appIdStr1);
    byte[] appIdBytes2 = appIdKeyConverter.encode(appIdStr2);
    byte[] appIdBytes3 = appIdKeyConverter.encode(appIdStr3);
    // App ids' should be encoded in a manner wherein descending order
    // is maintained.
    assertTrue(
        "Ordering of app ids' is incorrect",
        Bytes.compareTo(appIdBytes1, appIdBytes2) > 0
            && Bytes.compareTo(appIdBytes1, appIdBytes3) > 0
            && Bytes.compareTo(appIdBytes2, appIdBytes3) > 0);
    String decodedAppId1 = appIdKeyConverter.decode(appIdBytes1);
    String decodedAppId2 = appIdKeyConverter.decode(appIdBytes2);
    String decodedAppId3 = appIdKeyConverter.decode(appIdBytes3);
    assertTrue("Decoded app id is not same as the app id encoded",
        appIdStr1.equals(decodedAppId1));
    assertTrue("Decoded app id is not same as the app id encoded",
        appIdStr2.equals(decodedAppId2));
    assertTrue("Decoded app id is not same as the app id encoded",
        appIdStr3.equals(decodedAppId3));
  }

  @Test
  public void testEventColumnNameConverter() {
    String eventId = "=foo_=eve=nt=";
    byte[] valSepBytes = Bytes.toBytes(Separator.VALUES.getValue());
    byte[] maxByteArr =
        Bytes.createMaxByteArray(Bytes.SIZEOF_LONG - valSepBytes.length);
    byte[] ts = Bytes.add(valSepBytes, maxByteArr);
    Long eventTs = Bytes.toLong(ts);
    byte[] byteEventColName =
        new EventColumnName(eventId, eventTs, null).getColumnQualifier();
    KeyConverter<EventColumnName> eventColumnNameConverter =
        new EventColumnNameConverter();
    EventColumnName eventColName =
        eventColumnNameConverter.decode(byteEventColName);
    assertEquals(eventId, eventColName.getId());
    assertEquals(eventTs, eventColName.getTimestamp());
    assertNull(eventColName.getInfoKey());

    String infoKey = "f=oo_event_in=fo=_key";
    byteEventColName =
        new EventColumnName(eventId, eventTs, infoKey).getColumnQualifier();
    eventColName = eventColumnNameConverter.decode(byteEventColName);
    assertEquals(eventId, eventColName.getId());
    assertEquals(eventTs, eventColName.getTimestamp());
    assertEquals(infoKey, eventColName.getInfoKey());
  }

  @Test
  public void testLongKeyConverter() {
    LongKeyConverter longKeyConverter = new LongKeyConverter();
    confirmLongKeyConverter(longKeyConverter, Long.MIN_VALUE);
    confirmLongKeyConverter(longKeyConverter, -1234567890L);
    confirmLongKeyConverter(longKeyConverter, -128L);
    confirmLongKeyConverter(longKeyConverter, -127L);
    confirmLongKeyConverter(longKeyConverter, -1L);
    confirmLongKeyConverter(longKeyConverter, 0L);
    confirmLongKeyConverter(longKeyConverter, 1L);
    confirmLongKeyConverter(longKeyConverter, 127L);
    confirmLongKeyConverter(longKeyConverter, 128L);
    confirmLongKeyConverter(longKeyConverter, 1234567890L);
    confirmLongKeyConverter(longKeyConverter, Long.MAX_VALUE);
  }

  private void confirmLongKeyConverter(LongKeyConverter longKeyConverter,
      Long testValue) {
    Long decoded = longKeyConverter.decode(longKeyConverter.encode(testValue));
    assertEquals(testValue, decoded);
  }

  @Test
  public void testStringKeyConverter() {
    StringKeyConverter stringKeyConverter = new StringKeyConverter();
    String phrase = "QuackAttack now!";

    for (int i = 0; i < phrase.length(); i++) {
      String sub = phrase.substring(i, phrase.length());
      confirmStrignKeyConverter(stringKeyConverter, sub);
      confirmStrignKeyConverter(stringKeyConverter, sub + sub);
    }
  }

  private void confirmStrignKeyConverter(StringKeyConverter stringKeyConverter,
      String testValue) {
    String decoded =
        stringKeyConverter.decode(stringKeyConverter.encode(testValue));
    assertEquals(testValue, decoded);
  }

}
