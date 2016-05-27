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
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowRowKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityRowKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunRowKeyConverter;
import org.junit.Test;

public class TestKeyConverters {
  private final static String QUALIFIER_SEP = Separator.QUALIFIERS.getValue();
  private final static byte[] QUALIFIER_SEP_BYTES =
      Bytes.toBytes(QUALIFIER_SEP);
  private final static String CLUSTER = "cl" + QUALIFIER_SEP + "uster";
  private final static String USER = QUALIFIER_SEP + "user";
  private final static String FLOW_NAME =
      "dummy_" + QUALIFIER_SEP + "flow" + QUALIFIER_SEP;
  private final static Long FLOW_RUN_ID;
  private final static String APPLICATION_ID;
  static {
    long runid = Long.MAX_VALUE - 900L;
    byte[] longMaxByteArr = Bytes.toBytes(Long.MAX_VALUE);
    byte[] byteArr = Bytes.toBytes(runid);
    int sepByteLen = QUALIFIER_SEP_BYTES.length;
    if (sepByteLen <= byteArr.length) {
      for (int i = 0; i < sepByteLen; i++) {
        byteArr[i] = (byte)(longMaxByteArr[i] - QUALIFIER_SEP_BYTES[i]);
      }
    }
    FLOW_RUN_ID = Bytes.toLong(byteArr);
    long clusterTs = System.currentTimeMillis();
    byteArr = Bytes.toBytes(clusterTs);
    if (sepByteLen <= byteArr.length) {
      for (int i = 0; i < sepByteLen; i++) {
        byteArr[byteArr.length - sepByteLen + i] =
            (byte)(longMaxByteArr[byteArr.length - sepByteLen + i] -
                QUALIFIER_SEP_BYTES[i]);
      }
    }
    clusterTs = Bytes.toLong(byteArr);
    int seqId = 222;
    APPLICATION_ID = ApplicationId.newInstance(clusterTs, seqId).toString();
  }

  private static void verifyRowPrefixBytes(byte[] byteRowKeyPrefix) {
    int sepLen = QUALIFIER_SEP_BYTES.length;
    for (int i = 0; i < sepLen; i++) {
      assertTrue("Row key prefix not encoded properly.",
        byteRowKeyPrefix[byteRowKeyPrefix.length - sepLen  + i] ==
            QUALIFIER_SEP_BYTES[i]);
    }
  }

  @Test
  public void testFlowActivityRowKeyConverter() {
    Long ts = TimelineStorageUtils.getTopOfTheDayTimestamp(1459900830000L);
    byte[] byteRowKey = FlowActivityRowKeyConverter.getInstance().encode(
        new FlowActivityRowKey(CLUSTER, ts, USER, FLOW_NAME));
    FlowActivityRowKey rowKey =
        FlowActivityRowKeyConverter.getInstance().decode(byteRowKey);
    assertEquals(CLUSTER, rowKey.getClusterId());
    assertEquals(ts, rowKey.getDayTimestamp());
    assertEquals(USER, rowKey.getUserId());
    assertEquals(FLOW_NAME, rowKey.getFlowName());

    byte[] byteRowKeyPrefix = FlowActivityRowKeyConverter.getInstance().encode(
        new FlowActivityRowKey(CLUSTER, null, null, null));
    byte[][] splits = Separator.QUALIFIERS.split(byteRowKeyPrefix, new int[] {
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE });
    assertEquals(2, splits.length);
    assertEquals(0, splits[1].length);
    assertEquals(CLUSTER,
        Separator.QUALIFIERS.decode(Bytes.toString(splits[0])));
    verifyRowPrefixBytes(byteRowKeyPrefix);

    byteRowKeyPrefix = FlowActivityRowKeyConverter.getInstance().encode(
        new FlowActivityRowKey(CLUSTER, ts, null, null));
    splits = Separator.QUALIFIERS.split(byteRowKeyPrefix, new int[] {
        Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG, Separator.VARIABLE_SIZE });
    assertEquals(3, splits.length);
    assertEquals(0, splits[2].length);
    assertEquals(CLUSTER,
        Separator.QUALIFIERS.decode(Bytes.toString(splits[0])));
    assertEquals(ts, (Long) TimelineStorageUtils.invertLong(
        Bytes.toLong(splits[1])));
    verifyRowPrefixBytes(byteRowKeyPrefix);
  }

  @Test
  public void testFlowRunRowKeyConverter() {
    byte[] byteRowKey = FlowRunRowKeyConverter.getInstance().encode(
        new FlowRunRowKey(CLUSTER, USER, FLOW_NAME, FLOW_RUN_ID));
    FlowRunRowKey rowKey =
        FlowRunRowKeyConverter.getInstance().decode(byteRowKey);
    assertEquals(CLUSTER, rowKey.getClusterId());
    assertEquals(USER, rowKey.getUserId());
    assertEquals(FLOW_NAME, rowKey.getFlowName());
    assertEquals(FLOW_RUN_ID, rowKey.getFlowRunId());

    byte[] byteRowKeyPrefix = FlowRunRowKeyConverter.getInstance().encode(
        new FlowRunRowKey(CLUSTER, USER, FLOW_NAME, null));
    byte[][] splits = Separator.QUALIFIERS.split(byteRowKeyPrefix, new int[] {
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE });
    assertEquals(4, splits.length);
    assertEquals(0, splits[3].length);
    assertEquals(FLOW_NAME,
        Separator.QUALIFIERS.decode(Bytes.toString(splits[2])));
    verifyRowPrefixBytes(byteRowKeyPrefix);
  }

  @Test
  public void testApplicationRowKeyConverter() {
    byte[] byteRowKey = ApplicationRowKeyConverter.getInstance().encode(
        new ApplicationRowKey(CLUSTER, USER, FLOW_NAME, FLOW_RUN_ID,
            APPLICATION_ID));
    ApplicationRowKey rowKey =
        ApplicationRowKeyConverter.getInstance().decode(byteRowKey);
    assertEquals(CLUSTER, rowKey.getClusterId());
    assertEquals(USER, rowKey.getUserId());
    assertEquals(FLOW_NAME, rowKey.getFlowName());
    assertEquals(FLOW_RUN_ID, rowKey.getFlowRunId());
    assertEquals(APPLICATION_ID, rowKey.getAppId());

    byte[] byteRowKeyPrefix = ApplicationRowKeyConverter.getInstance().encode(
        new ApplicationRowKey(CLUSTER, USER, FLOW_NAME, FLOW_RUN_ID, null));
    byte[][] splits =
        Separator.QUALIFIERS.split(byteRowKeyPrefix, new int[] {
            Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
            Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG,
            Separator.VARIABLE_SIZE });
    assertEquals(5, splits.length);
    assertEquals(0, splits[4].length);
    assertEquals(FLOW_NAME,
        Separator.QUALIFIERS.decode(Bytes.toString(splits[2])));
    assertEquals(FLOW_RUN_ID, (Long)TimelineStorageUtils.invertLong(
        Bytes.toLong(splits[3])));
    verifyRowPrefixBytes(byteRowKeyPrefix);

    byteRowKeyPrefix = ApplicationRowKeyConverter.getInstance().encode(
        new ApplicationRowKey(CLUSTER, USER, FLOW_NAME, null, null));
    splits = Separator.QUALIFIERS.split(byteRowKeyPrefix, new int[] {
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE });
    assertEquals(4, splits.length);
    assertEquals(0, splits[3].length);
    assertEquals(FLOW_NAME,
        Separator.QUALIFIERS.decode(Bytes.toString(splits[2])));
    verifyRowPrefixBytes(byteRowKeyPrefix);
  }

  @Test
  public void testEntityRowKeyConverter() {
    String entityId = "!ent!ity!!id!";
    String entityType = "entity!Type";
    byte[] byteRowKey = EntityRowKeyConverter.getInstance().encode(
        new EntityRowKey(CLUSTER, USER, FLOW_NAME, FLOW_RUN_ID, APPLICATION_ID,
            entityType, entityId));
    EntityRowKey rowKey =
        EntityRowKeyConverter.getInstance().decode(byteRowKey);
    assertEquals(CLUSTER, rowKey.getClusterId());
    assertEquals(USER, rowKey.getUserId());
    assertEquals(FLOW_NAME, rowKey.getFlowName());
    assertEquals(FLOW_RUN_ID, rowKey.getFlowRunId());
    assertEquals(APPLICATION_ID, rowKey.getAppId());
    assertEquals(entityType, rowKey.getEntityType());
    assertEquals(entityId, rowKey.getEntityId());

    byte[] byteRowKeyPrefix = EntityRowKeyConverter.getInstance().encode(
        new EntityRowKey(CLUSTER, USER, FLOW_NAME, FLOW_RUN_ID, APPLICATION_ID,
            entityType, null));
    byte[][] splits =
        Separator.QUALIFIERS.split(byteRowKeyPrefix, new int[] {
            Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
            Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG,
            AppIdKeyConverter.getKeySize(), Separator.VARIABLE_SIZE,
            Separator.VARIABLE_SIZE });
    assertEquals(7, splits.length);
    assertEquals(0, splits[6].length);
    assertEquals(APPLICATION_ID,
        AppIdKeyConverter.getInstance().decode(splits[4]));
    assertEquals(entityType, Separator.QUALIFIERS.decode(
        Bytes.toString(splits[5])));
    verifyRowPrefixBytes(byteRowKeyPrefix);

    byteRowKeyPrefix = EntityRowKeyConverter.getInstance().encode(
        new EntityRowKey(CLUSTER, USER, FLOW_NAME, FLOW_RUN_ID, APPLICATION_ID,
        null, null));
    splits = Separator.QUALIFIERS.split(byteRowKeyPrefix, new int[] {
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
        Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG,
        AppIdKeyConverter.getKeySize(), Separator.VARIABLE_SIZE });
    assertEquals(6, splits.length);
    assertEquals(0, splits[5].length);
    assertEquals(APPLICATION_ID,
        AppIdKeyConverter.getInstance().decode(splits[4]));
    verifyRowPrefixBytes(byteRowKeyPrefix);
  }

  @Test
  public void testAppToFlowRowKeyConverter() {
    byte[] byteRowKey = AppToFlowRowKeyConverter.getInstance().encode(
        new AppToFlowRowKey(CLUSTER, APPLICATION_ID));
    AppToFlowRowKey rowKey =
        AppToFlowRowKeyConverter.getInstance().decode(byteRowKey);
    assertEquals(CLUSTER, rowKey.getClusterId());
    assertEquals(APPLICATION_ID, rowKey.getAppId());
  }

  @Test
  public void testAppIdKeyConverter() {
    long currentTs = System.currentTimeMillis();
    ApplicationId appId1 = ApplicationId.newInstance(currentTs, 1);
    ApplicationId appId2 = ApplicationId.newInstance(currentTs, 2);
    ApplicationId appId3 = ApplicationId.newInstance(currentTs + 300, 1);
    String appIdStr1 = appId1.toString();
    String appIdStr2 = appId2.toString();
    String appIdStr3 = appId3.toString();
    byte[] appIdBytes1 = AppIdKeyConverter.getInstance().encode(appIdStr1);
    byte[] appIdBytes2 = AppIdKeyConverter.getInstance().encode(appIdStr2);
    byte[] appIdBytes3 = AppIdKeyConverter.getInstance().encode(appIdStr3);
    // App ids' should be encoded in a manner wherein descending order
    // is maintained.
    assertTrue("Ordering of app ids' is incorrect",
        Bytes.compareTo(appIdBytes1, appIdBytes2) > 0 &&
        Bytes.compareTo(appIdBytes1, appIdBytes3) > 0 &&
        Bytes.compareTo(appIdBytes2, appIdBytes3) > 0);
    String decodedAppId1 = AppIdKeyConverter.getInstance().decode(appIdBytes1);
    String decodedAppId2 = AppIdKeyConverter.getInstance().decode(appIdBytes2);
    String decodedAppId3 = AppIdKeyConverter.getInstance().decode(appIdBytes3);
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
    byte[] byteEventColName = EventColumnNameConverter.getInstance().encode(
        new EventColumnName(eventId, eventTs, null));
    EventColumnName eventColName =
        EventColumnNameConverter.getInstance().decode(byteEventColName);
    assertEquals(eventId, eventColName.getId());
    assertEquals(eventTs, eventColName.getTimestamp());
    assertNull(eventColName.getInfoKey());

    String infoKey = "f=oo_event_in=fo=_key";
    byteEventColName = EventColumnNameConverter.getInstance().encode(
        new EventColumnName(eventId, eventTs, infoKey));
    eventColName =
        EventColumnNameConverter.getInstance().decode(byteEventColName);
    assertEquals(eventId, eventColName.getId());
    assertEquals(eventTs, eventColName.getTimestamp());
    assertEquals(infoKey, eventColName.getInfoKey());
  }
}
