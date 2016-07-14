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
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunRowKey;
import org.junit.Test;


public class TestRowKeys {

  private final static String QUALIFIER_SEP = Separator.QUALIFIERS.getValue();
  private final static byte[] QUALIFIER_SEP_BYTES = Bytes
      .toBytes(QUALIFIER_SEP);
  private final static String CLUSTER = "cl" + QUALIFIER_SEP + "uster";
  private final static String USER = QUALIFIER_SEP + "user";
  private final static String FLOW_NAME = "dummy_" + QUALIFIER_SEP + "flow"
      + QUALIFIER_SEP;
  private final static Long FLOW_RUN_ID;
  private final static String APPLICATION_ID;
  static {
    long runid = Long.MAX_VALUE - 900L;
    byte[] longMaxByteArr = Bytes.toBytes(Long.MAX_VALUE);
    byte[] byteArr = Bytes.toBytes(runid);
    int sepByteLen = QUALIFIER_SEP_BYTES.length;
    if (sepByteLen <= byteArr.length) {
      for (int i = 0; i < sepByteLen; i++) {
        byteArr[i] = (byte) (longMaxByteArr[i] - QUALIFIER_SEP_BYTES[i]);
      }
    }
    FLOW_RUN_ID = Bytes.toLong(byteArr);
    long clusterTs = System.currentTimeMillis();
    byteArr = Bytes.toBytes(clusterTs);
    if (sepByteLen <= byteArr.length) {
      for (int i = 0; i < sepByteLen; i++) {
        byteArr[byteArr.length - sepByteLen + i] =
            (byte) (longMaxByteArr[byteArr.length - sepByteLen + i] -
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
      assertTrue(
          "Row key prefix not encoded properly.",
          byteRowKeyPrefix[byteRowKeyPrefix.length - sepLen + i] ==
              QUALIFIER_SEP_BYTES[i]);
    }
  }

  @Test
  public void testApplicationRowKey() {
    byte[] byteRowKey =
        new ApplicationRowKey(CLUSTER, USER, FLOW_NAME, FLOW_RUN_ID,
            APPLICATION_ID).getRowKey();
    ApplicationRowKey rowKey = ApplicationRowKey.parseRowKey(byteRowKey);
    assertEquals(CLUSTER, rowKey.getClusterId());
    assertEquals(USER, rowKey.getUserId());
    assertEquals(FLOW_NAME, rowKey.getFlowName());
    assertEquals(FLOW_RUN_ID, rowKey.getFlowRunId());
    assertEquals(APPLICATION_ID, rowKey.getAppId());

    byte[] byteRowKeyPrefix =
        new ApplicationRowKeyPrefix(CLUSTER, USER, FLOW_NAME, FLOW_RUN_ID)
            .getRowKeyPrefix();
    byte[][] splits =
        Separator.QUALIFIERS.split(byteRowKeyPrefix,
            new int[] {Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
                Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG,
                Separator.VARIABLE_SIZE});
    assertEquals(5, splits.length);
    assertEquals(0, splits[4].length);
    assertEquals(FLOW_NAME,
        Separator.QUALIFIERS.decode(Bytes.toString(splits[2])));
    assertEquals(FLOW_RUN_ID,
        (Long) LongConverter.invertLong(Bytes.toLong(splits[3])));
    verifyRowPrefixBytes(byteRowKeyPrefix);

    byteRowKeyPrefix =
        new ApplicationRowKeyPrefix(CLUSTER, USER, FLOW_NAME).getRowKeyPrefix();
    splits =
        Separator.QUALIFIERS.split(byteRowKeyPrefix, new int[] {
            Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
            Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE });
    assertEquals(4, splits.length);
    assertEquals(0, splits[3].length);
    assertEquals(FLOW_NAME,
        Separator.QUALIFIERS.decode(Bytes.toString(splits[2])));
    verifyRowPrefixBytes(byteRowKeyPrefix);
  }

  /**
   * Tests the converters indirectly through the public methods of the
   * corresponding rowkey.
   */
  @Test
  public void testAppToFlowRowKey() {
    byte[] byteRowKey = new AppToFlowRowKey(CLUSTER,
        APPLICATION_ID).getRowKey();
    AppToFlowRowKey rowKey = AppToFlowRowKey.parseRowKey(byteRowKey);
    assertEquals(CLUSTER, rowKey.getClusterId());
    assertEquals(APPLICATION_ID, rowKey.getAppId());
  }

  @Test
  public void testEntityRowKey() {
    String entityId = "!ent!ity!!id!";
    String entityType = "entity!Type";
    byte[] byteRowKey =
        new EntityRowKey(CLUSTER, USER, FLOW_NAME, FLOW_RUN_ID, APPLICATION_ID,
            entityType, entityId).getRowKey();
    EntityRowKey rowKey = EntityRowKey.parseRowKey(byteRowKey);
    assertEquals(CLUSTER, rowKey.getClusterId());
    assertEquals(USER, rowKey.getUserId());
    assertEquals(FLOW_NAME, rowKey.getFlowName());
    assertEquals(FLOW_RUN_ID, rowKey.getFlowRunId());
    assertEquals(APPLICATION_ID, rowKey.getAppId());
    assertEquals(entityType, rowKey.getEntityType());
    assertEquals(entityId, rowKey.getEntityId());

    byte[] byteRowKeyPrefix =
        new EntityRowKeyPrefix(CLUSTER, USER, FLOW_NAME, FLOW_RUN_ID,
            APPLICATION_ID, entityType).getRowKeyPrefix();
    byte[][] splits =
        Separator.QUALIFIERS.split(
            byteRowKeyPrefix,
            new int[] {Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
                Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG,
                AppIdKeyConverter.getKeySize(), Separator.VARIABLE_SIZE,
                Separator.VARIABLE_SIZE});
    assertEquals(7, splits.length);
    assertEquals(0, splits[6].length);
    assertEquals(APPLICATION_ID, new AppIdKeyConverter().decode(splits[4]));
    assertEquals(entityType,
        Separator.QUALIFIERS.decode(Bytes.toString(splits[5])));
    verifyRowPrefixBytes(byteRowKeyPrefix);

    byteRowKeyPrefix =
        new EntityRowKeyPrefix(CLUSTER, USER, FLOW_NAME, FLOW_RUN_ID,
            APPLICATION_ID).getRowKeyPrefix();
    splits =
        Separator.QUALIFIERS.split(
            byteRowKeyPrefix,
            new int[] {Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
                Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG,
                AppIdKeyConverter.getKeySize(), Separator.VARIABLE_SIZE});
    assertEquals(6, splits.length);
    assertEquals(0, splits[5].length);
    AppIdKeyConverter appIdKeyConverter = new AppIdKeyConverter();
    assertEquals(APPLICATION_ID, appIdKeyConverter.decode(splits[4]));
    verifyRowPrefixBytes(byteRowKeyPrefix);
  }

  @Test
  public void testFlowActivityRowKey() {
    Long ts = 1459900830000L;
    Long dayTimestamp = TimelineStorageUtils.getTopOfTheDayTimestamp(ts);
    byte[] byteRowKey =
        new FlowActivityRowKey(CLUSTER, ts, USER, FLOW_NAME).getRowKey();
    FlowActivityRowKey rowKey = FlowActivityRowKey.parseRowKey(byteRowKey);
    assertEquals(CLUSTER, rowKey.getClusterId());
    assertEquals(dayTimestamp, rowKey.getDayTimestamp());
    assertEquals(USER, rowKey.getUserId());
    assertEquals(FLOW_NAME, rowKey.getFlowName());

    byte[] byteRowKeyPrefix =
        new FlowActivityRowKeyPrefix(CLUSTER).getRowKeyPrefix();
    byte[][] splits =
        Separator.QUALIFIERS.split(byteRowKeyPrefix, new int[] {
            Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE });
    assertEquals(2, splits.length);
    assertEquals(0, splits[1].length);
    assertEquals(CLUSTER,
        Separator.QUALIFIERS.decode(Bytes.toString(splits[0])));
    verifyRowPrefixBytes(byteRowKeyPrefix);

    byteRowKeyPrefix =
        new FlowActivityRowKeyPrefix(CLUSTER, ts).getRowKeyPrefix();
    splits =
        Separator.QUALIFIERS.split(byteRowKeyPrefix,
            new int[] {Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG,
                Separator.VARIABLE_SIZE});
    assertEquals(3, splits.length);
    assertEquals(0, splits[2].length);
    assertEquals(CLUSTER,
        Separator.QUALIFIERS.decode(Bytes.toString(splits[0])));
    assertEquals(ts,
        (Long) LongConverter.invertLong(Bytes.toLong(splits[1])));
    verifyRowPrefixBytes(byteRowKeyPrefix);
  }

  @Test
  public void testFlowRunRowKey() {
    byte[] byteRowKey =
        new FlowRunRowKey(CLUSTER, USER, FLOW_NAME, FLOW_RUN_ID).getRowKey();
    FlowRunRowKey rowKey = FlowRunRowKey.parseRowKey(byteRowKey);
    assertEquals(CLUSTER, rowKey.getClusterId());
    assertEquals(USER, rowKey.getUserId());
    assertEquals(FLOW_NAME, rowKey.getFlowName());
    assertEquals(FLOW_RUN_ID, rowKey.getFlowRunId());

    byte[] byteRowKeyPrefix =
        new FlowRunRowKey(CLUSTER, USER, FLOW_NAME, null).getRowKey();
    byte[][] splits =
        Separator.QUALIFIERS.split(byteRowKeyPrefix, new int[] {
            Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
            Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE });
    assertEquals(4, splits.length);
    assertEquals(0, splits[3].length);
    assertEquals(FLOW_NAME,
        Separator.QUALIFIERS.decode(Bytes.toString(splits[2])));
    verifyRowPrefixBytes(byteRowKeyPrefix);
  }

}
