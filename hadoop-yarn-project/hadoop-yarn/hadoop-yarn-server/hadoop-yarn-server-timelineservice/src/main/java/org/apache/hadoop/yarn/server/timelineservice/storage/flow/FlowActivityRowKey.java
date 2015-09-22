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
package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineWriterUtils;

/**
 * Represents a rowkey for the flow activity table.
 */
public class FlowActivityRowKey {

  private final String clusterId;
  private final long dayTs;
  private final String userId;
  private final String flowId;

  public FlowActivityRowKey(String clusterId, long dayTs, String userId,
      String flowId) {
    this.clusterId = clusterId;
    this.dayTs = dayTs;
    this.userId = userId;
    this.flowId = flowId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public long getDayTimestamp() {
    return dayTs;
  }

  public String getUserId() {
    return userId;
  }

  public String getFlowId() {
    return flowId;
  }

  public static byte[] getRowKeyPrefix(String clusterId) {
    return Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(clusterId, ""));
  }

  /**
   * Constructs a row key for the flow activity table as follows:
   * {@code clusterId!dayTimestamp!user!flowId}
   *
   * Will insert into current day's record in the table
   * @param clusterId
   * @param userId
   * @param flowId
   * @return byte array with the row key prefix
   */
  public static byte[] getRowKey(String clusterId, String userId,
      String flowId) {
    long dayTs = TimelineWriterUtils.getTopOfTheDayTimestamp(System
        .currentTimeMillis());
    return getRowKey(clusterId, dayTs, userId, flowId);
  }

  /**
   * Constructs a row key for the flow activity table as follows:
   * {@code clusterId!dayTimestamp!user!flowId}
   *
   * @param clusterId
   * @param dayTs
   * @param userId
   * @param flowId
   * @return byte array for the row key
   */
  public static byte[] getRowKey(String clusterId, long dayTs, String userId,
      String flowId) {
    return Separator.QUALIFIERS.join(
        Bytes.toBytes(Separator.QUALIFIERS.encode(clusterId)),
        Bytes.toBytes(TimelineWriterUtils.invert(dayTs)),
        Bytes.toBytes(Separator.QUALIFIERS.encode(userId)),
        Bytes.toBytes(Separator.QUALIFIERS.encode(flowId)));
  }

  /**
   * Given the raw row key as bytes, returns the row key as an object.
   */
  public static FlowActivityRowKey parseRowKey(byte[] rowKey) {
    byte[][] rowKeyComponents = Separator.QUALIFIERS.split(rowKey);

    if (rowKeyComponents.length < 4) {
      throw new IllegalArgumentException("the row key is not valid for "
          + "a flow activity");
    }

    String clusterId = Separator.QUALIFIERS.decode(Bytes
        .toString(rowKeyComponents[0]));
    long dayTs = TimelineWriterUtils.invert(Bytes.toLong(rowKeyComponents[1]));
    String userId = Separator.QUALIFIERS.decode(Bytes
        .toString(rowKeyComponents[2]));
    String flowId = Separator.QUALIFIERS.decode(Bytes
        .toString(rowKeyComponents[3]));
    return new FlowActivityRowKey(clusterId, dayTs, userId, flowId);
  }
}
