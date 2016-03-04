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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;

/**
 * Represents a rowkey for the flow activity table.
 */
public class FlowActivityRowKey {

  private final String clusterId;
  private final long dayTs;
  private final String userId;
  private final String flowName;

  public FlowActivityRowKey(String clusterId, long dayTs, String userId,
      String flowName) {
    this.clusterId = clusterId;
    this.dayTs = dayTs;
    this.userId = userId;
    this.flowName = flowName;
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

  public String getFlowName() {
    return flowName;
  }

  /**
   * Constructs a row key prefix for the flow activity table as follows:
   * {@code clusterId!}.
   *
   * @param clusterId Cluster Id.
   * @return byte array with the row key prefix
   */
  public static byte[] getRowKeyPrefix(String clusterId) {
    return Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(clusterId, ""));
  }

  /**
   * Constructs a row key prefix for the flow activity table as follows:
   * {@code clusterId!dayTimestamp!}.
   *
   * @param clusterId Cluster Id.
   * @param dayTs Start of the day timestamp.
   * @return byte array with the row key prefix
   */
  public static byte[] getRowKeyPrefix(String clusterId, long dayTs) {
    return Separator.QUALIFIERS.join(
        Bytes.toBytes(Separator.QUALIFIERS.encode(clusterId)),
        Bytes.toBytes(TimelineStorageUtils.invertLong(dayTs)), new byte[0]);
  }

  /**
   * Constructs a row key for the flow activity table as follows:
   * {@code clusterId!dayTimestamp!user!flowName}.
   *
   * @param clusterId Cluster Id.
   * @param eventTs event's TimeStamp.
   * @param userId User Id.
   * @param flowName Flow Name.
   * @return byte array for the row key
   */
  public static byte[] getRowKey(String clusterId, long eventTs, String userId,
      String flowName) {
    // convert it to Day's time stamp
    eventTs = TimelineStorageUtils.getTopOfTheDayTimestamp(eventTs);

    return Separator.QUALIFIERS.join(
        Bytes.toBytes(Separator.QUALIFIERS.encode(clusterId)),
        Bytes.toBytes(TimelineStorageUtils.invertLong(eventTs)),
        Bytes.toBytes(Separator.QUALIFIERS.encode(userId)),
        Bytes.toBytes(Separator.QUALIFIERS.encode(flowName)));
  }

  /**
   * Given the raw row key as bytes, returns the row key as an object.
   *
   * @param rowKey Byte representation of row key.
   * @return A <cite>FlowActivityRowKey</cite> object.
   */
  public static FlowActivityRowKey parseRowKey(byte[] rowKey) {
    byte[][] rowKeyComponents = Separator.QUALIFIERS.split(rowKey);

    if (rowKeyComponents.length < 4) {
      throw new IllegalArgumentException("the row key is not valid for "
          + "a flow activity");
    }

    String clusterId = Separator.QUALIFIERS.decode(Bytes
        .toString(rowKeyComponents[0]));
    long dayTs =
        TimelineStorageUtils.invertLong(Bytes.toLong(rowKeyComponents[1]));
    String userId = Separator.QUALIFIERS.decode(Bytes
        .toString(rowKeyComponents[2]));
    String flowName = Separator.QUALIFIERS.decode(Bytes
        .toString(rowKeyComponents[3]));
    return new FlowActivityRowKey(clusterId, dayTs, userId, flowName);
  }
}
