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
 * Represents a rowkey for the flow run table.
 */
public class FlowRunRowKey {
  private final String clusterId;
  private final String userId;
  private final String flowId;
  private final long flowRunId;

  public FlowRunRowKey(String clusterId, String userId, String flowId,
      long flowRunId) {
    this.clusterId = clusterId;
    this.userId = userId;
    this.flowId = flowId;
    this.flowRunId = flowRunId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getUserId() {
    return userId;
  }

  public String getFlowId() {
    return flowId;
  }

  public long getFlowRunId() {
    return flowRunId;
  }

  /**
   * Constructs a row key for the entity table as follows: {
   * clusterId!userI!flowId!Inverted Flow Run Id}
   *
   * @param clusterId
   * @param userId
   * @param flowId
   * @param flowRunId
   * @return byte array with the row key
   */
  public static byte[] getRowKey(String clusterId, String userId,
      String flowId, Long flowRunId) {
    byte[] first = Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(clusterId,
        userId, flowId));
    // Note that flowRunId is a long, so we can't encode them all at the same
    // time.
    byte[] second = Bytes.toBytes(TimelineWriterUtils.invert(flowRunId));
    return Separator.QUALIFIERS.join(first, second);
  }

  /**
   * Given the raw row key as bytes, returns the row key as an object.
   */
  public static FlowRunRowKey parseRowKey(byte[] rowKey) {
    byte[][] rowKeyComponents = Separator.QUALIFIERS.split(rowKey);

    if (rowKeyComponents.length < 4) {
      throw new IllegalArgumentException("the row key is not valid for " +
          "a flow run");
    }

    String clusterId =
        Separator.QUALIFIERS.decode(Bytes.toString(rowKeyComponents[0]));
    String userId =
        Separator.QUALIFIERS.decode(Bytes.toString(rowKeyComponents[1]));
    String flowId =
        Separator.QUALIFIERS.decode(Bytes.toString(rowKeyComponents[2]));
    long flowRunId =
        TimelineWriterUtils.invert(Bytes.toLong(rowKeyComponents[3]));
    return new FlowRunRowKey(clusterId, userId, flowId, flowRunId);
  }
}
