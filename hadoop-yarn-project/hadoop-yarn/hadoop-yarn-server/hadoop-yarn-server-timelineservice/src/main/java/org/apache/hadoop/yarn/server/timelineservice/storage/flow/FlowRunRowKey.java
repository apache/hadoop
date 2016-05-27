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

/**
 * Represents a rowkey for the flow run table.
 */
public class FlowRunRowKey {
  private final String clusterId;
  private final String userId;
  private final String flowName;
  private final Long flowRunId;

  public FlowRunRowKey(String clusterId, String userId, String flowName,
      Long flowRunId) {
    this.clusterId = clusterId;
    this.userId = userId;
    this.flowName = flowName;
    this.flowRunId = flowRunId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getUserId() {
    return userId;
  }

  public String getFlowName() {
    return flowName;
  }

  public Long getFlowRunId() {
    return flowRunId;
  }

  /**
   * Constructs a row key prefix for the flow run table as follows: {
   * clusterId!userI!flowName!}.
   *
   * @param clusterId Cluster Id.
   * @param userId User Id.
   * @param flowName Flow Name.
   * @return byte array with the row key prefix
   */
  public static byte[] getRowKeyPrefix(String clusterId, String userId,
      String flowName) {
    return FlowRunRowKeyConverter.getInstance().encode(new FlowRunRowKey(
        clusterId, userId, flowName, null));
  }

  /**
   * Constructs a row key for the entity table as follows: {
   * clusterId!userId!flowName!Inverted Flow Run Id}.
   *
   * @param clusterId Cluster Id.
   * @param userId User Id.
   * @param flowName Flow Name.
   * @param flowRunId Run Id for the flow name.
   * @return byte array with the row key
   */
  public static byte[] getRowKey(String clusterId, String userId,
      String flowName, Long flowRunId) {
    return FlowRunRowKeyConverter.getInstance().encode(new FlowRunRowKey(
        clusterId, userId, flowName, flowRunId));
  }

  /**
   * Given the raw row key as bytes, returns the row key as an object.
   *
   * @param rowKey Byte representation of row key.
   * @return A <cite>FlowRunRowKey</cite> object.
   */
  public static FlowRunRowKey parseRowKey(byte[] rowKey) {
    return FlowRunRowKeyConverter.getInstance().decode(rowKey);
  }

  /**
   * returns the Flow Key as a verbose String output.
   * @return String
   */
  @Override
  public String toString() {
    StringBuilder flowKeyStr = new StringBuilder();
    flowKeyStr.append("{clusterId=" + clusterId);
    flowKeyStr.append(" userId=" + userId);
    flowKeyStr.append(" flowName=" + flowName);
    flowKeyStr.append(" flowRunId=");
    flowKeyStr.append(flowRunId);
    flowKeyStr.append("}");
    return flowKeyStr.toString();
  }
}
