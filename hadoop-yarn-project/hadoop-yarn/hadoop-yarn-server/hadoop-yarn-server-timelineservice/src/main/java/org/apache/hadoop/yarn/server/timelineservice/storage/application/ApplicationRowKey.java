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

package org.apache.hadoop.yarn.server.timelineservice.storage.application;

/**
 * Represents a rowkey for the application table.
 */
public class ApplicationRowKey {
  private final String clusterId;
  private final String userId;
  private final String flowName;
  private final Long flowRunId;
  private final String appId;

  public ApplicationRowKey(String clusterId, String userId, String flowName,
      Long flowRunId, String appId) {
    this.clusterId = clusterId;
    this.userId = userId;
    this.flowName = flowName;
    this.flowRunId = flowRunId;
    this.appId = appId;
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

  public String getAppId() {
    return appId;
  }

  /**
   * Constructs a row key prefix for the application table as follows:
   * {@code clusterId!userName!flowName!}.
   *
   * @param clusterId Cluster Id.
   * @param userId User Id.
   * @param flowName Flow Name.
   * @return byte array with the row key prefix
   */
  public static byte[] getRowKeyPrefix(String clusterId, String userId,
      String flowName) {
    return ApplicationRowKeyConverter.getInstance().encode(
        new ApplicationRowKey(clusterId, userId, flowName, null, null));
  }

  /**
   * Constructs a row key prefix for the application table as follows:
   * {@code clusterId!userName!flowName!flowRunId!}.
   *
   * @param clusterId Cluster Id.
   * @param userId User Id.
   * @param flowName Flow Name.
   * @param flowRunId Run Id for the flow.
   * @return byte array with the row key prefix
   */
  public static byte[] getRowKeyPrefix(String clusterId, String userId,
      String flowName, Long flowRunId) {
    return ApplicationRowKeyConverter.getInstance().encode(
        new ApplicationRowKey(clusterId, userId, flowName, flowRunId, null));
  }

  /**
   * Constructs a row key for the application table as follows:
   * {@code clusterId!userName!flowName!flowRunId!AppId}.
   *
   * @param clusterId Cluster Id.
   * @param userId User Id.
   * @param flowName Flow Name.
   * @param flowRunId Run Id for the flow.
   * @param appId App Id.
   * @return byte array with the row key
   */
  public static byte[] getRowKey(String clusterId, String userId,
      String flowName, Long flowRunId, String appId) {
    return ApplicationRowKeyConverter.getInstance().encode(
        new ApplicationRowKey(clusterId, userId, flowName, flowRunId, appId));
  }

  /**
   * Given the raw row key as bytes, returns the row key as an object.
   *
   * @param rowKey Byte representation  of row key.
   * @return An <cite>ApplicationRowKey</cite> object.
   */
  public static ApplicationRowKey parseRowKey(byte[] rowKey) {
    return ApplicationRowKeyConverter.getInstance().decode(rowKey);
  }
}
