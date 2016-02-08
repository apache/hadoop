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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;

/**
 * Represents a rowkey for the application table.
 */
public class ApplicationRowKey {
  private final String clusterId;
  private final String userId;
  private final String flowName;
  private final long flowRunId;
  private final String appId;

  public ApplicationRowKey(String clusterId, String userId, String flowName,
      long flowRunId, String appId) {
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

  public long getFlowRunId() {
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
    byte[] first = Bytes.toBytes(
        Separator.QUALIFIERS.joinEncoded(clusterId, userId, flowName));
    return Separator.QUALIFIERS.join(first, new byte[0]);
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
    byte[] first = Bytes.toBytes(
        Separator.QUALIFIERS.joinEncoded(clusterId, userId, flowName));
    byte[] second = Bytes.toBytes(TimelineStorageUtils.invertLong(flowRunId));
    return Separator.QUALIFIERS.join(first, second, new byte[0]);
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
    byte[] first =
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(clusterId, userId,
            flowName));
    // Note that flowRunId is a long, so we can't encode them all at the same
    // time.
    byte[] second = Bytes.toBytes(TimelineStorageUtils.invertLong(flowRunId));
    byte[] third = TimelineStorageUtils.encodeAppId(appId);
    return Separator.QUALIFIERS.join(first, second, third);
  }

  /**
   * Given the raw row key as bytes, returns the row key as an object.
   *
   * @param rowKey Byte representation  of row key.
   * @return An <cite>ApplicationRowKey</cite> object.
   */
  public static ApplicationRowKey parseRowKey(byte[] rowKey) {
    byte[][] rowKeyComponents = Separator.QUALIFIERS.split(rowKey);

    if (rowKeyComponents.length < 5) {
      throw new IllegalArgumentException("the row key is not valid for " +
          "an application");
    }

    String clusterId =
        Separator.QUALIFIERS.decode(Bytes.toString(rowKeyComponents[0]));
    String userId =
        Separator.QUALIFIERS.decode(Bytes.toString(rowKeyComponents[1]));
    String flowName =
        Separator.QUALIFIERS.decode(Bytes.toString(rowKeyComponents[2]));
    long flowRunId =
        TimelineStorageUtils.invertLong(Bytes.toLong(rowKeyComponents[3]));
    String appId = TimelineStorageUtils.decodeAppId(rowKeyComponents[4]);
    return new ApplicationRowKey(clusterId, userId, flowName, flowRunId, appId);
  }
}
