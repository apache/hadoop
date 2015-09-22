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
package org.apache.hadoop.yarn.server.timelineservice.storage.entity;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineWriterUtils;

/**
 * Represents a rowkey for the entity table.
 */
public class EntityRowKey {
  private final String clusterId;
  private final String userId;
  private final String flowId;
  private final long flowRunId;
  private final String appId;
  private final String entityType;
  private final String entityId;

  public EntityRowKey(String clusterId, String userId, String flowId,
      long flowRunId, String appId, String entityType, String entityId) {
    this.clusterId = clusterId;
    this.userId = userId;
    this.flowId = flowId;
    this.flowRunId = flowRunId;
    this.appId = appId;
    this.entityType = entityType;
    this.entityId = entityId;
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

  public String getAppId() {
    return appId;
  }

  public String getEntityType() {
    return entityType;
  }

  public String getEntityId() {
    return entityId;
  }

  /**
   * Constructs a row key prefix for the entity table as follows:
   * {@code userName!clusterId!flowId!flowRunId!AppId}
   *
   * @param clusterId
   * @param userId
   * @param flowId
   * @param flowRunId
   * @param appId
   * @return byte array with the row key prefix
   */
  public static byte[] getRowKeyPrefix(String clusterId, String userId,
      String flowId, Long flowRunId, String appId) {
    byte[] first =
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(userId, clusterId,
            flowId));
    // Note that flowRunId is a long, so we can't encode them all at the same
    // time.
    byte[] second = Bytes.toBytes(TimelineWriterUtils.invert(flowRunId));
    byte[] third = Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(appId));
    return Separator.QUALIFIERS.join(first, second, third);
  }

  /**
   * Constructs a row key prefix for the entity table as follows:
   * {@code userName!clusterId!flowId!flowRunId!AppId!entityType!}
   *
   * @param clusterId
   * @param userId
   * @param flowId
   * @param flowRunId
   * @param appId
   * @param entityType
   * @return byte array with the row key prefix
   */
  public static byte[] getRowKeyPrefix(String clusterId, String userId,
      String flowId, Long flowRunId, String appId, String entityType) {
    byte[] first =
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(userId, clusterId,
            flowId));
    // Note that flowRunId is a long, so we can't encode them all at the same
    // time.
    byte[] second = Bytes.toBytes(TimelineWriterUtils.invert(flowRunId));
    byte[] third =
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(appId, entityType, ""));
    return Separator.QUALIFIERS.join(first, second, third);
  }

  /**
   * Constructs a row key for the entity table as follows:
   * {@code userName!clusterId!flowId!flowRunId!AppId!entityType!entityId}
   *
   * @param clusterId
   * @param userId
   * @param flowId
   * @param flowRunId
   * @param appId
   * @param entityType
   * @param entityId
   * @return byte array with the row key
   */
  public static byte[] getRowKey(String clusterId, String userId,
      String flowId, Long flowRunId, String appId, String entityType,
      String entityId) {
    byte[] first =
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(userId, clusterId,
            flowId));
    // Note that flowRunId is a long, so we can't encode them all at the same
    // time.
    byte[] second = Bytes.toBytes(TimelineWriterUtils.invert(flowRunId));
    byte[] third =
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(appId, entityType,
            entityId));
    return Separator.QUALIFIERS.join(first, second, third);
  }

  /**
   * Given the raw row key as bytes, returns the row key as an object.
   */
  public static EntityRowKey parseRowKey(byte[] rowKey) {
    byte[][] rowKeyComponents = Separator.QUALIFIERS.split(rowKey);

    if (rowKeyComponents.length < 7) {
      throw new IllegalArgumentException("the row key is not valid for " +
          "an entity");
    }

    String userId =
        Separator.QUALIFIERS.decode(Bytes.toString(rowKeyComponents[0]));
    String clusterId =
        Separator.QUALIFIERS.decode(Bytes.toString(rowKeyComponents[1]));
    String flowId =
        Separator.QUALIFIERS.decode(Bytes.toString(rowKeyComponents[2]));
    long flowRunId =
        TimelineWriterUtils.invert(Bytes.toLong(rowKeyComponents[3]));
    String appId =
        Separator.QUALIFIERS.decode(Bytes.toString(rowKeyComponents[4]));
    String entityType =
        Separator.QUALIFIERS.decode(Bytes.toString(rowKeyComponents[5]));
    String entityId =
        Separator.QUALIFIERS.decode(Bytes.toString(rowKeyComponents[6]));
    return new EntityRowKey(clusterId, userId, flowId, flowRunId, appId,
        entityType, entityId);
  }
}
