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

/**
 * Represents a rowkey for the entity table.
 */
public class EntityRowKey {
  private final String clusterId;
  private final String userId;
  private final String flowName;
  private final Long flowRunId;
  private final String appId;
  private final String entityType;
  private final String entityId;

  public EntityRowKey(String clusterId, String userId, String flowName,
      Long flowRunId, String appId, String entityType, String entityId) {
    this.clusterId = clusterId;
    this.userId = userId;
    this.flowName = flowName;
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

  public String getFlowName() {
    return flowName;
  }

  public Long getFlowRunId() {
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
   * {@code userName!clusterId!flowName!flowRunId!AppId}.
   *
   * @param clusterId Context cluster id.
   * @param userId User name.
   * @param flowName Flow name.
   * @param flowRunId Run Id for the flow.
   * @param appId Application Id.
   * @return byte array with the row key prefix.
   */
  public static byte[] getRowKeyPrefix(String clusterId, String userId,
      String flowName, Long flowRunId, String appId) {
    return EntityRowKeyConverter.getInstance().encode(new EntityRowKey(
        clusterId, userId, flowName, flowRunId, appId, null, null));
  }

  /**
   * Constructs a row key prefix for the entity table as follows:
   * {@code userName!clusterId!flowName!flowRunId!AppId!entityType!}.
   * Typically used while querying multiple entities of a particular entity
   * type.
   *
   * @param clusterId Context cluster id.
   * @param userId User name.
   * @param flowName Flow name.
   * @param flowRunId Run Id for the flow.
   * @param appId Application Id.
   * @param entityType Entity type.
   * @return byte array with the row key prefix.
   */
  public static byte[] getRowKeyPrefix(String clusterId, String userId,
      String flowName, Long flowRunId, String appId, String entityType) {
    return EntityRowKeyConverter.getInstance().encode(new EntityRowKey(
        clusterId, userId, flowName, flowRunId, appId, entityType, null));
  }

  /**
   * Constructs a row key for the entity table as follows:
   * {@code userName!clusterId!flowName!flowRunId!AppId!entityType!entityId}.
   * Typically used while querying a specific entity.
   *
   * @param clusterId Context cluster id.
   * @param userId User name.
   * @param flowName Flow name.
   * @param flowRunId Run Id for the flow.
   * @param appId Application Id.
   * @param entityType Entity type.
   * @param entityId Entity Id.
   * @return byte array with the row key.
   */
  public static byte[] getRowKey(String clusterId, String userId,
      String flowName, Long flowRunId, String appId, String entityType,
      String entityId) {
    return EntityRowKeyConverter.getInstance().encode(new EntityRowKey(
        clusterId, userId, flowName, flowRunId, appId, entityType, entityId));
  }

  /**
   * Given the raw row key as bytes, returns the row key as an object.
   *
   * @param rowKey byte representation of row key.
   * @return An <cite>EntityRowKey</cite> object.
   */
  public static EntityRowKey parseRowKey(byte[] rowKey) {
    return EntityRowKeyConverter.getInstance().decode(rowKey);
  }
}
