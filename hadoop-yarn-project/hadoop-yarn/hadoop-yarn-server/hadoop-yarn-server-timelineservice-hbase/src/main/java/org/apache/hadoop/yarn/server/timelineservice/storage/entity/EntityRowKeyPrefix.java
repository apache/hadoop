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

import org.apache.hadoop.yarn.server.timelineservice.storage.common.RowKeyPrefix;

/**
 * Represents a partial rowkey without the entityId or without entityType and
 * entityId for the entity table.
 *
 */
public class EntityRowKeyPrefix extends EntityRowKey implements
    RowKeyPrefix<EntityRowKey> {

  /**
   * Creates a prefix which generates the following rowKeyPrefixes for the
   * entity table:
   * {@code userName!clusterId!flowName!flowRunId!AppId!entityType!}.
   *
   * @param clusterId identifying the cluster
   * @param userId identifying the user
   * @param flowName identifying the flow
   * @param flowRunId identifying the individual run of this flow
   * @param appId identifying the application
   * @param entityType which entity type
   */
  public EntityRowKeyPrefix(String clusterId, String userId, String flowName,
      Long flowRunId, String appId, String entityType) {
    super(clusterId, userId, flowName, flowRunId, appId, entityType, null);
  }

  /**
   * Creates a prefix which generates the following rowKeyPrefixes for the
   * entity table:
   * {@code userName!clusterId!flowName!flowRunId!AppId!entityType!entityId}.
   *
   * @param clusterId identifying the cluster
   * @param userId identifying the user
   * @param flowName identifying the flow
   * @param flowRunId identifying the individual run of this flow
   * @param appId identifying the application
   */
  public EntityRowKeyPrefix(String clusterId, String userId, String flowName,
      Long flowRunId, String appId) {
    super(clusterId, userId, flowName, flowRunId, appId, null, null);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.application.
   * RowKeyPrefix#getRowKeyPrefix()
   */
  public byte[] getRowKeyPrefix() {
    return super.getRowKey();
  }

}
