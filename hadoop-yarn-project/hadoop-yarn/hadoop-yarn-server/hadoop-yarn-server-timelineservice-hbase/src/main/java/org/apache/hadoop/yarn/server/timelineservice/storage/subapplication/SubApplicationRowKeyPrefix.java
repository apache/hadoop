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
package org.apache.hadoop.yarn.server.timelineservice.storage.subapplication;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.RowKeyPrefix;

/**
 * Represents a partial rowkey without the entityId or without entityType and
 * entityId for the sub application table.
 *
 */
public class SubApplicationRowKeyPrefix extends SubApplicationRowKey
    implements RowKeyPrefix<SubApplicationRowKey> {

  /**
   * Creates a prefix which generates the following rowKeyPrefixes for the sub
   * application table:
   * {@code subAppUserId!clusterId!entityType!entityPrefix!userId}.
   *
   * @param subAppUserId
   *          identifying the subApp User
   * @param clusterId
   *          identifying the cluster
   * @param entityType
   *          which entity type
   * @param entityIdPrefix
   *          for entityId
   * @param entityId
   *          for an entity
   * @param userId
   *          for the user who runs the AM
   *
   * subAppUserId is usually the doAsUser.
   * userId is the yarn user that the AM runs as.
   *
   */
  public SubApplicationRowKeyPrefix(String subAppUserId, String clusterId,
      String entityType, Long entityIdPrefix, String entityId,
      String userId) {
    super(subAppUserId, clusterId, entityType, entityIdPrefix, entityId,
        userId);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.
   * RowKeyPrefix#getRowKeyPrefix()
   */
  public byte[] getRowKeyPrefix() {
    return super.getRowKey();
  }

}
