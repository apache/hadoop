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

import org.apache.hadoop.yarn.server.timelineservice.storage.common.RowKeyPrefix;

/**
 * Represents a partial rowkey (without flowName or without flowName and
 * flowRunId) for the application table.
 */
public class ApplicationRowKeyPrefix extends ApplicationRowKey implements
    RowKeyPrefix<ApplicationRowKey> {

  /**
   * Creates a prefix which generates the following rowKeyPrefixes for the
   * application table: {@code clusterId!userName!flowName!}.
   *
   * @param clusterId the cluster on which applications ran
   * @param userId the user that ran applications
   * @param flowName the name of the flow that was run by the user on the
   *          cluster
   */
  public ApplicationRowKeyPrefix(String clusterId, String userId,
      String flowName) {
    super(clusterId, userId, flowName, null, null);
  }

  /**
   * Creates a prefix which generates the following rowKeyPrefixes for the
   * application table: {@code clusterId!userName!flowName!flowRunId!}.
   *
   * @param clusterId identifying the cluster
   * @param userId identifying the user
   * @param flowName identifying the flow
   * @param flowRunId identifying the instance of this flow
   */
  public ApplicationRowKeyPrefix(String clusterId, String userId,
      String flowName, Long flowRunId) {
    super(clusterId, userId, flowName, flowRunId, null);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.application.
   * RowKeyPrefix#getRowKeyPrefix()
   */
  @Override
  public byte[] getRowKeyPrefix() {
    return super.getRowKey();
  }

}
