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

import org.apache.hadoop.yarn.server.timelineservice.storage.common.RowKeyPrefix;

/**
 * Represents a partial rowkey (without the flowRunId) for the flow run table.
 */
public class FlowRunRowKeyPrefix extends FlowRunRowKey implements
    RowKeyPrefix<FlowRunRowKey> {

  /**
   * Constructs a row key prefix for the flow run table as follows:
   * {@code clusterId!userI!flowName!}.
   *
   * @param clusterId identifying the cluster
   * @param userId identifying the user
   * @param flowName identifying the flow
   */
  public FlowRunRowKeyPrefix(String clusterId, String userId,
      String flowName) {
    super(clusterId, userId, flowName, null);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.application.
   * RowKeyPrefix#getRowKeyPrefix()
   */
  public byte[] getRowKeyPrefix() {
    // We know we're a FlowRunRowKey with null florRunId, so we can simply
    // delegate
    return super.getRowKey();
  }

}
