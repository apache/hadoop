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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.yarn.api.records.Resource;

/**
 * Scheduler should implement this interface if it wants to have multi-threading
 * plus global scheduling functionality
 */
public interface ResourceAllocationCommitter {

  /**
   * Try to commit the allocation Proposal. This also gives the option of
   * not updating a pending queued request.
   * @param cluster Cluster Resource.
   * @param proposal Proposal.
   * @param updatePending Decrement pending if successful.
   * @return Is successful or not.
   */
  boolean tryCommit(Resource cluster, ResourceCommitRequest proposal,
      boolean updatePending);
}
