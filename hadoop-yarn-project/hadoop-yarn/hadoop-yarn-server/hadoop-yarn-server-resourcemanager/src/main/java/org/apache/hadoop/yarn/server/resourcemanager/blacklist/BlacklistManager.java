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

package org.apache.hadoop.yarn.server.resourcemanager.blacklist;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;

/**
 * Tracks blacklists based on failures reported on nodes.
 */
@Private
public interface BlacklistManager {

  /**
   * Report failure of a container on node.
   * @param node that has a container failure
   */
  void addNode(String node);

  /**
   * Get {@link ResourceBlacklistRequest} that indicate which nodes should be
   * added or to removed from the blacklist.
   * @return {@link ResourceBlacklistRequest}
   */
  ResourceBlacklistRequest getBlacklistUpdates();

  /**
   * Refresh the number of NodeManagers available for scheduling.
   * @param nodeHostCount is the number of node hosts.
   */
  void refreshNodeHostCount(int nodeHostCount);
}
