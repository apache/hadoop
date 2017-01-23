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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;

import java.util.Iterator;
import java.util.List;

/**
 * This will be used by
 * {@link org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue}
 * to decide allocation ordering of child queues.
 */
public interface QueueOrderingPolicy {
  void setQueues(List<CSQueue> queues);

  /**
   * Return an iterator over the collection of CSQueues which orders
   * them for container assignment.
   *
   * Please note that, to avoid queue's set updated during sorting / iterating.
   * Caller need to make sure parent queue's read lock is properly acquired.
   *
   * @param partition nodePartition
   *
   * @return iterator of queues to allocate
   */
  Iterator<CSQueue> getAssignmentIterator(String partition);

  /**
   * Returns configuration name (which will be used to set ordering policy
   * @return configuration name
   */
  String getConfigName();
}
