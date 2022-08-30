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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Determines how to schedule opportunistic containers at the NodeManager,
 * i.e., whether or not to accept, queue, or reject a container run request.
 */
public enum OpportunisticContainersQueuePolicy {
  /**
   * Determines whether or not to run a container by the queue capacity:
   * {@link YarnConfiguration#NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH}.
   * If there's enough capacity in the queue,
   * queues the container, otherwise rejects it.
   */
  BY_QUEUE_LEN,
  /**
   * Determines whether or not to run a container based on the amount of
   * resource capacity the node has.
   * Sums up the resources running + already queued at the node, compares
   * it with the total capacity of the node, and accepts the new container only
   * if the computed resources above + resources used by the container
   * is less than or equal to the node capacity.
   */
  BY_RESOURCES;

  public static final OpportunisticContainersQueuePolicy DEFAULT = BY_QUEUE_LEN;
}
