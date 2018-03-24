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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

public class QueuePath {

  private String parentQueue;
  private String leafQueue;

  public QueuePath(final String leafQueue) {
    this.leafQueue = leafQueue;
  }

  public QueuePath(final String parentQueue, final String leafQueue) {
    this.parentQueue = parentQueue;
    this.leafQueue = leafQueue;
  }

  public String getParentQueue() {
    return parentQueue;
  }

  public String getLeafQueue() {
    return leafQueue;
  }

  public boolean hasParentQueue() {
    return parentQueue != null;
  }

  @Override
  public String toString() {
    return parentQueue + DOT + leafQueue;
  }
}
