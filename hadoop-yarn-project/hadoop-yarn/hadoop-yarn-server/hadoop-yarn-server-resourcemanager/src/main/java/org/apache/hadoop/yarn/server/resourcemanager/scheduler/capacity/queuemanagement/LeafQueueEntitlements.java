/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.queuemanagement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueManagementChange;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class LeafQueueEntitlements {
  private final Map<String, QueueCapacities> entitlements = new HashMap<>();

  public QueueCapacities getCapacityOfQueue(AutoCreatedLeafQueue leafQueue) {
    return getCapacityOfQueueByPath(leafQueue.getQueuePath());
  }

  public QueueCapacities getCapacityOfQueueByPath(String leafQueuePath) {
    if (!entitlements.containsKey(leafQueuePath)) {
      entitlements.put(leafQueuePath, new QueueCapacities(false));
    }
    return entitlements.get(leafQueuePath);
  }

  public Map<String, QueueCapacities> getEntitlements() {
    return entitlements;
  }

  public List<QueueManagementChange> mapToQueueManagementChanges(
      BiFunction<String, QueueCapacities, QueueManagementChange> func) {
    return entitlements.entrySet().stream().map(e -> func.apply(e.getKey(), e.getValue()))
        .collect(Collectors.toList());
  }
}
