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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CapacitySchedulerPreemptionUtils {
  public static Map<String, Resource> getResToObtainByPartitionForLeafQueue(
      CapacitySchedulerPreemptionContext context, String queueName,
      Resource clusterResource) {
    Map<String, Resource> resToObtainByPartition = new HashMap<>();
    // compute resToObtainByPartition considered inter-queue preemption
    for (TempQueuePerPartition qT : context.getQueuePartitions(queueName)) {
      if (qT.preemptionDisabled) {
        continue;
      }

      //  Only add resToObtainByPartition when actuallyToBePreempted resource >= 0
      if (Resources.greaterThan(context.getResourceCalculator(),
          clusterResource, qT.actuallyToBePreempted, Resources.none())) {
        resToObtainByPartition.put(qT.partition,
            Resources.clone(qT.actuallyToBePreempted));
      }
    }

    return resToObtainByPartition;
  }

  public static boolean isContainerAlreadySelected(RMContainer container,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates) {
    if (null == selectedCandidates) {
      return false;
    }

    Set<RMContainer> containers = selectedCandidates.get(
        container.getApplicationAttemptId());
    if (containers == null) {
      return false;
    }
    return containers.contains(container);
  }
}
