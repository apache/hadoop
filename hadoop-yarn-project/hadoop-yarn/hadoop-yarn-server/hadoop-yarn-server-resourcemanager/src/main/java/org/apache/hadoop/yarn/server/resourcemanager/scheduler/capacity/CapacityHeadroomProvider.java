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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Set;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.Resources;

public class CapacityHeadroomProvider {
  
  UsersManager.User user;
  LeafQueue queue;
  FiCaSchedulerApp application;
  LeafQueue.QueueResourceLimitsInfo queueResourceLimitsInfo;
  
  public CapacityHeadroomProvider(UsersManager.User user, LeafQueue queue,
      FiCaSchedulerApp application,
      LeafQueue.QueueResourceLimitsInfo queueResourceLimitsInfo) {

    this.user = user;
    this.queue = queue;
    this.application = application;
    this.queueResourceLimitsInfo = queueResourceLimitsInfo;
  }
  
  public Resource getHeadroom() {

    Resource queueCurrentLimit;
    Resource clusterResource;
    synchronized (queueResourceLimitsInfo) {
      queueCurrentLimit = queueResourceLimitsInfo.getQueueCurrentLimit();
      clusterResource = queueResourceLimitsInfo.getClusterResource();
    }
    Set<String> requestedPartitions =
        application.getAppSchedulingInfo().getRequestedPartitions();
    Resource headroom;
    if (requestedPartitions.isEmpty() || (requestedPartitions.size() == 1
        && requestedPartitions.contains(RMNodeLabelsManager.NO_LABEL))) {
      headroom = queue.getHeadroom(user, queueCurrentLimit, clusterResource,
          application);
    } else {
      headroom = Resource.newInstance(0, 0);
      for (String partition : requestedPartitions) {
        Resource partitionHeadRoom = queue.getHeadroom(user, queueCurrentLimit,
            clusterResource, application, partition);
        Resources.addTo(headroom, partitionHeadRoom);
      }
    }
    // Corner case to deal with applications being slightly over-limit
    if (headroom.getMemorySize() < 0) {
      headroom.setMemorySize(0);
    }
    return headroom;
  }
}
