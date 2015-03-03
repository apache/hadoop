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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.api.records.Resource;

public class CapacityHeadroomProvider {
  
  LeafQueue.User user;
  LeafQueue queue;
  FiCaSchedulerApp application;
  Resource required;
  LeafQueue.QueueResourceLimitsInfo queueResourceLimitsInfo;
  
  public CapacityHeadroomProvider(
    LeafQueue.User user,
    LeafQueue queue,
    FiCaSchedulerApp application,
    Resource required,
    LeafQueue.QueueResourceLimitsInfo queueResourceLimitsInfo) {
    
    this.user = user;
    this.queue = queue;
    this.application = application;
    this.required = required;
    this.queueResourceLimitsInfo = queueResourceLimitsInfo;
    
  }
  
  public Resource getHeadroom() {
    
    Resource queueCurrentLimit;
    Resource clusterResource;
    synchronized (queueResourceLimitsInfo) {
      queueCurrentLimit = queueResourceLimitsInfo.getQueueCurrentLimit();
      clusterResource = queueResourceLimitsInfo.getClusterResource();
    }
    Resource headroom = queue.getHeadroom(user, queueCurrentLimit, 
      clusterResource, application, required);
    
    // Corner case to deal with applications being slightly over-limit
    if (headroom.getMemory() < 0) {
      headroom.setMemory(0);
    }
    return headroom;
  
  }

}
