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


import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;


/**
 * Abstract temporary data-structure for tracking resource availability,pending
 * resource need, current utilization for app/queue.
 */
public class AbstractPreemptionEntity {
  // Following fields are copied from scheduler
  final String queueName;

  protected final Resource current;
  protected final Resource amUsed;
  protected final Resource reserved;
  protected Resource pending;

  // Following fields are settled and used by candidate selection policies
  Resource idealAssigned;
  Resource toBePreempted;
  Resource selected;
  private Resource actuallyToBePreempted;
  private Resource toBePreemptFromOther;

  AbstractPreemptionEntity(String queueName, Resource usedPerPartition,
      Resource amUsedPerPartition, Resource reserved,
      Resource pendingPerPartition) {
    this.queueName = queueName;
    this.current = usedPerPartition;
    this.pending = pendingPerPartition;
    this.reserved = reserved;
    this.amUsed = amUsedPerPartition;

    this.idealAssigned = Resource.newInstance(0, 0);
    this.actuallyToBePreempted = Resource.newInstance(0, 0);
    this.toBePreempted = Resource.newInstance(0, 0);
    this.toBePreemptFromOther = Resource.newInstance(0, 0);
    this.selected = Resource.newInstance(0, 0);
  }

  public Resource getUsed() {
    return current;
  }

  public Resource getUsedDeductAM() {
    return Resources.subtract(current, amUsed);
  }

  public Resource getAMUsed() {
    return amUsed;
  }

  public Resource getPending() {
    return pending;
  }

  public Resource getReserved() {
    return reserved;
  }

  public Resource getActuallyToBePreempted() {
    return actuallyToBePreempted;
  }

  public void setActuallyToBePreempted(Resource actuallyToBePreempted) {
    this.actuallyToBePreempted = actuallyToBePreempted;
  }

  public Resource getToBePreemptFromOther() {
    return toBePreemptFromOther;
  }

  public void setToBePreemptFromOther(Resource toBePreemptFromOther) {
    this.toBePreemptFromOther = toBePreemptFromOther;
  }

}
