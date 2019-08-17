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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.Arrays;
import java.util.EnumSet;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

@XmlRootElement
@XmlSeeAlso({ CapacitySchedulerInfo.class, FairSchedulerInfo.class,
  FifoSchedulerInfo.class })
public class SchedulerInfo {
  protected String schedulerName;
  protected ResourceInfo minAllocResource;
  protected ResourceInfo maxAllocResource;
  protected EnumSet<SchedulerResourceTypes> schedulingResourceTypes;
  protected int maximumClusterPriority;

  // JAXB needs this
  public SchedulerInfo() {
  }

  public SchedulerInfo(final ResourceManager rm) {
    ResourceScheduler rs = rm.getResourceScheduler();

    if (rs instanceof CapacityScheduler) {
      this.schedulerName = "Capacity Scheduler";
    } else if (rs instanceof FairScheduler) {
      this.schedulerName = "Fair Scheduler";
    } else if (rs instanceof FifoScheduler) {
      this.schedulerName = "Fifo Scheduler";
    } else {
      this.schedulerName = rs.getClass().getSimpleName();
    }
    this.minAllocResource = new ResourceInfo(rs.getMinimumResourceCapability());
    this.maxAllocResource = new ResourceInfo(rs.getMaximumResourceCapability());
    this.schedulingResourceTypes = rs.getSchedulingResourceTypes();
    this.maximumClusterPriority =
        rs.getMaxClusterLevelAppPriority().getPriority();
  }

  public String getSchedulerType() {
    return this.schedulerName;
  }

  public ResourceInfo getMinAllocation() {
    return this.minAllocResource;
  }

  public ResourceInfo getMaxAllocation() {
    return this.maxAllocResource;
  }

  public String getSchedulerResourceTypes() {
    if (minAllocResource != null) {
      return Arrays.toString(minAllocResource.getResource().getResources());
    }
    return null;
  }

  public int getMaxClusterLevelAppPriority() {
    return this.maximumClusterPriority;
  }
}
