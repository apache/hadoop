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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement(name = "scheduler")
@XmlAccessorType(XmlAccessType.FIELD)
public class SchedulerOverviewInfo {

  private String schedulerType;
  private String schedulingResourceType;
  private ResourceInfo minimumAllocation;
  private ResourceInfo maximumAllocation;
  private int applicationPriority;
  private int schedulerBusy;
  private int rmDispatcherEventQueueSize;
  private int schedulerDispatcherEventQueueSize;

  // JAXB needs this
  public SchedulerOverviewInfo() {

  }

  public SchedulerOverviewInfo(ResourceScheduler rs) {
    // Parse the schedule type
    this.schedulerType = getSchedulerName(rs);

    // Parse and allocate resource information
    this.minimumAllocation = new ResourceInfo(rs.getMinimumResourceCapability());
    this.maximumAllocation = new ResourceInfo(rs.getMaximumResourceCapability());

    // Parse App Priority
    this.applicationPriority = rs.getMaxClusterLevelAppPriority().getPriority();

    // Resolving resource types
    List<ResourceTypeInfo> resourceTypeInfos = ResourceUtils.getResourcesTypeInfo();
    resourceTypeInfos.sort((o1, o2) -> o1.getName().compareToIgnoreCase(o2.getName()));
    this.schedulingResourceType = StringUtils.join(resourceTypeInfos, ",");

    // clusterMetrics
    ClusterMetricsInfo clusterMetrics = new ClusterMetricsInfo(rs);
    this.schedulerBusy = clusterMetrics.getRmSchedulerBusyPercent();
    this.rmDispatcherEventQueueSize = clusterMetrics.getRmEventQueueSize();
    this.schedulerDispatcherEventQueueSize = clusterMetrics.getSchedulerEventQueueSize();
  }

  private static String getSchedulerName(ResourceScheduler rs) {
    if (rs instanceof CapacityScheduler) {
      return "Capacity Scheduler";
    }
    if (rs instanceof FairScheduler) {
      return "Fair Scheduler";
    }
    if (rs instanceof FifoScheduler) {
      return "Fifo Scheduler";
    }
    return rs.getClass().getSimpleName();
  }

  public String getSchedulerType() {
    return schedulerType;
  }

  public String getSchedulingResourceType() {
    return schedulingResourceType;
  }

  public ResourceInfo getMinimumAllocation() {
    return minimumAllocation;
  }

  public ResourceInfo getMaximumAllocation() {
    return maximumAllocation;
  }

  public int getApplicationPriority() {
    return applicationPriority;
  }

  public int getSchedulerBusy() {
    return schedulerBusy;
  }

  public int getRmDispatcherEventQueueSize() {
    return rmDispatcherEventQueueSize;
  }

  public int getSchedulerDispatcherEventQueueSize() {
    return schedulerDispatcherEventQueueSize;
  }
}
