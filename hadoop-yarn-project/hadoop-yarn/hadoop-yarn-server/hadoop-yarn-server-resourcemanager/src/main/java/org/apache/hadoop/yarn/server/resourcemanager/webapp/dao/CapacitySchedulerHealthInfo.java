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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@XmlAccessorType(XmlAccessType.FIELD)
public class CapacitySchedulerHealthInfo {

  @XmlAccessorType(XmlAccessType.FIELD)
  public static class OperationInformation {
    String nodeId;
    String containerId;
    String queue;

    OperationInformation() {
    }

    OperationInformation(SchedulerHealth.DetailedInformation di) {
      this.nodeId = di.getNodeId() == null ? "N/A" : di.getNodeId().toString();
      this.containerId =
          di.getContainerId() == null ? "N/A" : di.getContainerId().toString();
      this.queue = di.getQueue() == null ? "N/A" : di.getQueue();
    }

    public String getNodeId() {
      return nodeId;
    }

    public String getContainerId() {
      return containerId;
    }

    public String getQueue() {
      return queue;
    }
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  public static class LastRunDetails {
    String operation;
    long count;
    ResourceInfo resources;

    LastRunDetails() {
    }

    LastRunDetails(String operation, long count, Resource resource) {
      this.operation = operation;
      this.count = count;
      this.resources = new ResourceInfo(resource);
    }

    public String getOperation() {
      return operation;
    }

    public long getCount() {
      return count;
    }

    public ResourceInfo getResources() {
      return resources;
    }
  }

  long lastrun;
  Map<String, OperationInformation> operationsInfo;
  List<LastRunDetails> lastRunDetails;

  CapacitySchedulerHealthInfo() {
  }

  public long getLastrun() {
    return lastrun;
  }

  CapacitySchedulerHealthInfo(CapacityScheduler cs) {
    SchedulerHealth ht = cs.getSchedulerHealth();
    lastrun = ht.getLastSchedulerRunTime();
    operationsInfo = new HashMap<>();
    operationsInfo.put("last-allocation",
      new OperationInformation(ht.getLastAllocationDetails()));
    operationsInfo.put("last-release",
      new OperationInformation(ht.getLastReleaseDetails()));
    operationsInfo.put("last-preemption",
      new OperationInformation(ht.getLastPreemptionDetails()));
    operationsInfo.put("last-reservation",
      new OperationInformation(ht.getLastReservationDetails()));

    lastRunDetails = new ArrayList<>();
    lastRunDetails.add(new LastRunDetails("releases", ht.getReleaseCount(), ht
      .getResourcesReleased()));
    lastRunDetails.add(new LastRunDetails("allocations", ht
      .getAllocationCount(), ht.getResourcesAllocated()));
    lastRunDetails.add(new LastRunDetails("reservations", ht
      .getReservationCount(), ht.getResourcesReserved()));

  }
}
