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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.Resource;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class PartitionClusterMetricsInfo {

  private String partitionName;

  private long totalMB;
  private long totalVirtualCores;

  private long availableMB;
  private long availableVirtualCores;

  private long allocatedMB;
  private long allocatedVirtualCores;

  private long reservedMB;
  private long reservedVirtualCores;

  private long pendingMB;
  private long pendingVirtualCores;

  private int containersAllocated;
  private int containersReserved;
  private int containersPending;

  public PartitionClusterMetricsInfo() {
  }

  public PartitionClusterMetricsInfo(String partitionName, Resource total,
      Resource allocated, Resource reserved, Resource pending,
      Resource available, int containersAllocated, int containersReserved,
      int containersPending) {
    this.partitionName = partitionName;
    this.totalMB = total.getMemorySize();
    this.totalVirtualCores = total.getVirtualCores();
    this.allocatedMB = allocated.getMemorySize();
    this.allocatedVirtualCores = allocated.getVirtualCores();
    this.reservedMB = reserved.getMemorySize();
    this.reservedVirtualCores = reserved.getVirtualCores();
    this.pendingMB = pending.getMemorySize();
    this.pendingVirtualCores = pending.getVirtualCores();
    this.availableMB = available.getMemorySize();
    this.availableVirtualCores = available.getVirtualCores();
    this.containersAllocated = containersAllocated;
    this.containersReserved = containersReserved;
    this.containersPending = containersPending;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public long getTotalMB() {
    return totalMB;
  }

  public long getTotalVirtualCores() {
    return totalVirtualCores;
  }

  public long getAvailableMB() {
    return availableMB;
  }

  public long getAvailableVirtualCores() {
    return availableVirtualCores;
  }

  public long getAllocatedMB() {
    return allocatedMB;
  }

  public long getAllocatedVirtualCores() {
    return allocatedVirtualCores;
  }

  public long getReservedMB() {
    return reservedMB;
  }

  public long getReservedVirtualCores() {
    return reservedVirtualCores;
  }

  public long getPendingMB() {
    return pendingMB;
  }

  public long getPendingVirtualCores() {
    return pendingVirtualCores;
  }

  public int getContainersAllocated() {
    return containersAllocated;
  }

  public int getContainersReserved() {
    return containersReserved;
  }

  public int getContainersPending() {
    return containersPending;
  }
}
