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

import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;

@XmlRootElement(name = "clusterMetrics")
@XmlAccessorType(XmlAccessType.FIELD)
public class ClusterMetricsInfo {

  private int appsSubmitted;
  private int appsCompleted;
  private int appsPending;
  private int appsRunning;
  private int appsFailed;
  private int appsKilled;

  private long reservedMB;
  private long availableMB;
  private long allocatedMB;

  private long reservedVirtualCores;
  private long availableVirtualCores;
  private long allocatedVirtualCores;

  private int containersAllocated;
  private int containersReserved;
  private int containersPending;

  private long totalMB;
  private long totalVirtualCores;
  private int totalNodes;
  private int lostNodes;
  private int unhealthyNodes;
  private int decommissioningNodes;
  private int decommissionedNodes;
  private int rebootedNodes;
  private int activeNodes;
  private int shutdownNodes;

  // Total used resource of the cluster, including all partitions
  private ResourceInfo totalUsedResourcesAcrossPartition;

  // Total registered resources of the cluster, including all partitions
  private ResourceInfo totalClusterResourcesAcrossPartition;

  public ClusterMetricsInfo() {
  } // JAXB needs this

  public ClusterMetricsInfo(final ResourceManager rm) {
    this(rm.getResourceScheduler());
  }

  public ClusterMetricsInfo(final ResourceScheduler rs) {
    QueueMetrics metrics = rs.getRootQueueMetrics();
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();

    this.appsSubmitted = metrics.getAppsSubmitted();
    this.appsCompleted = metrics.getAppsCompleted();
    this.appsPending = metrics.getAppsPending();
    this.appsRunning = metrics.getAppsRunning();
    this.appsFailed = metrics.getAppsFailed();
    this.appsKilled = metrics.getAppsKilled();

    this.reservedMB = metrics.getReservedMB();
    this.availableMB = metrics.getAvailableMB();
    this.allocatedMB = metrics.getAllocatedMB();

    this.reservedVirtualCores = metrics.getReservedVirtualCores();
    this.availableVirtualCores = metrics.getAvailableVirtualCores();
    this.allocatedVirtualCores = metrics.getAllocatedVirtualCores();

    this.containersAllocated = metrics.getAllocatedContainers();
    this.containersPending = metrics.getPendingContainers();
    this.containersReserved = metrics.getReservedContainers();

    if (rs instanceof CapacityScheduler) {
      CapacityScheduler cs = (CapacityScheduler) rs;
      this.totalMB = availableMB + allocatedMB + reservedMB;
      this.totalVirtualCores =
          availableVirtualCores + allocatedVirtualCores + reservedVirtualCores;
      // TODO, add support of other schedulers to get total used resources
      // across partition.
      if (cs.getRootQueue() != null
          && cs.getRootQueue().getQueueResourceUsage() != null
          && cs.getRootQueue().getQueueResourceUsage().getAllUsed() != null) {
        totalUsedResourcesAcrossPartition = new ResourceInfo(
            cs.getRootQueue().getQueueResourceUsage().getAllUsed());
        totalClusterResourcesAcrossPartition = new ResourceInfo(
            cs.getClusterResource());
      }
    } else {
      this.totalMB = availableMB + allocatedMB;
      this.totalVirtualCores = availableVirtualCores + allocatedVirtualCores;
    }
    this.activeNodes = clusterMetrics.getNumActiveNMs();
    this.lostNodes = clusterMetrics.getNumLostNMs();
    this.unhealthyNodes = clusterMetrics.getUnhealthyNMs();
    this.decommissioningNodes = clusterMetrics.getNumDecommissioningNMs();
    this.decommissionedNodes = clusterMetrics.getNumDecommisionedNMs();
    this.rebootedNodes = clusterMetrics.getNumRebootedNMs();
    this.shutdownNodes = clusterMetrics.getNumShutdownNMs();
    this.totalNodes = activeNodes + lostNodes + decommissionedNodes
        + rebootedNodes + unhealthyNodes + decommissioningNodes + shutdownNodes;
  }

  public int getAppsSubmitted() {
    return this.appsSubmitted;
  }

  public int getAppsCompleted() {
    return appsCompleted;
  }

  public int getAppsPending() {
    return appsPending;
  }

  public int getAppsRunning() {
    return appsRunning;
  }

  public int getAppsFailed() {
    return appsFailed;
  }

  public int getAppsKilled() {
    return appsKilled;
  }

  public long getReservedMB() {
    return this.reservedMB;
  }

  public long getAvailableMB() {
    return this.availableMB;
  }

  public long getAllocatedMB() {
    return this.allocatedMB;
  }

  public long getReservedVirtualCores() {
    return this.reservedVirtualCores;
  }

  public long getAvailableVirtualCores() {
    return this.availableVirtualCores;
  }

  public long getAllocatedVirtualCores() {
    return this.allocatedVirtualCores;
  }

  public int getContainersAllocated() {
    return this.containersAllocated;
  }

  public int getReservedContainers() {
    return this.containersReserved;
  }

  public int getPendingContainers() {
    return this.containersPending;
  }

  public long getTotalMB() {
    return this.totalMB;
  }

  public long getTotalVirtualCores() {
    return this.totalVirtualCores;
  }

  public int getTotalNodes() {
    return this.totalNodes;
  }

  public int getActiveNodes() {
    return this.activeNodes;
  }

  public int getLostNodes() {
    return this.lostNodes;
  }

  public int getRebootedNodes() {
    return this.rebootedNodes;
  }

  public int getUnhealthyNodes() {
    return this.unhealthyNodes;
  }

  public int getDecommissioningNodes() {
    return this.decommissioningNodes;
  }

  public int getDecommissionedNodes() {
    return this.decommissionedNodes;
  }

  public int getShutdownNodes() {
    return this.shutdownNodes;
  }

  public void setContainersReserved(int containersReserved) {
    this.containersReserved = containersReserved;
  }

  public void setContainersPending(int containersPending) {
    this.containersPending = containersPending;
  }

  public void setAppsSubmitted(int appsSubmitted) {
    this.appsSubmitted = appsSubmitted;
  }

  public void setAppsCompleted(int appsCompleted) {
    this.appsCompleted = appsCompleted;
  }

  public void setAppsPending(int appsPending) {
    this.appsPending = appsPending;
  }

  public void setAppsRunning(int appsRunning) {
    this.appsRunning = appsRunning;
  }

  public void setAppsFailed(int appsFailed) {
    this.appsFailed = appsFailed;
  }

  public void setAppsKilled(int appsKilled) {
    this.appsKilled = appsKilled;
  }

  public void setReservedMB(long reservedMB) {
    this.reservedMB = reservedMB;
  }

  public void setAvailableMB(long availableMB) {
    this.availableMB = availableMB;
  }

  public void setAllocatedMB(long allocatedMB) {
    this.allocatedMB = allocatedMB;
  }

  public void setReservedVirtualCores(long reservedVirtualCores) {
    this.reservedVirtualCores = reservedVirtualCores;
  }

  public void setAvailableVirtualCores(long availableVirtualCores) {
    this.availableVirtualCores = availableVirtualCores;
  }

  public void setAllocatedVirtualCores(long allocatedVirtualCores) {
    this.allocatedVirtualCores = allocatedVirtualCores;
  }

  public void setContainersAllocated(int containersAllocated) {
    this.containersAllocated = containersAllocated;
  }

  public void setTotalMB(long totalMB) {
    this.totalMB = totalMB;
  }

  public void setTotalVirtualCores(long totalVirtualCores) {
    this.totalVirtualCores = totalVirtualCores;
  }

  public void setTotalNodes(int totalNodes) {
    this.totalNodes = totalNodes;
  }

  public void setLostNodes(int lostNodes) {
    this.lostNodes = lostNodes;
  }

  public void setUnhealthyNodes(int unhealthyNodes) {
    this.unhealthyNodes = unhealthyNodes;
  }

  public void setDecommissioningNodes(int decommissioningNodes) {
    this.decommissioningNodes = decommissioningNodes;
  }

  public void setDecommissionedNodes(int decommissionedNodes) {
    this.decommissionedNodes = decommissionedNodes;
  }

  public void setRebootedNodes(int rebootedNodes) {
    this.rebootedNodes = rebootedNodes;
  }

  public void setActiveNodes(int activeNodes) {
    this.activeNodes = activeNodes;
  }

  public void setShutdownNodes(int shutdownNodes) {
    this.shutdownNodes = shutdownNodes;
  }

  public ResourceInfo getTotalUsedResourcesAcrossPartition() {
    return totalUsedResourcesAcrossPartition;
  }

  public ResourceInfo getTotalClusterResourcesAcrossPartition() {
    return totalClusterResourcesAcrossPartition;
  }
}
