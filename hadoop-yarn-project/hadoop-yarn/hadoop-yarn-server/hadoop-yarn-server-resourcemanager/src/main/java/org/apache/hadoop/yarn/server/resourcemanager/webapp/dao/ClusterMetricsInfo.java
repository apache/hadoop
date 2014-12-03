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

@XmlRootElement(name = "clusterMetrics")
@XmlAccessorType(XmlAccessType.FIELD)
public class ClusterMetricsInfo {

  protected int appsSubmitted;
  protected int appsCompleted;
  protected int appsPending;
  protected int appsRunning;
  protected int appsFailed;
  protected int appsKilled;

  protected long reservedMB;
  protected long availableMB;
  protected long allocatedMB;

  protected long reservedVirtualCores;
  protected long availableVirtualCores;
  protected long allocatedVirtualCores;

  protected int containersAllocated;
  protected int containersReserved;
  protected int containersPending;

  protected long totalMB;
  protected long totalVirtualCores;
  protected int totalNodes;
  protected int lostNodes;
  protected int unhealthyNodes;
  protected int decommissionedNodes;
  protected int rebootedNodes;
  protected int activeNodes;

  public ClusterMetricsInfo() {
  } // JAXB needs this

  public ClusterMetricsInfo(final ResourceManager rm) {
    ResourceScheduler rs = rm.getResourceScheduler();
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

    this.totalMB = availableMB + allocatedMB;
    this.totalVirtualCores = availableVirtualCores + allocatedVirtualCores;
    this.activeNodes = clusterMetrics.getNumActiveNMs();
    this.lostNodes = clusterMetrics.getNumLostNMs();
    this.unhealthyNodes = clusterMetrics.getUnhealthyNMs();
    this.decommissionedNodes = clusterMetrics.getNumDecommisionedNMs();
    this.rebootedNodes = clusterMetrics.getNumRebootedNMs();
    this.totalNodes = activeNodes + lostNodes + decommissionedNodes
        + rebootedNodes + unhealthyNodes;
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

  public int getDecommissionedNodes() {
    return this.decommissionedNodes;
  }

}
