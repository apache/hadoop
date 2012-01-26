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
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

@XmlRootElement(name = "clusterMetrics")
@XmlAccessorType(XmlAccessType.FIELD)
public class ClusterMetricsInfo {

  private static final long MB_IN_GB = 1024;

  protected int appsSubmitted;
  protected long reservedMB;
  protected long availableMB;
  protected long allocatedMB;
  protected int containersAllocated;
  protected long totalMB;
  protected int totalNodes;
  protected int lostNodes;
  protected int unhealthyNodes;
  protected int decommissionedNodes;
  protected int rebootedNodes;
  protected int activeNodes;

  public ClusterMetricsInfo() {
  } // JAXB needs this

  public ClusterMetricsInfo(final ResourceManager rm, final RMContext rmContext) {
    ResourceScheduler rs = rm.getResourceScheduler();
    QueueMetrics metrics = rs.getRootQueueMetrics();
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();

    this.appsSubmitted = metrics.getAppsSubmitted();
    this.reservedMB = metrics.getReservedGB() * MB_IN_GB;
    this.availableMB = metrics.getAvailableGB() * MB_IN_GB;
    this.allocatedMB = metrics.getAllocatedGB() * MB_IN_GB;
    this.containersAllocated = metrics.getAllocatedContainers();
    this.totalMB = availableMB + reservedMB + allocatedMB;
    this.activeNodes = clusterMetrics.getNumActiveNMs();
    this.lostNodes = clusterMetrics.getNumLostNMs();
    this.unhealthyNodes = clusterMetrics.getUnhealthyNMs();
    this.decommissionedNodes = clusterMetrics.getNumDecommisionedNMs();
    this.rebootedNodes = clusterMetrics.getNumRebootedNMs();
    this.totalNodes = activeNodes + lostNodes + decommissionedNodes
        + rebootedNodes;
  }

  public int getAppsSubmitted() {
    return this.appsSubmitted;
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

  public int getContainersAllocated() {
    return this.containersAllocated;
  }

  public long getTotalMB() {
    return this.totalMB;
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
