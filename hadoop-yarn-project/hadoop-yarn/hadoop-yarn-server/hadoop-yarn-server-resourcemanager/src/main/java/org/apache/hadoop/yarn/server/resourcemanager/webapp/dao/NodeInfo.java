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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;

@XmlRootElement(name = "node")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeInfo {

  protected String rack;
  protected NodeState state;
  protected String id;
  protected String nodeHostName;
  protected String nodeHTTPAddress;
  protected long lastHealthUpdate;
  protected String version;
  protected String healthReport;
  protected int numContainers;
  protected long usedMemoryMB;
  protected long availMemoryMB;
  protected long usedVirtualCores;
  protected long availableVirtualCores;
  protected ArrayList<String> nodeLabels = new ArrayList<String>();
  protected ResourceUtilizationInfo resourceUtilization;

  public NodeInfo() {
  } // JAXB needs this

  public NodeInfo(RMNode ni, ResourceScheduler sched) {
    NodeId id = ni.getNodeID();
    SchedulerNodeReport report = sched.getNodeReport(id);
    this.numContainers = 0;
    this.usedMemoryMB = 0;
    this.availMemoryMB = 0;
    if (report != null) {
      this.numContainers = report.getNumContainers();
      this.usedMemoryMB = report.getUsedResource().getMemorySize();
      this.availMemoryMB = report.getAvailableResource().getMemorySize();
      this.usedVirtualCores = report.getUsedResource().getVirtualCores();
      this.availableVirtualCores = report.getAvailableResource().getVirtualCores();
    }
    this.id = id.toString();
    this.rack = ni.getRackName();
    this.nodeHostName = ni.getHostName();
    this.state = ni.getState();
    this.nodeHTTPAddress = ni.getHttpAddress();
    this.lastHealthUpdate = ni.getLastHealthReportTime();
    this.healthReport = String.valueOf(ni.getHealthReport());
    this.version = ni.getNodeManagerVersion();
    
    // add labels
    Set<String> labelSet = ni.getNodeLabels();
    if (labelSet != null) {
      nodeLabels.addAll(labelSet);
      Collections.sort(nodeLabels);
    }

    // update node and containers resource utilization
    this.resourceUtilization = new ResourceUtilizationInfo(ni);
  }

  public String getRack() {
    return this.rack;
  }

  public String getState() {
    return String.valueOf(this.state);
  }

  public String getNodeId() {
    return this.id;
  }

  public String getNodeHTTPAddress() {
    return this.nodeHTTPAddress;
  }

  public void setNodeHTTPAddress(String nodeHTTPAddress) {
    this.nodeHTTPAddress = nodeHTTPAddress;
  }

  public long getLastHealthUpdate() {
    return this.lastHealthUpdate;
  }

  public String getVersion() {
    return this.version;
  }

  public String getHealthReport() {
    return this.healthReport;
  }

  public int getNumContainers() {
    return this.numContainers;
  }

  public long getUsedMemory() {
    return this.usedMemoryMB;
  }

  public long getAvailableMemory() {
    return this.availMemoryMB;
  }

  public long getUsedVirtualCores() {
    return this.usedVirtualCores;
  }

  public long getAvailableVirtualCores() {
    return this.availableVirtualCores;
  }

  public ArrayList<String> getNodeLabels() {
    return this.nodeLabels;
  }

  public ResourceUtilizationInfo getResourceUtilization() {
    return this.resourceUtilization;
  }
}
