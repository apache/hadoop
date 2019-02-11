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
import java.util.Map;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;

import com.google.common.annotations.VisibleForTesting;

@XmlRootElement(name = "node")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeInfo {

  protected String rack;
  protected NodeState state;
  private String id;
  protected String nodeHostName;
  protected String nodeHTTPAddress;
  private long lastHealthUpdate;
  protected String version;
  protected String healthReport;
  protected int numContainers;
  protected long usedMemoryMB;
  protected long availMemoryMB;
  protected long usedVirtualCores;
  protected long availableVirtualCores;
  private int numRunningOpportContainers;
  private long usedMemoryOpportGB;
  private long usedVirtualCoresOpport;
  private int numQueuedContainers;
  protected ArrayList<String> nodeLabels = new ArrayList<String>();
  private AllocationTagsInfo allocationTags;
  protected ResourceUtilizationInfo resourceUtilization;
  protected ResourceInfo usedResource;
  protected ResourceInfo availableResource;
  protected NodeAttributesInfo nodeAttributesInfo;
  private ResourceInfo totalResource;

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
      this.availableVirtualCores =
          report.getAvailableResource().getVirtualCores();
      this.usedResource = new ResourceInfo(report.getUsedResource());
      this.availableResource = new ResourceInfo(report.getAvailableResource());
    }
    this.id = id.toString();
    this.rack = ni.getRackName();
    this.nodeHostName = ni.getHostName();
    this.state = ni.getState();
    this.nodeHTTPAddress = ni.getHttpAddress();
    this.lastHealthUpdate = ni.getLastHealthReportTime();
    this.healthReport = String.valueOf(ni.getHealthReport());
    this.version = ni.getNodeManagerVersion();
    this.totalResource = new ResourceInfo(ni.getTotalCapability());

    // Status of opportunistic containers.
    this.numRunningOpportContainers = 0;
    this.usedMemoryOpportGB = 0;
    this.usedVirtualCoresOpport = 0;
    this.numQueuedContainers = 0;
    OpportunisticContainersStatus opportStatus =
        ni.getOpportunisticContainersStatus();
    if (opportStatus != null) {
      this.numRunningOpportContainers =
          opportStatus.getRunningOpportContainers();
      this.usedMemoryOpportGB = opportStatus.getOpportMemoryUsed();
      this.usedVirtualCoresOpport = opportStatus.getOpportCoresUsed();
      this.numQueuedContainers = opportStatus.getQueuedOpportContainers();
    }

    // add labels
    Set<String> labelSet = ni.getNodeLabels();
    if (labelSet != null) {
      nodeLabels.addAll(labelSet);
      Collections.sort(nodeLabels);
    }

    // add attributes
    Set<NodeAttribute> attrs = ni.getAllNodeAttributes();
    nodeAttributesInfo = new NodeAttributesInfo();
    for (NodeAttribute attribute : attrs) {
      NodeAttributeInfo info = new NodeAttributeInfo(attribute);
      this.nodeAttributesInfo.addNodeAttributeInfo(info);
    }

    // add allocation tags
    allocationTags = new AllocationTagsInfo();
    Map<String, Long> allocationTagsInfo = ni.getAllocationTagsWithCount();
    if (allocationTagsInfo != null) {
      allocationTagsInfo.forEach((tag, count) ->
          allocationTags.addAllocationTag(new AllocationTagInfo(tag, count)));
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

  public int getNumRunningOpportContainers() {
    return numRunningOpportContainers;
  }

  public long getUsedMemoryOpportGB() {
    return usedMemoryOpportGB;
  }

  public long getUsedVirtualCoresOpport() {
    return usedVirtualCoresOpport;
  }

  public int getNumQueuedContainers() {
    return numQueuedContainers;
  }

  public ArrayList<String> getNodeLabels() {
    return this.nodeLabels;
  }

  public ResourceInfo getUsedResource() {
    return usedResource;
  }

  public void setUsedResource(ResourceInfo used) {
    this.usedResource = used;
  }

  public ResourceInfo getAvailableResource() {
    return availableResource;
  }

  public void setAvailableResource(ResourceInfo avail) {
    this.availableResource = avail;
  }

  public ResourceUtilizationInfo getResourceUtilization() {
    return this.resourceUtilization;
  }

  public String getAllocationTagsSummary() {
    return this.allocationTags == null ? "" :
        this.allocationTags.toString();
  }

  @VisibleForTesting
  public void setId(String id) {
    this.id = id;
  }

  @VisibleForTesting
  public void setLastHealthUpdate(long lastHealthUpdate) {
    this.lastHealthUpdate = lastHealthUpdate;
  }

  public void setTotalResource(ResourceInfo total) {
    this.totalResource = total;
  }

  public ResourceInfo getTotalResource() {
    return this.totalResource;
  }
}
