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

import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Set;

/**
 * Simple class representing a resource request.
 */
@XmlRootElement(name = "resourceRequests")
@XmlAccessorType(XmlAccessType.FIELD)
public class ResourceRequestInfo {

  @XmlElement(name = "priority")
  private int priority;
  @XmlElement(name = "allocationRequestId")
  private long allocationRequestId;
  @XmlElement(name = "resourceName")
  private String resourceName;
  @XmlElement(name = "capability")
  private ResourceInfo capability;
  @XmlElement(name = "numContainers")
  private int numContainers;
  @XmlElement(name = "relaxLocality")
  private boolean relaxLocality;
  @XmlElement(name = "nodeLabelExpression")
  private String nodeLabelExpression;

  @XmlElement(name = "executionTypeRequest")
  private ExecutionTypeRequestInfo executionTypeRequest;

  @XmlElement(name = "placementConstraint")
  private String placementConstraint;
  @XmlElement(name = "allocationTags")
  private Set<String> allocationTags;

  public ResourceRequestInfo() {
  }

  public ResourceRequestInfo(ResourceRequest request) {
    priority = request.getPriority().getPriority();
    allocationRequestId = request.getAllocationRequestId();
    resourceName = request.getResourceName();
    capability = new ResourceInfo(request.getCapability());
    numContainers = request.getNumContainers();
    relaxLocality = request.getRelaxLocality();
    nodeLabelExpression = request.getNodeLabelExpression();
    if (request.getExecutionTypeRequest() != null) {
      executionTypeRequest =
          new ExecutionTypeRequestInfo(request.getExecutionTypeRequest());
    }
  }

  public ResourceRequestInfo(SchedulingRequest request) {
    priority = request.getPriority().getPriority();
    allocationRequestId = request.getAllocationRequestId();
    capability = new ResourceInfo(request.getResourceSizing().getResources());
    numContainers = request.getResourceSizing().getNumAllocations();
    if (request.getExecutionType() != null) {
      executionTypeRequest =
          new ExecutionTypeRequestInfo(request.getExecutionType());
    }
    allocationTags = request.getAllocationTags();
    if (request.getPlacementConstraint() != null) {
      placementConstraint = request.getPlacementConstraint().toString();
    }
  }

  public Priority getPriority() {
    return Priority.newInstance(priority);
  }

  public void setPriority(Priority priority) {
    this.priority = priority.getPriority();
  }

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public ResourceInfo getCapability() {
    return capability;
  }

  public void setCapability(ResourceInfo capability) {
    this.capability = capability;
  }

  public int getNumContainers() {
    return numContainers;
  }

  public void setNumContainers(int numContainers) {
    this.numContainers = numContainers;
  }

  public boolean getRelaxLocality() {
    return relaxLocality;
  }

  public void setRelaxLocality(boolean relaxLocality) {
    this.relaxLocality = relaxLocality;
  }

  public String getNodeLabelExpression() {
    return nodeLabelExpression;
  }

  public void setNodeLabelExpression(String nodeLabelExpression) {
    this.nodeLabelExpression = nodeLabelExpression;
  }

  public void setExecutionTypeRequest(
      ExecutionTypeRequest executionTypeRequest) {
    this.executionTypeRequest =
        new ExecutionTypeRequestInfo(executionTypeRequest);
  }

  public ExecutionTypeRequestInfo getExecutionTypeRequest() {
    return executionTypeRequest;
  }

  public String getPlacementConstraint() {
    return placementConstraint;
  }

  public void setPlacementConstraint(String placementConstraint) {
    this.placementConstraint = placementConstraint;
  }

  public Set<String> getAllocationTags() {
    return allocationTags;
  }

  public void setAllocationTags(Set<String> allocationTags) {
    this.allocationTags = allocationTags;
  }

  public long getAllocationRequestId() {
    return allocationRequestId;
  }

  public void setAllocationRequestId(long allocationRequestId) {
    this.allocationRequestId = allocationRequestId;
  }
}
