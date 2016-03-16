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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.AssignmentInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.List;

@Private
@Unstable
public class CSAssignment {
  public static final CSAssignment NULL_ASSIGNMENT =
      new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);

  public static final CSAssignment SKIP_ASSIGNMENT = new CSAssignment(true);

  private Resource resource;
  private NodeType type;
  private RMContainer excessReservation;
  private FiCaSchedulerApp application;
  private final boolean skipped;
  private boolean fulfilledReservation;
  private final AssignmentInformation assignmentInformation;
  private boolean increaseAllocation;
  private List<RMContainer> containersToKill;

  public CSAssignment(Resource resource, NodeType type) {
    this(resource, type, null, null, false, false);
  }

  public CSAssignment(FiCaSchedulerApp application,
      RMContainer excessReservation) {
    this(excessReservation.getContainer().getResource(), NodeType.NODE_LOCAL,
      excessReservation, application, false, false);
  }

  public CSAssignment(boolean skipped) {
    this(Resource.newInstance(0, 0), NodeType.NODE_LOCAL, null, null, skipped,
      false);
  }

  public CSAssignment(Resource resource, NodeType type,
      RMContainer excessReservation, FiCaSchedulerApp application,
      boolean skipped, boolean fulfilledReservation) {
    this.resource = resource;
    this.type = type;
    this.excessReservation = excessReservation;
    this.application = application;
    this.skipped = skipped;
    this.fulfilledReservation = fulfilledReservation;
    this.assignmentInformation = new AssignmentInformation();
  }

  public Resource getResource() {
    return resource;
  }
  
  public void setResource(Resource resource) {
    this.resource = resource;
  }

  public NodeType getType() {
    return type;
  }
  
  public void setType(NodeType type) {
    this.type = type;
  }
  
  public FiCaSchedulerApp getApplication() {
    return application;
  }

  public void setApplication(FiCaSchedulerApp application) {
    this.application = application;
  }

  public RMContainer getExcessReservation() {
    return excessReservation;
  }

  public void setExcessReservation(RMContainer rmContainer) {
    excessReservation = rmContainer;
  }

  public boolean getSkipped() {
    return skipped;
  }
  
  @Override
  public String toString() {
    String ret = "resource:" + resource.toString();
    ret += "; type:" + type;
    ret += "; excessReservation:" + excessReservation;
    ret +=
        "; applicationid:"
            + (application != null ? application.getApplicationId().toString()
                : "null");
    ret += "; skipped:" + skipped;
    ret += "; fulfilled reservation:" + fulfilledReservation;
    ret +=
        "; allocations(count/resource):"
            + assignmentInformation.getNumAllocations() + "/"
            + assignmentInformation.getAllocated().toString();
    ret +=
        "; reservations(count/resource):"
            + assignmentInformation.getNumReservations() + "/"
            + assignmentInformation.getReserved().toString();
    return ret;
  }
  
  public void setFulfilledReservation(boolean fulfilledReservation) {
    this.fulfilledReservation = fulfilledReservation;
  }

  public boolean isFulfilledReservation() {
    return this.fulfilledReservation;
  }
  
  public AssignmentInformation getAssignmentInformation() {
    return this.assignmentInformation;
  }
  
  public boolean isIncreasedAllocation() {
    return increaseAllocation;
  }

  public void setIncreasedAllocation(boolean flag) {
    increaseAllocation = flag;
  }

  public void setContainersToKill(List<RMContainer> containersToKill) {
    this.containersToKill = containersToKill;
  }

  public List<RMContainer> getContainersToKill() {
    return containersToKill;
  }
}