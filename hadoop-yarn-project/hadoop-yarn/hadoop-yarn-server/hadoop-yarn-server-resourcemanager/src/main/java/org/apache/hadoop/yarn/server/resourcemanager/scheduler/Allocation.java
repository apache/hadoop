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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

public class Allocation {

  final List<Container> containers;
  final Set<ContainerId> strictContainers;
  final Set<ContainerId> fungibleContainers;
  final List<ResourceRequest> fungibleResources;
  final List<NMToken> nmTokens;
  final List<Container> increasedContainers;
  final List<Container> decreasedContainers;
  final List<Container> promotedContainers;
  final List<Container> demotedContainers;
  private final List<Container> previousAttemptContainers;
  private Resource resourceLimit;


  public Allocation(List<Container> containers, Resource resourceLimit,
      Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
      List<ResourceRequest> fungibleResources) {
    this(containers,  resourceLimit,strictContainers,  fungibleContainers,
      fungibleResources, null);
  }

  public Allocation(List<Container> containers, Resource resourceLimit,
      Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
      List<ResourceRequest> fungibleResources, List<NMToken> nmTokens) {
    this(containers,  resourceLimit,strictContainers,  fungibleContainers,
      fungibleResources, nmTokens, null, null, null, null, null);
  }

  public Allocation(List<Container> containers, Resource resourceLimit,
      Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
      List<ResourceRequest> fungibleResources, List<NMToken> nmTokens,
      List<Container> increasedContainers, List<Container> decreasedContainer) {
    this(containers,  resourceLimit,strictContainers,  fungibleContainers,
        fungibleResources, nmTokens, increasedContainers, decreasedContainer,
        null, null, null);
  }

  public Allocation(List<Container> containers, Resource resourceLimit,
      Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
      List<ResourceRequest> fungibleResources, List<NMToken> nmTokens,
      List<Container> increasedContainers, List<Container> decreasedContainer,
      List<Container> promotedContainers, List<Container> demotedContainer,
      List<Container> previousAttemptContainers) {
    this.containers = containers;
    this.resourceLimit = resourceLimit;
    this.strictContainers = strictContainers;
    this.fungibleContainers = fungibleContainers;
    this.fungibleResources = fungibleResources;
    this.nmTokens = nmTokens;
    this.increasedContainers = increasedContainers;
    this.decreasedContainers = decreasedContainer;
    this.promotedContainers = promotedContainers;
    this.demotedContainers = demotedContainer;
    this.previousAttemptContainers = previousAttemptContainers;
  }

  public List<Container> getContainers() {
    return containers;
  }

  public Resource getResourceLimit() {
    return resourceLimit;
  }

  public Set<ContainerId> getStrictContainerPreemptions() {
    return strictContainers;
  }

  public Set<ContainerId> getContainerPreemptions() {
    return fungibleContainers;
  }

  public List<ResourceRequest> getResourcePreemptions() {
    return fungibleResources;
  }

  public List<NMToken> getNMTokens() {
    return nmTokens;
  }
  
  public List<Container> getIncreasedContainers() {
    return increasedContainers;
  }
  
  public List<Container> getDecreasedContainers() {
    return decreasedContainers;
  }

  public List<Container> getPromotedContainers() {
    return promotedContainers;
  }

  public List<Container> getDemotedContainers() {
    return demotedContainers;
  }

  public List<Container> getPreviousAttemptContainers() {
    return previousAttemptContainers;
  }

  @VisibleForTesting
  public void setResourceLimit(Resource resource) {
    this.resourceLimit = resource;
  }

  @Override
  public String toString() {
    return "Allocation{" + "containers=" + containers + ", strictContainers="
        + strictContainers + ", fungibleContainers=" + fungibleContainers
        + ", fungibleResources=" + fungibleResources + ", nmTokens=" + nmTokens
        + ", increasedContainers=" + increasedContainers
        + ", decreasedContainers=" + decreasedContainers
        + ", promotedContainers=" + promotedContainers + ", demotedContainers="
        + demotedContainers + ", previousAttemptContainers="
        + previousAttemptContainers + ", resourceLimit=" + resourceLimit + '}';
  }
}
