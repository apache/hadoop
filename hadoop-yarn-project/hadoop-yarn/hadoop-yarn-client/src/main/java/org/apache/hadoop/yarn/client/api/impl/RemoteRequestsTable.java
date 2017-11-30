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

package org.apache.hadoop.yarn.client.api.impl;

import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ProfileCapability;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl.ResourceRequestInfo;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl.ProfileCapabilityComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RemoteRequestsTable<T> implements Iterable<ResourceRequestInfo>{

  private static final Logger LOG =
          LoggerFactory.getLogger(RemoteRequestsTable.class);

  private ProfileCapabilityComparator resourceComparator;

  /**
   * Nested Iterator that iterates over just the ResourceRequestInfo
   * object.
   */
  class RequestInfoIterator implements Iterator<ResourceRequestInfo> {
    private Iterator<Map<String, Map<ExecutionType, TreeMap<ProfileCapability,
        ResourceRequestInfo>>>> iLocMap;
    private Iterator<Map<ExecutionType, TreeMap<ProfileCapability,
        ResourceRequestInfo>>> iExecTypeMap;
    private Iterator<TreeMap<ProfileCapability, ResourceRequestInfo>> iCapMap;
    private Iterator<ResourceRequestInfo> iResReqInfo;

    public RequestInfoIterator(Iterator<Map<String,
        Map<ExecutionType, TreeMap<ProfileCapability, ResourceRequestInfo>>>>
        iLocationMap) {
      this.iLocMap = iLocationMap;
      if (iLocMap.hasNext()) {
        iExecTypeMap = iLocMap.next().values().iterator();
      } else {
        iExecTypeMap =
            new LinkedList<Map<ExecutionType, TreeMap<ProfileCapability,
                ResourceRequestInfo>>>().iterator();
      }
      if (iExecTypeMap.hasNext()) {
        iCapMap = iExecTypeMap.next().values().iterator();
      } else {
        iCapMap =
            new LinkedList<TreeMap<ProfileCapability, ResourceRequestInfo>>()
                .iterator();
      }
      if (iCapMap.hasNext()) {
        iResReqInfo = iCapMap.next().values().iterator();
      } else {
        iResReqInfo = new LinkedList<ResourceRequestInfo>().iterator();
      }
    }

    @Override
    public boolean hasNext() {
      return iLocMap.hasNext()
          || iExecTypeMap.hasNext()
          || iCapMap.hasNext()
          || iResReqInfo.hasNext();
    }

    @Override
    public ResourceRequestInfo next() {
      if (!iResReqInfo.hasNext()) {
        if (!iCapMap.hasNext()) {
          if (!iExecTypeMap.hasNext()) {
            iExecTypeMap = iLocMap.next().values().iterator();
          }
          iCapMap = iExecTypeMap.next().values().iterator();
        }
        iResReqInfo = iCapMap.next().values().iterator();
      }
      return iResReqInfo.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported" +
          "for this iterator !!");
    }
  }

  // Nest map with Primary key :
  // Priority -> ResourceName(String) -> ExecutionType -> Capability(Resource)
  // and value : ResourceRequestInfo
  private Map<Priority, Map<String, Map<ExecutionType, TreeMap<ProfileCapability,
      ResourceRequestInfo>>>> remoteRequestsTable = new HashMap<>();

  @Override
  public Iterator<ResourceRequestInfo> iterator() {
    return new RequestInfoIterator(remoteRequestsTable.values().iterator());
  }

  ResourceRequestInfo get(Priority priority, String location,
      ExecutionType execType, ProfileCapability capability) {
    TreeMap<ProfileCapability, ResourceRequestInfo> capabilityMap =
        getCapabilityMap(priority, location, execType);
    if (capabilityMap == null) {
      return null;
    }
    return capabilityMap.get(capability);
  }

  @SuppressWarnings("unchecked")
  void put(Priority priority, String resourceName, ExecutionType execType,
      ProfileCapability capability, ResourceRequestInfo resReqInfo) {
    Map<String, Map<ExecutionType, TreeMap<ProfileCapability,
        ResourceRequestInfo>>> locationMap =
        remoteRequestsTable.get(priority);
    if (locationMap == null) {
      locationMap = new HashMap<>();
      this.remoteRequestsTable.put(priority, locationMap);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Added priority=" + priority);
      }
    }
    Map<ExecutionType, TreeMap<ProfileCapability, ResourceRequestInfo>>
        execTypeMap = locationMap.get(resourceName);
    if (execTypeMap == null) {
      execTypeMap = new HashMap<>();
      locationMap.put(resourceName, execTypeMap);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Added resourceName=" + resourceName);
      }
    }
    TreeMap<ProfileCapability, ResourceRequestInfo> capabilityMap =
        execTypeMap.get(execType);
    if (capabilityMap == null) {
      // this can happen if the user doesn't register with the RM before
      // calling addResourceRequest
      if (resourceComparator == null) {
        resourceComparator = new ProfileCapabilityComparator(new HashMap<>());
      }
      capabilityMap = new TreeMap<>(resourceComparator);
      execTypeMap.put(execType, capabilityMap);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Added Execution Type=" + execType);
      }
    }
    capabilityMap.put(capability, resReqInfo);
  }

  ResourceRequestInfo remove(Priority priority, String resourceName,
      ExecutionType execType, ProfileCapability capability) {
    ResourceRequestInfo retVal = null;
    Map<String, Map<ExecutionType, TreeMap<ProfileCapability,
        ResourceRequestInfo>>> locationMap = remoteRequestsTable.get(priority);
    if (locationMap == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No such priority=" + priority);
      }
      return null;
    }
    Map<ExecutionType, TreeMap<ProfileCapability, ResourceRequestInfo>>
        execTypeMap = locationMap.get(resourceName);
    if (execTypeMap == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No such resourceName=" + resourceName);
      }
      return null;
    }
    TreeMap<ProfileCapability, ResourceRequestInfo> capabilityMap =
        execTypeMap.get(execType);
    if (capabilityMap == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No such Execution Type=" + execType);
      }
      return null;
    }
    retVal = capabilityMap.remove(capability);
    if (capabilityMap.size() == 0) {
      execTypeMap.remove(execType);
      if (execTypeMap.size() == 0) {
        locationMap.remove(resourceName);
        if (locationMap.size() == 0) {
          this.remoteRequestsTable.remove(priority);
        }
      }
    }
    return retVal;
  }

  Map<String, Map<ExecutionType, TreeMap<ProfileCapability,
      ResourceRequestInfo>>> getLocationMap(Priority priority) {
    return remoteRequestsTable.get(priority);
  }

  Map<ExecutionType, TreeMap<ProfileCapability, ResourceRequestInfo>>
      getExecutionTypeMap(Priority priority, String location) {
    Map<String, Map<ExecutionType, TreeMap<ProfileCapability,
        ResourceRequestInfo>>> locationMap = getLocationMap(priority);
    if (locationMap == null) {
      return null;
    }
    return locationMap.get(location);
  }

  TreeMap<ProfileCapability, ResourceRequestInfo> getCapabilityMap(Priority
      priority, String location,
      ExecutionType execType) {
    Map<ExecutionType, TreeMap<ProfileCapability, ResourceRequestInfo>>
        executionTypeMap = getExecutionTypeMap(priority, location);
    if (executionTypeMap == null) {
      return null;
    }
    return executionTypeMap.get(execType);
  }

  @SuppressWarnings("unchecked")
  List<ResourceRequestInfo> getAllResourceRequestInfos(Priority priority,
      Collection<String> locations) {
    List retList = new LinkedList<>();
    for (String location : locations) {
      for (ExecutionType eType : ExecutionType.values()) {
        TreeMap<ProfileCapability, ResourceRequestInfo> capabilityMap =
            getCapabilityMap(priority, location, eType);
        if (capabilityMap != null) {
          retList.addAll(capabilityMap.values());
        }
      }
    }
    return retList;
  }

  List<ResourceRequestInfo> getMatchingRequests(
      Priority priority, String resourceName, ExecutionType executionType,
      ProfileCapability capability) {
    List<ResourceRequestInfo> list = new LinkedList<>();
    TreeMap<ProfileCapability, ResourceRequestInfo> capabilityMap =
        getCapabilityMap(priority, resourceName, executionType);
    if (capabilityMap != null) {
      ResourceRequestInfo resourceRequestInfo = capabilityMap.get(capability);
      if (resourceRequestInfo != null) {
        list.add(resourceRequestInfo);
      } else {
        list.addAll(capabilityMap.tailMap(capability).values());
      }
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  ResourceRequestInfo addResourceRequest(Long allocationRequestId,
      Priority priority, String resourceName, ExecutionTypeRequest execTypeReq,
      ProfileCapability capability, T req, boolean relaxLocality,
      String labelExpression) {
    ResourceRequestInfo resourceRequestInfo =
        get(priority, resourceName, execTypeReq.getExecutionType(), capability);
    if (resourceRequestInfo == null) {
      resourceRequestInfo =
          new ResourceRequestInfo(allocationRequestId, priority, resourceName,
              capability.getProfileCapabilityOverride(), relaxLocality,
              capability.getProfileName());
      put(priority, resourceName, execTypeReq.getExecutionType(), capability,
          resourceRequestInfo);
    }
    resourceRequestInfo.remoteRequest.setExecutionTypeRequest(execTypeReq);
    resourceRequestInfo.remoteRequest.setNumContainers(
        resourceRequestInfo.remoteRequest.getNumContainers() + 1);

    if (relaxLocality) {
      resourceRequestInfo.containerRequests.add(req);
    }

    if (ResourceRequest.ANY.equals(resourceName)) {
      resourceRequestInfo.remoteRequest.setNodeLabelExpression(labelExpression);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding request to ask " + resourceRequestInfo.remoteRequest);
    }
    return resourceRequestInfo;
  }

  ResourceRequestInfo decResourceRequest(Priority priority, String resourceName,
      ExecutionTypeRequest execTypeReq, ProfileCapability capability, T req) {
    ResourceRequestInfo resourceRequestInfo = get(priority, resourceName,
        execTypeReq.getExecutionType(), capability);

    if (resourceRequestInfo == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not decrementing resource as ResourceRequestInfo with" +
            "priority=" + priority + ", " +
            "resourceName=" + resourceName + ", " +
            "executionType=" + execTypeReq + ", " +
            "capability=" + capability + " is not present in request table");
      }
      return null;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("BEFORE decResourceRequest:" + " applicationId="
          + " priority=" + priority.getPriority()
          + " resourceName=" + resourceName + " numContainers="
          + resourceRequestInfo.remoteRequest.getNumContainers());
    }

    resourceRequestInfo.remoteRequest.setNumContainers(
        resourceRequestInfo.remoteRequest.getNumContainers() - 1);

    resourceRequestInfo.containerRequests.remove(req);

    if (resourceRequestInfo.remoteRequest.getNumContainers() < 0) {
      // guard against spurious removals
      resourceRequestInfo.remoteRequest.setNumContainers(0);
    }
    return resourceRequestInfo;
  }

  boolean isEmpty() {
    return remoteRequestsTable.isEmpty();
  }

  @SuppressWarnings("unchecked")
  public void setResourceComparator(ProfileCapabilityComparator comparator) {
    ProfileCapabilityComparator old = this.resourceComparator;
    this.resourceComparator = comparator;
    if (old != null) {
      // we've already set a resource comparator - re-create the maps with the
      // new one. this is needed in case someone adds container requests before
      // registering with the RM. In such a case, the comparator won't have
      // the resource profiles map. After registration, the map is available
      // so re-create the capabilities maps

      for (Map.Entry<Priority, Map<String, Map<ExecutionType,
          TreeMap<ProfileCapability, ResourceRequestInfo>>>>
          priEntry : remoteRequestsTable.entrySet()) {
        for (Map.Entry<String, Map<ExecutionType, TreeMap<ProfileCapability,
            ResourceRequestInfo>>> nameEntry : priEntry.getValue().entrySet()) {
          for (Map.Entry<ExecutionType, TreeMap<ProfileCapability,
              ResourceRequestInfo>> execEntry : nameEntry
              .getValue().entrySet()) {
            Map<ProfileCapability, ResourceRequestInfo> capabilityMap =
                execEntry.getValue();
            TreeMap<ProfileCapability, ResourceRequestInfo> newCapabiltyMap =
                new TreeMap<>(resourceComparator);
            newCapabiltyMap.putAll(capabilityMap);
            execEntry.setValue(newCapabiltyMap);
          }
        }
      }
    }
  }
}
