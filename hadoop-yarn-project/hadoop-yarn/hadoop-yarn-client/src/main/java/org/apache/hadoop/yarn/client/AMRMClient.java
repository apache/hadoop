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

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.Service;

import com.google.common.collect.ImmutableList;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface AMRMClient<T extends AMRMClient.ContainerRequest> extends Service {

  /**
   * Object to represent container request for resources.
   * Resources may be localized to nodes and racks.
   * Resources may be assigned priorities.
   * All getters return unmodifiable collections.
   * Can ask for multiple containers of a given type.
   */
  public static class ContainerRequest {
    final Resource capability;
    final ImmutableList<String> hosts;
    final ImmutableList<String> racks;
    final Priority priority;
    final int containerCount;
        
    public ContainerRequest(Resource capability, String[] hosts,
        String[] racks, Priority priority, int containerCount) {
      this.capability = capability;
      this.hosts = (hosts != null ? ImmutableList.copyOf(hosts) : null);
      this.racks = (racks != null ? ImmutableList.copyOf(racks) : null);
      this.priority = priority;
      this.containerCount = containerCount;
    }
    
    public Resource getCapability() {
      return capability;
    }
    
    public List<String> getHosts() {
      return hosts;
    }
    
    public List<String> getRacks() {
      return racks;
    }
    
    public Priority getPriority() {
      return priority;
    }
    
    public int getContainerCount() {
      return containerCount;
    }
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Capability[").append(capability).append("]");
      sb.append("Priority[").append(priority).append("]");
      sb.append("ContainerCount[").append(containerCount).append("]");
      return sb.toString();
    }
  }
 
  /**
   * This creates a <code>ContainerRequest</code> for 1 container and the
   * AMRMClient stores this request internally. <code>getMatchingRequests</code>
   * can be used to retrieve these requests from AMRMClient. These requests may 
   * be matched with an allocated container to determine which request to assign
   * the container to. <code>removeContainerRequest</code> must be called using 
   * the same assigned <code>StoredContainerRequest</code> object so that 
   * AMRMClient can remove it from its internal store.
   */
  public static class StoredContainerRequest extends ContainerRequest {    
    public StoredContainerRequest(Resource capability, String[] hosts,
        String[] racks, Priority priority) {
      super(capability, hosts, racks, priority, 1);
    }
  }
  
  /**
   * Register the application master. This must be called before any 
   * other interaction
   * @param appHostName Name of the host on which master is running
   * @param appHostPort Port master is listening on
   * @param appTrackingUrl URL at which the master info can be seen
   * @return <code>RegisterApplicationMasterResponse</code>
   * @throws YarnException
   * @throws IOException
   */
  public RegisterApplicationMasterResponse 
               registerApplicationMaster(String appHostName,
                                         int appHostPort,
                                         String appTrackingUrl) 
               throws YarnException, IOException;
  
  /**
   * Request additional containers and receive new container allocations.
   * Requests made via <code>addContainerRequest</code> are sent to the 
   * <code>ResourceManager</code>. New containers assigned to the master are 
   * retrieved. Status of completed containers and node health updates are 
   * also retrieved.
   * This also doubles up as a heartbeat to the ResourceManager and must be 
   * made periodically.
   * The call may not always return any new allocations of containers.
   * App should not make concurrent allocate requests. May cause request loss.
   * @param progressIndicator Indicates progress made by the master
   * @return the response of the allocate request
   * @throws YarnException
   * @throws IOException
   */
  public AllocateResponse allocate(float progressIndicator) 
                           throws YarnException, IOException;
  
  /**
   * Unregister the application master. This must be called in the end.
   * @param appStatus Success/Failure status of the master
   * @param appMessage Diagnostics message on failure
   * @param appTrackingUrl New URL to get master info
   * @throws YarnException
   * @throws IOException
   */
  public void unregisterApplicationMaster(FinalApplicationStatus appStatus,
                                           String appMessage,
                                           String appTrackingUrl) 
               throws YarnException, IOException;
  
  /**
   * Request containers for resources before calling <code>allocate</code>
   * @param req Resource request
   */
  public void addContainerRequest(T req);
  
  /**
   * Remove previous container request. The previous container request may have 
   * already been sent to the ResourceManager. So even after the remove request 
   * the app must be prepared to receive an allocation for the previous request 
   * even after the remove request
   * @param req Resource request
   */
  public void removeContainerRequest(T req);
  
  /**
   * Release containers assigned by the Resource Manager. If the app cannot use
   * the container or wants to give up the container then it can release them.
   * The app needs to make new requests for the released resource capability if
   * it still needs it. eg. it released non-local resources
   * @param containerId
   */
  public void releaseAssignedContainer(ContainerId containerId);
  
  /**
   * Get the currently available resources in the cluster.
   * A valid value is available after a call to allocate has been made
   * @return Currently available resources
   */
  public Resource getClusterAvailableResources();
  
  /**
   * Get the current number of nodes in the cluster.
   * A valid values is available after a call to allocate has been made
   * @return Current number of nodes in the cluster
   */
  public int getClusterNodeCount();

  /**
   * Get outstanding <code>StoredContainerRequest</code>s matching the given 
   * parameters. These StoredContainerRequests should have been added via
   * <code>addContainerRequest</code> earlier in the lifecycle. For performance,
   * the AMRMClient may return its internal collection directly without creating 
   * a copy. Users should not perform mutable operations on the return value.
   * Each collection in the list contains requests with identical 
   * <code>Resource</code> size that fit in the given capability. In a 
   * collection, requests will be returned in the same order as they were added.
   * @return Collection of request matching the parameters
   */
  public List<? extends Collection<T>> getMatchingRequests(
                                           Priority priority, 
                                           String resourceName, 
                                           Resource capability);

}
