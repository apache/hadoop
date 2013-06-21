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

package org.apache.hadoop.yarn.client.api;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.collect.ImmutableList;

@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class AMRMClient<T extends AMRMClient.ContainerRequest> extends
    AbstractService {

  /**
   * Create a new instance of AMRMClient.
   * For usage:
   * <pre>
   * {@code
   * AMRMClient.<T>createAMRMClientContainerRequest(appAttemptId)
   * }</pre>
   * @param appAttemptId the appAttemptId associated with the AMRMClient
   * @return the newly create AMRMClient instance.
   */
  @Public
  public static <T extends ContainerRequest> AMRMClient<T> createAMRMClient(
      ApplicationAttemptId appAttemptId) {
    AMRMClient<T> client = new AMRMClientImpl<T>(appAttemptId);
    return client;
  }

  @Private
  protected AMRMClient(String name) {
    super(name);
  }

  /**
   * Object to represent container request for resources. Scheduler
   * documentation should be consulted for the specifics of how the parameters
   * are honored.
   * All getters return immutable values.
   * 
   * @param capability
   *    The {@link Resource} to be requested for each container.
   * @param nodes
   *    Any hosts to request that the containers are placed on.
   * @param racks
   *    Any racks to request that the containers are placed on. The racks
   *    corresponding to any hosts requested will be automatically added to
   *    this list.
   * @param priority
   *    The priority at which to request the containers. Higher priorities have
   *    lower numerical values.
   * @param containerCount
   *    The number of containers to request.
   */
  public static class ContainerRequest {
    final Resource capability;
    final List<String> nodes;
    final List<String> racks;
    final Priority priority;
    final int containerCount;
        
    public ContainerRequest(Resource capability, String[] nodes,
        String[] racks, Priority priority, int containerCount) {
      this.capability = capability;
      this.nodes = (nodes != null ? ImmutableList.copyOf(nodes) : null);
      this.racks = (racks != null ? ImmutableList.copyOf(racks) : null);
      this.priority = priority;
      this.containerCount = containerCount;
    }
    
    public Resource getCapability() {
      return capability;
    }
    
    public List<String> getNodes() {
      return nodes;
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
    public StoredContainerRequest(Resource capability, String[] nodes,
        String[] racks, Priority priority) {
      super(capability, nodes, racks, priority, 1);
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
  public abstract RegisterApplicationMasterResponse 
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
  public abstract AllocateResponse allocate(float progressIndicator) 
                           throws YarnException, IOException;
  
  /**
   * Unregister the application master. This must be called in the end.
   * @param appStatus Success/Failure status of the master
   * @param appMessage Diagnostics message on failure
   * @param appTrackingUrl New URL to get master info
   * @throws YarnException
   * @throws IOException
   */
  public abstract void unregisterApplicationMaster(FinalApplicationStatus appStatus,
                                           String appMessage,
                                           String appTrackingUrl) 
               throws YarnException, IOException;
  
  /**
   * Request containers for resources before calling <code>allocate</code>
   * @param req Resource request
   */
  public abstract void addContainerRequest(T req);
  
  /**
   * Remove previous container request. The previous container request may have 
   * already been sent to the ResourceManager. So even after the remove request 
   * the app must be prepared to receive an allocation for the previous request 
   * even after the remove request
   * @param req Resource request
   */
  public abstract void removeContainerRequest(T req);
  
  /**
   * Release containers assigned by the Resource Manager. If the app cannot use
   * the container or wants to give up the container then it can release them.
   * The app needs to make new requests for the released resource capability if
   * it still needs it. eg. it released non-local resources
   * @param containerId
   */
  public abstract void releaseAssignedContainer(ContainerId containerId);
  
  /**
   * Get the currently available resources in the cluster.
   * A valid value is available after a call to allocate has been made
   * @return Currently available resources
   */
  public abstract Resource getAvailableResources();
  
  /**
   * Get the current number of nodes in the cluster.
   * A valid values is available after a call to allocate has been made
   * @return Current number of nodes in the cluster
   */
  public abstract int getClusterNodeCount();

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
  public abstract List<? extends Collection<T>> getMatchingRequests(
                                           Priority priority, 
                                           String resourceName, 
                                           Resource capability);
}
