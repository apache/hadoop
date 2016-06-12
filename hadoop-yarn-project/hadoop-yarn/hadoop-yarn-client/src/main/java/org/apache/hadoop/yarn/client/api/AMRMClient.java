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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class AMRMClient<T extends AMRMClient.ContainerRequest> extends
    AbstractService {
  private static final Log LOG = LogFactory.getLog(AMRMClient.class);

  /**
   * Create a new instance of AMRMClient.
   * For usage:
   * <pre>
   * {@code
   * AMRMClient.<T>createAMRMClientContainerRequest()
   * }</pre>
   * @return the newly create AMRMClient instance.
   */
  @Public
  public static <T extends ContainerRequest> AMRMClient<T> createAMRMClient() {
    AMRMClient<T> client = new AMRMClientImpl<T>();
    return client;
  }

  private NMTokenCache nmTokenCache;

  @Private
  protected AMRMClient(String name) {
    super(name);
    nmTokenCache = NMTokenCache.getSingleton();
  }

  /**
   * Object to represent a single container request for resources. Scheduler
   * documentation should be consulted for the specifics of how the parameters
   * are honored.
   * 
   * By default, YARN schedulers try to allocate containers at the requested
   * locations but they may relax the constraints in order to expedite meeting
   * allocations limits. They first relax the constraint to the same rack as the
   * requested node and then to anywhere in the cluster. The relaxLocality flag
   * may be used to disable locality relaxation and request containers at only 
   * specific locations. The following conditions apply.
   * <ul>
   * <li>Within a priority, all container requests must have the same value for
   * locality relaxation. Either enabled or disabled.</li>
   * <li>If locality relaxation is disabled, then across requests, locations at
   * different network levels may not be specified. E.g. its invalid to make a
   * request for a specific node and another request for a specific rack.</li>
   * <li>If locality relaxation is disabled, then only within the same request,  
   * a node and its rack may be specified together. This allows for a specific   
   * rack with a preference for a specific node within that rack.</li>
   * <li></li>
   * </ul>
   * To re-enable locality relaxation at a given priority, all pending requests 
   * with locality relaxation disabled must be first removed. Then they can be 
   * added back with locality relaxation enabled.
   * 
   * All getters return immutable values.
   */
  public static class ContainerRequest {
    final Resource capability;
    final List<String> nodes;
    final List<String> racks;
    final Priority priority;
    final boolean relaxLocality;
    final String nodeLabelsExpression;
    final ExecutionTypeRequest executionTypeRequest;
    
    /**
     * Instantiates a {@link ContainerRequest} with the given constraints and
     * locality relaxation enabled.
     * 
     * @param capability
     *          The {@link Resource} to be requested for each container.
     * @param nodes
     *          Any hosts to request that the containers are placed on.
     * @param racks
     *          Any racks to request that the containers are placed on. The
     *          racks corresponding to any hosts requested will be automatically
     *          added to this list.
     * @param priority
     *          The priority at which to request the containers. Higher
     *          priorities have lower numerical values.
     */
    public ContainerRequest(Resource capability, String[] nodes,
        String[] racks, Priority priority) {
      this(capability, nodes, racks, priority, true, null);
    }
    
    /**
     * Instantiates a {@link ContainerRequest} with the given constraints.
     * 
     * @param capability
     *          The {@link Resource} to be requested for each container.
     * @param nodes
     *          Any hosts to request that the containers are placed on.
     * @param racks
     *          Any racks to request that the containers are placed on. The
     *          racks corresponding to any hosts requested will be automatically
     *          added to this list.
     * @param priority
     *          The priority at which to request the containers. Higher
     *          priorities have lower numerical values.
     * @param relaxLocality
     *          If true, containers for this request may be assigned on hosts
     *          and racks other than the ones explicitly requested.
     */
    public ContainerRequest(Resource capability, String[] nodes,
        String[] racks, Priority priority, boolean relaxLocality) {
      this(capability, nodes, racks, priority, relaxLocality, null);
    }

    /**
     * Instantiates a {@link ContainerRequest} with the given constraints.
     *
     * @param capability
     *          The {@link Resource} to be requested for each container.
     * @param nodes
     *          Any hosts to request that the containers are placed on.
     * @param racks
     *          Any racks to request that the containers are placed on. The
     *          racks corresponding to any hosts requested will be automatically
     *          added to this list.
     * @param priority
     *          The priority at which to request the containers. Higher
     *          priorities have lower numerical values.
     * @param relaxLocality
     *          If true, containers for this request may be assigned on hosts
     *          and racks other than the ones explicitly requested.
     * @param nodeLabelsExpression
     *          Set node labels to allocate resource, now we only support
     *          asking for only a single node label
     */
    public ContainerRequest(Resource capability, String[] nodes, String[] racks,
        Priority priority, boolean relaxLocality, String nodeLabelsExpression) {
      this(capability, nodes, racks, priority, relaxLocality,
          nodeLabelsExpression,
          ExecutionTypeRequest.newInstance());
    }
          
    /**
     * Instantiates a {@link ContainerRequest} with the given constraints.
     * 
     * @param capability
     *          The {@link Resource} to be requested for each container.
     * @param nodes
     *          Any hosts to request that the containers are placed on.
     * @param racks
     *          Any racks to request that the containers are placed on. The
     *          racks corresponding to any hosts requested will be automatically
     *          added to this list.
     * @param priority
     *          The priority at which to request the containers. Higher
     *          priorities have lower numerical values.
     * @param relaxLocality
     *          If true, containers for this request may be assigned on hosts
     *          and racks other than the ones explicitly requested.
     * @param nodeLabelsExpression
     *          Set node labels to allocate resource, now we only support
     *          asking for only a single node label
     * @param executionTypeRequest
     *          Set the execution type of the container request.
     */
    public ContainerRequest(Resource capability, String[] nodes, String[] racks,
        Priority priority, boolean relaxLocality, String nodeLabelsExpression,
        ExecutionTypeRequest executionTypeRequest) {
      // Validate request
      Preconditions.checkArgument(capability != null,
          "The Resource to be requested for each container " +
              "should not be null ");
      Preconditions.checkArgument(priority != null,
          "The priority at which to request containers should not be null ");
      Preconditions.checkArgument(
              !(!relaxLocality && (racks == null || racks.length == 0) 
                  && (nodes == null || nodes.length == 0)),
              "Can't turn off locality relaxation on a " + 
              "request with no location constraints");
      this.capability = capability;
      this.nodes = (nodes != null ? ImmutableList.copyOf(nodes) : null);
      this.racks = (racks != null ? ImmutableList.copyOf(racks) : null);
      this.priority = priority;
      this.relaxLocality = relaxLocality;
      this.nodeLabelsExpression = nodeLabelsExpression;
      this.executionTypeRequest = executionTypeRequest;
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
    
    public boolean getRelaxLocality() {
      return relaxLocality;
    }
    
    public String getNodeLabelExpression() {
      return nodeLabelsExpression;
    }
    
    public ExecutionTypeRequest getExecutionTypeRequest() {
      return executionTypeRequest;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Capability[").append(capability).append("]");
      sb.append("Priority[").append(priority).append("]");
      sb.append("ExecutionTypeRequest[").append(executionTypeRequest)
          .append("]");
      return sb.toString();
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
   * retrieved. Status of completed containers and node health updates are also
   * retrieved. This also doubles up as a heartbeat to the ResourceManager and
   * must be made periodically. The call may not always return any new
   * allocations of containers. App should not make concurrent allocate
   * requests. May cause request loss.
   * 
   * <p>
   * Note : If the user has not removed container requests that have already
   * been satisfied, then the re-register may end up sending the entire
   * container requests to the RM (including matched requests). Which would mean
   * the RM could end up giving it a lot of new allocated containers.
   * </p>
   * 
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
   * Request container resource change before calling <code>allocate</code>.
   * Any previous pending resource change request of the same container will be
   * removed.
   *
   * Application that calls this method is expected to maintain the
   * <code>Container</code>s that are returned from previous successful
   * allocations or resource changes. By passing in the existing container and a
   * target resource capability to this method, the application requests the
   * ResourceManager to change the existing resource allocation to the target
   * resource allocation.
   *
   * @param container The container returned from the last successful resource
   *                  allocation or resource change
   * @param capability  The target resource capability of the container
   */
  public abstract void requestContainerResourceChange(
      Container container, Resource capability);

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
   * Get outstanding <code>ContainerRequest</code>s matching the given 
   * parameters. These ContainerRequests should have been added via
   * <code>addContainerRequest</code> earlier in the lifecycle. For performance,
   * the AMRMClient may return its internal collection directly without creating 
   * a copy. Users should not perform mutable operations on the return value.
   * Each collection in the list contains requests with identical 
   * <code>Resource</code> size that fit in the given capability. In a 
   * collection, requests will be returned in the same order as they were added.
   * @return Collection of request matching the parameters
   */
  @InterfaceStability.Evolving
  public abstract List<? extends Collection<T>> getMatchingRequests(
                                           Priority priority, 
                                           String resourceName, 
                                           Resource capability);

  /**
   * Get outstanding <code>ContainerRequest</code>s matching the given
   * parameters. These ContainerRequests should have been added via
   * <code>addContainerRequest</code> earlier in the lifecycle. For performance,
   * the AMRMClient may return its internal collection directly without creating
   * a copy. Users should not perform mutable operations on the return value.
   * Each collection in the list contains requests with identical
   * <code>Resource</code> size that fit in the given capability. In a
   * collection, requests will be returned in the same order as they were added.
   * specify an <code>ExecutionType</code> .
   * @param priority Priority
   * @param resourceName Location
   * @param executionType ExecutionType
   * @param capability Capability
   * @return Collection of request matching the parameters
   */
  @InterfaceStability.Evolving
  public List<? extends Collection<T>> getMatchingRequests(
      Priority priority, String resourceName, ExecutionType executionType,
      Resource capability) {
    throw new UnsupportedOperationException("The sub-class extending" +
        " AMRMClient is expected to implement this !!");
  }
  
  /**
   * Update application's blacklist with addition or removal resources.
   * 
   * @param blacklistAdditions list of resources which should be added to the 
   *        application blacklist
   * @param blacklistRemovals list of resources which should be removed from the 
   *        application blacklist
   */
  public abstract void updateBlacklist(List<String> blacklistAdditions,
      List<String> blacklistRemovals);

  /**
   * Set the NM token cache for the <code>AMRMClient</code>. This cache must
   * be shared with the {@link NMClient} used to manage containers for the
   * <code>AMRMClient</code>
   * <p>
   * If a NM token cache is not set, the {@link NMTokenCache#getSingleton()}
   * singleton instance will be used.
   *
   * @param nmTokenCache the NM token cache to use.
   */
  public void setNMTokenCache(NMTokenCache nmTokenCache) {
    this.nmTokenCache = nmTokenCache;
  }

  /**
   * Get the NM token cache of the <code>AMRMClient</code>. This cache must be
   * shared with the {@link NMClient} used to manage containers for the
   * <code>AMRMClient</code>.
   * <p>
   * If a NM token cache is not set, the {@link NMTokenCache#getSingleton()}
   * singleton instance will be used.
   *
   * @return the NM token cache.
   */
  public NMTokenCache getNMTokenCache() {
    return nmTokenCache;
  }

  /**
   * Wait for <code>check</code> to return true for each 1000 ms.
   * See also {@link #waitFor(com.google.common.base.Supplier, int)}
   * and {@link #waitFor(com.google.common.base.Supplier, int, int)}
   * @param check
   */
  public void waitFor(Supplier<Boolean> check) throws InterruptedException {
    waitFor(check, 1000);
  }

  /**
   * Wait for <code>check</code> to return true for each
   * <code>checkEveryMillis</code> ms.
   * See also {@link #waitFor(com.google.common.base.Supplier, int, int)}
   * @param check user defined checker
   * @param checkEveryMillis interval to call <code>check</code>
   */
  public void waitFor(Supplier<Boolean> check, int checkEveryMillis)
      throws InterruptedException {
    waitFor(check, checkEveryMillis, 1);
  }

  /**
   * Wait for <code>check</code> to return true for each
   * <code>checkEveryMillis</code> ms. In the main loop, this method will log
   * the message "waiting in main loop" for each <code>logInterval</code> times
   * iteration to confirm the thread is alive.
   * @param check user defined checker
   * @param checkEveryMillis interval to call <code>check</code>
   * @param logInterval interval to log for each
   */
  public void waitFor(Supplier<Boolean> check, int checkEveryMillis,
      int logInterval) throws InterruptedException {
    Preconditions.checkNotNull(check, "check should not be null");
    Preconditions.checkArgument(checkEveryMillis >= 0,
        "checkEveryMillis should be positive value");
    Preconditions.checkArgument(logInterval >= 0,
        "logInterval should be positive value");

    int loggingCounter = logInterval;
    do {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Check the condition for main loop.");
      }

      boolean result = check.get();
      if (result) {
        LOG.info("Exits the main loop.");
        return;
      }
      if (--loggingCounter <= 0) {
        LOG.info("Waiting in main loop.");
        loggingCounter = logInterval;
      }

      Thread.sleep(checkEveryMillis);
    } while (true);
  }

}
