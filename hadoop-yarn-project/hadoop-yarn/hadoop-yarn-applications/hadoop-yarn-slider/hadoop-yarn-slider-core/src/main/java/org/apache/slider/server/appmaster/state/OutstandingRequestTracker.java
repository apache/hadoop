/*
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

package org.apache.slider.server.appmaster.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.CancelSingleRequest;
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

/**
 * Tracks outstanding requests made with a specific placement option.
 * <p>
 *   <ol>
 *     <li>Used to decide when to return a node to 'can request containers here' list</li>
 *     <li>Used to identify requests where placement has timed out, and so issue relaxed requests</li>
 *   </ol>
 * <p>
 * If an allocation comes in that is not in the map: either the allocation
 * was unplaced, or the placed allocation could not be met on the specified
 * host, and the RM/scheduler fell back to another location. 
 */

public class OutstandingRequestTracker {
  protected static final Logger log =
    LoggerFactory.getLogger(OutstandingRequestTracker.class);

  /**
   * no requests; saves creating a new list if not needed
   */
  private final List<AbstractRMOperation> NO_REQUESTS = new ArrayList<>(0);
 
  private Map<RoleHostnamePair, OutstandingRequest> placedRequests = new HashMap<>();

  /**
   * List of open requests; no specific details on them.
   */
  private List<OutstandingRequest> openRequests = new ArrayList<>();

  /**
   * Create a new request for the specific role.
   * <p>
   * If a location is set, the request is added to {@link #placedRequests}.
   * If not, it is added to {@link #openRequests}
   * <p>
   * This does not update the node instance's role's request count
   * @param instance node instance to manager
   * @param role role index
   * @return a new request
   */
  public synchronized OutstandingRequest newRequest(NodeInstance instance, int role) {
    OutstandingRequest request = new OutstandingRequest(role, instance);
    if (request.isLocated()) {
      placedRequests.put(request.getIndex(), request);
    } else {
      openRequests.add(request);
    }
    return request;
  }

  /**
   * Create a new Anti-affine request for the specific role
   * <p>
   * It is added to {@link #openRequests}
   * <p>
   * This does not update the node instance's role's request count
   * @param role role index
   * @param nodes list of suitable nodes
   * @param label label to use
   * @return a new request
   */
  public synchronized OutstandingRequest newAARequest(int role,
      List<NodeInstance> nodes,
      String label) {
    Preconditions.checkArgument(!nodes.isEmpty());
    // safety check to verify the allocation will hold
    for (NodeInstance node : nodes) {
      Preconditions.checkState(node.canHost(role, label),
        "Cannot allocate role ID %d to node %s", role, node);
    }
    OutstandingRequest request = new OutstandingRequest(role, nodes);
    openRequests.add(request);
    return request;
  }

  /**
   * Look up any oustanding request to a (role, hostname). 
   * @param role role index
   * @param hostname hostname
   * @return the request or null if there was no outstanding one in the {@link #placedRequests}
   */
  @VisibleForTesting
  public synchronized OutstandingRequest lookupPlacedRequest(int role, String hostname) {
    Preconditions.checkArgument(hostname != null, "null hostname");
    return placedRequests.get(new RoleHostnamePair(role, hostname));
  }

  /**
   * Remove a request
   * @param request matching request to find
   * @return the request or null for no match in the {@link #placedRequests}
   */
  @VisibleForTesting
  public synchronized OutstandingRequest removePlacedRequest(OutstandingRequest request) {
    return placedRequests.remove(request);
  }

  /**
   * Notification that a container has been allocated
   *
   * <ol>
   *   <li>drop it from the {@link #placedRequests} structure.</li>
   *   <li>generate the cancellation request</li>
   *   <li>for AA placement, any actions needed</li>
   * </ol>
   *
   * @param role role index
   * @param hostname hostname
   * @return the allocation outcome
   */
  public synchronized ContainerAllocationResults onContainerAllocated(int role,
      String hostname,
      Container container) {
    final String containerDetails = SliderUtils.containerToString(container);
    log.debug("Processing allocation for role {}  on {}", role,
        containerDetails);
    ContainerAllocationResults allocation = new ContainerAllocationResults();
    ContainerAllocationOutcome outcome;
    OutstandingRequest request = placedRequests.remove(new OutstandingRequest(role, hostname));
    if (request != null) {
      //satisfied request
      log.debug("Found oustanding placed request for container: {}", request);
      request.completed();
      // derive outcome from status of tracked request
      outcome = request.isEscalated()
          ? ContainerAllocationOutcome.Escalated
          : ContainerAllocationOutcome.Placed;
    } else {
      // not in the list; this is an open placement
      // scan through all containers in the open request list
      request = removeOpenRequest(container);
      if (request != null) {
        log.debug("Found open outstanding request for container: {}", request);
        request.completed();
        outcome = ContainerAllocationOutcome.Open;
      } else {
        log.warn("No oustanding request found for container {}, outstanding queue has {} entries ",
            containerDetails,
            openRequests.size());
        outcome = ContainerAllocationOutcome.Unallocated;
      }
    }
    if (request != null && request.getIssuedRequest() != null) {
      allocation.operations.add(request.createCancelOperation());
    } else {
      // there's a request, but no idea what to cancel.
      // rather than try to recover from it inelegantly, (and cause more confusion),
      // log the event, but otherwise continue
      log.warn("Unexpected allocation of container " + SliderUtils.containerToString(container));
    }

    allocation.origin = request;
    allocation.outcome = outcome;
    return allocation;
  }

  /**
   * Find and remove an open request. Determine it by scanning open requests
   * for one whose priority & resource requirements match that of the container
   * allocated.
   * @param container container allocated
   * @return a request which matches the allocation, or null for "no match"
   */
  private OutstandingRequest removeOpenRequest(Container container) {
    int pri = container.getPriority().getPriority();
    Resource resource = container.getResource();
    OutstandingRequest request = null;
    ListIterator<OutstandingRequest> openlist = openRequests.listIterator();
    while (openlist.hasNext() && request == null) {
      OutstandingRequest r = openlist.next();
      if (r.getPriority() == pri) {
        // matching resource
        if (r.resourceRequirementsMatch(resource)) {
          // match of priority and resources
          request = r;
          openlist.remove();
        } else {
          log.debug("Matched priorities but resources different");
        }
      }
    }
    return request;
  }
  
  /**
   * Determine which host was a role type most recently used on, so that
   * if a choice is made of which (potentially surplus) containers to use,
   * the most recent one is picked first. This operation <i>does not</i>
   * change the role history, though it queries it.
   */
  static class newerThan implements Comparator<Container> {
    private RoleHistory rh;
    
    public newerThan(RoleHistory rh) {
      this.rh = rh;
    }

    /**
     * Get the age of a node hosting container. If it is not known in the history, 
     * return 0.
     * @param c container
     * @return age, null if there's no entry for it. 
     */
    private long getAgeOf(Container c) {
      long age = 0;
      NodeInstance node = rh.getExistingNodeInstance(c);
      int role = ContainerPriority.extractRole(c);
      if (node != null) {
        NodeEntry nodeEntry = node.get(role);
        if (nodeEntry != null) {
          age = nodeEntry.getLastUsed();
        }
      }
      return age;
    }

    /**
     * Comparator: which host is more recent?
     * @param c1 container 1
     * @param c2 container 2
     * @return 1 if c2 older-than c1, 0 if equal; -1 if c1 older-than c2
     */
    @Override
    public int compare(Container c1, Container c2) {
      int role1 = ContainerPriority.extractRole(c1);
      int role2 = ContainerPriority.extractRole(c2);
      if (role1 < role2) return -1;
      if (role1 > role2) return 1;

      long age = getAgeOf(c1);
      long age2 = getAgeOf(c2);

      if (age > age2) {
        return -1;
      } else if (age < age2) {
        return 1;
      }
      // equal
      return 0;
    }
  }

  /**
   * Take a list of requests and split them into specific host requests and
   * generic assignments. This is to give requested hosts priority
   * in container assignments if more come back than expected
   * @param rh RoleHistory instance
   * @param inAllocated the list of allocated containers
   * @param outPlaceRequested initially empty list of requested locations 
   * @param outUnplaced initially empty list of unrequested hosts
   */
  public synchronized void partitionRequests(RoleHistory rh,
      List<Container> inAllocated,
      List<Container> outPlaceRequested,
      List<Container> outUnplaced) {
    Collections.sort(inAllocated, new newerThan(rh));
    for (Container container : inAllocated) {
      int role = ContainerPriority.extractRole(container);
      String hostname = RoleHistoryUtils.hostnameOf(container);
      if (placedRequests.containsKey(new OutstandingRequest(role, hostname))) {
        outPlaceRequested.add(container);
      } else {
        outUnplaced.add(container);
      }
    }
  }
  

  /**
   * Reset list all outstanding requests for a role: return the hostnames
   * of any canceled requests
   *
   * @param role role to cancel
   * @return possibly empty list of hostnames
   */
  public synchronized List<NodeInstance> resetOutstandingRequests(int role) {
    List<NodeInstance> hosts = new ArrayList<>();
    Iterator<Map.Entry<RoleHostnamePair, OutstandingRequest>> iterator =
      placedRequests.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<RoleHostnamePair, OutstandingRequest> next =
        iterator.next();
      OutstandingRequest request = next.getValue();
      if (request.roleId == role) {
        iterator.remove();
        request.completed();
        hosts.add(request.node);
      }
    }
    ListIterator<OutstandingRequest> openlist = openRequests.listIterator();
    while (openlist.hasNext()) {
      OutstandingRequest next = openlist.next();
      if (next.roleId == role) {
        openlist.remove();
      }
    }
    return hosts;
  }

  /**
   * Get a list of outstanding requests. The list is cloned, but the contents
   * are shared
   * @return a list of the current outstanding requests
   */
  public synchronized List<OutstandingRequest> listPlacedRequests() {
    return new ArrayList<>(placedRequests.values());
  }

  /**
   * Get a list of outstanding requests. The list is cloned, but the contents
   * are shared
   * @return a list of the current outstanding requests
   */
  public synchronized List<OutstandingRequest> listOpenRequests() {
    return new ArrayList<>(openRequests);
  }

  /**
   * Escalate operation as triggered by external timer.
   * @return a (usually empty) list of cancel/request operations.
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  public synchronized List<AbstractRMOperation> escalateOutstandingRequests(long now) {
    if (placedRequests.isEmpty()) {
      return NO_REQUESTS;
    }

    List<AbstractRMOperation> operations = new ArrayList<>();
    for (OutstandingRequest outstandingRequest : placedRequests.values()) {
      synchronized (outstandingRequest) {
        // sync escalation check with operation so that nothing can happen to state
        // of the request during the escalation
        if (outstandingRequest.shouldEscalate(now)) {

          // time to escalate
          CancelSingleRequest cancel = outstandingRequest.createCancelOperation();
          operations.add(cancel);
          AMRMClient.ContainerRequest escalated = outstandingRequest.escalate();
          operations.add(new ContainerRequestOperation(escalated));
        }
      }
      
    }
    return operations;
  }

  /**
   * Cancel all outstanding AA requests from the lists of requests.
   *
   * This does not remove them from the role status; they must be reset
   * by the caller.
   *
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  public synchronized List<AbstractRMOperation> cancelOutstandingAARequests() {

    log.debug("Looking for AA request to cancel");
    List<AbstractRMOperation> operations = new ArrayList<>();

    // first, all placed requests
    List<RoleHostnamePair> requestsToRemove = new ArrayList<>(placedRequests.size());
    for (Map.Entry<RoleHostnamePair, OutstandingRequest> entry : placedRequests.entrySet()) {
      OutstandingRequest outstandingRequest = entry.getValue();
      synchronized (outstandingRequest) {
        if (outstandingRequest.isAntiAffine()) {
          // time to escalate
          operations.add(outstandingRequest.createCancelOperation());
          requestsToRemove.add(entry.getKey());
        }
      }
    }
    for (RoleHostnamePair keys : requestsToRemove) {
      placedRequests.remove(keys);
    }

    // second, all open requests
    ListIterator<OutstandingRequest> orit = openRequests.listIterator();
    while (orit.hasNext()) {
      OutstandingRequest outstandingRequest =  orit.next();
      synchronized (outstandingRequest) {
        if (outstandingRequest.isAntiAffine()) {
          // time to escalate
          operations.add(outstandingRequest.createCancelOperation());
          orit.remove();
        }
      }
    }
    log.info("Cancelling {} outstanding AA requests", operations.size());

    return operations;
  }

  /**
   * Extract a specific number of open requests for a role
   * @param roleId role Id
   * @param count count to extract
   * @return a list of requests which are no longer in the open request list
   */
  public synchronized List<OutstandingRequest> extractOpenRequestsForRole(int roleId, int count) {
    List<OutstandingRequest> results = new ArrayList<>();
    ListIterator<OutstandingRequest> openlist = openRequests.listIterator();
    while (openlist.hasNext() && count > 0) {
      OutstandingRequest openRequest = openlist.next();
      if (openRequest.roleId == roleId) {
        results.add(openRequest);
        openlist.remove();
        count--;
      }
    }
    return results;
  }

  /**
   * Extract a specific number of placed requests for a role
   * @param roleId role Id
   * @param count count to extract
   * @return a list of requests which are no longer in the placed request data structure
   */
  public synchronized List<OutstandingRequest> extractPlacedRequestsForRole(int roleId, int count) {
    List<OutstandingRequest> results = new ArrayList<>();
    Iterator<Map.Entry<RoleHostnamePair, OutstandingRequest>>
        iterator = placedRequests.entrySet().iterator();
    while (iterator.hasNext() && count > 0) {
      OutstandingRequest request = iterator.next().getValue();
      if (request.roleId == roleId) {
        results.add(request);
        count--;
      }
    }
    // now cull them from the map
    for (OutstandingRequest result : results) {
      placedRequests.remove(result);
    }

    return results;
  }

}
