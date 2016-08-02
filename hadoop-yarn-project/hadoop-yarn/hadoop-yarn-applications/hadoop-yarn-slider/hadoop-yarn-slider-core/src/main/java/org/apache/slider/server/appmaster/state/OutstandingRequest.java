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
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.InvalidContainerRequestException;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.server.appmaster.operations.CancelSingleRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Tracks an outstanding request. This is used to correlate an allocation response
 * with the node and role used in the request.
 * <p>
 * The node identifier may be null -which indicates that a request was made without
 * a specific target node
 * <p>
 * Equality and the hash code are based <i>only</i> on the role and hostname,
 * which are fixed in the constructor. This means that a simple 
 * instance constructed with (role, hostname) can be used to look up
 * a complete request instance in the {@link OutstandingRequestTracker} map
 */
public final class OutstandingRequest extends RoleHostnamePair {
  protected static final Logger log =
    LoggerFactory.getLogger(OutstandingRequest.class);

  /**
   * Node the request is for -may be null
   */
  public final NodeInstance node;

  /**
   * A list of all possible nodes to list in an AA request. For a non-AA
   * request where {@link #node} is set, element 0 of the list is the same
   * value.
   */
  public final List<NodeInstance> nodes = new ArrayList<>(1);

  /**
   * Optional label. This is cached as the request option (explicit-location + label) is forbidden,
   * yet the label needs to be retained for escalation.
   */
  public String label;

  /**
   * Requested time in millis.
   * <p>
   * Only valid after {@link #buildContainerRequest(Resource, RoleStatus, long)}
   */
  private AMRMClient.ContainerRequest issuedRequest;
  
  /**
   * Requested time in millis.
   * <p>
   * Only valid after {@link #buildContainerRequest(Resource, RoleStatus, long)}
   */
  private long requestedTimeMillis;

  /**
   * Time in millis after which escalation should be triggered..
   * <p>
   * Only valid after {@link #buildContainerRequest(Resource, RoleStatus, long)}
   */
  private long escalationTimeoutMillis;

  /**
   * Has the placement request been escalated?
   */
  private boolean escalated;

  /**
   * Flag to indicate that escalation is allowed
   */
  private boolean mayEscalate;

  /**
   * Priority of request; only valid after the request is built up
   */
  private int priority = -1;

  /**
   * Is this an Anti-affine request which should be cancelled on
   * a cluster resize?
   */
  private boolean antiAffine = false;

  /**
   * Create a request
   * @param roleId role
   * @param node node -can be null
   */
  public OutstandingRequest(int roleId,
                            NodeInstance node) {
    super(roleId, node != null ? node.hostname : null);
    this.node = node;
    nodes.add(node);
  }

  /**
   * Create an outstanding request with the given role and hostname
   * Important: this is useful only for map lookups -the other constructor
   * with the NodeInstance parameter is needed to generate node-specific
   * container requests
   * @param roleId role
   * @param hostname hostname
   */
  public OutstandingRequest(int roleId, String hostname) {
    super(roleId, hostname);
    this.node = null;
  }

  /**
   * Create an Anti-affine reques, including all listed nodes (there must be one)
   * as targets.
   * @param roleId role
   * @param nodes list of nodes
   */
  public OutstandingRequest(int roleId, List<NodeInstance> nodes) {
    super(roleId, nodes.get(0).hostname);
    this.node = null;
    this.antiAffine = true;
    this.nodes.addAll(nodes);
  }

  /**
   * Is the request located in the cluster, that is: does it have a node.
   * @return true if a node instance was supplied in the constructor
   */
  public boolean isLocated() {
    return node != null;
  }

  public long getRequestedTimeMillis() {
    return requestedTimeMillis;
  }

  public long getEscalationTimeoutMillis() {
    return escalationTimeoutMillis;
  }

  public synchronized boolean isEscalated() {
    return escalated;
  }

  public boolean mayEscalate() {
    return mayEscalate;
  }

  public AMRMClient.ContainerRequest getIssuedRequest() {
    return issuedRequest;
  }

  public int getPriority() {
    return priority;
  }

  public boolean isAntiAffine() {
    return antiAffine;
  }

  public void setAntiAffine(boolean antiAffine) {
    this.antiAffine = antiAffine;
  }

  /**
   * Build a container request.
   * <p>
   *  The value of {@link #node} is used to direct a lot of policy. If null,
   *  placement is relaxed.
   *  If not null, the choice of whether to use the suggested node
   *  is based on the placement policy and failure history.
   * <p>
   * If the request has an address, it is set in the container request
   * (with a flag to enable relaxed priorities).
   * <p>
   * This operation sets the requested time flag, used for tracking timeouts
   * on outstanding requests
   * @param resource resource
   * @param role role
   * @param time time in millis to record as request time
   * @return the request to raise
   */
  public synchronized AMRMClient.ContainerRequest buildContainerRequest(
      Resource resource, RoleStatus role, long time) {
    Preconditions.checkArgument(resource != null, "null `resource` arg");
    Preconditions.checkArgument(role != null, "null `role` arg");

    // cache label for escalation
    label = role.getLabelExpression();
    requestedTimeMillis = time;
    escalationTimeoutMillis = time + role.getPlacementTimeoutSeconds() * 1000;
    String[] hosts;
    boolean relaxLocality;
    boolean strictPlacement = role.isStrictPlacement();
    NodeInstance target = this.node;
    String nodeLabels;

    if (isAntiAffine()) {
      int size = nodes.size();
      log.info("Creating anti-affine request across {} nodes; first node = {}",
          size, hostname);
      hosts = new String[size];
      StringBuilder builder = new StringBuilder(size * 16);
      int c = 0;
      for (NodeInstance nodeInstance : nodes) {
        hosts[c++] = nodeInstance.hostname;
        builder.append(nodeInstance.hostname).append(" ");
      }
      log.debug("Full host list: [ {}]", builder);
      escalated = false;
      mayEscalate = false;
      relaxLocality = false;
      nodeLabels = null;
    } else if (target != null) {
      // placed request. Hostname is used in request
      hosts = new String[1];
      hosts[0] = target.hostname;
      // and locality flag is set to false; Slider will decide when
      // to relax things
      relaxLocality = false;

      log.info("Submitting request for container on {}", hosts[0]);
      // enable escalation for all but strict placements.
      escalated = false;
      mayEscalate = !strictPlacement;
      nodeLabels = null;
    } else {
      // no hosts
      hosts = null;
      // relax locality is mandatory on an unconstrained placement
      relaxLocality = true;
      // declare that the the placement is implicitly escalated.
      escalated = true;
      // and forbid it happening
      mayEscalate = false;
      nodeLabels = label;
    }
    Priority pri = ContainerPriority.createPriority(roleId, !relaxLocality);
    priority = pri.getPriority();
    issuedRequest = new AMRMClient.ContainerRequest(resource,
                                      hosts,
                                      null,
                                      pri,
                                      relaxLocality,
                                      nodeLabels);
    validate();
    return issuedRequest;
  }


  /**
   * Build an escalated container request, updating {@link #issuedRequest} with
   * the new value.
   * @return the new container request, which has the same resource and label requirements
   * as the original one, and the same host, but: relaxed placement, and a changed priority
   * so as to place it into the relaxed list.
   */
  public synchronized AMRMClient.ContainerRequest escalate() {
    Preconditions.checkNotNull(issuedRequest, "cannot escalate if request not issued " + this);
    log.debug("Escalating {}", this.toString());
    escalated = true;

    // this is now the priority
    // it is tagged as unlocated because it needs to go into a different
    // set of outstanding requests from the strict placements
    Priority pri = ContainerPriority.createPriority(roleId, false);
    // update the field
    priority = pri.getPriority();

    String[] nodes;
    List<String> issuedRequestNodes = issuedRequest.getNodes();
    if (SliderUtils.isUnset(label) && issuedRequestNodes != null) {
      nodes = issuedRequestNodes.toArray(new String[issuedRequestNodes.size()]);
    } else {
      nodes = null;
    }

    issuedRequest = new AMRMClient.ContainerRequest(issuedRequest.getCapability(),
        nodes,
        null,
        pri,
        true,
        label);
    validate();
    return issuedRequest;
  }
      
  /**
   * Mark the request as completed (or canceled).
   * <p>
   *   Current action: if a node is defined, its request count is decremented
   */
  public void completed() {
    if (node != null) {
      node.getOrCreate(roleId).requestCompleted();
    }
  }

  /**
   * Query to see if the request is available and ready to be escalated
   * @param time time to check against
   * @return true if escalation should begin
   */
  public synchronized boolean shouldEscalate(long time) {
    return mayEscalate
           && !escalated
           && issuedRequest != null
           && escalationTimeoutMillis < time;
  }

  /**
   * Query for the resource requirements matching; always false before a request is issued
   * @param resource
   * @return
   */
  public synchronized boolean resourceRequirementsMatch(Resource resource) {
    return issuedRequest != null && Resources.fitsIn(issuedRequest.getCapability(), resource);
  }

  @Override
  public String toString() {
    boolean requestHasLocation = ContainerPriority.hasLocation(getPriority());
    final StringBuilder sb = new StringBuilder("OutstandingRequest{");
    sb.append("roleId=").append(roleId);
    if (hostname != null) {
      sb.append(", hostname='").append(hostname).append('\'');
    }
    sb.append(", node=").append(node);
    sb.append(", hasLocation=").append(requestHasLocation);
    sb.append(", label=").append(label);
    sb.append(", requestedTimeMillis=").append(requestedTimeMillis);
    sb.append(", mayEscalate=").append(mayEscalate);
    sb.append(", escalated=").append(escalated);
    sb.append(", escalationTimeoutMillis=").append(escalationTimeoutMillis);
    sb.append(", issuedRequest=").append(
        issuedRequest != null ? SliderUtils.requestToString(issuedRequest) : "(null)");
    sb.append('}');
    return sb.toString();
  }

  /**
   * Create a cancel operation
   * @return an operation that can be used to cancel the request
   */
  public CancelSingleRequest createCancelOperation() {
    Preconditions.checkState(issuedRequest != null, "No issued request to cancel");
    return new CancelSingleRequest(issuedRequest);
  }

  /**
   * Valid if a node label expression specified on container request is valid or
   * not. Mimics the logic in AMRMClientImpl, so can be used for preflight checking
   * and in mock tests
   *
   */
  public void validate() throws InvalidContainerRequestException {
    Preconditions.checkNotNull(issuedRequest, "request has not yet been built up");
    AMRMClient.ContainerRequest containerRequest = issuedRequest;
    String requestDetails = this.toString();
    validateContainerRequest(containerRequest, priority, requestDetails);
  }

  /**
   * Inner Validation logic for container request
   * @param containerRequest request
   * @param priority raw priority of role
   * @param requestDetails details for error messages
   */
  @VisibleForTesting
  public static void validateContainerRequest(AMRMClient.ContainerRequest containerRequest,
    int priority, String requestDetails) {
    String exp = containerRequest.getNodeLabelExpression();
    boolean hasRacks = containerRequest.getRacks() != null &&
      (!containerRequest.getRacks().isEmpty());
    boolean hasNodes = containerRequest.getNodes() != null &&
      (!containerRequest.getNodes().isEmpty());

    boolean hasLabel = SliderUtils.isSet(exp);

    // Don't support specifying >= 2 node labels in a node label expression now
    if (hasLabel && (exp.contains("&&") || exp.contains("||"))) {
      throw new InvalidContainerRequestException(
          "Cannot specify more than two node labels"
              + " in a single node label expression: " + requestDetails);
    }

    // Don't allow specify node label against ANY request listing hosts or racks
    if (hasLabel && ( hasRacks || hasNodes)) {
      throw new InvalidContainerRequestException(
          "Cannot specify node label with rack or node: " + requestDetails);
    }
  }

  /**
   * Create a new role/hostname pair for indexing.
   * @return a new index.
   */
  public RoleHostnamePair getIndex() {
    return new RoleHostnamePair(roleId, hostname);
  }

}
