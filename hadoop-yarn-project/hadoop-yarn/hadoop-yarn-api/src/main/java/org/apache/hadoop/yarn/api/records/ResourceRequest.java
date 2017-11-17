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

package org.apache.hadoop.yarn.api.records;

import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code ResourceRequest} represents the request made
 * by an application to the {@code ResourceManager}
 * to obtain various {@code Container} allocations.
 * <p>
 * It includes:
 * <ul>
 *   <li>{@link Priority} of the request.</li>
 *   <li>
 *     The <em>name</em> of the host or rack on which the allocation is
 *     desired. A special value of <em>*</em> signifies that
 *     <em>any</em> host/rack is acceptable to the application.
 *   </li>
 *   <li>{@link Resource} required for each request.</li>
 *   <li>
 *     Number of containers, of above specifications, which are required
 *     by the application.
 *   </li>
 *   <li>
 *     A boolean <em>relaxLocality</em> flag, defaulting to {@code true},
 *     which tells the {@code ResourceManager} if the application wants
 *     locality to be loose (i.e. allows fall-through to rack or <em>any</em>)
 *     or strict (i.e. specify hard constraint on resource allocation).
 *   </li>
 * </ul>
 * 
 * @see Resource
 * @see ApplicationMasterProtocol#allocate(org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest)
 */
@Public
@Stable
public abstract class ResourceRequest implements Comparable<ResourceRequest> {

  @Public
  @Stable
  public static ResourceRequest newInstance(Priority priority, String hostName,
      Resource capability, int numContainers) {
    return ResourceRequest.newBuilder().priority(priority)
        .resourceName(hostName).capability(capability)
        .numContainers(numContainers).build();
  }

  @Public
  @Stable
  public static ResourceRequest newInstance(Priority priority, String hostName,
      Resource capability, int numContainers, boolean relaxLocality) {
    return ResourceRequest.newBuilder().priority(priority)
        .resourceName(hostName).capability(capability)
        .numContainers(numContainers).relaxLocality(relaxLocality).build();
  }
  
  @Public
  @Stable
  public static ResourceRequest newInstance(Priority priority, String hostName,
      Resource capability, int numContainers, boolean relaxLocality,
      String labelExpression) {
    return ResourceRequest.newBuilder().priority(priority)
        .resourceName(hostName).capability(capability)
        .numContainers(numContainers).relaxLocality(relaxLocality)
        .nodeLabelExpression(labelExpression).build();
  }

  @Public
  @Evolving
  public static ResourceRequest newInstance(Priority priority, String hostName,
      Resource capability, int numContainers, boolean relaxLocality, String
      labelExpression, ExecutionTypeRequest executionTypeRequest) {
    return ResourceRequest.newBuilder().priority(priority)
        .resourceName(hostName).capability(capability)
        .numContainers(numContainers).relaxLocality(relaxLocality)
        .nodeLabelExpression(labelExpression)
        .executionTypeRequest(executionTypeRequest).build();
  }

  @Public
  @Unstable
  public static ResourceRequestBuilder newBuilder() {
    return new ResourceRequestBuilder();
  }

  /**
   * Class to construct instances of {@link ResourceRequest} with specific
   * options.
   */
  @Public
  @Stable
  public static final class ResourceRequestBuilder {
    private ResourceRequest resourceRequest =
        Records.newRecord(ResourceRequest.class);

    private ResourceRequestBuilder() {
      resourceRequest.setResourceName(ANY);
      resourceRequest.setNumContainers(1);
      resourceRequest.setPriority(Priority.newInstance(0));
      resourceRequest.setRelaxLocality(true);
      resourceRequest.setExecutionTypeRequest(
          ExecutionTypeRequest.newInstance());
    }

    /**
     * Set the <code>priority</code> of the request.
     * @see ResourceRequest#setPriority(Priority)
     * @param priority <code>priority</code> of the request
     * @return {@link ResourceRequestBuilder}
     */
    @Public
    @Stable
    public ResourceRequestBuilder priority(Priority priority) {
      resourceRequest.setPriority(priority);
      return this;
    }

    /**
     * Set the <code>resourceName</code> of the request.
     * @see ResourceRequest#setResourceName(String)
     * @param resourceName <code>resourceName</code> of the request
     * @return {@link ResourceRequestBuilder}
     */
    @Public
    @Stable
    public ResourceRequestBuilder resourceName(String resourceName) {
      resourceRequest.setResourceName(resourceName);
      return this;
    }

    /**
     * Set the <code>capability</code> of the request.
     * @see ResourceRequest#setCapability(Resource)
     * @param capability <code>capability</code> of the request
     * @return {@link ResourceRequestBuilder}
     */
    @Public
    @Stable
    public ResourceRequestBuilder capability(Resource capability) {
      resourceRequest.setCapability(capability);
      return this;
    }

    /**
     * Set the <code>numContainers</code> of the request.
     * @see ResourceRequest#setNumContainers(int)
     * @param numContainers <code>numContainers</code> of the request
     * @return {@link ResourceRequestBuilder}
     */
    @Public
    @Stable
    public ResourceRequestBuilder numContainers(int numContainers) {
      resourceRequest.setNumContainers(numContainers);
      return this;
    }

    /**
     * Set the <code>relaxLocality</code> of the request.
     * @see ResourceRequest#setRelaxLocality(boolean)
     * @param relaxLocality <code>relaxLocality</code> of the request
     * @return {@link ResourceRequestBuilder}
     */
    @Public
    @Stable
    public ResourceRequestBuilder relaxLocality(boolean relaxLocality) {
      resourceRequest.setRelaxLocality(relaxLocality);
      return this;
    }

    /**
     * Set the <code>nodeLabelExpression</code> of the request.
     * @see ResourceRequest#setNodeLabelExpression(String)
     * @param nodeLabelExpression
     *          <code>nodeLabelExpression</code> of the request
     * @return {@link ResourceRequestBuilder}
     */
    @Public
    @Evolving
    public ResourceRequestBuilder nodeLabelExpression(
        String nodeLabelExpression) {
      resourceRequest.setNodeLabelExpression(nodeLabelExpression);
      return this;
    }

    /**
     * Set the <code>executionTypeRequest</code> of the request.
     * @see ResourceRequest#setExecutionTypeRequest(
     * ExecutionTypeRequest)
     * @param executionTypeRequest
     *          <code>executionTypeRequest</code> of the request
     * @return {@link ResourceRequestBuilder}
     */
    @Public
    @Evolving
    public ResourceRequestBuilder executionTypeRequest(
        ExecutionTypeRequest executionTypeRequest) {
      resourceRequest.setExecutionTypeRequest(executionTypeRequest);
      return this;
    }

    /**
     * Set the <code>executionTypeRequest</code> of the request with 'ensure
     * execution type' flag set to true.
     * @see ResourceRequest#setExecutionTypeRequest(
     * ExecutionTypeRequest)
     * @param executionType <code>executionType</code> of the request.
     * @return {@link ResourceRequestBuilder}
     */
    @Public
    @Evolving
    public ResourceRequestBuilder executionType(ExecutionType executionType) {
      resourceRequest.setExecutionTypeRequest(
          ExecutionTypeRequest.newInstance(executionType, true));
      return this;
    }

    /**
     * Set the <code>allocationRequestId</code> of the request.
     * @see ResourceRequest#setAllocationRequestId(long)
     * @param allocationRequestId
     *          <code>allocationRequestId</code> of the request
     * @return {@link ResourceRequestBuilder}
     */
    @Public
    @Evolving
    public ResourceRequestBuilder allocationRequestId(
        long allocationRequestId) {
      resourceRequest.setAllocationRequestId(allocationRequestId);
      return this;
    }

    /**
     * Return generated {@link ResourceRequest} object.
     * @return {@link ResourceRequest}
     */
    @Public
    @Stable
    public ResourceRequest build() {
      return resourceRequest;
    }
  }

  @Public
  @Stable
  public static class ResourceRequestComparator implements
      java.util.Comparator<ResourceRequest>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public int compare(ResourceRequest r1, ResourceRequest r2) {

      // Compare priority, host and capability
      int ret = r1.getPriority().compareTo(r2.getPriority());
      if (ret == 0) {
        ret = Long.compare(
            r1.getAllocationRequestId(), r2.getAllocationRequestId());
      }
      if (ret == 0) {
        String h1 = r1.getResourceName();
        String h2 = r2.getResourceName();
        ret = h1.compareTo(h2);
      }
      if (ret == 0) {
        ret = r1.getExecutionTypeRequest()
            .compareTo(r2.getExecutionTypeRequest());
      }
      if (ret == 0) {
        ret = r1.getCapability().compareTo(r2.getCapability());
      }
      return ret;
    }
  }

  /**
   * The constant string representing no locality.
   * It should be used by all references that want to pass an arbitrary host
   * name in.
   */
  public static final String ANY = "*";

  /**
   * Check whether the given <em>host/rack</em> string represents an arbitrary
   * host name.
   *
   * @param hostName <em>host/rack</em> on which the allocation is desired
   * @return whether the given <em>host/rack</em> string represents an arbitrary
   * host name
   */
  @Public
  @Stable
  public static boolean isAnyLocation(String hostName) {
    return ANY.equals(hostName);
  }

  /**
   * Get the <code>Priority</code> of the request.
   * @return <code>Priority</code> of the request
   */
  @Public
  @Stable
  public abstract Priority getPriority();

  /**
   * Set the <code>Priority</code> of the request
   * @param priority <code>Priority</code> of the request
   */
  @Public
  @Stable
  public abstract void setPriority(Priority priority);
  
  /**
   * Get the resource (e.g. <em>host/rack</em>) on which the allocation 
   * is desired.
   * 
   * A special value of <em>*</em> signifies that <em>any</em> resource 
   * (host/rack) is acceptable.
   * 
   * @return resource (e.g. <em>host/rack</em>) on which the allocation 
   *                  is desired
   */
  @Public
  @Stable
  public abstract String getResourceName();

  /**
   * Set the resource name (e.g. <em>host/rack</em>) on which the allocation 
   * is desired.
   * 
   * A special value of <em>*</em> signifies that <em>any</em> resource name
   * (e.g. host/rack) is acceptable. 
   * 
   * @param resourceName (e.g. <em>host/rack</em>) on which the 
   *                     allocation is desired
   */
  @Public
  @Stable
  public abstract void setResourceName(String resourceName);
  
  /**
   * Get the number of containers required with the given specifications.
   * @return number of containers required with the given specifications
   */
  @Public
  @Stable
  public abstract int getNumContainers();
  
  /**
   * Set the number of containers required with the given specifications
   * @param numContainers number of containers required with the given 
   *                      specifications
   */
  @Public
  @Stable
  public abstract void setNumContainers(int numContainers);

  /**
   * Get whether locality relaxation is enabled with this
   * <code>ResourceRequest</code>. Defaults to true.
   * 
   * @return whether locality relaxation is enabled with this
   * <code>ResourceRequest</code>.
   */
  @Public
  @Stable
  public abstract boolean getRelaxLocality();

  /**
   * Set the <code>ExecutionTypeRequest</code> of the requested container.
   *
   * @param execSpec
   *          ExecutionTypeRequest of the requested container
   */
  @Public
  @Evolving
  public void setExecutionTypeRequest(ExecutionTypeRequest execSpec) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get whether locality relaxation is enabled with this
   * <code>ResourceRequest</code>. Defaults to true.
   *
   * @return whether locality relaxation is enabled with this
   * <code>ResourceRequest</code>.
   */
  @Public
  @Evolving
  public ExecutionTypeRequest getExecutionTypeRequest() {
    throw new UnsupportedOperationException();
  }

  /**
   * <p>For a request at a network hierarchy level, set whether locality can be relaxed
   * to that level and beyond.<p>
   * 
   * <p>If the flag is off on a rack-level <code>ResourceRequest</code>,
   * containers at that request's priority will not be assigned to nodes on that
   * request's rack unless requests specifically for those nodes have also been
   * submitted.<p>
   * 
   * <p>If the flag is off on an {@link ResourceRequest#ANY}-level
   * <code>ResourceRequest</code>, containers at that request's priority will
   * only be assigned on racks for which specific requests have also been
   * submitted.<p>
   * 
   * <p>For example, to request a container strictly on a specific node, the
   * corresponding rack-level and any-level requests should have locality
   * relaxation set to false.  Similarly, to request a container strictly on a
   * specific rack, the corresponding any-level request should have locality
   * relaxation set to false.<p>
   * 
   * @param relaxLocality whether locality relaxation is enabled with this
   * <code>ResourceRequest</code>.
   */
  @Public
  @Stable
  public abstract void setRelaxLocality(boolean relaxLocality);
  
  /**
   * Get node-label-expression for this Resource Request. If this is set, all
   * containers allocated to satisfy this resource-request will be only on those
   * nodes that satisfy this node-label-expression.
   *  
   * Please note that node label expression now can only take effect when the
   * resource request has resourceName = ANY
   * 
   * @return node-label-expression
   */
  @Public
  @Evolving
  public abstract String getNodeLabelExpression();
  
  /**
   * Set node label expression of this resource request. Now only support
   * specifying a single node label. In the future we will support more complex
   * node label expression specification like {@code AND(&&), OR(||)}, etc.
   * 
   * Any please note that node label expression now can only take effect when
   * the resource request has resourceName = ANY
   * 
   * @param nodelabelExpression
   *          node-label-expression of this ResourceRequest
   */
  @Public
  @Evolving
  public abstract void setNodeLabelExpression(String nodelabelExpression);

  /**
   * Get the optional <em>ID</em> corresponding to this allocation request. This
   * ID is an identifier for different {@code ResourceRequest}s from the <b>same
   * application</b>. The allocated {@code Container}(s) received as part of the
   * {@code AllocateResponse} response will have the ID corresponding to the
   * original {@code ResourceRequest} for which the RM made the allocation.
   * <p>
   * The scheduler may return multiple {@code AllocateResponse}s corresponding
   * to the same ID as and when scheduler allocates {@code Container}(s).
   * <b>Applications</b> can continue to completely ignore the returned ID in
   * the response and use the allocation for any of their outstanding requests.
   * <p>
   * If one wishes to replace an entire {@code ResourceRequest} corresponding to
   * a specific ID, they can simply cancel the corresponding {@code
   * ResourceRequest} and submit a new one afresh.
   *
   * @return the <em>ID</em> corresponding to this allocation request.
   */
  @Public
  @Evolving
  public long getAllocationRequestId() {
    throw new UnsupportedOperationException();
  }

  /**
   * Set the optional <em>ID</em> corresponding to this allocation request. This
   * ID is an identifier for different {@code ResourceRequest}s from the <b>same
   * application</b>. The allocated {@code Container}(s) received as part of the
   * {@code AllocateResponse} response will have the ID corresponding to the
   * original {@code ResourceRequest} for which the RM made the allocation.
   * <p>
   * The scheduler may return multiple {@code AllocateResponse}s corresponding
   * to the same ID as and when scheduler allocates {@code Container}(s).
   * <b>Applications</b> can continue to completely ignore the returned ID in
   * the response and use the allocation for any of their outstanding requests.
   * <p>
   * If one wishes to replace an entire {@code ResourceRequest} corresponding to
   * a specific ID, they can simply cancel the corresponding {@code
   * ResourceRequest} and submit a new one afresh.
   * <p>
   * If the ID is not set, scheduler will continue to work as previously and all
   * allocated {@code Container}(s) will have the default ID, -1.
   *
   * @param allocationRequestID the <em>ID</em> corresponding to this allocation
   *                            request.
   */
  @Public
  @Evolving
  public void setAllocationRequestId(long allocationRequestID) {
    throw new UnsupportedOperationException();
  }

  /**
   * Set the <code>Resource</code> capability of the request.
   * @param capability <code>Resource</code> capability of the request
   */
  @Public
  @Stable
  public abstract void setCapability(Resource capability);

  /**
   * Get the <code>Resource</code> capability of the request.
   * @return <code>Resource</code> capability of the request
   */
  @Public
  @Stable
  public abstract Resource getCapability();
  
  @Override
  public int hashCode() {
    final int prime = 2153;
    int result = 2459;
    Resource capability = getCapability();
    String hostName = getResourceName();
    Priority priority = getPriority();
    result =
        prime * result + ((capability == null) ? 0 : capability.hashCode());
    result = prime * result + ((hostName == null) ? 0 : hostName.hashCode());
    result = prime * result + getNumContainers();
    result = prime * result + ((priority == null) ? 0 : priority.hashCode());
    result = prime * result + Long.valueOf(getAllocationRequestId()).hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ResourceRequest other = (ResourceRequest) obj;
    Resource capability = getCapability();
    if (capability == null) {
      if (other.getCapability() != null)
        return false;
    } else if (!capability.equals(other.getCapability()))
      return false;
    String hostName = getResourceName();
    if (hostName == null) {
      if (other.getResourceName() != null)
        return false;
    } else if (!hostName.equals(other.getResourceName()))
      return false;
    if (getNumContainers() != other.getNumContainers())
      return false;
    Priority priority = getPriority();
    if (priority == null) {
      if (other.getPriority() != null)
        return false;
    } else if (!priority.equals(other.getPriority()))
      return false;
    ExecutionTypeRequest execTypeRequest = getExecutionTypeRequest();
    if (execTypeRequest == null) {
      if (other.getExecutionTypeRequest() != null) {
        return false;
      }
    } else if (!execTypeRequest.equals(other.getExecutionTypeRequest())) {
      return false;
    }

    if (getAllocationRequestId() != other.getAllocationRequestId()) {
      return false;
    }

    if (getNodeLabelExpression() == null) {
      if (other.getNodeLabelExpression() != null) {
        return false;
      }
    } else {
      // do normalize on label expression before compare
      String label1 = getNodeLabelExpression().replaceAll("[\\t ]", "");
      String label2 =
          other.getNodeLabelExpression() == null ? null : other
              .getNodeLabelExpression().replaceAll("[\\t ]", "");
      if (!label1.equals(label2)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int compareTo(ResourceRequest other) {
    int priorityComparison = this.getPriority().compareTo(other.getPriority());
    if (priorityComparison == 0) {
      int hostNameComparison =
          this.getResourceName().compareTo(other.getResourceName());
      if (hostNameComparison == 0) {
        int execTypeReqComparison = this.getExecutionTypeRequest()
            .compareTo(other.getExecutionTypeRequest());
        if (execTypeReqComparison == 0) {
          int capabilityComparison =
              this.getCapability().compareTo(other.getCapability());
          if (capabilityComparison == 0) {
            int numContainerComparison =
                this.getNumContainers() - other.getNumContainers();
            if (numContainerComparison == 0) {
              return Long.compare(getAllocationRequestId(),
                  other.getAllocationRequestId());
            } else {
              return numContainerComparison;
            }
          } else {
            return capabilityComparison;
          }
        } else {
          return execTypeReqComparison;
        }
      } else {
        return hostNameComparison;
      }
    } else {
      return priorityComparison;
    }
  }
}
