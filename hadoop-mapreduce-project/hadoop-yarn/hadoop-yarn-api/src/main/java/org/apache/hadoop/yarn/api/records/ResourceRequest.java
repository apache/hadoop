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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.AMRMProtocol;

/**
 * <p><code>ResourceRequest</code> represents the request made by an
 * application to the <code>ResourceManager</code> to obtain various 
 * <code>Container</code> allocations.</p>
 * 
 * <p>It includes:
 *   <ul>
 *     <li>{@link Priority} of the request.</li>
 *     <li>
 *       The <em>name</em> of the machine or rack on which the allocation is 
 *       desired. A special value of <em>*</em> signifies that 
 *       <em>any</em> host/rack is acceptable to the application.
 *     </li>
 *     <li>{@link Resource} required for each request.</li>
 *     <li>
 *       Number of containers of such specifications which are required 
 *       by the application.
 *     </li>
 *   </ul>
 * </p>
 * 
 * @see Resource
 * @see AMRMProtocol#allocate(org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest)
 */
@Public
@Stable
public interface ResourceRequest extends Comparable<ResourceRequest> {
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
   * Get the <em>host/rack</em> on which the allocation is desired.
   * 
   * A special value of <em>*</em> signifies that <em>any</em> host/rack is 
   * acceptable.
   * 
   * @return <em>host/rack</em> on which the allocation is desired
   */
  @Public
  @Stable
  public abstract String getHostName();

  /**
   * Set <em>host/rack</em> on which the allocation is desired.
   * 
   * A special value of <em>*</em> signifies that <em>any</em> host/rack is 
   * acceptable.
   * 
   * @param hostName <em>host/rack</em> on which the allocation is desired
   */
  @Public
  @Stable
  public abstract void setHostName(String hostName);
  
  /**
   * Get the <code>Resource</code> capability of the request.
   * @return <code>Resource</code> capability of the request
   */
  @Public
  @Stable
  public abstract Resource getCapability();
  
  /**
   * Set the <code>Resource</code> capability of the request
   * @param capability <code>Resource</code> capability of the request
   */
  @Public
  @Stable
  public abstract void setCapability(Resource capability);

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
}
