/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.api.records;

import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code ContainerMoveRequest} represents the request made
 * by an application to the {@code ResourceManager}
 * to relocate a single container to another node of the yarn cluster.
 * <p>
 * It includes:
 * <ul>
 *   <li>{@link Priority} of the request.</li>
 *   <li>
 *     <em>originContainerId</em> of the request, which is a
 *     {@link ContainerId} that identifies the container to be relocated.
 *   </li>
 *   <li>
 *     The <em>targetHost</em> of the request, which identifies the node where
 *     the origin container should be relocated to.
 *   </li>
 * </ul>
 *
 * @see ApplicationMasterProtocol#allocate(org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest)
 */
@Public
@Unstable
public abstract class ContainerMoveRequest implements Comparable<ContainerMoveRequest> {
  
  @Public
  @Unstable
  public static ContainerMoveRequest newInstance(Priority priority,
      ContainerId originContainerId, String targetHost) {
    ContainerMoveRequest request = Records.newRecord(ContainerMoveRequest.class);
    request.setPriority(priority);
    request.setOriginContainerId(originContainerId);
    request.setTargetHost(targetHost);
    return request;
  }
  
  @Public
  @Unstable
  public static class ContainerMoveRequestComparator implements
      java.util.Comparator<ContainerMoveRequest>, Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @Override
    public int compare(ContainerMoveRequest r1, ContainerMoveRequest r2) {
      
      // Compare by priority, originContainerId and targetHost
      int ret = r1.getPriority().compareTo(r2.getPriority());
      if (ret == 0) {
        ret = r1.getOriginContainerId().compareTo(r2.getOriginContainerId());
      }
      if (ret == 0) {
        ret = r1.getTargetHost().compareTo(r2.getTargetHost());
      }
      return ret;
    }
  }
  
  /**
   * Get the <code>Priority</code> of the request.
   * @return <code>Priority</code> of the request
   */
  @Public
  @Unstable
  public abstract Priority getPriority();
  
  /**
   * Set the <code>Priority</code> of the request.
   * @param priority <code>Priority</code> of the request
   */
  @Public
  @Unstable
  public abstract void setPriority(Priority priority);
  
  /**
   * Gets the origin container id of this container move request.
   * It identifies the container that should be relocated.
   *
   * @return the origin container id of this container move request
   */
  @Public
  @Unstable
  public abstract ContainerId getOriginContainerId();
  
  /**
   * Sets the origin container id of this container move request.
   * It identifies the container that should be relocated.
   *
   * @param originContainerId the origin container id of this container move
   *                          request
   */
  @Public
  @Unstable
  public abstract void setOriginContainerId(ContainerId originContainerId);
  
  /**
   * Gets the target host of this container move request, which identifies
   * the node where the container should be relocated to.
   *
   * @return the target host of this container move request
   */
  @Public
  @Unstable
  public abstract String getTargetHost();
  
  /**
   * Sets the target host of this container move request, which identifies
   * the node where the container should be relocated to.
   *
   * @param targetHost the target host of this container move request
   */
  @Public
  @Unstable
  public abstract void setTargetHost(String targetHost);
  
  @Override
  public int hashCode() {
    final int prime = 2153;
    int result = 2459;
    Priority priority = getPriority();
    ContainerId originContainerId = getOriginContainerId();
    String targetHost = getTargetHost();
    
    result = prime * result + ((priority == null) ? 0 : priority.hashCode());
    result = prime * result +
        ((originContainerId == null) ? 0 : originContainerId.hashCode());
    result = prime * result + ((targetHost == null) ? 0 : targetHost.hashCode());
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
    ContainerMoveRequest other = (ContainerMoveRequest) obj;
    ContainerId originContainerId = getOriginContainerId();
    ContainerId otherOriginContainerId = other.getOriginContainerId();
    if (originContainerId == null) {
      if (otherOriginContainerId != null)
        return false;
    } else if (!originContainerId.equals(otherOriginContainerId))
      return false;
    String targetHost = getTargetHost();
    String otherTargetHost = other.getTargetHost();
    if (targetHost == null) {
      if (otherTargetHost != null)
        return false;
    } else if (!targetHost.equals(otherTargetHost))
      return false;
    Priority priority = getPriority();
    Priority otherPriority = other.getPriority();
    if (priority == null) {
      if (otherPriority != null)
        return false;
    } else if (!priority.equals(otherPriority))
      return false;
    return true;
  }
  
  @Override
  public int compareTo(ContainerMoveRequest other) {
    int priorityComparison = this.getPriority().compareTo(other.getPriority());
    if (priorityComparison == 0) {
      int originContainerIdComparison =
          this.getOriginContainerId().compareTo(other.getOriginContainerId());
      if (originContainerIdComparison == 0) {
        return this.getTargetHost().compareTo(other.getTargetHost());
      } else {
        return originContainerIdComparison;
      }
    } else {
      return priorityComparison;
    }
  }
}

