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
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code ContainerResourceChangeRequest} represents the request made by an
 * application to the {@code ResourceManager} to change resource allocation of
 * a running {@code Container}.
 * <p>
 * It includes:
 * <ul>
 *   <li>{@link ContainerId} for the container.</li>
 *   <li>
 *     {@link Resource} capability of the container after the resource change
 *     is completed.
 *   </li>
 * </ul>
 *
 * @see ApplicationMasterProtocol#allocate(org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest)
 */
@Public
@Unstable
public abstract class ContainerResourceChangeRequest {

  @Public
  @Unstable
  public static ContainerResourceChangeRequest newInstance(
      ContainerId existingContainerId, Resource targetCapability) {
    ContainerResourceChangeRequest context = Records
        .newRecord(ContainerResourceChangeRequest.class);
    context.setContainerId(existingContainerId);
    context.setCapability(targetCapability);
    return context;
  }

  /**
   * Get the <code>ContainerId</code> of the container.
   * @return <code>ContainerId</code> of the container
   */
  @Public
  @Unstable
  public abstract ContainerId getContainerId();

  /**
   * Set the <code>ContainerId</code> of the container.
   * @param containerId <code>ContainerId</code> of the container
   */
  @Public
  @Unstable
  public abstract void setContainerId(ContainerId containerId);

  /**
   * Get the <code>Resource</code> capability of the container.
   * @return <code>Resource</code> capability of the container
   */
  @Public
  @Unstable
  public abstract Resource getCapability();

  /**
   * Set the <code>Resource</code> capability of the container.
   * @param capability <code>Resource</code> capability of the container
   */
  @Public
  @Unstable
  public abstract void setCapability(Resource capability);

  @Override
  public int hashCode() {
    return getCapability().hashCode() + getContainerId().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ContainerResourceChangeRequest) {
      ContainerResourceChangeRequest ctx =
          (ContainerResourceChangeRequest) other;

      if (getContainerId() == null && ctx.getContainerId() != null) {
        return false;
      } else if (!getContainerId().equals(ctx.getContainerId())) {
        return false;
      }

      if (getCapability() == null && ctx.getCapability() != null) {
        return false;
      } else if (!getCapability().equals(ctx.getCapability())) {
        return false;
      }

      return true;
    } else {
      return false;
    }
  }
}
