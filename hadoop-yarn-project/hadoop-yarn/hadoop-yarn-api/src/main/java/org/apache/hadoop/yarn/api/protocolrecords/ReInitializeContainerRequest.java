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

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.util.Records;

/**
 * This encapsulates all the required fields needed for a Container
 * ReInitialization.
 */
@Public
@Unstable
public abstract class ReInitializeContainerRequest {

  /**
   * Creates a new instance of the ReInitializationContainerRequest.
   * @param containerId Container Id.
   * @param containerLaunchContext Container Launch Context.
   * @param autoCommit AutoCommit.
   * @return ReInitializationContainerRequest.
   */
  @Public
  @Unstable
  public static ReInitializeContainerRequest newInstance(
      ContainerId containerId, ContainerLaunchContext containerLaunchContext,
      boolean autoCommit) {
    ReInitializeContainerRequest record =
        Records.newRecord(ReInitializeContainerRequest.class);
    record.setContainerId(containerId);
    record.setContainerLaunchContext(containerLaunchContext);
    record.setAutoCommit(autoCommit);
    return record;
  }

  /**
   * Get the <code>ContainerId</code> of the container to re-initialize.
   *
   * @return <code>ContainerId</code> of the container to re-initialize.
   */
  @Public
  @Unstable
  public abstract ContainerId getContainerId();

  /**
   * Set the <code>ContainerId</code> of the container to re-initialize.
   * @param containerId the containerId of the container.
   */
  @Private
  @Unstable
  public abstract void setContainerId(ContainerId containerId);

  /**
   * Get the <code>ContainerLaunchContext</code> to re-initialize the container
   * with.
   *
   * @return <code>ContainerLaunchContext</code> of to re-initialize the
   * container with.
   */
  @Public
  @Unstable
  public abstract ContainerLaunchContext getContainerLaunchContext();

  /**
   * Set the <code>ContainerLaunchContext</code> to re-initialize the container
   * with.
   * @param containerLaunchContext the Launch Context.
   */
  @Private
  @Unstable
  public abstract void setContainerLaunchContext(
      ContainerLaunchContext containerLaunchContext);


  /**
   * Check if AutoCommit is set for this ReInitialization.
   * @return If AutoCommit is set for this ReInitialization.
   */
  @Public
  @Unstable
  public abstract boolean getAutoCommit();

  /**
   * Set AutoCommit flag for this ReInitialization.
   * @param autoCommit Auto Commit.
   */
  @Private
  @Unstable
  public abstract void setAutoCommit(boolean autoCommit);
}
