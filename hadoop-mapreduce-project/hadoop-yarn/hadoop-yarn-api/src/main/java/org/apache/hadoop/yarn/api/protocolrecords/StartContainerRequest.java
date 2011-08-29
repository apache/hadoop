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

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

/**
 * <p>The request sent by the <code>ApplicationMaster</code> to the
 * <code>NodeManager</code> to <em>start</em> a container via
 * {@link ContainerManager#startContainer(StartContainerRequest)}.</p>
 * 
 * <p>The <code>ApplicationMaster</code> has to provide details such as
 * allocated resource capability, security tokens (if enabled), command
 * to be executed to start the container, environment for the process, 
 * necessary binaries/jar/shared-objects etc. via the 
 * {@link ContainerLaunchContext}.</p>
 *
 */
@Public
@Stable
public interface StartContainerRequest {
  /**
   * Get the <code>ContainerLaunchContext</code> for the container to be started
   * by the <code>NodeManager</code>.
   * 
   * @return <code>ContainerLaunchContext</code> for the container to be started
   *         by the <code>NodeManager</code>
   */
  @Public
  @Stable
  public abstract ContainerLaunchContext getContainerLaunchContext();
  
  @Private
  @Unstable
  public abstract void setContainerLaunchContext(ContainerLaunchContext context);
}
