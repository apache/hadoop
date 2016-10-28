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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The request sent by the <code>ApplicationMaster</code> to the
 * <code>NodeManager</code> to <em>start</em> a container.</p>
 * 
 * <p>The <code>ApplicationMaster</code> has to provide details such as
 * allocated resource capability, security tokens (if enabled), command
 * to be executed to start the container, environment for the process, 
 * necessary binaries/jar/shared-objects etc. via the 
 * {@link ContainerLaunchContext}.</p>
 *
 * <p>The <em>isMove</em> flag tells whether this request corresponds to a container
 * relocation. If true, then no container launch context is needed as it will be
 * transferred directly from the origin node manager to the target node manager.
 * The container to be relocated is identified by the <em>originNodeId</em> and
 * <em>originContainerId</em>.
 * The originNMToken is sent along so that the origin container can be shut down
 * by the target node manager.
 * </p>
 *
 * @see ContainerManagementProtocol#startContainers(StartContainersRequest)
 */
@Public
@Stable
public abstract class StartContainerRequest {
  @Public
  @Stable
  public static StartContainerRequest newInstance(
      ContainerLaunchContext context, Token container) {
    StartContainerRequest request =
        Records.newRecord(StartContainerRequest.class);
    request.setContainerLaunchContext(context);
    request.setContainerToken(container);
    request.setIsMove(false);
    return request;
  }
  
  @Public
  @Stable
  public static StartContainerRequest newInstance(
      Token container,  ContainerId originContainerId, NodeId originNodeId, Token originNMToken) {
    StartContainerRequest request =
        Records.newRecord(StartContainerRequest.class);
    request.setContainerToken(container);
    request.setIsMove(true);
    request.setOriginContainerId(originContainerId);
    request.setOriginNodeId(originNodeId);
    request.setOriginNMToken(originNMToken);
    return request;
  }

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
  
  /**
   * Set the <code>ContainerLaunchContext</code> for the container to be started
   * by the <code>NodeManager</code>
   * @param context <code>ContainerLaunchContext</code> for the container to be 
   *                started by the <code>NodeManager</code>
   */
  @Public
  @Stable
  public abstract void setContainerLaunchContext(ContainerLaunchContext context);

  /**
   * Get the container token to be used for authorization during starting
   * container.
   * <p>
   * Note: {@link NMToken} will be used for authenticating communication with
   * {@code NodeManager}.
   * @return the container token to be used for authorization during starting
   * container.
   * @see NMToken
   * @see ContainerManagementProtocol#startContainers(StartContainersRequest)
   */
  @Public
  @Stable
  public abstract Token getContainerToken();

  @Public
  @Stable
  public abstract void setContainerToken(Token container);
  
  /**
   * Gets whether this start container request corresponds to a container relocation.
   * @return whether this start container request corresponds to a container relocation
   */
  @Public
  @Unstable
  public abstract boolean getIsMove();
  
  /**
   * Sets whether this start container request corresponds to a container relocation.
   * @param isMove whether this start container request corresponds to a container
   *               relocation
   */
  @Public
  @Unstable
  public abstract void setIsMove(boolean isMove);
  
  /**
   * Gets the origin container id for this start container request.
   * The origin container id is set if and only if this start container request
   * corresponds to a container relocation. It identifies the container that should
   * be relocated.
   *
   * @return the origin container id for this start container request
   */
  @Public
  @Unstable
  public abstract ContainerId getOriginContainerId();
  
  /**
   * Sets the origin container id for this start container request.
   * The origin container id should be set if and only if this start container request
   * corresponds to a container relocation. It identifies the container that should
   * be relocated.
   *
   * @param originContainerId the origin container id for this start container request
   */
  @Public
  @Unstable
  public abstract void setOriginContainerId(ContainerId originContainerId);
  
  /**
   * Gets the origin node id for this start container request.
   * The origin node id is set if and only if this resource start container corresponds
   * to a container relocation. It identifies the node of the container that should
   * be relocated.
   *
   * @return the origin node id for this start container request
   */
  @Public
  @Unstable
  public abstract NodeId getOriginNodeId();
  
  /**
   * Sets the origin node id for this start container request.
   * The origin node id should be set if and only if this start container request
   * corresponds to a container relocation. It identifies the node of the container
   * that should be relocated.
   *
   * @param originNodeId the origin node id for this start container request
   */
  @Public
  @Unstable
  public abstract void setOriginNodeId(NodeId originNodeId);
  
  /**
   * Gets the security token for the origin node.
   * The origin NM token is set if and only if this start container request corresponds
   * to a container relocation. It is used for shutting down the origin container.
   *
   * @return the security token for the origin node
   */
  @Public
  @Unstable
  public abstract Token getOriginNMToken();
  
  /**
   * Sets the security token for the origin node.
   * The origin NM token should be set if and only if this start container request corresponds
   * to a container relocation. It is used for shutting down the origin container.
   *
   * @param originNMToken the security token for the origin node
   */
  @Public
  @Unstable
  public abstract void setOriginNMToken(Token originNMToken);
}
