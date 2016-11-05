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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.util.Records;

/**
 * The response sent by the <code>ResourceManager</code> the
 * <code>ApplicationMaster</code> during resource negotiation.
 * <p>
 * The response, includes:
 * <ul>
 *   <li>Response ID to track duplicate responses.</li>
 *   <li>
 *     An AMCommand sent by ResourceManager to let the
 *     {@code ApplicationMaster} take some actions (resync, shutdown etc.).
 *   </li>
 *   <li>A list of newly allocated {@link Container}.</li>
 *   <li>A list of completed {@link Container}s' statuses.</li>
 *   <li>
 *     The available headroom for resources in the cluster for the
 *     application.
 *   </li>
 *   <li>A list of nodes whose status has been updated.</li>
 *   <li>The number of available nodes in a cluster.</li>
 *   <li>A description of resources requested back by the cluster</li>
 *   <li>AMRMToken, if AMRMToken has been rolled over</li>
 *   <li>
 *     A list of {@link Container} representing the containers
 *     whose resource has been increased.
 *   </li>
 *   <li>
 *     A list of {@link Container} representing the containers
 *     whose resource has been decreased.
 *   </li>
 * </ul>
 * 
 * @see ApplicationMasterProtocol#allocate(AllocateRequest)
 */
@Public
@Stable
public abstract class AllocateResponse {

  @Public
  @Stable
  public static AllocateResponse newInstance(int responseId,
      List<ContainerStatus> completedContainers,
      List<Container> allocatedContainers, List<NodeReport> updatedNodes,
      Resource availResources, AMCommand command, int numClusterNodes,
      PreemptionMessage preempt, List<NMToken> nmTokens) {
    AllocateResponse response = Records.newRecord(AllocateResponse.class);
    response.setNumClusterNodes(numClusterNodes);
    response.setResponseId(responseId);
    response.setCompletedContainersStatuses(completedContainers);
    response.setAllocatedContainers(allocatedContainers);
    response.setUpdatedNodes(updatedNodes);
    response.setAvailableResources(availResources);
    response.setAMCommand(command);
    response.setPreemptionMessage(preempt);
    response.setNMTokens(nmTokens);
    return response;
  }

  @Public
  @Unstable
  public static AllocateResponse newInstance(int responseId,
      List<ContainerStatus> completedContainers,
      List<Container> allocatedContainers, List<NodeReport> updatedNodes,
      Resource availResources, AMCommand command, int numClusterNodes,
      PreemptionMessage preempt, List<NMToken> nmTokens,
      List<UpdatedContainer> updatedContainers) {
    AllocateResponse response = newInstance(responseId, completedContainers,
        allocatedContainers, updatedNodes, availResources, command,
        numClusterNodes, preempt, nmTokens);
    response.setUpdatedContainers(updatedContainers);
    return response;
  }

  @Private
  @Unstable
  public static AllocateResponse newInstance(int responseId,
      List<ContainerStatus> completedContainers,
      List<Container> allocatedContainers, List<NodeReport> updatedNodes,
      Resource availResources, AMCommand command, int numClusterNodes,
      PreemptionMessage preempt, List<NMToken> nmTokens, Token amRMToken,
      List<UpdatedContainer> updatedContainers) {
    AllocateResponse response =
        newInstance(responseId, completedContainers, allocatedContainers,
          updatedNodes, availResources, command, numClusterNodes, preempt,
          nmTokens, updatedContainers);
    response.setAMRMToken(amRMToken);
    return response;
  }

  @Public
  @Unstable
  public static AllocateResponse newInstance(int responseId,
      List<ContainerStatus> completedContainers,
      List<Container> allocatedContainers, List<NodeReport> updatedNodes,
      Resource availResources, AMCommand command, int numClusterNodes,
      PreemptionMessage preempt, List<NMToken> nmTokens, Token amRMToken,
      List<UpdatedContainer> updatedContainers, String collectorAddr) {
    AllocateResponse response =
        newInstance(responseId, completedContainers, allocatedContainers,
          updatedNodes, availResources, command, numClusterNodes, preempt,
          nmTokens, updatedContainers);
    response.setAMRMToken(amRMToken);
    response.setCollectorAddr(collectorAddr);
    return response;
  }

  /**
   * If the <code>ResourceManager</code> needs the
   * <code>ApplicationMaster</code> to take some action then it will send an
   * AMCommand to the <code>ApplicationMaster</code>. See <code>AMCommand</code> 
   * for details on commands and actions for them.
   * @return <code>AMCommand</code> if the <code>ApplicationMaster</code> should
   *         take action, <code>null</code> otherwise
   * @see AMCommand
   */
  @Public
  @Stable
  public abstract AMCommand getAMCommand();

  @Private
  @Unstable
  public abstract void setAMCommand(AMCommand command);

  /**
   * Get the <em>last response id</em>.
   * @return <em>last response id</em>
   */
  @Public
  @Stable
  public abstract int getResponseId();

  @Private
  @Unstable
  public abstract void setResponseId(int responseId);

  /**
   * Get the list of <em>newly allocated</em> <code>Container</code> by the
   * <code>ResourceManager</code>.
   * @return list of <em>newly allocated</em> <code>Container</code>
   */
  @Public
  @Stable
  public abstract List<Container> getAllocatedContainers();

  /**
   * Set the list of <em>newly allocated</em> <code>Container</code> by the
   * <code>ResourceManager</code>.
   * @param containers list of <em>newly allocated</em> <code>Container</code>
   */
  @Private
  @Unstable
  public abstract void setAllocatedContainers(List<Container> containers);

  /**
   * Get the <em>available headroom</em> for resources in the cluster for the
   * application.
   * @return limit of available headroom for resources in the cluster for the
   * application
   */
  @Public
  @Stable
  public abstract Resource getAvailableResources();

  @Private
  @Unstable
  public abstract void setAvailableResources(Resource limit);

  /**
   * Get the list of <em>completed containers' statuses</em>.
   * @return the list of <em>completed containers' statuses</em>
   */
  @Public
  @Stable
  public abstract List<ContainerStatus> getCompletedContainersStatuses();

  @Private
  @Unstable
  public abstract void setCompletedContainersStatuses(List<ContainerStatus> containers);

  /**
   * Get the list of <em>updated <code>NodeReport</code>s</em>. Updates could
   * be changes in health, availability etc of the nodes.
   * @return The delta of updated nodes since the last response
   */
  @Public
  @Stable
  public abstract  List<NodeReport> getUpdatedNodes();

  @Private
  @Unstable
  public abstract void setUpdatedNodes(final List<NodeReport> updatedNodes);

  /**
   * Get the number of hosts available on the cluster.
   * @return the available host count.
   */
  @Public
  @Stable
  public abstract int getNumClusterNodes();
  
  @Private
  @Unstable
  public abstract void setNumClusterNodes(int numNodes);

  /**
   * Get the description of containers owned by the AM, but requested back by
   * the cluster. Note that the RM may have an inconsistent view of the
   * resources owned by the AM. These messages are advisory, and the AM may
   * elect to ignore them.
   * <p>
   * The message is a snapshot of the resources the RM wants back from the AM.
   * While demand persists, the RM will repeat its request; applications should
   * not interpret each message as a request for <em>additional</em>
   * resources on top of previous messages. Resources requested consistently
   * over some duration may be forcibly killed by the RM.
   *
   * @return A specification of the resources to reclaim from this AM.
   */
  @Public
  @Evolving
  public abstract PreemptionMessage getPreemptionMessage();

  @Private
  @Unstable
  public abstract void setPreemptionMessage(PreemptionMessage request);

  /**
   * Get the list of NMTokens required for communicating with NM. New NMTokens
   * issued only if
   * <p>
   * 1) AM is receiving first container on underlying NodeManager.<br>
   * OR<br>
   * 2) NMToken master key rolled over in ResourceManager and AM is getting new
   * container on the same underlying NodeManager.
   * <p>
   * AM will receive one NMToken per NM irrespective of the number of containers
   * issued on same NM. AM is expected to store these tokens until issued a
   * new token for the same NM.
   * @return list of NMTokens required for communicating with NM
   */
  @Public
  @Stable
  public abstract List<NMToken> getNMTokens();

  @Private
  @Unstable
  public abstract void setNMTokens(List<NMToken> nmTokens);
  
  /**
   * Get the list of newly updated containers by
   * <code>ResourceManager</code>.
   * @return list of newly increased containers
   */
  @Public
  @Unstable
  public abstract List<UpdatedContainer> getUpdatedContainers();

  /**
   * Set the list of newly updated containers by
   * <code>ResourceManager</code>.
   *
   * @param updatedContainers List of Updated Containers.
   */
  @Private
  @Unstable
  public abstract void setUpdatedContainers(
      List<UpdatedContainer> updatedContainers);

  /**
   * The AMRMToken that belong to this attempt
   *
   * @return The AMRMToken that belong to this attempt
   */
  @Public
  @Unstable
  public abstract Token getAMRMToken();

  @Private
  @Unstable
  public abstract void setAMRMToken(Token amRMToken);

  /**
   * Priority of the application
   *
   * @return get application priority
   */
  @Public
  @Unstable
  public abstract Priority getApplicationPriority();

  @Private
  @Unstable
  public abstract void setApplicationPriority(Priority priority);

  /**
   * The address of collector that belong to this app
   *
   * @return The address of collector that belong to this attempt
   */
  @Public
  @Unstable
  public abstract String getCollectorAddr();

  @Private
  @Unstable
  public abstract void setCollectorAddr(String collectorAddr);

  /**
   * Get the list of container update errors to inform the
   * Application Master about the container updates that could not be
   * satisfied due to error.
   *
   * @return List of Update Container Errors.
   */
  @Public
  @Unstable
  public List<UpdateContainerError> getUpdateErrors() {
    return new ArrayList<>();
  }

  /**
   * Set the list of container update errors to inform the
   * Application Master about the container updates that could not be
   * satisfied due to error.
   * @param updateErrors list of <code>UpdateContainerError</code> for
   *                       containers updates requests that were in error
   */
  @Public
  @Unstable
  public void setUpdateErrors(List<UpdateContainerError> updateErrors) {
  }
}
