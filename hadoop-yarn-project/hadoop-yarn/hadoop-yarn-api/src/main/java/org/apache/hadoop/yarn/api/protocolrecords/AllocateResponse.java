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
import org.apache.hadoop.yarn.api.records.CollectorInfo;
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
    return AllocateResponse.newBuilder().numClusterNodes(numClusterNodes)
        .responseId(responseId)
        .completedContainersStatuses(completedContainers)
        .allocatedContainers(allocatedContainers).updatedNodes(updatedNodes)
        .availableResources(availResources).amCommand(command)
        .preemptionMessage(preempt).nmTokens(nmTokens).build();
  }

  @Private
  @Unstable
  public static AllocateResponse newInstance(int responseId,
      List<ContainerStatus> completedContainers,
      List<Container> allocatedContainers, List<NodeReport> updatedNodes,
      Resource availResources, AMCommand command, int numClusterNodes,
      PreemptionMessage preempt, List<NMToken> nmTokens,
      CollectorInfo collectorInfo) {
    return AllocateResponse.newBuilder().numClusterNodes(numClusterNodes)
        .responseId(responseId)
        .completedContainersStatuses(completedContainers)
        .allocatedContainers(allocatedContainers).updatedNodes(updatedNodes)
        .availableResources(availResources).amCommand(command)
        .preemptionMessage(preempt).nmTokens(nmTokens)
        .collectorInfo(collectorInfo).build();
  }

  @Private
  @Unstable
  public static AllocateResponse newInstance(int responseId,
      List<ContainerStatus> completedContainers,
      List<Container> allocatedContainers, List<NodeReport> updatedNodes,
      Resource availResources, AMCommand command, int numClusterNodes,
      PreemptionMessage preempt, List<NMToken> nmTokens, Token amRMToken,
      List<UpdatedContainer> updatedContainers) {
    return AllocateResponse.newBuilder().numClusterNodes(numClusterNodes)
        .responseId(responseId)
        .completedContainersStatuses(completedContainers)
        .allocatedContainers(allocatedContainers).updatedNodes(updatedNodes)
        .availableResources(availResources).amCommand(command)
        .preemptionMessage(preempt).nmTokens(nmTokens)
        .updatedContainers(updatedContainers).amRmToken(amRMToken).build();
  }

  @Public
  @Unstable
  public static AllocateResponse newInstance(int responseId,
      List<ContainerStatus> completedContainers,
      List<Container> allocatedContainers, List<NodeReport> updatedNodes,
      Resource availResources, AMCommand command, int numClusterNodes,
      PreemptionMessage preempt, List<NMToken> nmTokens, Token amRMToken,
      List<UpdatedContainer> updatedContainers, CollectorInfo collectorInfo) {
    return AllocateResponse.newBuilder().numClusterNodes(numClusterNodes)
        .responseId(responseId)
        .completedContainersStatuses(completedContainers)
        .allocatedContainers(allocatedContainers).updatedNodes(updatedNodes)
        .availableResources(availResources).amCommand(command)
        .preemptionMessage(preempt).nmTokens(nmTokens)
        .updatedContainers(updatedContainers).amRmToken(amRMToken)
        .collectorInfo(collectorInfo).build();
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
   * The data associated with the collector that belongs to this app. Contains
   * address and token alongwith identification information.
   *
   * @return The data of collector that belong to this attempt
   */
  @Public
  @Unstable
  public abstract CollectorInfo getCollectorInfo();

  @Private
  @Unstable
  public abstract void setCollectorInfo(CollectorInfo info);

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

  @Private
  @Unstable
  public static AllocateResponseBuilder newBuilder() {
    return new AllocateResponseBuilder();
  }

  /**
   * Class to construct instances of {@link AllocateResponse} with specific
   * options.
   */
  @Private
  @Unstable
  public static final class AllocateResponseBuilder {
    private AllocateResponse allocateResponse =
        Records.newRecord(AllocateResponse.class);

    private AllocateResponseBuilder() {
      allocateResponse.setApplicationPriority(Priority.newInstance(0));
    }

    /**
     * Set the <code>amCommand</code> of the response.
     * @see AllocateResponse#setAMCommand(AMCommand)
     * @param amCommand <code>amCommand</code> of the response
     * @return {@link AllocateResponseBuilder}
     */
    @Private
    @Unstable
    public AllocateResponseBuilder amCommand(AMCommand amCommand) {
      allocateResponse.setAMCommand(amCommand);
      return this;
    }

    /**
     * Set the <code>responseId</code> of the response.
     * @see AllocateResponse#setResponseId(int)
     * @param responseId <code>responseId</code> of the response
     * @return {@link AllocateResponseBuilder}
     */
    @Private
    @Unstable
    public AllocateResponseBuilder responseId(int responseId) {
      allocateResponse.setResponseId(responseId);
      return this;
    }

    /**
     * Set the <code>allocatedContainers</code> of the response.
     * @see AllocateResponse#setAllocatedContainers(List)
     * @param allocatedContainers
     *     <code>allocatedContainers</code> of the response
     * @return {@link AllocateResponseBuilder}
     */
    @Private
    @Unstable
    public AllocateResponseBuilder allocatedContainers(
        List<Container> allocatedContainers) {
      allocateResponse.setAllocatedContainers(allocatedContainers);
      return this;
    }

    /**
     * Set the <code>availableResources</code> of the response.
     * @see AllocateResponse#setAvailableResources(Resource)
     * @param availableResources
     *     <code>availableResources</code> of the response
     * @return {@link AllocateResponseBuilder}
     */
    @Private
    @Unstable
    public AllocateResponseBuilder availableResources(
        Resource availableResources) {
      allocateResponse.setAvailableResources(availableResources);
      return this;
    }

    /**
     * Set the <code>completedContainersStatuses</code> of the response.
     * @see AllocateResponse#setCompletedContainersStatuses(List)
     * @param completedContainersStatuses
     *     <code>completedContainersStatuses</code> of the response
     * @return {@link AllocateResponseBuilder}
     */
    @Private
    @Unstable
    public AllocateResponseBuilder completedContainersStatuses(
        List<ContainerStatus> completedContainersStatuses) {
      allocateResponse
          .setCompletedContainersStatuses(completedContainersStatuses);
      return this;
    }

    /**
     * Set the <code>updatedNodes</code> of the response.
     * @see AllocateResponse#setUpdatedNodes(List)
     * @param updatedNodes <code>updatedNodes</code> of the response
     * @return {@link AllocateResponseBuilder}
     */
    @Private
    @Unstable
    public AllocateResponseBuilder updatedNodes(
        List<NodeReport> updatedNodes) {
      allocateResponse.setUpdatedNodes(updatedNodes);
      return this;
    }

    /**
     * Set the <code>numClusterNodes</code> of the response.
     * @see AllocateResponse#setNumClusterNodes(int)
     * @param numClusterNodes <code>numClusterNodes</code> of the response
     * @return {@link AllocateResponseBuilder}
     */
    @Private
    @Unstable
    public AllocateResponseBuilder numClusterNodes(int numClusterNodes) {
      allocateResponse.setNumClusterNodes(numClusterNodes);
      return this;
    }

    /**
     * Set the <code>preemptionMessage</code> of the response.
     * @see AllocateResponse#setPreemptionMessage(PreemptionMessage)
     * @param preemptionMessage <code>preemptionMessage</code> of the response
     * @return {@link AllocateResponseBuilder}
     */
    @Private
    @Unstable
    public AllocateResponseBuilder preemptionMessage(
        PreemptionMessage preemptionMessage) {
      allocateResponse.setPreemptionMessage(preemptionMessage);
      return this;
    }

    /**
     * Set the <code>nmTokens</code> of the response.
     * @see AllocateResponse#setNMTokens(List)
     * @param nmTokens <code>nmTokens</code> of the response
     * @return {@link AllocateResponseBuilder}
     */
    @Private
    @Unstable
    public AllocateResponseBuilder nmTokens(List<NMToken> nmTokens) {
      allocateResponse.setNMTokens(nmTokens);
      return this;
    }

    /**
     * Set the <code>updatedContainers</code> of the response.
     * @see AllocateResponse#setUpdatedContainers(List)
     * @param updatedContainers <code>updatedContainers</code> of the response
     * @return {@link AllocateResponseBuilder}
     */
    @Private
    @Unstable
    public AllocateResponseBuilder updatedContainers(
        List<UpdatedContainer> updatedContainers) {
      allocateResponse.setUpdatedContainers(updatedContainers);
      return this;
    }

    /**
     * Set the <code>amRmToken</code> of the response.
     * @see AllocateResponse#setAMRMToken(Token)
     * @param amRmToken <code>amRmToken</code> of the response
     * @return {@link AllocateResponseBuilder}
     */
    @Private
    @Unstable
    public AllocateResponseBuilder amRmToken(Token amRmToken) {
      allocateResponse.setAMRMToken(amRmToken);
      return this;
    }

    /**
     * Set the <code>applicationPriority</code> of the response.
     * @see AllocateResponse#setApplicationPriority(Priority)
     * @param applicationPriority
     *     <code>applicationPriority</code> of the response
     * @return {@link AllocateResponseBuilder}
     */
    @Private
    @Unstable
    public AllocateResponseBuilder applicationPriority(
        Priority applicationPriority) {
      allocateResponse.setApplicationPriority(applicationPriority);
      return this;
    }

    /**
     * Set the <code>collectorInfo</code> of the response.
     * @see AllocateResponse#setCollectorInfo(CollectorInfo)
     * @param collectorInfo <code>collectorInfo</code> of the response which
     *    contains collector address, RM id, version and collector token.
     * @return {@link AllocateResponseBuilder}
     */
    @Private
    @Unstable
    public AllocateResponseBuilder collectorInfo(
        CollectorInfo collectorInfo) {
      allocateResponse.setCollectorInfo(collectorInfo);
      return this;
    }

    /**
     * Set the <code>updateErrors</code> of the response.
     * @see AllocateResponse#setUpdateErrors(List)
     * @param updateErrors <code>updateErrors</code> of the response
     * @return {@link AllocateResponseBuilder}
     */
    @Private
    @Unstable
    public AllocateResponseBuilder updateErrors(
        List<UpdateContainerError> updateErrors) {
      allocateResponse.setUpdateErrors(updateErrors);
      return this;
    }

    /**
     * Return generated {@link AllocateResponse} object.
     * @return {@link AllocateResponse}
     */
    @Private
    @Unstable
    public AllocateResponse build() {
      return allocateResponse;
    }
  }
}
