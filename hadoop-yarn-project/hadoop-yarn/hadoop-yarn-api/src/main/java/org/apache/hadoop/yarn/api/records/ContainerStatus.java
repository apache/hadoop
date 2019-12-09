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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * {@code ContainerStatus} represents the current status of a
 * {@code Container}.
 * <p>
 * It provides details such as:
 * <ul>
 *   <li>{@code ContainerId} of the container.</li>
 *   <li>{@code ExecutionType} of the container.</li>
 *   <li>{@code ContainerState} of the container.</li>
 *   <li><em>Exit status</em> of a completed container.</li>
 *   <li><em>Diagnostic</em> message for a failed container.</li>
 *   <li>{@link Resource} allocated to the container.</li>
 * </ul>
 */
@Public
@Stable
public abstract class ContainerStatus {

  @Private
  @Unstable
  public static ContainerStatus newInstance(ContainerId containerId,
      ContainerState containerState, String diagnostics, int exitStatus) {
    return newInstance(containerId, ExecutionType.GUARANTEED, containerState,
        diagnostics, exitStatus);
  }

  @Private
  @Unstable
  public static ContainerStatus newInstance(ContainerId containerId,
      ExecutionType executionType, ContainerState containerState,
      String diagnostics, int exitStatus) {
    ContainerStatus containerStatus = Records.newRecord(ContainerStatus.class);
    containerStatus.setExecutionType(executionType);
    containerStatus.setState(containerState);
    containerStatus.setContainerId(containerId);
    containerStatus.setDiagnostics(diagnostics);
    containerStatus.setExitStatus(exitStatus);
    return containerStatus;
  }

  /**
   * Get the <code>ContainerId</code> of the container.
   * @return <code>ContainerId</code> of the container
   */
  @Public
  @Stable
  public abstract ContainerId getContainerId();
  
  @Private
  @Unstable
  public abstract void setContainerId(ContainerId containerId);

  /**
   * Get the <code>ExecutionType</code> of the container.
   * @return <code>ExecutionType</code> of the container
   */
  @Public
  @Evolving
  public ExecutionType getExecutionType() {
    throw new UnsupportedOperationException(
        "subclass must implement this method");
  }

  @Private
  @Unstable
  public void setExecutionType(ExecutionType executionType) {
    throw new UnsupportedOperationException(
        "subclass must implement this method");
  }

  /**
   * Get the <code>ContainerState</code> of the container.
   * @return <code>ContainerState</code> of the container
   */
  @Public
  @Stable
  public abstract ContainerState getState();
  
  @Private
  @Unstable
  public abstract void setState(ContainerState state);

  /**
   * <p>Get the <em>exit status</em> for the container.</p>
   *  
   * <p>Note: This is valid only for completed containers i.e. containers
   * with state {@link ContainerState#COMPLETE}. 
   * Otherwise, it returns an ContainerExitStatus.INVALID.
   * </p>
   * 
   * <p>Containers killed by the framework, either due to being released by
   * the application or being 'lost' due to node failures etc. have a special
   * exit code of ContainerExitStatus.ABORTED.</p>
   * 
   * <p>When threshold number of the nodemanager-local-directories or
   * threshold number of the nodemanager-log-directories become bad, then
   * container is not launched and is exited with ContainersExitStatus.DISKS_FAILED.
   * </p>
   *  
   * @return <em>exit status</em> for the container
   */
  @Public
  @Unstable
  public abstract int getExitStatus();
  
  @Private
  @Unstable
  public abstract void setExitStatus(int exitStatus);

  /**
   * Get <em>diagnostic messages</em> for failed containers.
   * @return <em>diagnostic messages</em> for failed containers
   */
  @Public
  @Stable
  public abstract String getDiagnostics();
  
  @Private
  @Unstable
  public abstract void setDiagnostics(String diagnostics);

  /**
   * Get the <code>Resource</code> allocated to the container.
   * @return <code>Resource</code> allocated to the container
   */
  @Public
  @Unstable
  public Resource getCapability() {
    throw new UnsupportedOperationException(
        "subclass must implement this method");
  }

  @Private
  @Unstable
  public void setCapability(Resource capability) {
    throw new UnsupportedOperationException(
        "subclass must implement this method");
  }

  /**
   * Get all the IP addresses with which the container run.
   * @return The IP address where the container runs.
   */
  @Public
  @Unstable
  public List<String> getIPs() {
    throw new UnsupportedOperationException(
        "subclass must implement this method");
  }

  @Private
  @Unstable
  public void setIPs(List<String> ips) {
    throw new UnsupportedOperationException(
        "subclass must implement this method");
  }

  /**
   * Get the hostname where the container runs.
   * @return The hostname where the container runs.
   */
  @Public
  @Unstable
  public String getHost() {
    throw new UnsupportedOperationException(
        "subclass must implement this method");
  }

  @Private
  @Unstable
  public void setHost(String host) {
    throw new UnsupportedOperationException(
        "subclass must implement this method");
  }

  /**
   * Add Extra state information of the container (SCHEDULED, LOCALIZING etc.).
   * @param subState Extra State Information.
   */
  @Private
  @Unstable
  public void setContainerSubState(ContainerSubState subState) {
    throw new UnsupportedOperationException(
        "subclass must implement this method");
  }

  /**
   * Get Extra state information of the container (SCHEDULED, LOCALIZING etc.).
   * @return Extra State information.
   */
  @Private
  @Unstable
  public ContainerSubState getContainerSubState() {
    throw new UnsupportedOperationException(
        "subclass must implement this method");
  }
}
