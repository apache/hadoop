/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * SubClusterHeartbeatRequest is a report of the runtime information of the
 * subcluster that is participating in federation.
 *
 * <p>
 * It includes information such as:
 * <ul>
 * <li>{@link SubClusterId}</li>
 * <li>The URL of the subcluster</li>
 * <li>The timestamp representing the last start time of the subCluster</li>
 * <li>{@code FederationsubClusterState}</li>
 * <li>The current capacity and utilization of the subCluster</li>
 * </ul>
 */
@Private
@Unstable
public abstract class SubClusterHeartbeatRequest {

  @Private
  @Unstable
  public static SubClusterHeartbeatRequest newInstance(
      SubClusterId subClusterId, SubClusterState state, String capability) {
    return newInstance(subClusterId, 0, state, capability);
  }

  @Private
  @Unstable
  public static SubClusterHeartbeatRequest newInstance(
      SubClusterId subClusterId, long lastHeartBeat, SubClusterState state,
      String capability) {
    SubClusterHeartbeatRequest subClusterHeartbeatRequest =
        Records.newRecord(SubClusterHeartbeatRequest.class);
    subClusterHeartbeatRequest.setSubClusterId(subClusterId);
    subClusterHeartbeatRequest.setLastHeartBeat(lastHeartBeat);
    subClusterHeartbeatRequest.setState(state);
    subClusterHeartbeatRequest.setCapability(capability);
    return subClusterHeartbeatRequest;
  }

  /**
   * Get the {@link SubClusterId} representing the unique identifier of the
   * subcluster.
   *
   * @return the subcluster identifier
   */
  @Public
  @Unstable
  public abstract SubClusterId getSubClusterId();

  /**
   * Set the {@link SubClusterId} representing the unique identifier of the
   * subCluster.
   *
   * @param subClusterId the subCluster identifier
   */
  @Private
  @Unstable
  public abstract void setSubClusterId(SubClusterId subClusterId);

  /**
   * Get the last heart beat time of the subcluster.
   *
   * @return the state of the subcluster
   */
  @Public
  @Unstable
  public abstract long getLastHeartBeat();

  /**
   * Set the last heartbeat time of the subcluster.
   *
   * @param time the last heartbeat time of the subcluster
   */
  @Private
  @Unstable
  public abstract void setLastHeartBeat(long time);

  /**
   * Get the {@link SubClusterState} of the subcluster.
   *
   * @return the state of the subcluster
   */
  @Public
  @Unstable
  public abstract SubClusterState getState();

  /**
   * Set the {@link SubClusterState} of the subcluster.
   *
   * @param state the state of the subCluster
   */
  @Private
  @Unstable
  public abstract void setState(SubClusterState state);

  /**
   * Get the current capacity and utilization of the subcluster. This is the
   * JAXB marshalled string representation of the <code>ClusterMetrics</code>.
   *
   * @return the current capacity and utilization of the subcluster
   */
  @Public
  @Unstable
  public abstract String getCapability();

  /**
   * Set the current capacity and utilization of the subCluster. This is the
   * JAXB marshalled string representation of the <code>ClusterMetrics</code>.
   *
   * @param capability the current capacity and utilization of the subcluster
   */
  @Private
  @Unstable
  public abstract void setCapability(String capability);

  @Override
  public String toString() {
    return "SubClusterHeartbeatRequest [getSubClusterId() = "
        + getSubClusterId() + ", getState() = " + getState()
        + ", getLastHeartBeat = " + getLastHeartBeat() + ", getCapability() = "
        + getCapability() + "]";
  }

}
