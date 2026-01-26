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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * SubClusterInfo is a report of the runtime information of the subcluster that
 * is participating in federation.
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
public abstract class SubClusterInfo {

  @Private
  @Unstable
  @SuppressWarnings("checkstyle:ParameterNumber")
  public static SubClusterInfo newInstance(SubClusterId subClusterId,
      String amRMServiceAddress, String clientRMServiceAddress,
      String rmAdminServiceAddress, String rmWebServiceAddress,
      SubClusterState state, long lastStartTime, String capability) {
    return newInstance(subClusterId, amRMServiceAddress, clientRMServiceAddress,
        rmAdminServiceAddress, rmWebServiceAddress, 0, state, lastStartTime,
        capability);
  }

  @Private
  @Unstable
  @SuppressWarnings("checkstyle:ParameterNumber")
  public static SubClusterInfo newInstance(SubClusterId subClusterId,
      String amRMServiceAddress, String clientRMServiceAddress,
      String rmAdminServiceAddress, String rmWebServiceAddress,
      long lastHeartBeat, SubClusterState state, long lastStartTime,
      String capability) {
    SubClusterInfo subClusterInfo = Records.newRecord(SubClusterInfo.class);
    subClusterInfo.setSubClusterId(subClusterId);
    subClusterInfo.setAMRMServiceAddress(amRMServiceAddress);
    subClusterInfo.setClientRMServiceAddress(clientRMServiceAddress);
    subClusterInfo.setRMAdminServiceAddress(rmAdminServiceAddress);
    subClusterInfo.setRMWebServiceAddress(rmWebServiceAddress);
    subClusterInfo.setLastHeartBeat(lastHeartBeat);
    subClusterInfo.setState(state);
    subClusterInfo.setLastStartTime(lastStartTime);
    subClusterInfo.setCapability(capability);
    return subClusterInfo;
  }

  public static SubClusterInfo newInstance(SubClusterId subClusterId,
      String rmWebServiceAddress, SubClusterState state, long lastStartTime, long lastHeartBeat,
      String capability) {
    return newInstance(subClusterId, null, null, null,
        rmWebServiceAddress, lastHeartBeat, state, lastStartTime, capability);
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
   * Get the URL of the AM-RM service endpoint of the subcluster
   * <code>ResourceManager</code>.
   *
   * @return the URL of the AM-RM service endpoint of the subcluster
   *         <code>ResourceManager</code>
   */
  @Public
  @Unstable
  public abstract String getAMRMServiceAddress();

  /**
   * Set the URL of the AM-RM service endpoint of the subcluster
   * <code>ResourceManager</code>.
   *
   * @param amRMServiceAddress the URL of the AM-RM service endpoint of the
   *          subcluster <code>ResourceManager</code>
   */
  @Private
  @Unstable
  public abstract void setAMRMServiceAddress(String amRMServiceAddress);

  /**
   * Get the URL of the client-RM service endpoint of the subcluster
   * <code>ResourceManager</code>.
   *
   * @return the URL of the client-RM service endpoint of the subcluster
   *         <code>ResourceManager</code>
   */
  @Public
  @Unstable
  public abstract String getClientRMServiceAddress();

  /**
   * Set the URL of the client-RM service endpoint of the subcluster
   * <code>ResourceManager</code>.
   *
   * @param clientRMServiceAddress the URL of the client-RM service endpoint of
   *          the subCluster <code>ResourceManager</code>
   */
  @Private
  @Unstable
  public abstract void setClientRMServiceAddress(String clientRMServiceAddress);

  /**
   * Get the URL of the <code>ResourceManager</code> administration service.
   *
   * @return the URL of the <code>ResourceManager</code> administration service
   */
  @Public
  @Unstable
  public abstract String getRMAdminServiceAddress();

  /**
   * Set the URL of the <code>ResourceManager</code> administration service.
   *
   * @param rmAdminServiceAddress the URL of the <code>ResourceManager</code>
   *          administration service.
   */
  @Private
  @Unstable
  public abstract void setRMAdminServiceAddress(String rmAdminServiceAddress);

  /**
   * Get the URL of the <code>ResourceManager</code> web application interface.
   *
   * @return the URL of the <code>ResourceManager</code> web application
   *         interface.
   */
  @Public
  @Unstable
  public abstract String getRMWebServiceAddress();

  /**
   * Set the URL of the <code>ResourceManager</code> web application interface.
   *
   * @param rmWebServiceAddress the URL of the <code>ResourceManager</code> web
   *          application interface.
   */
  @Private
  @Unstable
  public abstract void setRMWebServiceAddress(String rmWebServiceAddress);

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
   * Get the timestamp representing the last start time of the subcluster.
   *
   * @return the timestamp representing the last start time of the subcluster
   */
  @Public
  @Unstable
  public abstract long getLastStartTime();

  /**
   * Set the timestamp representing the last start time of the subcluster.
   *
   * @param lastStartTime the timestamp representing the last start time of the
   *          subcluster
   */
  @Private
  @Unstable
  public abstract void setLastStartTime(long lastStartTime);

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
    StringBuilder sb = new StringBuilder();
    sb.append("SubClusterInfo: [")
        .append("SubClusterId: ").append(getSubClusterId()).append(", ")
        .append("AMRMServiceAddress: ").append(getAMRMServiceAddress()).append(", ")
        .append("ClientRMServiceAddress: ").append(getClientRMServiceAddress()).append(", ")
        .append("RMAdminServiceAddress: ").append(getRMAdminServiceAddress()).append(", ")
        .append("RMWebServiceAddress: ").append(getRMWebServiceAddress()).append(", ")
        .append("State: ").append(getState()).append(", ")
        .append("LastStartTime: ").append(getLastStartTime()).append(", ")
        .append("Capability: ").append(getCapability())
        .append("]");
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    if (obj instanceof SubClusterInfo) {
      SubClusterInfo other = (SubClusterInfo) obj;
      return new EqualsBuilder()
          .append(this.getSubClusterId(), other.getSubClusterId())
          .append(this.getAMRMServiceAddress(), other.getAMRMServiceAddress())
          .append(this.getClientRMServiceAddress(), other.getClientRMServiceAddress())
          .append(this.getRMAdminServiceAddress(), other.getRMAdminServiceAddress())
          .append(this.getRMWebServiceAddress(), other.getRMWebServiceAddress())
          .append(this.getState(), other.getState())
          .append(this.getLastStartTime(), other.getLastStartTime())
          .isEquals();
    }

    return false;
    // Capability and HeartBeat fields are not included as they are temporal
    // (i.e. timestamps), so they change during the lifetime of the same
    // sub-cluster
  }

  @Override
  public int hashCode() {

    return new HashCodeBuilder()
        .append(this.getSubClusterId())
        .append(this.getAMRMServiceAddress())
        .append(this.getClientRMServiceAddress())
        .append(this.getRMAdminServiceAddress())
        .append(this.getRMWebServiceAddress())
        .append(this.getState())
        .append(this.getLastStartTime())
        .toHashCode();
    // Capability and HeartBeat fields are not included as they are temporal
    // (i.e. timestamps), so they change during the lifetime of the same
    // sub-cluster
  }
}
