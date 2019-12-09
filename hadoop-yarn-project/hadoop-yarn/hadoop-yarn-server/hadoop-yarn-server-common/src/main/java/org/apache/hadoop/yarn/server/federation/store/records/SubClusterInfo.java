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
    return "SubClusterInfo [getSubClusterId() = " + getSubClusterId()
        + ", getAMRMServiceAddress() = " + getAMRMServiceAddress()
        + ", getClientRMServiceAddress() = " + getClientRMServiceAddress()
        + ", getRMAdminServiceAddress() = " + getRMAdminServiceAddress()
        + ", getRMWebServiceAddress() = " + getRMWebServiceAddress()
        + ", getState() = " + getState() + ", getLastStartTime() = "
        + getLastStartTime() + ", getCapability() = " + getCapability() + "]";
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
    SubClusterInfo other = (SubClusterInfo) obj;
    if (!this.getSubClusterId().equals(other.getSubClusterId())) {
      return false;
    }
    if (!this.getAMRMServiceAddress().equals(other.getAMRMServiceAddress())) {
      return false;
    }
    if (!this.getClientRMServiceAddress()
        .equals(other.getClientRMServiceAddress())) {
      return false;
    }
    if (!this.getRMAdminServiceAddress()
        .equals(other.getRMAdminServiceAddress())) {
      return false;
    }
    if (!this.getRMWebServiceAddress().equals(other.getRMWebServiceAddress())) {
      return false;
    }
    if (!this.getState().equals(other.getState())) {
      return false;
    }
    return this.getLastStartTime() == other.getLastStartTime();
    // Capability and HeartBeat fields are not included as they are temporal
    // (i.e. timestamps), so they change during the lifetime of the same
    // sub-cluster
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((getSubClusterId() == null) ? 0 : getSubClusterId().hashCode());
    result = prime * result + ((getAMRMServiceAddress() == null) ? 0
        : getAMRMServiceAddress().hashCode());
    result = prime * result + ((getClientRMServiceAddress() == null) ? 0
        : getClientRMServiceAddress().hashCode());
    result = prime * result + ((getRMAdminServiceAddress() == null) ? 0
        : getRMAdminServiceAddress().hashCode());
    result = prime * result + ((getRMWebServiceAddress() == null) ? 0
        : getRMWebServiceAddress().hashCode());
    result =
        prime * result + ((getState() == null) ? 0 : getState().hashCode());
    result = prime * result
        + (int) (getLastStartTime() ^ (getLastStartTime() >>> 32));
    return result;
    // Capability and HeartBeat fields are not included as they are temporal
    // (i.e. timestamps), so they change during the lifetime of the same
    // sub-cluster
  }
}
