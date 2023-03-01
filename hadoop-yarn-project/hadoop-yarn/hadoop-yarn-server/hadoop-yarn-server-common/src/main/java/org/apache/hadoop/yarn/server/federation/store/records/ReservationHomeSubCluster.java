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
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * ReservationHomeSubCluster is a report of the runtime information of the
 * reservation that is running in the federated cluster.
 *
 * <p>
 * It includes information such as:
 * <ul>
 * <li>{@link ReservationId}</li>
 * <li>{@link SubClusterId}</li>
 * </ul>
 *
 */
@Private
@Unstable
public abstract class ReservationHomeSubCluster {

  @Private
  @Unstable
  public static ReservationHomeSubCluster newInstance(ReservationId resId,
      SubClusterId homeSubCluster) {
    ReservationHomeSubCluster appMapping = Records.newRecord(ReservationHomeSubCluster.class);
    appMapping.setReservationId(resId);
    appMapping.setHomeSubCluster(homeSubCluster);
    return appMapping;
  }

  /**
   * Get the {@link ReservationId} representing the unique identifier of the
   * Reservation.
   *
   * @return the reservation identifier
   */
  @Public
  @Unstable
  public abstract ReservationId getReservationId();

  /**
   * Set the {@link ReservationId} representing the unique identifier of the
   * Reservation.
   *
   * @param resId the reservation identifier
   */
  @Private
  @Unstable
  public abstract void setReservationId(ReservationId resId);

  /**
   * Get the {@link SubClusterId} representing the unique identifier of the home
   * subcluster in which the reservation is mapped to.
   *
   * @return the home subcluster identifier
   */
  @Public
  @Unstable
  public abstract SubClusterId getHomeSubCluster();

  /**
   * Set the {@link SubClusterId} representing the unique identifier of the home
   * subcluster in which the ReservationMaster of the reservation is running.
   *
   * @param subClusterId the home subcluster identifier
   */
  @Private
  @Unstable
  public abstract void setHomeSubCluster(SubClusterId subClusterId);

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (obj instanceof ReservationHomeSubCluster) {
      ReservationHomeSubCluster other = (ReservationHomeSubCluster) obj;
      return new EqualsBuilder()
          .append(this.getReservationId(), other.getReservationId())
          .append(this.getHomeSubCluster(), other.getHomeSubCluster())
          .isEquals();
    }

    return false;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().
        append(this.getReservationId()).
        append(this.getHomeSubCluster()).
        toHashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ReservationHomeSubCluster: [")
        .append("ReservationId: ").append(getReservationId()).append(", ")
        .append("HomeSubCluster: ").append(getHomeSubCluster())
        .append("]");
    return sb.toString();
  }
}
