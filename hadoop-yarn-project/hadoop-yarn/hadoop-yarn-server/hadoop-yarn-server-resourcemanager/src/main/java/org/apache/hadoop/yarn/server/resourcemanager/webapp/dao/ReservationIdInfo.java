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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.yarn.api.records.ReservationId;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Simple class that represent a reservation ID.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationIdInfo {
  @XmlElement(name = "cluster-timestamp")
  private long clusterTimestamp;

  @XmlElement(name = "reservation-id")
  private long reservationId;

  public ReservationIdInfo() {
    this.clusterTimestamp = 0;
    this.reservationId = 0;
  }

  public ReservationIdInfo(ReservationId reservationId) {
    this.clusterTimestamp = reservationId.getClusterTimestamp();
    this.reservationId = reservationId.getId();
  }

  public long getClusterTimestamp() {
    return this.clusterTimestamp;
  }

  public void setClusterTimestamp(long newClusterTimestamp) {
    this.clusterTimestamp = newClusterTimestamp;
  }

  public long getReservationId() {
    return this.reservationId;
  }

  public void setReservationId(long newReservationId) {
    this.reservationId = newReservationId;
  }
}
