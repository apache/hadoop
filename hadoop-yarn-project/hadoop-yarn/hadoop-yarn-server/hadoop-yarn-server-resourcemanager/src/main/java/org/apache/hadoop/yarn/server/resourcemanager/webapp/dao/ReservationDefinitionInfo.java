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

import org.apache.hadoop.yarn.api.records.ReservationDefinition;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Simple class that represent a reservation definition.
 */
@XmlRootElement(name = "reservation-definition")
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationDefinitionInfo {

  @XmlElement(name = "arrival")
  private long arrival;

  @XmlElement(name = "deadline")
  private long deadline;

  @XmlElement(name = "reservation-requests")
  private ReservationRequestsInfo reservationRequests;

  @XmlElement(name = "reservation-name")
  private String reservationName;

  public ReservationDefinitionInfo() {

  }

  public ReservationDefinitionInfo(ReservationDefinition definition) {
    arrival = definition.getArrival();
    deadline = definition.getDeadline();
    reservationName = definition.getReservationName();
    reservationRequests = new ReservationRequestsInfo(definition
            .getReservationRequests());
  }

  public long getArrival() {
    return arrival;
  }

  public void setArrival(long arrival) {
    this.arrival = arrival;
  }

  public long getDeadline() {
    return deadline;
  }

  public void setDeadline(long deadline) {
    this.deadline = deadline;
  }

  public ReservationRequestsInfo getReservationRequests() {
    return reservationRequests;
  }

  public void setReservationRequests(
      ReservationRequestsInfo reservationRequests) {
    this.reservationRequests = reservationRequests;
  }

  public String getReservationName() {
    return reservationName;
  }

  public void setReservationName(String reservationName) {
    this.reservationName = reservationName;
  }

}
