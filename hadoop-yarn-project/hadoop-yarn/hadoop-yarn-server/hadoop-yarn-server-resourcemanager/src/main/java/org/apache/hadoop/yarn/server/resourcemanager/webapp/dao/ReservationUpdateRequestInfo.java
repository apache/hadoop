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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Simple class to allow users to send information required to update an
 * existing reservation.
 */
@XmlRootElement(name = "reservation-update-context")
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationUpdateRequestInfo {

  @XmlElement(name = "reservation-id")
  private String reservationId;

  @XmlElement(name = "reservation-definition")
  private ReservationDefinitionInfo reservationDefinition;

  public ReservationUpdateRequestInfo() {
  }

  public String getReservationId() {
    return reservationId;
  }

  public void setReservationId(String reservationId) {
    this.reservationId = reservationId;
  }

  public ReservationDefinitionInfo getReservationDefinition() {
    return reservationDefinition;
  }

  public void setReservationDefinition(
      ReservationDefinitionInfo reservationDefinition) {
    this.reservationDefinition = reservationDefinition;
  }

}
