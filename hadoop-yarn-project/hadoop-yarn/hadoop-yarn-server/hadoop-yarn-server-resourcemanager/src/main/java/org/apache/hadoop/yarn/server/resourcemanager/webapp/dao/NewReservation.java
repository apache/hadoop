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
 * <p>The response sent by the <code>ResourceManager</code> to the client for
 * a request to get a new {@code ReservationId} for submitting reservations
 * using the REST API.</p>
 *
 * <p>Clients can submit a reservation with the returned {@code ReservationId}.
 * </p>
 *
 * {@code RMWebServices#createNewReservation(HttpServletRequest)}
 */
@XmlRootElement(name="new-reservation")
@XmlAccessorType(XmlAccessType.FIELD)
public class NewReservation {

  @XmlElement(name="reservation-id")
  private String reservationId;

  public NewReservation() {
    reservationId = "";
  }

  public NewReservation(String resId) {
    reservationId = resId;
  }

  public String getReservationId() {
    return reservationId;
  }

}
