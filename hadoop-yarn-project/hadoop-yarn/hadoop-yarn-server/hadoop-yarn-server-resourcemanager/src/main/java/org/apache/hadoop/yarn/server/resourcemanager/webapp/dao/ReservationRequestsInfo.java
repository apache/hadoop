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

import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequests;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Simple class representing a list of ReservationRequest and the
 * interpreter which capture the semantic of this list (all/any/order).
 */
@XmlRootElement(name = "reservation-definition")
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationRequestsInfo {

  @XmlElement(name = "reservation-request-interpreter")
  private int reservationRequestsInterpreter;
  @XmlElement(name = "reservation-request")
  private ArrayList<ReservationRequestInfo> reservationRequest;

  public ReservationRequestsInfo() {

  }

  public ReservationRequestsInfo(ReservationRequests requests) {
    reservationRequest = new ArrayList<>();
    for (ReservationRequest request : requests.getReservationResources()) {
      reservationRequest.add(new ReservationRequestInfo(request));
    }
    reservationRequestsInterpreter = requests.getInterpreter().ordinal();
  }

  public int getReservationRequestsInterpreter() {
    return reservationRequestsInterpreter;
  }

  public void setReservationRequestsInterpreter(
      int reservationRequestsInterpreter) {
    this.reservationRequestsInterpreter = reservationRequestsInterpreter;
  }

  public ArrayList<ReservationRequestInfo> getReservationRequest() {
    return reservationRequest;
  }

  public void setReservationRequest(
      ArrayList<ReservationRequestInfo> reservationRequest) {
    this.reservationRequest = reservationRequest;
  }

}