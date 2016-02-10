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

import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.records.ReservationAllocationState;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple class that represent a list of reservations.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationListInfo {
  @XmlElement(name = "reservations")
  private List<ReservationInfo> reservations;

  public ReservationListInfo() {
    reservations = new ArrayList<>();
  }

  public ReservationListInfo(ReservationListResponse response,
        boolean includeResourceAllocations) throws Exception {
    this();

    for (ReservationAllocationState allocation :
            response.getReservationAllocationState()) {
      reservations.add(new ReservationInfo(allocation,
              includeResourceAllocations));
    }
  }
}
