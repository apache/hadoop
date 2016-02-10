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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Simple class representing a reservation request.
 */
@XmlRootElement(name = "reservation-definition")
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationRequestInfo {

  @XmlElement(name = "capability")
  private ResourceInfo capability;
  @XmlElement(name = "min-concurrency")
  private int minConcurrency;
  @XmlElement(name = "num-containers")
  private int numContainers;
  @XmlElement(name = "duration")
  private long duration;

  public ReservationRequestInfo() {

  }

  public ReservationRequestInfo(ReservationRequest request) {
    capability = new ResourceInfo(request.getCapability());
    minConcurrency = request.getConcurrency();
    duration = request.getDuration();
    numContainers = request.getNumContainers();
  }

  public ResourceInfo getCapability() {
    return capability;
  }

  public void setCapability(ResourceInfo capability) {
    this.capability = capability;
  }

  public int getMinConcurrency() {
    return minConcurrency;
  }

  public void setMinConcurrency(int minConcurrency) {
    this.minConcurrency = minConcurrency;
  }

  public int getNumContainers() {
    return numContainers;
  }

  public void setNumContainers(int numContainers) {
    this.numContainers = numContainers;
  }

  public long getDuration() {
    return duration;
  }

  public void setDuration(long duration) {
    this.duration = duration;
  }

}
