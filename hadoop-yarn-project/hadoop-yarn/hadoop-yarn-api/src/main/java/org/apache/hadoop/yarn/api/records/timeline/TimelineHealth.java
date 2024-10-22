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
package org.apache.hadoop.yarn.api.records.timeline;


import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * This class holds health information for ATS.
 */
@XmlRootElement(name = "health")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Unstable
public class TimelineHealth {

  /**
   * Timline health status.
   *
   * RUNNING - Service is up and running
   * CONNECTION_FAULURE - isConnectionAlive() of reader / writer implementation
   *    reported an error
   */
  public enum TimelineHealthStatus {
    RUNNING,
    CONNECTION_FAILURE
  }

  private TimelineHealthStatus healthStatus;
  private String diagnosticsInfo;

  public TimelineHealth(TimelineHealthStatus healthy, String diagnosticsInfo) {
    this.healthStatus = healthy;
    this.diagnosticsInfo = diagnosticsInfo;
  }

  public TimelineHealth() {
  }

  @XmlElement(name = "healthStatus")
  public TimelineHealthStatus getHealthStatus() {
    return healthStatus;
  }

  @XmlElement(name = "diagnosticsInfo")
  public String getDiagnosticsInfo() {
    return diagnosticsInfo;
  }


  public void setHealthStatus(TimelineHealthStatus healthStatus) {
    this.healthStatus = healthStatus;
  }

  public void setDiagnosticsInfo(String diagnosticsInfo) {
    this.diagnosticsInfo = diagnosticsInfo;
  }
}