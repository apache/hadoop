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

package org.apache.hadoop.yarn.server.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;

@Public
@Evolving
@XmlRootElement(name = "appAttempt")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppAttemptInfo {

  protected String appAttemptId;
  protected String host;
  protected int rpcPort;
  protected String trackingUrl;
  protected String originalTrackingUrl;
  protected String diagnosticsInfo;
  protected YarnApplicationAttemptState appAttemptState;
  protected String amContainerId;

  public AppAttemptInfo() {
    // JAXB needs this
  }

  public AppAttemptInfo(ApplicationAttemptReport appAttempt) {
    appAttemptId = appAttempt.getApplicationAttemptId().toString();
    host = appAttempt.getHost();
    rpcPort = appAttempt.getRpcPort();
    trackingUrl = appAttempt.getTrackingUrl();
    originalTrackingUrl = appAttempt.getOriginalTrackingUrl();
    diagnosticsInfo = appAttempt.getDiagnostics();
    appAttemptState = appAttempt.getYarnApplicationAttemptState();
    if (appAttempt.getAMContainerId() != null) {
      amContainerId = appAttempt.getAMContainerId().toString();
    }
  }

  public String getAppAttemptId() {
    return appAttemptId;
  }

  public String getHost() {
    return host;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public String getTrackingUrl() {
    return trackingUrl;
  }

  public String getOriginalTrackingUrl() {
    return originalTrackingUrl;
  }

  public String getDiagnosticsInfo() {
    return diagnosticsInfo;
  }

  public YarnApplicationAttemptState getAppAttemptState() {
    return appAttemptState;
  }

  public String getAmContainerId() {
    return amContainerId;
  }

}
