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

import static org.apache.hadoop.yarn.util.StringHelper.join;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Times;

@XmlRootElement(name = "app")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppInfo {

  @XmlTransient
  protected String appIdNum;
  @XmlTransient
  protected boolean trackingUrlIsNotReady;
  @XmlTransient
  protected String trackingUrlPretty;
  @XmlTransient
  protected boolean amContainerLogsExist = false;
  @XmlTransient
  protected ApplicationId applicationId;

  // these are ok for any user to see
  protected String id;
  protected String user;
  protected String name;
  protected String queue;
  protected RMAppState state;
  protected FinalApplicationStatus finalStatus;
  protected float progress;
  protected String trackingUI;
  protected String trackingUrl;
  protected String diagnostics;
  protected long clusterId;
  protected String applicationType;
  
  // these are only allowed if acls allow
  protected long startedTime;
  protected long finishedTime;
  protected long elapsedTime;
  protected String amContainerLogs;
  protected String amHostHttpAddress;

  public AppInfo() {
  } // JAXB needs this

  public AppInfo(RMApp app, Boolean hasAccess, String host) {
    this(app, hasAccess);
  }

  public AppInfo(RMApp app, Boolean hasAccess) {

    if (app != null) {
      String trackingUrl = app.getTrackingUrl();
      this.state = app.getState();
      this.trackingUrlIsNotReady = trackingUrl == null || trackingUrl.isEmpty()
          || RMAppState.NEW == this.state
          || RMAppState.NEW_SAVING == this.state
          || RMAppState.SUBMITTED == this.state
          || RMAppState.ACCEPTED == this.state;
      this.trackingUI = this.trackingUrlIsNotReady ? "UNASSIGNED" : (app
          .getFinishTime() == 0 ? "ApplicationMaster" : "History");
      if (!trackingUrlIsNotReady) {
        this.trackingUrl = join(HttpConfig.getSchemePrefix(), trackingUrl);
      }
      this.trackingUrlPretty = trackingUrlIsNotReady ? "UNASSIGNED" : join(
          HttpConfig.getSchemePrefix(), trackingUrl);
      this.applicationId = app.getApplicationId();
      this.applicationType = app.getApplicationType();
      this.appIdNum = String.valueOf(app.getApplicationId().getId());
      this.id = app.getApplicationId().toString();
      this.user = app.getUser().toString();
      this.name = app.getName().toString();
      this.queue = app.getQueue().toString();
      this.progress = app.getProgress() * 100;
      this.diagnostics = app.getDiagnostics().toString();
      if (diagnostics == null || diagnostics.isEmpty()) {
        this.diagnostics = "";
      }
      this.finalStatus = app.getFinalApplicationStatus();
      this.clusterId = ResourceManager.clusterTimeStamp;
      if (hasAccess) {
        this.startedTime = app.getStartTime();
        this.finishedTime = app.getFinishTime();
        this.elapsedTime = Times.elapsed(app.getStartTime(),
            app.getFinishTime());

        RMAppAttempt attempt = app.getCurrentAppAttempt();
        if (attempt != null) {
          Container masterContainer = attempt.getMasterContainer();
          if (masterContainer != null) {
            this.amContainerLogsExist = true;
            String url = join(HttpConfig.getSchemePrefix(),
                masterContainer.getNodeHttpAddress(),
                "/node", "/containerlogs/",
                ConverterUtils.toString(masterContainer.getId()),
                "/", app.getUser());
            this.amContainerLogs = url;
            this.amHostHttpAddress = masterContainer.getNodeHttpAddress();
          }
        }
      }
    }
  }

  public boolean isTrackingUrlReady() {
    return !this.trackingUrlIsNotReady;
  }

  public ApplicationId getApplicationId() {
    return this.applicationId;
  }

  public String getAppId() {
    return this.id;
  }

  public String getAppIdNum() {
    return this.appIdNum;
  }

  public String getUser() {
    return this.user;
  }

  public String getQueue() {
    return this.queue;
  }

  public String getName() {
    return this.name;
  }

  public String getState() {
    return this.state.toString();
  }

  public float getProgress() {
    return this.progress;
  }

  public String getTrackingUI() {
    return this.trackingUI;
  }

  public String getNote() {
    return this.diagnostics;
  }

  public String getFinalStatus() {
    return this.finalStatus.toString();
  }

  public String getTrackingUrl() {
    return this.trackingUrl;
  }

  public String getTrackingUrlPretty() {
    return this.trackingUrlPretty;
  }

  public long getStartTime() {
    return this.startedTime;
  }

  public long getFinishTime() {
    return this.finishedTime;
  }

  public long getElapsedTime() {
    return this.elapsedTime;
  }

  public String getAMContainerLogs() {
    return this.amContainerLogs;
  }

  public String getAMHostHttpAddress() {
    return this.amHostHttpAddress;
  }

  public boolean amContainerLogsExist() {
    return this.amContainerLogsExist;
  }

  public long getClusterId() {
    return this.clusterId;
  }

  public String getApplicationType() {
    return this.applicationType;
  }

}
