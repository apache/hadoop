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
import javax.xml.bind.annotation.XmlRootElement;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

@XmlRootElement(name = "appAttempt")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppAttemptInfo {

  protected int id;
  protected long startTime;
  protected long finishedTime;
  protected String containerId;
  protected String nodeHttpAddress;
  protected String nodeId;
  protected String logsLink;
  protected String blacklistedNodes;
  private String nodesBlacklistedBySystem;
  protected String appAttemptId;
  private String exportPorts;

  public AppAttemptInfo() {
  }

  public AppAttemptInfo(ResourceManager rm, RMAppAttempt attempt,
      Boolean hasAccess, String user, String schemePrefix) {
    this.startTime = 0;
    this.containerId = "";
    this.nodeHttpAddress = "";
    this.nodeId = "";
    this.logsLink = "";
    this.blacklistedNodes = "";
    this.exportPorts = "";
    if (attempt != null) {
      this.id = attempt.getAppAttemptId().getAttemptId();
      this.startTime = attempt.getStartTime();
      this.finishedTime = attempt.getFinishTime();
      Container masterContainer = attempt.getMasterContainer();
      if (masterContainer != null && hasAccess) {
        this.containerId = masterContainer.getId().toString();
        this.nodeHttpAddress = masterContainer.getNodeHttpAddress();
        this.nodeId = masterContainer.getNodeId().toString();
        this.logsLink = WebAppUtils.getRunningLogURL(schemePrefix
            + masterContainer.getNodeHttpAddress(),
            masterContainer.getId().toString(), user);

        Gson gson = new Gson();
        this.exportPorts = gson.toJson(masterContainer.getExposedPorts());

        nodesBlacklistedBySystem =
            StringUtils.join(attempt.getAMBlacklistManager()
              .getBlacklistUpdates().getBlacklistAdditions(), ", ");
        if (rm.getResourceScheduler() instanceof AbstractYarnScheduler) {
          AbstractYarnScheduler ayScheduler =
              (AbstractYarnScheduler) rm.getResourceScheduler();
          SchedulerApplicationAttempt sattempt =
              ayScheduler.getApplicationAttempt(attempt.getAppAttemptId());
          if (sattempt != null) {
            blacklistedNodes =
                StringUtils.join(sattempt.getBlacklistedNodes(), ", ");
          }
        }
      }
      this.appAttemptId = attempt.getAppAttemptId().toString();
    }
  }

  public int getAttemptId() {
    return this.id;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public long getFinishedTime() {
    return this.finishedTime;
  }

  public String getNodeHttpAddress() {
    return this.nodeHttpAddress;
  }

  public String getLogsLink() {
    return this.logsLink;
  }

  public String getAppAttemptId() {
    return this.appAttemptId;
  }
}
