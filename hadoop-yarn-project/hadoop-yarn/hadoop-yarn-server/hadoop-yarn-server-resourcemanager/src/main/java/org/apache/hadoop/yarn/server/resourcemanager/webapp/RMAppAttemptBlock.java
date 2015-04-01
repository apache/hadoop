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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.util.Collection;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.webapp.AppAttemptBlock;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;

import com.google.inject.Inject;

public class RMAppAttemptBlock extends AppAttemptBlock{

  private final ResourceManager rm;
  protected Configuration conf;

  @Inject
  RMAppAttemptBlock(ViewContext ctx, ResourceManager rm, Configuration conf) {
    super(rm.getClientRMService(), ctx);
    this.rm = rm;
    this.conf = conf;
  }

  @Override
  protected void render(Block html) {
    super.render(html);
  }

  private RMAppAttempt getRMAppAttempt() {
    ApplicationId appId = this.appAttemptId.getApplicationId();
    RMAppAttempt attempt = null;
    RMApp rmApp = rm.getRMContext().getRMApps().get(appId);
    if (rmApp != null) { 
      attempt = rmApp.getAppAttempts().get(appAttemptId);
    }
    return attempt;
  }

  protected void generateOverview(ApplicationAttemptReport appAttemptReport,
      Collection<ContainerReport> containers, AppAttemptInfo appAttempt,
      String node) {

    String blacklistedNodes = "-";
    Set<String> nodes =
        getBlacklistedNodes(rm, getRMAppAttempt().getAppAttemptId());
    if (nodes != null) {
      if (!nodes.isEmpty()) {
        blacklistedNodes = StringUtils.join(nodes, ", ");
      }
    }

    info("Application Attempt Overview")
      ._(
        "Application Attempt State:",
        appAttempt.getAppAttemptState() == null ? UNAVAILABLE : appAttempt
          .getAppAttemptState())
      ._(
        "AM Container:",
        appAttempt.getAmContainerId() == null || containers == null
            || !hasAMContainer(appAttemptReport.getAMContainerId(), containers)
            ? null : root_url("container", appAttempt.getAmContainerId()),
        String.valueOf(appAttempt.getAmContainerId()))
      ._("Node:", node)
      ._(
        "Tracking URL:",
        appAttempt.getTrackingUrl() == null
            || appAttempt.getTrackingUrl().equals(UNAVAILABLE) ? null
            : root_url(appAttempt.getTrackingUrl()),
        appAttempt.getTrackingUrl() == null
            || appAttempt.getTrackingUrl().equals(UNAVAILABLE)
            ? "Unassigned"
            : appAttempt.getAppAttemptState() == YarnApplicationAttemptState.FINISHED
                || appAttempt.getAppAttemptState() == YarnApplicationAttemptState.FAILED
                || appAttempt.getAppAttemptState() == YarnApplicationAttemptState.KILLED
                ? "History" : "ApplicationMaster")
      ._(
        "Diagnostics Info:",
        appAttempt.getDiagnosticsInfo() == null ? "" : appAttempt
          .getDiagnosticsInfo())._("Blacklisted Nodes:", blacklistedNodes);
  }

  public static Set<String> getBlacklistedNodes(ResourceManager rm,
      ApplicationAttemptId appid) {
    if (rm.getResourceScheduler() instanceof AbstractYarnScheduler) {
      AbstractYarnScheduler ayScheduler =
          (AbstractYarnScheduler) rm.getResourceScheduler();
      SchedulerApplicationAttempt attempt =
          ayScheduler.getApplicationAttempt(appid);
      if (attempt != null) {
        return attempt.getBlacklistedNodes();
      }
    }
    return null;
  }
}
