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
package org.apache.hadoop.yarn.server.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APPLICATION_ATTEMPT_ID;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.server.api.ApplicationContext;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class AppAttemptBlock extends HtmlBlock {

  private static final Log LOG = LogFactory.getLog(AppAttemptBlock.class);
  private final ApplicationContext appContext;

  @Inject
  public AppAttemptBlock(ApplicationContext appContext) {
    this.appContext = appContext;
  }

  @Override
  protected void render(Block html) {
    String attemptid = $(APPLICATION_ATTEMPT_ID);
    if (attemptid.isEmpty()) {
      puts("Bad request: requires application attempt ID");
      return;
    }

    ApplicationAttemptId appAttemptId = null;
    try {
      appAttemptId = ConverterUtils.toApplicationAttemptId(attemptid);
    } catch (IllegalArgumentException e) {
      puts("Invalid application attempt ID: " + attemptid);
      return;
    }

    ApplicationAttemptReport appAttemptReport;
    try {
      appAttemptReport = appContext.getApplicationAttempt(appAttemptId);
    } catch (IOException e) {
      String message =
          "Failed to read the application attempt " + appAttemptId + ".";
      LOG.error(message, e);
      html.p()._(message)._();
      return;
    }
    if (appAttemptReport == null) {
      puts("Application Attempt not found: " + attemptid);
      return;
    }
    AppAttemptInfo appAttempt = new AppAttemptInfo(appAttemptReport);

    setTitle(join("Application Attempt ", attemptid));

    String node = "N/A";
    if (appAttempt.getHost() != null && appAttempt.getRpcPort() >= 0
        && appAttempt.getRpcPort() < 65536) {
      node = appAttempt.getHost() + ":" + appAttempt.getRpcPort();
    }
    info("Application Attempt Overview")
      ._("State", appAttempt.getAppAttemptState())
      ._(
        "Master Container",
        appAttempt.getAmContainerId() == null ? "#" : root_url("container",
          appAttempt.getAmContainerId()),
        String.valueOf(appAttempt.getAmContainerId()))
      ._("Node:", node)
      ._(
        "Tracking URL:",
        appAttempt.getTrackingUrl() == null ? "#" : root_url(appAttempt
          .getTrackingUrl()), "History")
      ._("Diagnostics Info:", appAttempt.getDiagnosticsInfo());

    html._(InfoBlock.class);

    Collection<ContainerReport> containers;
    try {
      containers = appContext.getContainers(appAttemptId).values();
    } catch (IOException e) {
      html
        .p()
        ._(
          "Sorry, Failed to get containers for application attempt" + attemptid
              + ".")._();
      return;
    }

    // Container Table
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#containers").thead().tr().th(".id", "Container ID")
          .th(".node", "Node").th(".exitstatus", "Container Exit Status")
          .th(".logs", "Logs")._()._().tbody();

    StringBuilder containersTableData = new StringBuilder("[\n");
    for (ContainerReport containerReport : containers) {
      ContainerInfo container = new ContainerInfo(containerReport);
      // ConatinerID numerical value parsed by parseHadoopID in
      // yarn.dt.plugins.js
      containersTableData
        .append("[\"<a href='")
        .append(url("container", container.getContainerId()))
        .append("'>")
        .append(container.getContainerId())
        .append("</a>\",\"<a href='")
        .append(container.getAssignedNodeId())
        .append("'>")
        .append(
          StringEscapeUtils.escapeJavaScript(StringEscapeUtils
            .escapeHtml(container.getAssignedNodeId()))).append("</a>\",\"")
        .append(container.getContainerExitStatus()).append("\",\"<a href='")
        .append(container.getLogUrl() == null ?
            "#" : container.getLogUrl()).append("'>")
        .append(container.getLogUrl() == null ?
            "N/A" : "Logs").append("</a>\"],\n");
    }
    if (containersTableData.charAt(containersTableData.length() - 2) == ',') {
      containersTableData.delete(containersTableData.length() - 2,
        containersTableData.length() - 1);
    }
    containersTableData.append("]");
    html.script().$type("text/javascript")
      ._("var containersTableData=" + containersTableData)._();

    tbody._()._();
  }
}