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
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_ID;

import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class ContainerBlock extends HtmlBlock {

  private static final Log LOG = LogFactory.getLog(ContainerBlock.class);
  protected ApplicationBaseProtocol appBaseProt;

  @Inject
  public ContainerBlock(ApplicationBaseProtocol appBaseProt, ViewContext ctx) {
    super(ctx);
    this.appBaseProt = appBaseProt;
  }

  @Override
  protected void render(Block html) {
    String containerid = $(CONTAINER_ID);
    if (containerid.isEmpty()) {
      puts("Bad request: requires container ID");
      return;
    }

    ContainerId containerId = null;
    try {
      containerId = ContainerId.fromString(containerid);
    } catch (IllegalArgumentException e) {
      puts("Invalid container ID: " + containerid);
      return;
    }

    UserGroupInformation callerUGI = getCallerUGI();
    ContainerReport containerReport = null;
    try {
      final GetContainerReportRequest request =
          GetContainerReportRequest.newInstance(containerId);
      if (callerUGI == null) {
        containerReport = appBaseProt.getContainerReport(request)
            .getContainerReport();
      } else {
        containerReport = callerUGI.doAs(
            new PrivilegedExceptionAction<ContainerReport> () {
          @Override
          public ContainerReport run() throws Exception {
            return appBaseProt.getContainerReport(request)
                .getContainerReport();
          }
        });
      }
    } catch (Exception e) {
      String message = "Failed to read the container " + containerid + ".";
      LOG.error(message, e);
      html.p()._(message)._();
      return;
    }

    if (containerReport == null) {
      puts("Container not found: " + containerid);
      return;
    }

    ContainerInfo container = new ContainerInfo(containerReport);
    setTitle(join("Container ", containerid));

    info("Container Overview")
      ._(
        "Container State:",
        container.getContainerState() == null ? UNAVAILABLE : container
          .getContainerState())
      ._("Exit Status:", container.getContainerExitStatus())
      ._(
        "Node:",
        container.getNodeHttpAddress() == null ? "#" : container
          .getNodeHttpAddress(),
        container.getNodeHttpAddress() == null ? "N/A" : container
          .getNodeHttpAddress())
      ._("Priority:", container.getPriority())
      ._("Started:", Times.format(container.getStartedTime()))
      ._(
        "Elapsed:",
        StringUtils.formatTime(Times.elapsed(container.getStartedTime(),
          container.getFinishedTime())))
      ._(
        "Resource:",
        container.getAllocatedMB() + " Memory, "
            + container.getAllocatedVCores() + " VCores")
      ._("Logs:", container.getLogUrl() == null ? "#" : container.getLogUrl(),
          container.getLogUrl() == null ? "N/A" : "Logs")
      ._("Diagnostics:", container.getDiagnosticsInfo() == null ?
          "" : container.getDiagnosticsInfo());

    html._(InfoBlock.class);
  }
}