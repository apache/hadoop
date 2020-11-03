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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_ID;

public class ContainerBlock extends HtmlBlock {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerBlock.class);
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
        containerReport = getContainerReport(request);
      } else {
        containerReport = callerUGI.doAs(
            new PrivilegedExceptionAction<ContainerReport> () {
          @Override
          public ContainerReport run() throws Exception {
            return getContainerReport(request);
          }
        });
      }
    } catch (Exception e) {
      String message = "Failed to read the container " + containerid + ".";
      LOG.error(message, e);
      html.p().__(message).__();
      return;
    }

    if (containerReport == null) {
      puts("Container not found: " + containerid);
      return;
    }

    ContainerInfo container = new ContainerInfo(containerReport);
    setTitle(join("Container ", containerid));

    info("Container Overview")
      .__(
        "Container State:",
        container.getContainerState() == null ? UNAVAILABLE : container
          .getContainerState())
      .__("Exit Status:", container.getContainerExitStatus())
      .__(
        "Node:",
        container.getNodeHttpAddress() == null ? "#" : container
          .getNodeHttpAddress(),
        container.getNodeHttpAddress() == null ? "N/A" : container
          .getNodeHttpAddress())
      .__("Priority:", container.getPriority())
      .__("Started:", Times.format(container.getStartedTime()))
      .__(
        "Elapsed:",
        StringUtils.formatTime(Times.elapsed(container.getStartedTime(),
          container.getFinishedTime())))
      .__(
        "Resource:", getResources(container))
      .__("Logs:", container.getLogUrl() == null ? "#" : container.getLogUrl(),
          container.getLogUrl() == null ? "N/A" : "Logs")
      .__("Diagnostics:", container.getDiagnosticsInfo() == null ?
          "" : container.getDiagnosticsInfo());

    html.__(InfoBlock.class);
  }

  /**
   * Creates a string representation of allocated resources to a container.
   * Memory, followed with VCores are always the first two resources of
   * the resulted string, followed with any custom resources, if any is present.
   */
  @VisibleForTesting
  String getResources(ContainerInfo container) {
    Map<String, Long> allocatedResources = container.getAllocatedResources();

    StringBuilder sb = new StringBuilder();
    sb.append(getResourceAsString(ResourceInformation.MEMORY_URI,
        allocatedResources.get(ResourceInformation.MEMORY_URI))).append(", ");
    sb.append(getResourceAsString(ResourceInformation.VCORES_URI,
        allocatedResources.get(ResourceInformation.VCORES_URI)));

    if (container.hasCustomResources()) {
      container.getAllocatedResources().forEach((key, value) -> {
        if (!key.equals(ResourceInformation.MEMORY_URI) &&
            !key.equals(ResourceInformation.VCORES_URI)) {
          sb.append(", ");
          sb.append(getResourceAsString(key, value));
        }
      });
    }

    return sb.toString();
  }

  private String getResourceAsString(String resourceName, long value) {
    final String translatedResourceName;
    switch (resourceName) {
    case ResourceInformation.MEMORY_URI:
      translatedResourceName = "Memory";
      break;
    case ResourceInformation.VCORES_URI:
      translatedResourceName = "VCores";
      break;
    default:
      translatedResourceName = resourceName;
      break;
    }
    return String.valueOf(value) + " " + translatedResourceName;
  }

  protected ContainerReport getContainerReport(
      final GetContainerReportRequest request)
      throws YarnException, IOException {
    return appBaseProt.getContainerReport(request).getContainerReport();
  }
}