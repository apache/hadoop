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

package org.apache.hadoop.yarn.server.nodemanager.webapp.dao;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.util.StringHelper.ujoin;

import javax.xml.bind.annotation.*;

import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.webapp.ContainerLogsUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "container")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainerInfo {

  protected String id;
  protected String state;
  protected int exitCode;
  protected String diagnostics;
  protected String user;
  protected long totalMemoryNeededMB;
  protected long totalVCoresNeeded;
  protected String containerLogsLink;
  protected String nodeId;
  @XmlTransient
  protected String containerLogsShortLink;
  @XmlTransient
  protected String exitStatus;

  protected List<String> containerLogFiles;

  public ContainerInfo() {
  } // JAXB needs this

  public ContainerInfo(final Context nmContext, final Container container) {
    this(nmContext, container, "", "", "");
  }

  public ContainerInfo(final Context nmContext, final Container container,
       String requestUri, String pathPrefix, String remoteUser) {

    this.id = container.getContainerId().toString();
    this.nodeId = nmContext.getNodeId().toString();
    ContainerStatus containerData = container.cloneAndGetContainerStatus();
    this.exitCode = containerData.getExitStatus();
    this.exitStatus =
        (this.exitCode == ContainerExitStatus.INVALID) ?
            "N/A" : String.valueOf(exitCode);
    this.state = container.getContainerState().toString();
    this.diagnostics = containerData.getDiagnostics();
    if (this.diagnostics == null || this.diagnostics.isEmpty()) {
      this.diagnostics = "";
    }

    this.user = container.getUser();
    Resource res = container.getResource();
    if (res != null) {
      this.totalMemoryNeededMB = res.getMemorySize();
      this.totalVCoresNeeded = res.getVirtualCores();
    }
    this.containerLogsShortLink = ujoin("containerlogs", this.id,
        container.getUser());

    if (requestUri == null) {
      requestUri = "";
    }
    if (pathPrefix == null) {
      pathPrefix = "";
    }
    this.containerLogsLink = join(requestUri, pathPrefix,
        this.containerLogsShortLink);
    this.containerLogFiles =
        getContainerLogFiles(container.getContainerId(), remoteUser, nmContext);
  }

  public String getId() {
    return this.id;
  }

  public String getNodeId() {
    return this.nodeId;
  }

  public String getState() {
    return this.state;
  }

  public int getExitCode() {
    return this.exitCode;
  }

  public String getExitStatus() {
    return this.exitStatus;
  }

  public String getDiagnostics() {
    return this.diagnostics;
  }

  public String getUser() {
    return this.user;
  }

  public String getShortLogLink() {
    return this.containerLogsShortLink;
  }

  public String getLogLink() {
    return this.containerLogsLink;
  }

  public long getMemoryNeeded() {
    return this.totalMemoryNeededMB;
  }

  public long getVCoresNeeded() {
    return this.totalVCoresNeeded;
  }

  public List<String> getContainerLogFiles() {
    return this.containerLogFiles;
  }

  private List<String> getContainerLogFiles(ContainerId id, String remoteUser,
      Context nmContext) {
    List<String> logFiles = new ArrayList<>();
    try {
      List<File> logDirs =
          ContainerLogsUtils.getContainerLogDirs(id, remoteUser, nmContext);
      for (File containerLogsDir : logDirs) {
        File[] logs = containerLogsDir.listFiles();
        if (logs != null) {
          for (File log : logs) {
            if (log.isFile()) {
              logFiles.add(log.getName());
            }
          }
        }
      }
    } catch (Exception ye) {
      return logFiles;
    }
    return logFiles;
  }

}
