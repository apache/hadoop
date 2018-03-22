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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.ContainerLogFileInfo;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.webapp.ContainerLogsUtils;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfo;
import org.apache.hadoop.yarn.util.Times;

/**
 * NMContainerLogsInfo represents the meta data for container logs
 * which exist in NM local log directory.
 * This class extends {@link ContainerLogsInfo}.
 */
@XmlRootElement(name = "containerLogsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class NMContainerLogsInfo extends ContainerLogsInfo {

  //JAXB needs this
  public NMContainerLogsInfo() {}

  public NMContainerLogsInfo(final Context nmContext,
      final ContainerId containerId, String remoteUser,
      ContainerLogAggregationType logType) throws YarnException {
    this.logType = logType.toString();
    this.containerId = containerId.toString();
    this.nodeId = nmContext.getNodeId().toString();
    this.containerLogsInfo = getContainerLogsInfo(
        containerId, remoteUser, nmContext);
  }

  private static List<ContainerLogFileInfo> getContainerLogsInfo(
      ContainerId id, String remoteUser, Context nmContext)
      throws YarnException {
    List<ContainerLogFileInfo> logFiles = new ArrayList<>();
    List<File> logDirs = ContainerLogsUtils.getContainerLogDirs(
        id, remoteUser, nmContext);
    for (File containerLogsDir : logDirs) {
      File[] logs = containerLogsDir.listFiles();
      if (logs != null) {
        for (File log : logs) {
          if (log.isFile()) {
            ContainerLogFileInfo logMeta = new ContainerLogFileInfo(
                log.getName(), Long.toString(log.length()),
                Times.format(log.lastModified()));
            logFiles.add(logMeta);
          }
        }
      }
    }
    return logFiles;
  }
}
