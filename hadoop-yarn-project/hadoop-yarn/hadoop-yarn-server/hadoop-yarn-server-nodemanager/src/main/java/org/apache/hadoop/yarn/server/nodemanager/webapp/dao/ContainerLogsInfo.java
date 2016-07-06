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
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.webapp.ContainerLogsUtils;

/**
 * {@code ContainerLogsInfo} includes the log meta-data of containers.
 * <p>
 * The container log meta-data includes details such as:
 * <ul>
 *   <li>The filename of the container log.</li>
 *   <li>The size of the container log.</li>
 * </ul>
 */

@XmlRootElement(name = "containerLogsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainerLogsInfo {

  @XmlElement(name = "containerLogInfo")
  protected List<ContainerLogInfo> containerLogsInfo;

  //JAXB needs this
  public ContainerLogsInfo() {}

  public ContainerLogsInfo(final Context nmContext,
      final ContainerId containerId, String remoteUser)
      throws YarnException {
    this.containerLogsInfo = getContainerLogsInfo(
        containerId, remoteUser, nmContext);
  }

  public List<ContainerLogInfo> getContainerLogsInfo() {
    return this.containerLogsInfo;
  }

  private static List<ContainerLogInfo> getContainerLogsInfo(ContainerId id,
      String remoteUser, Context nmContext) throws YarnException {
    List<ContainerLogInfo> logFiles = new ArrayList<ContainerLogInfo>();
    List<File> logDirs = ContainerLogsUtils.getContainerLogDirs(
        id, remoteUser, nmContext);
    for (File containerLogsDir : logDirs) {
      File[] logs = containerLogsDir.listFiles();
      if (logs != null) {
        for (File log : logs) {
          if (log.isFile()) {
            ContainerLogInfo logMeta = new ContainerLogInfo(
                log.getName(), log.length());
            logFiles.add(logMeta);
          }
        }
      }
    }
    return logFiles;
  }

  private static class ContainerLogInfo {
    private String fileName;
    private long fileSize;

    //JAXB needs this
    public ContainerLogInfo() {}

    public ContainerLogInfo(String fileName, long fileSize) {
      this.setFileName(fileName);
      this.setFileSize(fileSize);
    }

    public String getFileName() {
      return fileName;
    }

    public void setFileName(String fileName) {
      this.fileName = fileName;
    }

    public long getFileSize() {
      return fileSize;
    }

    public void setFileSize(long fileSize) {
      this.fileSize = fileSize;
    }
  }
}
