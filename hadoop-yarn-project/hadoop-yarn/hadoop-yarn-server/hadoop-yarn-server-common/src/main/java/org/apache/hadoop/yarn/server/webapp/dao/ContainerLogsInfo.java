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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.exceptions.YarnException;

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

  public ContainerLogsInfo(Map<String, String> containerLogMeta)
      throws YarnException {
    this.containerLogsInfo = new ArrayList<ContainerLogInfo>();
    for (Entry<String, String> meta : containerLogMeta.entrySet()) {
      ContainerLogInfo info = new ContainerLogInfo(meta.getKey(),
          meta.getValue());
      containerLogsInfo.add(info);
    }
  }

  public List<ContainerLogInfo> getContainerLogsInfo() {
    return this.containerLogsInfo;
  }

  /**
   * It includes the log meta-data of a container.
   *
   */
  @Private
  @VisibleForTesting
  public static class ContainerLogInfo {
    private String fileName;
    private String fileSize;

    //JAXB needs this
    public ContainerLogInfo() {}

    public ContainerLogInfo(String fileName, String fileSize) {
      this.setFileName(fileName);
      this.setFileSize(fileSize);
    }

    public String getFileName() {
      return fileName;
    }

    public void setFileName(String fileName) {
      this.fileName = fileName;
    }

    public String getFileSize() {
      return fileSize;
    }

    public void setFileSize(String fileSize) {
      this.fileSize = fileSize;
    }
  }
}
