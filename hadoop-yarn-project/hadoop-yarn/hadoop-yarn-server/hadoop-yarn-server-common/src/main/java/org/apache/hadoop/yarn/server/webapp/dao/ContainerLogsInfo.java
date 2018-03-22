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

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.ContainerLogFileInfo;

/**
 * {@code ContainerLogsInfo} includes the log meta-data of containers.
 * <p>
 * The container log meta-data includes details such as:
 * <ul>
 *   <li>A list of {@link ContainerLogFileInfo}.</li>
 *   <li>The container Id.</li>
 *   <li>The NodeManager Id.</li>
 *   <li>The logType: could be local or aggregated</li>
 * </ul>
 */

@XmlRootElement(name = "containerLogsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainerLogsInfo {

  @XmlElement(name = "containerLogInfo")
  protected List<ContainerLogFileInfo> containerLogsInfo;

  @XmlElement(name = "logAggregationType")
  protected String logType;

  @XmlElement(name = "containerId")
  protected String containerId;

  @XmlElement(name = "nodeId")
  protected String nodeId;

  //JAXB needs this
  public ContainerLogsInfo() {}

  public ContainerLogsInfo(ContainerLogMeta logMeta,
      ContainerLogAggregationType logType) throws YarnException {
    this.containerLogsInfo = new ArrayList<ContainerLogFileInfo>(
        logMeta.getContainerLogMeta());
    this.logType = logType.toString();
    this.containerId = logMeta.getContainerId();
    this.nodeId = logMeta.getNodeId();
  }

  public List<ContainerLogFileInfo> getContainerLogsInfo() {
    return this.containerLogsInfo;
  }

  public String getLogType() {
    return this.logType;
  }

  public String getContainerId() {
    return this.containerId;
  }

  public String getNodeId() {
    return this.nodeId;
  }
}
