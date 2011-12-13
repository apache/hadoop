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
package org.apache.hadoop.mapreduce.v2.hs.webapp.dao;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.util.StringHelper.ujoin;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.BuilderUtils;

@XmlRootElement(name = "amAttempt")
@XmlAccessorType(XmlAccessType.FIELD)
public class AMAttemptInfo {

  protected String nodeHttpAddress;
  protected String nodeId;
  protected int id;
  protected long startTime;
  protected String containerId;
  protected String logsLink;

  @XmlTransient
  protected String shortLogsLink;

  public AMAttemptInfo() {
  }

  public AMAttemptInfo(AMInfo amInfo, String jobId, String user, String host,
      String pathPrefix) {
    this.nodeHttpAddress = amInfo.getNodeManagerHost() + ":"
        + amInfo.getNodeManagerHttpPort();
    NodeId nodeId = BuilderUtils.newNodeId(amInfo.getNodeManagerHost(),
        amInfo.getNodeManagerPort());
    this.nodeId = nodeId.toString();
    this.id = amInfo.getAppAttemptId().getAttemptId();
    this.startTime = amInfo.getStartTime();
    this.containerId = amInfo.getContainerId().toString();
    this.logsLink = join(
        host,
        pathPrefix,
        ujoin("logs", nodeId.toString(), amInfo.getContainerId().toString(),
            jobId, user));
    this.shortLogsLink = ujoin("logs", nodeId.toString(), amInfo
        .getContainerId().toString(), jobId, user);
  }

  public String getNodeHttpAddress() {
    return this.nodeHttpAddress;
  }

  public String getNodeId() {
    return this.nodeId;
  }

  public int getAttemptId() {
    return this.id;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public String getContainerId() {
    return this.containerId;
  }

  public String getLogsLink() {
    return this.logsLink;
  }

  public String getShortLogsLink() {
    return this.shortLogsLink;
  }

}
