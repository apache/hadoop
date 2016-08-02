/*
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

package org.apache.slider.server.appmaster.web.rest.agent;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.ArrayList;
import java.util.List;

/**
 *
 *
 * Data model for agent heartbeat for server (ambari or app master).
 *
 */

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class HeartBeat {
  private long responseId = -1;
  private long timestamp;
  private String hostname;
  List<CommandReport> reports = new ArrayList<CommandReport>();
  List<ComponentStatus> componentStatus = new ArrayList<ComponentStatus>();
  private List<DiskInfo> mounts = new ArrayList<DiskInfo>();
  HostStatus nodeStatus;
  private AgentEnv agentEnv = null;
  private String fqdn;
  private String pkg;

  public long getResponseId() {
    return responseId;
  }

  public void setResponseId(long responseId) {
    this.responseId=responseId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getFqdn() {
    return fqdn;
  }

  public void setFqdn(String fqdn) {
    this.fqdn = fqdn;
  }

  @JsonProperty("reports")
  public List<CommandReport> getReports() {
    return this.reports;
  }

  @JsonProperty("reports")
  public void setReports(List<CommandReport> reports) {
    this.reports = reports;
  }

  public HostStatus getNodeStatus() {
    return nodeStatus;
  }

  public void setNodeStatus(HostStatus nodeStatus) {
    this.nodeStatus = nodeStatus;
  }

  public AgentEnv getAgentEnv() {
    return agentEnv;
  }

  public void setAgentEnv(AgentEnv env) {
    agentEnv = env;
  }

  @JsonProperty("componentStatus")
  public List<ComponentStatus> getComponentStatus() {
    return componentStatus;
  }

  @JsonProperty("componentStatus")
  public void setComponentStatus(List<ComponentStatus> componentStatus) {
    this.componentStatus = componentStatus;
  }

  @JsonProperty("mounts")
  public List<DiskInfo> getMounts() {
    return this.mounts;
  }

  @JsonProperty("mounts")
  public void setMounts(List<DiskInfo> mounts) {
    this.mounts = mounts;
  }

  @JsonProperty("package")
  public String getPackage() {
    return pkg;
  }

  @JsonProperty("package")
  public void setPackage(String pkg) {
    this.pkg = pkg;
  }

  @Override
  public String toString() {
    return "HeartBeat{" +
           "responseId=" + responseId +
           ", timestamp=" + timestamp +
           ", hostname='" + hostname + '\'' +
           ", reports=" + reports +
           ", componentStatus=" + componentStatus +
           ", package=" + pkg +
           ", nodeStatus=" + nodeStatus +
           '}';
  }
}
