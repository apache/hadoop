/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.slider.providers.agent.State;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.Map;

/** Data model for agent to send heartbeat to ambari and/or app master. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class Register {
  private int responseId = -1;
  private long timestamp;
  private String label;
  private int currentPingPort;
  private HostInfo hardwareProfile;
  private String publicHostname;
  private String tags;
  private AgentEnv agentEnv;
  private String agentVersion;
  private State actualState;
  private State expectedState;
  private Map<String, String> allocatedPorts;
  private Map<String, String> logFolders;
  private String pkg;
  private String appVersion;

  @JsonProperty("responseId")
  public int getResponseId() {
    return responseId;
  }

  @JsonProperty("responseId")
  public void setResponseId(int responseId) {
    this.responseId = responseId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public String getTags() {
    return tags;
  }

  public void setTags(String tags) {
    this.tags = tags;
  }

  public HostInfo getHardwareProfile() {
    return hardwareProfile;
  }

  public void setHardwareProfile(HostInfo hardwareProfile) {
    this.hardwareProfile = hardwareProfile;
  }

  public String getPublicHostname() {
    return publicHostname;
  }

  public void setPublicHostname(String name) {
    this.publicHostname = name;
  }

  public AgentEnv getAgentEnv() {
    return agentEnv;
  }

  public void setAgentEnv(AgentEnv env) {
    this.agentEnv = env;
  }

  public String getAgentVersion() {
    return agentVersion;
  }

  public void setAgentVersion(String agentVersion) {
    this.agentVersion = agentVersion;
  }

  public int getCurrentPingPort() {
    return currentPingPort;
  }

  public void setCurrentPingPort(int currentPingPort) {
    this.currentPingPort = currentPingPort;
  }

  public State getActualState() {
    return actualState;
  }

  public void setActualState(State actualState) {
    this.actualState = actualState;
  }

  public State getExpectedState() {
    return expectedState;
  }

  public void setExpectedState(State expectedState) {
    this.expectedState = expectedState;
  }

  /** @return the allocated ports, or <code>null</code> if none are present */
  @JsonProperty("allocatedPorts")
  public Map<String, String> getAllocatedPorts() {
    return allocatedPorts;
  }

  /** @param ports allocated ports */
  @JsonProperty("allocatedPorts")
  public void setAllocatedPorts(Map<String, String> ports) {
    this.allocatedPorts = ports;
  }

  /** @return the log folders, or <code>null</code> if none are present */
  @JsonProperty("logFolders")
  public Map<String, String> getLogFolders() {
    return logFolders;
  }

  /** @param logFolders assigned log folders */
  @JsonProperty("logFolders")
  public void setLogFolders(Map<String, String> logFolders) {
    this.logFolders = logFolders;
  }

  public String getPkg() {
    return pkg;
  }

  public void setPkg(String pkg) {
    this.pkg = pkg;
  }

  @JsonProperty("appVersion")
  public String getAppVersion() {
    return appVersion;
  }

  @JsonProperty("appVersion")
  public void setAppVersion(String appVersion) {
    this.appVersion = appVersion;
  }

  @Override
  public String toString() {
    String ret = "responseId=" + responseId + "\n" +
                 "timestamp=" + timestamp + "\n" +
                 "label=" + label + "\n" +
                 "hostname=" + publicHostname + "\n" +
                 "expectedState=" + expectedState + "\n" +
                 "actualState=" + actualState + "\n" +
                 "appVersion=" + appVersion + "\n";

    if (hardwareProfile != null) {
      ret = ret + "hardwareprofile=" + this.hardwareProfile.toString();
    }
    return ret;
  }
}
