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

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.Map;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class ComponentStatus {
  String componentName;
  String msg;
  String status;
  String serviceName;
  String clusterName;
  String roleCommand;
  String ip;
  String hostname;
  @JsonProperty("configurations")
  private Map<String, Map<String, String>> configurations;

  public String getRoleCommand() {
    return roleCommand;
  }

  public void setRoleCommand(String roleCommand) {
    this.roleCommand = roleCommand;
  }

  public String getComponentName() {
    return this.componentName;
  }

  public void setComponentName(String componentName) {
    this.componentName = componentName;
  }

  public String getMessage() {
    return this.msg;
  }

  public void setMessage(String msg) {
    this.msg = msg;
  }

  public String getStatus() {
    return this.status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  /** @return the config tags that match this command, or <code>null</code> if none are present */
  public Map<String, Map<String, String>> getConfigs() {
    return configurations;
  }

  /** @param configs the config tags that match this status */
  public void setConfigs(Map<String, Map<String, String>> configs) {
    this.configurations = configs;
  }

  @Override
  public String toString() {
    return "ComponentStatus{" +
           "componentName='" + componentName + '\'' +
           ", msg='" + msg + '\'' +
           ", status='" + status + '\'' +
           ", serviceName='" + serviceName + '\'' +
           ", clusterName='" + clusterName + '\'' +
           ", roleCommand='" + roleCommand + '\'' +
           ", ip='" + ip + '\'' +
           ", hostname='" + hostname + '\'' +
           '}';
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }
}
