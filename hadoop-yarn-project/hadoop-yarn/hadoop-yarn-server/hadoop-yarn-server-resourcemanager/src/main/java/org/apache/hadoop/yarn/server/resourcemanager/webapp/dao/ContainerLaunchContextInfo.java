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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.ApplicationAccessType;

/**
 * Simple class to allow users to send information required to create a
 * ContainerLaunchContext which can then be used as part of the
 * ApplicationSubmissionContext
 * 
 */
@XmlRootElement(name = "container-launch-context-info")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainerLaunchContextInfo {

  @XmlElementWrapper(name = "local-resources")
  HashMap<String, LocalResourceInfo> local_resources;
  HashMap<String, String> environment;

  @XmlElementWrapper(name = "commands")
  @XmlElement(name = "command", type = String.class)
  List<String> commands;

  @XmlElementWrapper(name = "service-data")
  HashMap<String, String> servicedata;

  @XmlElement(name = "credentials")
  CredentialsInfo credentials;

  @XmlElementWrapper(name = "application-acls")
  HashMap<ApplicationAccessType, String> acls;

  public ContainerLaunchContextInfo() {
    local_resources = new HashMap<String, LocalResourceInfo>();
    environment = new HashMap<String, String>();
    commands = new ArrayList<String>();
    servicedata = new HashMap<String, String>();
    credentials = new CredentialsInfo();
    acls = new HashMap<ApplicationAccessType, String>();
  }

  public Map<String, LocalResourceInfo> getResources() {
    return local_resources;
  }

  public Map<String, String> getEnvironment() {
    return environment;
  }

  public List<String> getCommands() {
    return commands;
  }

  public Map<String, String> getAuxillaryServiceData() {
    return servicedata;
  }

  public CredentialsInfo getCredentials() {
    return credentials;
  }

  public Map<ApplicationAccessType, String> getAcls() {
    return acls;
  }

  public void setResources(HashMap<String, LocalResourceInfo> resources) {
    this.local_resources = resources;
  }

  public void setEnvironment(HashMap<String, String> environment) {
    this.environment = environment;
  }

  public void setCommands(List<String> commands) {
    this.commands = commands;
  }

  public void setAuxillaryServiceData(HashMap<String, String> serviceData) {
    this.servicedata = serviceData;
  }

  public void setCredentials(CredentialsInfo credentials) {
    this.credentials = credentials;
  }

  public void setAcls(HashMap<ApplicationAccessType, String> acls) {
    this.acls = acls;
  }
}
