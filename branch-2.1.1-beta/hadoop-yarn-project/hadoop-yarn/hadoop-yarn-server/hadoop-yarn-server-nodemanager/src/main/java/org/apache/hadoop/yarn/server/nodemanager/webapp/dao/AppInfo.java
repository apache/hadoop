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

import java.util.ArrayList;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.util.ConverterUtils;

@XmlRootElement(name = "app")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppInfo {

  protected String id;
  protected String state;
  protected String user;
  protected ArrayList<String> containerids;

  public AppInfo() {
  } // JAXB needs this

  public AppInfo(final Application app) {
    this.id = ConverterUtils.toString(app.getAppId());
    this.state = app.getApplicationState().toString();
    this.user = app.getUser();

    this.containerids = new ArrayList<String>();
    Map<ContainerId, Container> appContainers = app.getContainers();
    for (ContainerId containerId : appContainers.keySet()) {
      String containerIdStr = ConverterUtils.toString(containerId);
      containerids.add(containerIdStr);
    }
  }

  public String getId() {
    return this.id;
  }

  public String getUser() {
    return this.user;
  }

  public String getState() {
    return this.state;
  }

  public ArrayList<String> getContainers() {
    return this.containerids;
  }

}
