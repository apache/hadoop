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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ClusterInfo {

  protected long id;
  protected long startedOn;
  protected STATE state;
  protected HAServiceProtocol.HAServiceState haState;
  protected String rmStateStoreName;
  protected String resourceManagerVersion;
  protected String resourceManagerBuildVersion;
  protected String resourceManagerVersionBuiltOn;
  protected String hadoopVersion;
  protected String hadoopBuildVersion;
  protected String hadoopVersionBuiltOn;
  protected String haZooKeeperConnectionState;

  public ClusterInfo() {
  } // JAXB needs this

  public ClusterInfo(ResourceManager rm) {
    long ts = ResourceManager.getClusterTimeStamp();

    this.id = ts;
    this.state = rm.getServiceState();
    this.haState = rm.getRMContext().getHAServiceState();
    this.rmStateStoreName = rm.getRMContext().getStateStore().getClass()
        .getName();
    this.startedOn = ts;
    this.resourceManagerVersion = YarnVersionInfo.getVersion();
    this.resourceManagerBuildVersion = YarnVersionInfo.getBuildVersion();
    this.resourceManagerVersionBuiltOn = YarnVersionInfo.getDate();
    this.hadoopVersion = VersionInfo.getVersion();
    this.hadoopBuildVersion = VersionInfo.getBuildVersion();
    this.hadoopVersionBuiltOn = VersionInfo.getDate();
    this.haZooKeeperConnectionState =
        rm.getRMContext().getHAZookeeperConnectionState();
  }

  public String getState() {
    return this.state.toString();
  }

  public String getHAState() {
    return this.haState.toString();
  }

  public String getRMStateStore() {
    return this.rmStateStoreName;
  }

  public String getRMVersion() {
    return this.resourceManagerVersion;
  }

  public String getRMBuildVersion() {
    return this.resourceManagerBuildVersion;
  }

  public String getRMVersionBuiltOn() {
    return this.resourceManagerVersionBuiltOn;
  }

  public String getHadoopVersion() {
    return this.hadoopVersion;
  }

  public String getHadoopBuildVersion() {
    return this.hadoopBuildVersion;
  }

  public String getHadoopVersionBuiltOn() {
    return this.hadoopVersionBuiltOn;
  }

  public long getClusterId() {
    return this.id;
  }

  public long getStartedOn() {
    return this.startedOn;
  }

  public String getHAZookeeperConnectionState() {
    return this.haZooKeeperConnectionState;
  }
}
