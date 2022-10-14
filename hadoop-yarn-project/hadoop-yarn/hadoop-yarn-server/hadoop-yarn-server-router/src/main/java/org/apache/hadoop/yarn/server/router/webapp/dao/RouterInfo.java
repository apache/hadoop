/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.router.webapp.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RouterInfo {
  private long id;
  private long startedOn;
  private Service.STATE state;
  private String routerStateStoreName;
  private String routerVersion;
  private String routerBuildVersion;
  private String routerVersionBuiltOn;
  private String hadoopVersion;
  private String hadoopBuildVersion;
  private String hadoopVersionBuiltOn;

  public RouterInfo() {
  } // JAXB needs this

  public RouterInfo(Router router) {
    long ts = Router.getClusterTimeStamp();
    this.id = ts;
    this.state = router.getServiceState();
    Configuration configuration = router.getConfig();
    this.routerStateStoreName = configuration.get(
        YarnConfiguration.FEDERATION_STATESTORE_CLIENT_CLASS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CLIENT_CLASS);
    this.routerVersion = YarnVersionInfo.getVersion();
    this.routerBuildVersion = YarnVersionInfo.getBuildVersion();
    this.routerVersionBuiltOn = YarnVersionInfo.getDate();
    this.hadoopVersion = VersionInfo.getVersion();
    this.hadoopBuildVersion = VersionInfo.getBuildVersion();
    this.hadoopVersionBuiltOn = VersionInfo.getDate();
    this.startedOn = ts;
  }

  public String getState() {
    return this.state.toString();
  }

  public String getRouterStateStore() {
    return this.routerStateStoreName;
  }

  public String getRouterVersion() {
    return this.routerVersion;
  }

  public String getRouterBuildVersion() {
    return this.routerBuildVersion;
  }

  public String getRouterVersionBuiltOn() {
    return this.routerVersionBuiltOn;
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
}
