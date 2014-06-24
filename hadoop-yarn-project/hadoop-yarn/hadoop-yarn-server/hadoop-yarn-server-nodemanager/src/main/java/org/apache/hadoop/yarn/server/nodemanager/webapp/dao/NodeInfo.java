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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeInfo {

  private static final long BYTES_IN_MB = 1024 * 1024;

  protected String healthReport;
  protected long totalVmemAllocatedContainersMB;
  protected long totalPmemAllocatedContainersMB;
  protected long totalVCoresAllocatedContainers;
  protected boolean vmemCheckEnabled;
  protected boolean pmemCheckEnabled;
  protected long lastNodeUpdateTime;
  protected boolean nodeHealthy;
  protected String nodeManagerVersion;
  protected String nodeManagerBuildVersion;
  protected String nodeManagerVersionBuiltOn;
  protected String hadoopVersion;
  protected String hadoopBuildVersion;
  protected String hadoopVersionBuiltOn;
  protected String id;
  protected String nodeHostName;

  public NodeInfo() {
  } // JAXB needs this

  public NodeInfo(final Context context, final ResourceView resourceView) {

    this.id = context.getNodeId().toString();
    this.nodeHostName = context.getNodeId().getHost();
    this.totalVmemAllocatedContainersMB = resourceView
        .getVmemAllocatedForContainers() / BYTES_IN_MB;
    this.vmemCheckEnabled = resourceView.isVmemCheckEnabled();
    this.totalPmemAllocatedContainersMB = resourceView
        .getPmemAllocatedForContainers() / BYTES_IN_MB;
    this.pmemCheckEnabled = resourceView.isPmemCheckEnabled();
    this.totalVCoresAllocatedContainers = resourceView
        .getVCoresAllocatedForContainers();
    this.nodeHealthy = context.getNodeHealthStatus().getIsNodeHealthy();
    this.lastNodeUpdateTime = context.getNodeHealthStatus()
        .getLastHealthReportTime();

    this.healthReport = context.getNodeHealthStatus().getHealthReport();

    this.nodeManagerVersion = YarnVersionInfo.getVersion();
    this.nodeManagerBuildVersion = YarnVersionInfo.getBuildVersion();
    this.nodeManagerVersionBuiltOn = YarnVersionInfo.getDate();
    this.hadoopVersion = VersionInfo.getVersion();
    this.hadoopBuildVersion = VersionInfo.getBuildVersion();
    this.hadoopVersionBuiltOn = VersionInfo.getDate();
  }

  public String getNodeId() {
    return this.id;
  }

  public String getNodeHostName() {
    return this.nodeHostName;
  }

  public String getNMVersion() {
    return this.nodeManagerVersion;
  }

  public String getNMBuildVersion() {
    return this.nodeManagerBuildVersion;
  }

  public String getNMVersionBuiltOn() {
    return this.nodeManagerVersionBuiltOn;
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

  public boolean getHealthStatus() {
    return this.nodeHealthy;
  }

  public long getLastNodeUpdateTime() {
    return this.lastNodeUpdateTime;
  }

  public String getHealthReport() {
    return this.healthReport;
  }

  public long getTotalVmemAllocated() {
    return this.totalVmemAllocatedContainersMB;
  }

  public long getTotalVCoresAllocated() {
    return this.totalVCoresAllocatedContainers;
  }

  public boolean isVmemCheckEnabled() {
    return this.vmemCheckEnabled;
  }

  public long getTotalPmemAllocated() {
    return this.totalPmemAllocatedContainersMB;
  }

  public boolean isPmemCheckEnabled() {
    return this.pmemCheckEnabled;
  }

}
