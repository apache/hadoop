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

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RouterClusterMetrics {

  protected static final long BYTES_IN_MB = 1024 * 1024;
  private static final Logger LOG = LoggerFactory.getLogger(RouterClusterMetrics.class);

  // webPageTitlePrefix
  private String webPageTitlePrefix = "Federation";

  // Application Information.
  private String appsSubmitted = "N/A";
  private String appsCompleted = "N/A";
  private String appsPending = "N/A";
  private String appsRunning = "N/A";
  private String appsFailed = "N/A";
  private String appsKilled = "N/A";

  // Memory Information.
  private String totalMemory = "N/A";
  private String reservedMemory = "N/A";
  private String availableMemory = "N/A";
  private String allocatedMemory = "N/A";
  private String pendingMemory = "N/A";

  // VirtualCores Information.
  private String reservedVirtualCores = "N/A";
  private String availableVirtualCores = "N/A";
  private String allocatedVirtualCores = "N/A";
  private String pendingVirtualCores = "N/A";
  private String totalVirtualCores = "N/A";

  // Resources Information.
  private String usedResources = "N/A";
  private String totalResources = "N/A";
  private String reservedResources = "N/A";
  private String allocatedContainers = "N/A";

  // Resource Percent Information.
  private String utilizedMBPercent = "N/A";
  private String utilizedVirtualCoresPercent = "N/A";

  // Node Information.
  private String activeNodes = "N/A";
  private String decommissioningNodes = "N/A";
  private String decommissionedNodes = "N/A";
  private String lostNodes = "N/A";
  private String unhealthyNodes = "N/A";
  private String rebootedNodes = "N/A";
  private String shutdownNodes = "N/A";

  public RouterClusterMetrics() {

  }

  public RouterClusterMetrics(ClusterMetricsInfo metrics) {
    if (metrics != null) {
      // Application Information Conversion.
      conversionApplicationInformation(metrics);

      // Memory Information Conversion.
      conversionMemoryInformation(metrics);

      // Resources Information Conversion.
      conversionResourcesInformation(metrics);

      // Percent Information Conversion.
      conversionResourcesPercent(metrics);

      // Node Information Conversion.
      conversionNodeInformation(metrics);
    }
  }

  public RouterClusterMetrics(ClusterMetricsInfo metrics,
      String webPageTitlePrefix) {
    this(metrics);
    this.webPageTitlePrefix = webPageTitlePrefix;
  }

  // Get Key Metric Information
  public String getAppsSubmitted() {
    return appsSubmitted;
  }

  public String getAppsCompleted() {
    return appsCompleted;
  }

  public String getAppsPending() {
    return appsPending;
  }

  public String getAppsRunning() {
    return appsRunning;
  }

  public String getAppsFailed() {
    return appsFailed;
  }

  public String getAppsKilled() {
    return appsKilled;
  }

  public String getTotalMemory() {
    return totalMemory;
  }

  public String getReservedMemory() {
    return reservedMemory;
  }

  public String getAvailableMemory() {
    return availableMemory;
  }

  public String getAllocatedMemory() {
    return allocatedMemory;
  }

  public String getPendingMemory() {
    return pendingMemory;
  }

  public String getReservedVirtualCores() {
    return reservedVirtualCores;
  }

  public String getAvailableVirtualCores() {
    return availableVirtualCores;
  }

  public String getAllocatedVirtualCores() {
    return allocatedVirtualCores;
  }

  public String getPendingVirtualCores() {
    return pendingVirtualCores;
  }

  public String getTotalVirtualCores() {
    return totalVirtualCores;
  }

  public String getUsedResources() {
    return usedResources;
  }

  public String getTotalResources() {
    return totalResources;
  }

  public String getReservedResources() {
    return reservedResources;
  }

  public String getAllocatedContainers() {
    return allocatedContainers;
  }

  public String getUtilizedMBPercent() {
    return utilizedMBPercent;
  }

  public String getUtilizedVirtualCoresPercent() {
    return utilizedVirtualCoresPercent;
  }

  public String getActiveNodes() {
    return activeNodes;
  }

  public String getDecommissioningNodes() {
    return decommissioningNodes;
  }

  public String getDecommissionedNodes() {
    return decommissionedNodes;
  }

  public String getLostNodes() {
    return lostNodes;
  }

  public String getUnhealthyNodes() {
    return unhealthyNodes;
  }

  public String getRebootedNodes() {
    return rebootedNodes;
  }

  public String getShutdownNodes() {
    return shutdownNodes;
  }

  // Metric Information Conversion
  public void conversionApplicationInformation(ClusterMetricsInfo metrics) {
    try {
      // Application Information.
      this.appsSubmitted = String.valueOf(metrics.getAppsSubmitted());
      this.appsCompleted = String.valueOf(metrics.getAppsCompleted() +
           metrics.getAppsFailed() + metrics.getAppsKilled());
      this.appsPending = String.valueOf(metrics.getAppsPending());
      this.appsRunning = String.valueOf(metrics.getAppsRunning());
      this.appsFailed = String.valueOf(metrics.getAppsFailed());
      this.appsKilled = String.valueOf(metrics.getAppsKilled());
    } catch (Exception e) {
      LOG.error("conversionApplicationInformation error.", e);
    }
  }

  // Metric Memory Information
  public void conversionMemoryInformation(ClusterMetricsInfo metrics) {
    try {
      // Memory Information.
      this.totalMemory = StringUtils.byteDesc(metrics.getTotalMB() * BYTES_IN_MB);
      this.reservedMemory = StringUtils.byteDesc(metrics.getReservedMB() * BYTES_IN_MB);
      this.availableMemory = StringUtils.byteDesc(metrics.getAvailableMB() * BYTES_IN_MB);
      this.allocatedMemory = StringUtils.byteDesc(metrics.getAllocatedMB() * BYTES_IN_MB);
      this.pendingMemory = StringUtils.byteDesc(metrics.getPendingMB() * BYTES_IN_MB);
    } catch (Exception e) {
      LOG.error("conversionMemoryInformation error.", e);
    }
  }

  // ResourcesInformation Conversion
  public void conversionResourcesInformation(ClusterMetricsInfo metrics) {
    try {
      // Parse resource information from metrics.
      Resource metricUsedResources;
      Resource metricTotalResources;
      Resource metricReservedResources;

      int metricAllocatedContainers;
      if (metrics.getCrossPartitionMetricsAvailable()) {
        metricAllocatedContainers = metrics.getTotalAllocatedContainersAcrossPartition();
        metricUsedResources = metrics.getTotalUsedResourcesAcrossPartition().getResource();
        metricTotalResources = metrics.getTotalClusterResourcesAcrossPartition().getResource();
        metricReservedResources = metrics.getTotalReservedResourcesAcrossPartition().getResource();
        // getTotalUsedResourcesAcrossPartition includes reserved resources.
        Resources.subtractFrom(metricUsedResources, metricReservedResources);
      } else {
        metricAllocatedContainers = metrics.getContainersAllocated();
        metricUsedResources = Resource.newInstance(metrics.getAllocatedMB(),
            (int) metrics.getAllocatedVirtualCores());
        metricTotalResources = Resource.newInstance(metrics.getTotalMB(),
            (int) metrics.getTotalVirtualCores());
        metricReservedResources = Resource.newInstance(metrics.getReservedMB(),
            (int) metrics.getReservedVirtualCores());
      }

      // Convert to standard format.
      usedResources = metricUsedResources.getFormattedString();
      totalResources = metricTotalResources.getFormattedString();
      reservedResources = metricReservedResources.getFormattedString();
      allocatedContainers =  String.valueOf(metricAllocatedContainers);

    } catch (Exception e) {
      LOG.error("conversionResourcesInformation error.", e);
    }
  }

  // ResourcesPercent Conversion
  public void conversionResourcesPercent(ClusterMetricsInfo metrics) {
    try {
      this.utilizedMBPercent = String.valueOf(metrics.getUtilizedMBPercent());
      this.utilizedVirtualCoresPercent = String.valueOf(metrics.getUtilizedVirtualCoresPercent());
    } catch (Exception e) {
      LOG.error("conversionResourcesPercent error.", e);
    }
  }

  // NodeInformation Conversion
  public void conversionNodeInformation(ClusterMetricsInfo metrics) {
    try {
      this.activeNodes = String.valueOf(metrics.getActiveNodes());
      this.decommissioningNodes = String.valueOf(metrics.getDecommissioningNodes());
      this.decommissionedNodes = String.valueOf(metrics.getDecommissionedNodes());
      this.lostNodes = String.valueOf(metrics.getLostNodes());
      this.unhealthyNodes = String.valueOf(metrics.getUnhealthyNodes());
      this.rebootedNodes = String.valueOf(metrics.getRebootedNodes());
      this.shutdownNodes = String.valueOf(metrics.getShutdownNodes());
    } catch (Exception e) {
      LOG.error("conversionNodeInformation error.", e);
    }
  }

  public String getWebPageTitlePrefix() {
    return webPageTitlePrefix;
  }
}
