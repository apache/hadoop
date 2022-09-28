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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInformationsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RouterSchedulerMetrics {

  /** Metrics Log. **/
  private static final Logger LOG = LoggerFactory.getLogger(RouterSchedulerMetrics.class);

  /** Scheduler Information. **/
  private String subCluster = "N/A";
  private String schedulerType = "N/A";
  private String schedulingResourceType = "N/A";
  private String minimumAllocation = "N/A";
  private String maximumAllocation = "N/A";
  private String applicationPriority = "N/A";
  private String schedulerBusy = "N/A";
  private String rmDispatcherEventQueueSize = "N/A";
  private String schedulerDispatcherEventQueueSize = "N/A";

  public RouterSchedulerMetrics() {

  }

  public RouterSchedulerMetrics(SubClusterInfo subClusterInfo, RouterClusterMetrics metrics,
      SchedulerTypeInfo typeInfo) {
    try {
      // Parse Scheduler Information.
      SchedulerInfo schedulerInfo = typeInfo.getSchedulerInfo();
      this.subCluster = subClusterInfo.getSubClusterId().getId();
      this.schedulerType = schedulerInfo.getSchedulerType();

      // Resource Information
      ResourceInformationsInfo resourceInformationsInfo =
          schedulerInfo.getMinAllocation().getResourcesInformations();
      List<String> items = new ArrayList<>();
      for (ResourceInformation info : resourceInformationsInfo.getApps()) {
        if (StringUtils.isNotBlank(info.getUnits())) {
          items.add(info.getName() + "(" + info.getUnits() + ")");
        } else {
          items.add(info.getName());
        }
      }
      this.schedulingResourceType = "[" + StringUtils.join(items, ",") + "]";
      this.minimumAllocation = schedulerInfo.getMinAllocation().toString();
      this.maximumAllocation = schedulerInfo.getMaxAllocation().toString();
      this.applicationPriority = String.valueOf(schedulerInfo.getMaxClusterLevelAppPriority());

    } catch (Exception ex) {
      LOG.error("RouterSchedulerMetrics Error.", ex);
    }
  }

  public String getSubCluster() {
    return subCluster;
  }

  public String getSchedulerType() {
    return schedulerType;
  }

  public String getSchedulingResourceType() {
    return schedulingResourceType;
  }

  public String getMinimumAllocation() {
    return minimumAllocation;
  }

  public String getMaximumAllocation() {
    return maximumAllocation;
  }

  public String getApplicationPriority() {
    return applicationPriority;
  }

  public String getRmDispatcherEventQueueSize() {
    return rmDispatcherEventQueueSize;
  }

  public String getSchedulerDispatcherEventQueueSize() {
    return schedulerDispatcherEventQueueSize;
  }

  public String getSchedulerBusy() {
    return schedulerBusy;
  }
}
