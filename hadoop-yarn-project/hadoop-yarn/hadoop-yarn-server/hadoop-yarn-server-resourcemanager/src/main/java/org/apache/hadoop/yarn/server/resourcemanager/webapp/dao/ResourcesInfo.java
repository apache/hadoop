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
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;

/**
 * DAO which wraps PartitionResourceUsageInfo applicable for a queue/user
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ResourcesInfo {
  List<PartitionResourcesInfo> resourceUsagesByPartition =
      new ArrayList<>();

  public ResourcesInfo() {
  }

  public ResourcesInfo(ResourceUsage resourceUsage,
      boolean considerAMUsage) {
    if (resourceUsage == null) {
      return;
    }
    for (String partitionName : resourceUsage.getNodePartitionsSet()) {
      resourceUsagesByPartition.add(new PartitionResourcesInfo(partitionName,
          new ResourceInfo(resourceUsage.getUsed(partitionName)),
          new ResourceInfo(resourceUsage.getReserved(partitionName)),
          new ResourceInfo(resourceUsage.getPending(partitionName)),
          considerAMUsage ? new ResourceInfo(resourceUsage
              .getAMUsed(partitionName)) : null,
          considerAMUsage ? new ResourceInfo(resourceUsage
              .getAMLimit(partitionName)) : null,
          considerAMUsage ? new ResourceInfo(resourceUsage
              .getUserAMLimit(partitionName)) : null));
    }
  }

  public ResourcesInfo(ResourceUsage resourceUsage) {
    this(resourceUsage, true);
  }

  public List<PartitionResourcesInfo> getPartitionResourceUsages() {
    return resourceUsagesByPartition;
  }

  public void setPartitionResourceUsages(
      List<PartitionResourcesInfo> resources) {
    this.resourceUsagesByPartition = resources;
  }

  public PartitionResourcesInfo getPartitionResourceUsageInfo(
      String partitionName) {
    for (PartitionResourcesInfo partitionResourceUsageInfo :
      resourceUsagesByPartition) {
      if (partitionResourceUsageInfo.getPartitionName().equals(partitionName)) {
        return partitionResourceUsageInfo;
      }
    }
    return new PartitionResourcesInfo();
  }
}
