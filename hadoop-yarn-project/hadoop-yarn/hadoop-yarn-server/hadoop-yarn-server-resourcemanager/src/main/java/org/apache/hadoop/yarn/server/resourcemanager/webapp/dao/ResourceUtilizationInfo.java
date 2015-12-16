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

import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

/**
 * DAO object represents resource utilization of node and containers.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ResourceUtilizationInfo {

  protected int nodePhysicalMemoryMB;
  protected int nodeVirtualMemoryMB;
  protected double nodeCPUUsage;
  protected int aggregatedContainersPhysicalMemoryMB;
  protected int aggregatedContainersVirtualMemoryMB;
  protected double containersCPUUsage;

  public ResourceUtilizationInfo() {
  } // JAXB needs this

  public ResourceUtilizationInfo(RMNode ni) {

    // update node and containers resource utilization
    ResourceUtilization nodeUtilization = ni.getNodeUtilization();
    if (nodeUtilization != null) {
      this.nodePhysicalMemoryMB = nodeUtilization.getPhysicalMemory();
      this.nodeVirtualMemoryMB = nodeUtilization.getVirtualMemory();
      this.nodeCPUUsage = nodeUtilization.getCPU();
    }

    ResourceUtilization containerAggrUtilization = ni
        .getAggregatedContainersUtilization();
    if (containerAggrUtilization != null) {
      this.aggregatedContainersPhysicalMemoryMB = containerAggrUtilization
          .getPhysicalMemory();
      this.aggregatedContainersVirtualMemoryMB = containerAggrUtilization
          .getVirtualMemory();
      this.containersCPUUsage = containerAggrUtilization.getCPU();
    }
  }

  public int getNodePhysicalMemoryMB() {
    return nodePhysicalMemoryMB;
  }

  public int getNodeVirtualMemoryMB() {
    return nodeVirtualMemoryMB;
  }

  public int getAggregatedContainersPhysicalMemoryMB() {
    return aggregatedContainersPhysicalMemoryMB;
  }

  public int getAggregatedContainersVirtualMemoryMB() {
    return aggregatedContainersVirtualMemoryMB;
  }

  public double getNodeCPUUsage() {
    return nodeCPUUsage;
  }

  public double getContainersCPUUsage() {
    return containersCPUUsage;
  }
}
