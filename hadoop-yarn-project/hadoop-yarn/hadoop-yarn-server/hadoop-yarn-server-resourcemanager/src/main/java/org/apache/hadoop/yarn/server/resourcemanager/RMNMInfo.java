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

package org.apache.hadoop.yarn.server.resourcemanager;


import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.eclipse.jetty.util.ajax.JSON;

/**
 * JMX bean listing statuses of all node managers.
 */
public class RMNMInfo implements RMNMInfoBeans {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMNMInfo.class);
  private RMContext rmContext;
  private ResourceScheduler scheduler;

  /**
   * Constructor for RMNMInfo registers the bean with JMX.
   * 
   * @param rmc resource manager's context object
   * @param sched resource manager's scheduler object
   */
  public RMNMInfo(RMContext rmc, ResourceScheduler sched) {
    this.rmContext = rmc;
    this.scheduler = sched;

    StandardMBean bean;
    try {
        bean = new StandardMBean(this,RMNMInfoBeans.class);
        MBeans.register("ResourceManager", "RMNMInfo", bean);
    } catch (NotCompliantMBeanException e) {
        LOG.warn("Error registering RMNMInfo MBean", e);
    }
    LOG.info("Registered RMNMInfo MBean");
  }


  static class InfoMap extends LinkedHashMap<String, Object> {
    private static final long serialVersionUID = 1L;
  }

  /**
   * Implements getLiveNodeManagers()
   * 
   * @return JSON formatted string containing statuses of all node managers
   */
  @Override // RMNMInfoBeans
  public String getLiveNodeManagers() {
    Collection<RMNode> nodes = this.rmContext.getRMNodes().values();
    List<InfoMap> nodesInfo = new ArrayList<InfoMap>();

    for (final RMNode ni : nodes) {
        SchedulerNodeReport report = scheduler.getNodeReport(ni.getNodeID());
        InfoMap info = new InfoMap();
        info.put("HostName", ni.getHostName());
        info.put("Rack", ni.getRackName());
        info.put("State", ni.getState().toString());
        info.put("NodeId", ni.getNodeID());
        info.put("NodeHTTPAddress", ni.getHttpAddress());
        info.put("LastHealthUpdate",
                        ni.getLastHealthReportTime());
        info.put("HealthReport",
                        ni.getHealthReport());
        info.put("NodeManagerVersion",
                ni.getNodeManagerVersion());
        if(report != null) {
          info.put("NumContainers", report.getNumContainers());
          info.put("UsedMemoryMB", report.getUsedResource().getMemorySize());
          info.put("AvailableMemoryMB",
              report.getAvailableResource().getMemorySize());
        }

        nodesInfo.add(info);
    }

    return JSON.toString(nodesInfo);
  }
}
