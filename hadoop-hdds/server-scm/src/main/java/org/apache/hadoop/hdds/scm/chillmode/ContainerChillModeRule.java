/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.chillmode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class defining Chill mode exit criteria for Containers.
 */
public class ContainerChillModeRule implements
    ChillModeExitRule<NodeRegistrationContainerReport> {

  // Required cutoff % for containers with at least 1 reported replica.
  private double chillModeCutoff;
  // Containers read from scm db (excluding containers in ALLOCATED state).
  private Map<Long, ContainerInfo> containerMap;
  private double maxContainer;

  private AtomicLong containerWithMinReplicas = new AtomicLong(0);
  private final SCMChillModeManager chillModeManager;

  public ContainerChillModeRule(Configuration conf,
      List<ContainerInfo> containers, SCMChillModeManager manager) {
    chillModeCutoff = conf.getDouble(
        HddsConfigKeys.HDDS_SCM_CHILLMODE_THRESHOLD_PCT,
        HddsConfigKeys.HDDS_SCM_CHILLMODE_THRESHOLD_PCT_DEFAULT);
    chillModeManager = manager;
    containerMap = new ConcurrentHashMap<>();
    if(containers != null) {
      containers.forEach(c -> {
        // TODO: There can be containers in OPEN state which were never
        // created by the client. We are not considering these containers for
        // now. These containers can be handled by tracking pipelines.
        if (c != null && c.getState() != null &&
            !c.getState().equals(HddsProtos.LifeCycleState.OPEN)) {
          containerMap.put(c.getContainerID(), c);
        }
      });
      maxContainer = containerMap.size();
    }
  }

  @Override
  public boolean validate() {
    if (maxContainer == 0) {
      return true;
    }
    return getCurrentContainerThreshold() >= chillModeCutoff;
  }

  @VisibleForTesting
  public double getCurrentContainerThreshold() {
    if (maxContainer == 0) {
      return 1;
    }
    return (containerWithMinReplicas.doubleValue() / maxContainer);
  }

  @Override
  public void process(NodeRegistrationContainerReport reportsProto) {
    if (maxContainer == 0) {
      // No container to check.
      return;
    }

    reportsProto.getReport().getReportsList().forEach(c -> {
      if (containerMap.containsKey(c.getContainerID())) {
        if(containerMap.remove(c.getContainerID()) != null) {
          containerWithMinReplicas.getAndAdd(1);
        }
      }
    });
    if(chillModeManager.getInChillMode()) {
      SCMChillModeManager.getLogger().info(
          "SCM in chill mode. {} % containers have at least one"
              + " reported replica.",
          (containerWithMinReplicas.get() / maxContainer) * 100);
    }
  }

  @Override
  public void cleanup() {
    containerMap.clear();
  }
}