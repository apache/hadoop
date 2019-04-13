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
package org.apache.hadoop.hdds.scm.safemode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer
    .NodeRegistrationContainerReport;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;

/**
 * Class defining Safe mode exit criteria for Containers.
 */
public class ContainerSafeModeRule extends
    SafeModeExitRule<NodeRegistrationContainerReport>{

  // Required cutoff % for containers with at least 1 reported replica.
  private double safeModeCutoff;
  // Containers read from scm db (excluding containers in ALLOCATED state).
  private Map<Long, ContainerInfo> containerMap;
  private double maxContainer;

  private AtomicLong containerWithMinReplicas = new AtomicLong(0);

  public ContainerSafeModeRule(String ruleName, EventQueue eventQueue,
      Configuration conf,
      List<ContainerInfo> containers, SCMSafeModeManager manager) {
    super(manager, ruleName, eventQueue);
    safeModeCutoff = conf.getDouble(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT);

    Preconditions.checkArgument(
        (safeModeCutoff >= 0.0 && safeModeCutoff <= 1.0),
        HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT  +
            " value should be >= 0.0 and <= 1.0");

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
  protected TypedEvent<NodeRegistrationContainerReport> getEventType() {
    return SCMEvents.NODE_REGISTRATION_CONT_REPORT;
  }


  @Override
  protected boolean validate() {
    return getCurrentContainerThreshold() >= safeModeCutoff;
  }

  @VisibleForTesting
  public double getCurrentContainerThreshold() {
    if (maxContainer == 0) {
      return 1;
    }
    return (containerWithMinReplicas.doubleValue() / maxContainer);
  }

  @Override
  protected void process(NodeRegistrationContainerReport reportsProto) {

    reportsProto.getReport().getReportsList().forEach(c -> {
      if (containerMap.containsKey(c.getContainerID())) {
        if(containerMap.remove(c.getContainerID()) != null) {
          containerWithMinReplicas.getAndAdd(1);
        }
      }
    });

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. {} % containers have at least one"
              + " reported replica.",
          (containerWithMinReplicas.doubleValue() / maxContainer) * 100);
    }
  }

  @Override
  protected void cleanup() {
    containerMap.clear();
  }
}