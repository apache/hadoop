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

import java.util.HashSet;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;

/**
 * Class defining Chill mode exit criteria according to number of DataNodes
 * registered with SCM.
 */
public class DataNodeChillModeRule implements
    ChillModeExitRule<NodeRegistrationContainerReport>,
    EventHandler<NodeRegistrationContainerReport> {

  // Min DataNodes required to exit chill mode.
  private int requiredDns;
  private int registeredDns = 0;
  // Set to track registered DataNodes.
  private HashSet<UUID> registeredDnSet;

  private final SCMChillModeManager chillModeManager;

  public DataNodeChillModeRule(Configuration conf,
      SCMChillModeManager manager) {
    requiredDns = conf.getInt(
        HddsConfigKeys.HDDS_SCM_CHILLMODE_MIN_DATANODE,
        HddsConfigKeys.HDDS_SCM_CHILLMODE_MIN_DATANODE_DEFAULT);
    registeredDnSet = new HashSet<>(requiredDns * 2);
    chillModeManager = manager;
  }

  @Override
  public boolean validate() {
    return registeredDns >= requiredDns;
  }

  @VisibleForTesting
  public double getRegisteredDataNodes() {
    return registeredDns;
  }

  @Override
  public void process(NodeRegistrationContainerReport reportsProto) {

    registeredDnSet.add(reportsProto.getDatanodeDetails().getUuid());
    registeredDns = registeredDnSet.size();

  }

  @Override
  public void onMessage(NodeRegistrationContainerReport
      nodeRegistrationContainerReport, EventPublisher publisher) {
    // TODO: when we have remove handlers, we can remove getInChillmode check

    if (chillModeManager.getInChillMode()) {
      if (validate()) {
        return;
      }

      process(nodeRegistrationContainerReport);

      if (chillModeManager.getInChillMode()) {
        SCMChillModeManager.getLogger().info(
            "SCM in chill mode. {} DataNodes registered, {} required.",
            registeredDns, requiredDns);
      }

      if (validate()) {
        chillModeManager.validateChillModeExitRules(publisher);
      }
    }
  }

  @Override
  public void cleanup() {
    registeredDnSet.clear();
  }
}