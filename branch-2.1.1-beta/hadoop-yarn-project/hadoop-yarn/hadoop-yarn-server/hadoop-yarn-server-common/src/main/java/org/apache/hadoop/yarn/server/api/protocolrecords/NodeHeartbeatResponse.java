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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;

public interface NodeHeartbeatResponse {
  int getResponseId();
  NodeAction getNodeAction();

  List<ContainerId> getContainersToCleanup();

  List<ApplicationId> getApplicationsToCleanup();

  void setResponseId(int responseId);
  void setNodeAction(NodeAction action);

  MasterKey getContainerTokenMasterKey();
  void setContainerTokenMasterKey(MasterKey secretKey);
  
  MasterKey getNMTokenMasterKey();
  void setNMTokenMasterKey(MasterKey secretKey);

  void addAllContainersToCleanup(List<ContainerId> containers);
  
  void addAllApplicationsToCleanup(List<ApplicationId> applications);

  long getNextHeartBeatInterval();
  void setNextHeartBeatInterval(long nextHeartBeatInterval);
  
  String getDiagnosticsMessage();

  void setDiagnosticsMessage(String diagnosticsMessage);
}
