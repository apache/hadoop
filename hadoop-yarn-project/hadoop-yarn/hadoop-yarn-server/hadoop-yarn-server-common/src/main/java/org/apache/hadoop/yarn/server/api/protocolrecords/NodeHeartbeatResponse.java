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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;

public interface NodeHeartbeatResponse {
  int getResponseId();
  NodeAction getNodeAction();

  List<ContainerId> getContainersToCleanup();
  List<ContainerId> getContainersToBeRemovedFromNM();

  List<ApplicationId> getApplicationsToCleanup();

  void setResponseId(int responseId);
  void setNodeAction(NodeAction action);

  MasterKey getContainerTokenMasterKey();
  void setContainerTokenMasterKey(MasterKey secretKey);
  
  MasterKey getNMTokenMasterKey();
  void setNMTokenMasterKey(MasterKey secretKey);

  void addAllContainersToCleanup(List<ContainerId> containers);

  // This tells NM to remove finished containers from its context. Currently, NM
  // will remove finished containers from its context only after AM has actually
  // received the finished containers in a previous allocate response
  void addContainersToBeRemovedFromNM(List<ContainerId> containers);
  
  void addAllApplicationsToCleanup(List<ApplicationId> applications);

  List<SignalContainerRequest> getContainersToSignalList();
  void addAllContainersToSignal(List<SignalContainerRequest> containers);
  long getNextHeartBeatInterval();
  void setNextHeartBeatInterval(long nextHeartBeatInterval);
  
  String getDiagnosticsMessage();

  void setDiagnosticsMessage(String diagnosticsMessage);

  // Credentials (i.e. hdfs tokens) needed by NodeManagers for application
  // localizations and logAggreations.
  Map<ApplicationId, ByteBuffer> getSystemCredentialsForApps();

  void setSystemCredentialsForApps(
      Map<ApplicationId, ByteBuffer> systemCredentials);
  
  boolean getAreNodeLabelsAcceptedByRM();
  void setAreNodeLabelsAcceptedByRM(boolean areNodeLabelsAcceptedByRM);

  Resource getResource();
  void setResource(Resource resource);

  List<Container> getContainersToDecrease();
  void addAllContainersToDecrease(Collection<Container> containersToDecrease);

  ContainerQueuingLimit getContainerQueuingLimit();
  void setContainerQueuingLimit(ContainerQueuingLimit containerQueuingLimit);
}
