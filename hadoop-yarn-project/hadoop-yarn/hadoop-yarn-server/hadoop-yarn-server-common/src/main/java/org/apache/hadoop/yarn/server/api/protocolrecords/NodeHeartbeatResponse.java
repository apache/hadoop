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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SystemCredentialsForAppsProto;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;

/**
 * Node Manager's heartbeat response.
 */
public abstract class NodeHeartbeatResponse {
  public abstract int getResponseId();

  public abstract NodeAction getNodeAction();

  public abstract List<ContainerId> getContainersToCleanup();

  public abstract List<ContainerId> getContainersToBeRemovedFromNM();

  public abstract List<ApplicationId> getApplicationsToCleanup();

  // This tells NM the collectors' address info of related apps
  public abstract Map<ApplicationId, AppCollectorData> getAppCollectors();
  public abstract void setAppCollectors(
      Map<ApplicationId, AppCollectorData> appCollectorsMap);

  public abstract void setResponseId(int responseId);

  public abstract void setNodeAction(NodeAction action);

  public abstract MasterKey getContainerTokenMasterKey();

  public abstract void setContainerTokenMasterKey(MasterKey secretKey);

  public abstract MasterKey getNMTokenMasterKey();

  public abstract void setNMTokenMasterKey(MasterKey secretKey);

  public abstract void addAllContainersToCleanup(List<ContainerId> containers);

  // This tells NM to remove finished containers from its context. Currently, NM
  // will remove finished containers from its context only after AM has actually
  // received the finished containers in a previous allocate response
  public abstract void addContainersToBeRemovedFromNM(
      List<ContainerId> containers);

  public abstract void addAllApplicationsToCleanup(
      List<ApplicationId> applications);

  public abstract List<SignalContainerRequest> getContainersToSignalList();

  public abstract void addAllContainersToSignal(
      List<SignalContainerRequest> containers);

  public abstract long getNextHeartBeatInterval();

  public abstract void setNextHeartBeatInterval(long nextHeartBeatInterval);

  public abstract String getDiagnosticsMessage();

  public abstract void setDiagnosticsMessage(String diagnosticsMessage);

  public abstract boolean getAreNodeLabelsAcceptedByRM();

  public abstract void setAreNodeLabelsAcceptedByRM(
      boolean areNodeLabelsAcceptedByRM);

  public abstract Resource getResource();

  public abstract void setResource(Resource resource);

  public abstract List<Container> getContainersToUpdate();

  public abstract void addAllContainersToUpdate(
      Collection<Container> containersToUpdate);

  public abstract ContainerQueuingLimit getContainerQueuingLimit();

  public abstract void setContainerQueuingLimit(
      ContainerQueuingLimit containerQueuingLimit);

  public abstract List<Container> getContainersToDecrease();

  public abstract void addAllContainersToDecrease(
      Collection<Container> containersToDecrease);

  public abstract boolean getAreNodeAttributesAcceptedByRM();

  public abstract void setAreNodeAttributesAcceptedByRM(
      boolean areNodeAttributesAcceptedByRM);

  public abstract void setTokenSequenceNo(long tokenSequenceNo);

  public abstract long getTokenSequenceNo();

  // Credentials (i.e. hdfs tokens) needed by NodeManagers for application
  // localizations and logAggregations.
  public abstract void setSystemCredentialsForApps(
      Collection<SystemCredentialsForAppsProto> systemCredentials);

  public abstract Collection<SystemCredentialsForAppsProto>
      getSystemCredentialsForApps();
}
