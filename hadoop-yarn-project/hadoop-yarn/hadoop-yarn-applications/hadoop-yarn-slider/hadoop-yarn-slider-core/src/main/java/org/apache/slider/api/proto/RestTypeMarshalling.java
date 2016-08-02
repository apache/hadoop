/*
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

package org.apache.slider.api.proto;

import com.google.protobuf.ByteString;
import org.apache.commons.io.IOUtils;
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.api.types.ComponentInformation;
import org.apache.slider.api.types.ContainerInformation;
import org.apache.slider.api.types.NodeEntryInformation;
import org.apache.slider.api.types.NodeInformation;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.persist.AggregateConfSerDeser;
import org.apache.slider.core.persist.ConfTreeSerDeser;
import org.apache.slider.server.services.security.SecurityStore;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to handle marshalling of REST
 * types to/from Protobuf records.
 */
public class RestTypeMarshalling {

  public static Messages.ApplicationLivenessInformationProto
  marshall(ApplicationLivenessInformation info) {

    Messages.ApplicationLivenessInformationProto.Builder builder =
        Messages.ApplicationLivenessInformationProto.newBuilder();
    builder.setAllRequestsSatisfied(info.allRequestsSatisfied);
    builder.setRequestsOutstanding(info.requestsOutstanding);
    return builder.build();
  }

  public static ApplicationLivenessInformation
  unmarshall(Messages.ApplicationLivenessInformationProto wire) {
    ApplicationLivenessInformation info = new ApplicationLivenessInformation();
    info.allRequestsSatisfied = wire.getAllRequestsSatisfied();
    info.requestsOutstanding = wire.getRequestsOutstanding();
    return info;
  }

  public static ComponentInformation
  unmarshall(Messages.ComponentInformationProto wire) {
    ComponentInformation info = new ComponentInformation();
    info.name = wire.getName();
    info.priority = wire.getPriority();
    info.placementPolicy = wire.getPlacementPolicy();
    
    info.actual = wire.getActual();
    info.completed = wire.getCompleted();
    info.desired = wire.getDesired();
    info.failed = wire.getFailed();
    info.releasing = wire.getReleasing();
    info.requested = wire.getRequested();
    info.started = wire.getStarted();
    info.startFailed = wire.getStartFailed();
    info.totalRequested = wire.getTotalRequested();
    info.containers = new ArrayList<>(wire.getContainersList());
    if (wire.hasFailureMessage()) {
      info.failureMessage = wire.getFailureMessage();
    }
    if (wire.hasPendingAntiAffineRequestCount()) {
      info.pendingAntiAffineRequestCount = wire.getPendingAntiAffineRequestCount();
    }
    if (wire.hasIsAARequestOutstanding()) {
      info.isAARequestOutstanding = wire.getIsAARequestOutstanding();
    }
    return info;
  }

  public static Messages.GetCertificateStoreResponseProto marshall(
      SecurityStore securityStore) throws IOException {
    Messages.GetCertificateStoreResponseProto.Builder builder =
        Messages.GetCertificateStoreResponseProto.newBuilder();
    builder.setStore(ByteString.copyFrom(getStoreBytes(securityStore)));

    return builder.build();
  }

  private static byte[] getStoreBytes(SecurityStore securityStore)
      throws IOException {
    InputStream is = null;
    byte[] storeBytes;
    try {
      is = new FileInputStream(securityStore.getFile());
      storeBytes = IOUtils.toByteArray(is);
    } finally {
      if (is != null) {
        is.close();
      }
    }
    return storeBytes;
  }

  public static byte[] unmarshall(Messages.GetCertificateStoreResponseProto response) {
    return response.getStore().toByteArray();
  }

  public static Messages.ComponentInformationProto marshall(ComponentInformation info) {

    Messages.ComponentInformationProto.Builder builder =
        Messages.ComponentInformationProto.newBuilder();
    builder.setName(info.name);
    builder.setPriority(info.priority);
    builder.setPlacementPolicy(info.placementPolicy);
    
    builder.setActual(info.actual);
    builder.setCompleted(info.completed);
    builder.setDesired(info.desired);
    builder.setFailed(info.failed);
    builder.setReleasing(info.releasing);
    builder.setRequested(info.requested);
    builder.setStarted(info.started);
    builder.setStartFailed(info.startFailed);
    builder.setTotalRequested(info.totalRequested);
    builder.setNodeFailed(info.nodeFailed);
    builder.setPreempted(info.preempted);
    builder.setFailedRecently(info.failedRecently);
    if (info.failureMessage != null) {
      builder.setFailureMessage(info.failureMessage);
    }
    if (info.containers != null) {
      builder.addAllContainers(info.containers);
    }
    builder.setPendingAntiAffineRequestCount(info.pendingAntiAffineRequestCount);
    builder.setIsAARequestOutstanding(info.isAARequestOutstanding);
    return builder.build();
  }

  public static Messages.NodeInformationProto marshall(NodeInformation info) {

    Messages.NodeInformationProto.Builder builder =
        Messages.NodeInformationProto.newBuilder();
    builder.setHostname(info.hostname);
    builder.setLastUpdated(info.lastUpdated);
    builder.setState(info.state != null? info.state : "unknown");
    builder.setRackName(info.rackName != null ? info.rackName : "");
    builder.setHealthReport(info.healthReport != null ? info.healthReport : "");
    builder.setHttpAddress(info.httpAddress != null ? info.httpAddress : "");
    builder.setLabels(info.labels != null ? info.labels: "");


    if (info.entries != null) {
      for (Map.Entry<String, NodeEntryInformation> elt : info.entries.entrySet()) {
        NodeEntryInformation entry = elt.getValue();
        Messages.NodeEntryInformationProto.Builder node =
            Messages.NodeEntryInformationProto.newBuilder();
        node.setPriority(entry.priority);
        node.setName(elt.getKey());
        node.setFailed(entry.failed);
        node.setFailedRecently(entry.failedRecently);
        node.setLive(entry.live);
        node.setLastUsed(entry.lastUsed);
        node.setPreempted(entry.preempted);
        node.setRequested(entry.requested);
        node.setReleasing(entry.releasing);
        node.setStartFailed(entry.startFailed);
        node.setStarting(entry.starting);
        builder.addEntries(node.build());
      }
    }
    return builder.build();
  }

  public static NodeInformation unmarshall(Messages.NodeInformationProto wire) {
    NodeInformation info = new NodeInformation();
    info.healthReport = wire.getHealthReport();
    info.hostname = wire.getHostname();
    info.httpAddress = wire.getHttpAddress();
    info.labels = wire.getLabels();
    info.lastUpdated = wire.getLastUpdated();
    info.rackName = wire.getRackName();
    info.state = wire.getState();
    List<Messages.NodeEntryInformationProto> entriesList = wire.getEntriesList();
    if (entriesList != null) {
      info.entries = new HashMap<>(entriesList.size());
      for (Messages.NodeEntryInformationProto entry : entriesList) {
        NodeEntryInformation nei = new NodeEntryInformation();
        nei.failed = entry.getFailed();
        nei.failedRecently = entry.getFailedRecently();
        nei.lastUsed = entry.getLastUsed();
        nei.live = entry.getLive();
        nei.preempted = entry.getPreempted();
        nei.priority = entry.getPriority();
        nei.requested = entry.getRequested();
        nei.releasing = entry.getReleasing();
        nei.startFailed = entry.getStartFailed();
        nei.starting = entry.getStarting();
        info.entries.put(entry.getName(), nei);
      }
    }
    return info;
  }

  public static ContainerInformation unmarshall(Messages.ContainerInformationProto wire) {
    ContainerInformation info = new ContainerInformation();
    info.containerId = wire.getContainerId();
    info.component = wire.getComponent();
    info.appVersion = wire.getAppVersion();
    info.state = wire.getState();
    if (wire.hasReleased()) {
      info.released = wire.getReleased();
    }
    if (wire.hasExitCode()) {
      info.exitCode = wire.getExitCode();
    }
    if (wire.hasDiagnostics()) {
      info.diagnostics = wire.getDiagnostics();
    }
    if (wire.hasHost()) {
      info.host = wire.getHost();
    }
    if (wire.hasHostURL()) {
      info.host = wire.getHostURL();
    }
    info.createTime = wire.getCreateTime();
    info.startTime = wire.getStartTime();
    info.output = wire.getOutputList().toArray(
        new String[wire.getOutputCount()]
        );
    if (wire.hasPlacement()) {
      info.placement = wire.getPlacement();
    }
    return info;
  }

  public static List<ContainerInformation> unmarshall(Messages.GetLiveContainersResponseProto wire) {
    List<ContainerInformation> infoList = new ArrayList<>(wire.getContainersList().size());
    for (Messages.ContainerInformationProto container : wire.getContainersList()) {
      infoList.add(unmarshall(container));
    }
    return infoList;
  }

  public static Messages.ContainerInformationProto marshall(ContainerInformation info) {

    Messages.ContainerInformationProto.Builder builder =
        Messages.ContainerInformationProto.newBuilder();
    if (info.containerId != null) {
      builder.setContainerId(info.containerId);
    }
    if (info.component != null) {
      builder.setComponent(info.component);
    }
    if (info.appVersion != null) {
      builder.setAppVersion(info.appVersion);
    }
    builder.setCreateTime(info.createTime);
    if (info.diagnostics != null) {
      builder.setDiagnostics(info.diagnostics);
    }
    if (info.host != null) {
      builder.setHost(info.host);
    }
    if (info.hostURL != null) {
      builder.setHostURL(info.hostURL);
    }
    if (info.output != null) {
      builder.addAllOutput(Arrays.asList(info.output));
    }
    if (info.released != null) {
      builder.setReleased(info.released);
    }
    if (info.placement != null) {
      builder.setPlacement(info.placement);
    }
    builder.setStartTime(info.startTime);
    builder.setState(info.state);
    return builder.build();
  }

  public static String unmarshall(Messages.WrappedJsonProto wire) {
    return wire.getJson();
  }

  public static ConfTree unmarshallToConfTree(Messages.WrappedJsonProto wire) throws
      IOException {
    return new ConfTreeSerDeser().fromJson(wire.getJson());
  }
  
  public static ConfTreeOperations unmarshallToCTO(Messages.WrappedJsonProto wire) throws
      IOException {
    return new ConfTreeOperations(new ConfTreeSerDeser().fromJson(wire.getJson()));
  }

  public static AggregateConf unmarshallToAggregateConf(Messages.WrappedJsonProto wire) throws
      IOException {
    return new AggregateConfSerDeser().fromJson(wire.getJson());
  }

}
