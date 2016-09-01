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

package org.apache.hadoop.yarn.server.utils;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.server.api.ContainerType;

/**
 * Builder utilities to construct various objects.
 *
 */
@Private
public class BuilderUtils {

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  public static class ApplicationIdComparator implements
      Comparator<ApplicationId>, Serializable {
    @Override
    public int compare(ApplicationId a1, ApplicationId a2) {
      return a1.compareTo(a2);
    }
  }

  public static class ContainerIdComparator implements
      java.util.Comparator<ContainerId>, Serializable {

    @Override
    public int compare(ContainerId c1,
        ContainerId c2) {
      return c1.compareTo(c2);
    }
  }

  public static LocalResource newLocalResource(URL url, LocalResourceType type,
      LocalResourceVisibility visibility, long size, long timestamp,
      boolean shouldBeUploadedToSharedCache) {
    LocalResource resource =
      recordFactory.newRecordInstance(LocalResource.class);
    resource.setResource(url);
    resource.setType(type);
    resource.setVisibility(visibility);
    resource.setSize(size);
    resource.setTimestamp(timestamp);
    resource.setShouldBeUploadedToSharedCache(shouldBeUploadedToSharedCache);
    return resource;
  }

  public static LocalResource newLocalResource(URI uri,
      LocalResourceType type, LocalResourceVisibility visibility, long size,
      long timestamp, boolean shouldBeUploadedToSharedCache) {
    return newLocalResource(URL.fromURI(uri), type,
        visibility, size, timestamp, shouldBeUploadedToSharedCache);
  }

  public static ApplicationId newApplicationId(RecordFactory recordFactory,
      long clustertimestamp, CharSequence id) {
    return ApplicationId.newInstance(clustertimestamp,
        Integer.parseInt(id.toString()));
  }

  public static ApplicationId newApplicationId(RecordFactory recordFactory,
      long clusterTimeStamp, int id) {
    return ApplicationId.newInstance(clusterTimeStamp, id);
  }

  public static ApplicationId newApplicationId(long clusterTimeStamp, int id) {
    return ApplicationId.newInstance(clusterTimeStamp, id);
  }

  public static ApplicationAttemptId newApplicationAttemptId(
      ApplicationId appId, int attemptId) {
    return ApplicationAttemptId.newInstance(appId, attemptId);
  }

  public static ApplicationId convert(long clustertimestamp, CharSequence id) {
    return ApplicationId.newInstance(clustertimestamp,
        Integer.parseInt(id.toString()));
  }

  public static ContainerId newContainerId(ApplicationAttemptId appAttemptId,
      long containerId) {
    return ContainerId.newContainerId(appAttemptId, containerId);
  }

  public static ContainerId newContainerId(int appId, int appAttemptId,
      long timestamp, long id) {
    ApplicationId applicationId = newApplicationId(timestamp, appId);
    ApplicationAttemptId applicationAttemptId = newApplicationAttemptId(
        applicationId, appAttemptId);
    ContainerId cId = newContainerId(applicationAttemptId, id);
    return cId;
  }

  public static Token newContainerToken(ContainerId cId, int containerVersion,
      String host, int port, String user, Resource r, long expiryTime,
      int masterKeyId, byte[] password, long rmIdentifier) throws IOException {
    ContainerTokenIdentifier identifier =
        new ContainerTokenIdentifier(cId, containerVersion, host + ":" + port,
            user, r, expiryTime, masterKeyId, rmIdentifier,
            Priority.newInstance(0), 0, null, CommonNodeLabelsManager.NO_LABEL,
            ContainerType.TASK, ExecutionType.GUARANTEED);
    return newContainerToken(BuilderUtils.newNodeId(host, port), password,
        identifier);
  }

  public static ContainerId newContainerId(RecordFactory recordFactory,
      ApplicationId appId, ApplicationAttemptId appAttemptId,
      int containerId) {
    return ContainerId.newContainerId(appAttemptId, containerId);
  }

  public static NodeId newNodeId(String host, int port) {
    return NodeId.newInstance(host, port);
  }
  
  public static NodeReport newNodeReport(NodeId nodeId, NodeState nodeState,
      String httpAddress, String rackName, Resource used, Resource capability,
      int numContainers, String healthReport, long lastHealthReportTime) {
    return newNodeReport(nodeId, nodeState, httpAddress, rackName, used,
        capability, numContainers, healthReport, lastHealthReportTime, null);
  }

  public static NodeReport newNodeReport(NodeId nodeId, NodeState nodeState,
      String httpAddress, String rackName, Resource used, Resource capability,
      int numContainers, String healthReport, long lastHealthReportTime,
      Set<String> nodeLabels) {
    return newNodeReport(nodeId, nodeState, httpAddress, rackName, used,
        capability, numContainers, healthReport, lastHealthReportTime,
        nodeLabels, null, null);
  }

  public static NodeReport newNodeReport(NodeId nodeId, NodeState nodeState,
      String httpAddress, String rackName, Resource used, Resource capability,
      int numContainers, String healthReport, long lastHealthReportTime,
      Set<String> nodeLabels, ResourceUtilization containersUtilization,
      ResourceUtilization nodeUtilization) {
    NodeReport nodeReport = recordFactory.newRecordInstance(NodeReport.class);
    nodeReport.setNodeId(nodeId);
    nodeReport.setNodeState(nodeState);
    nodeReport.setHttpAddress(httpAddress);
    nodeReport.setRackName(rackName);
    nodeReport.setUsed(used);
    nodeReport.setCapability(capability);
    nodeReport.setNumContainers(numContainers);
    nodeReport.setHealthReport(healthReport);
    nodeReport.setLastHealthReportTime(lastHealthReportTime);
    nodeReport.setNodeLabels(nodeLabels);
    nodeReport.setAggregatedContainersUtilization(containersUtilization);
    nodeReport.setNodeUtilization(nodeUtilization);
    return nodeReport;
  }

  public static ContainerStatus newContainerStatus(ContainerId containerId,
      ContainerState containerState, String diagnostics, int exitStatus,
      Resource capability) {
    return newContainerStatus(containerId, containerState, diagnostics,
        exitStatus, capability, ExecutionType.GUARANTEED);
  }

  public static ContainerStatus newContainerStatus(ContainerId containerId,
      ContainerState containerState, String diagnostics, int exitStatus,
      Resource capability, ExecutionType executionType) {
    ContainerStatus containerStatus = recordFactory
      .newRecordInstance(ContainerStatus.class);
    containerStatus.setState(containerState);
    containerStatus.setContainerId(containerId);
    containerStatus.setDiagnostics(diagnostics);
    containerStatus.setExitStatus(exitStatus);
    containerStatus.setCapability(capability);
    containerStatus.setExecutionType(executionType);
    return containerStatus;
  }

  public static Container newContainer(ContainerId containerId, NodeId nodeId,
      String nodeHttpAddress, Resource resource, Priority priority,
      Token containerToken, ExecutionType executionType,
      long allocationRequestId) {
    Container container = recordFactory.newRecordInstance(Container.class);
    container.setId(containerId);
    container.setNodeId(nodeId);
    container.setNodeHttpAddress(nodeHttpAddress);
    container.setResource(resource);
    container.setPriority(priority);
    container.setContainerToken(containerToken);
    container.setExecutionType(executionType);
    container.setAllocationRequestId(allocationRequestId);
    return container;
  }

  public static Container newContainer(ContainerId containerId, NodeId nodeId,
      String nodeHttpAddress, Resource resource, Priority priority,
      Token containerToken) {
    return newContainer(containerId, nodeId, nodeHttpAddress, resource,
        priority, containerToken, ExecutionType.GUARANTEED, 0);
  }

  public static Container newContainer(ContainerId containerId, NodeId nodeId,
      String nodeHttpAddress, Resource resource, Priority priority,
      Token containerToken, long allocationRequestId) {
    return newContainer(containerId, nodeId, nodeHttpAddress, resource,
        priority, containerToken, ExecutionType.GUARANTEED,
        allocationRequestId);
  }

  public static <T extends Token> T newToken(Class<T> tokenClass,
      byte[] identifier, String kind, byte[] password, String service) {
    T token = recordFactory.newRecordInstance(tokenClass);
    token.setIdentifier(ByteBuffer.wrap(identifier));
    token.setKind(kind);
    token.setPassword(ByteBuffer.wrap(password));
    token.setService(service);
    return token;
  }

  public static Token newDelegationToken(byte[] identifier,
      String kind, byte[] password, String service) {
    return newToken(Token.class, identifier, kind, password, service);
  }

  public static Token newClientToAMToken(byte[] identifier, String kind,
      byte[] password, String service) {
    return newToken(Token.class, identifier, kind, password, service);
  }

  public static Token newAMRMToken(byte[] identifier, String kind,
                                   byte[] password, String service) {
    return newToken(Token.class, identifier, kind, password, service);
  }

  @VisibleForTesting
  public static Token newContainerToken(NodeId nodeId,
      byte[] password, ContainerTokenIdentifier tokenIdentifier) {
    // RPC layer client expects ip:port as service for tokens
    InetSocketAddress addr =
        NetUtils.createSocketAddrForHost(nodeId.getHost(), nodeId.getPort());
    // NOTE: use SecurityUtil.setTokenService if this becomes a "real" token
    Token containerToken =
        newToken(Token.class, tokenIdentifier.getBytes(),
          ContainerTokenIdentifier.KIND.toString(), password, SecurityUtil
            .buildTokenService(addr).toString());
    return containerToken;
  }

  public static ContainerTokenIdentifier newContainerTokenIdentifier(
      Token containerToken) throws IOException {
    org.apache.hadoop.security.token.Token<ContainerTokenIdentifier> token =
        new org.apache.hadoop.security.token.Token<ContainerTokenIdentifier>(
            containerToken.getIdentifier()
                .array(), containerToken.getPassword().array(), new Text(
                containerToken.getKind()),
            new Text(containerToken.getService()));
    return token.decodeIdentifier();
  }

  public static ContainerLaunchContext newContainerLaunchContext(
      Map<String, LocalResource> localResources,
      Map<String, String> environment, List<String> commands,
      Map<String, ByteBuffer> serviceData, ByteBuffer tokens,
      Map<ApplicationAccessType, String> acls) {
    ContainerLaunchContext container = recordFactory
        .newRecordInstance(ContainerLaunchContext.class);
    container.setLocalResources(localResources);
    container.setEnvironment(environment);
    container.setCommands(commands);
    container.setServiceData(serviceData);
    container.setTokens(tokens);
    container.setApplicationACLs(acls);
    return container;
  }

  public static Priority newPriority(int p) {
    Priority priority = recordFactory.newRecordInstance(Priority.class);
    priority.setPriority(p);
    return priority;
  }

  public static ResourceRequest newResourceRequest(Priority priority,
      String hostName, Resource capability, int numContainers) {
    ResourceRequest request = recordFactory
        .newRecordInstance(ResourceRequest.class);
    request.setPriority(priority);
    request.setResourceName(hostName);
    request.setCapability(capability);
    request.setNumContainers(numContainers);
    request.setExecutionTypeRequest(ExecutionTypeRequest.newInstance());
    return request;
  }

  public static ResourceRequest newResourceRequest(Priority priority,
      String hostName, Resource capability, int numContainers, String label) {
    ResourceRequest request =
        recordFactory.newRecordInstance(ResourceRequest.class);
    request.setPriority(priority);
    request.setResourceName(hostName);
    request.setCapability(capability);
    request.setNumContainers(numContainers);
    request.setNodeLabelExpression(label);
    request.setExecutionTypeRequest(ExecutionTypeRequest.newInstance());
    return request;
  }

  public static ResourceRequest newResourceRequest(ResourceRequest r) {
    ResourceRequest request = recordFactory
        .newRecordInstance(ResourceRequest.class);
    request.setPriority(r.getPriority());
    request.setResourceName(r.getResourceName());
    request.setCapability(r.getCapability());
    request.setNumContainers(r.getNumContainers());
    request.setNodeLabelExpression(r.getNodeLabelExpression());
    request.setExecutionTypeRequest(r.getExecutionTypeRequest());
    return request;
  }

  public static ApplicationReport newApplicationReport(
      ApplicationId applicationId, ApplicationAttemptId applicationAttemptId,
      String user, String queue, String name, String host, int rpcPort,
      Token clientToAMToken, YarnApplicationState state, String diagnostics,
      String url, long startTime, long finishTime,
      FinalApplicationStatus finalStatus,
      ApplicationResourceUsageReport appResources, String origTrackingUrl,
      float progress, String appType, Token amRmToken, Set<String> tags,
      Priority priority) {
    ApplicationReport report = recordFactory
        .newRecordInstance(ApplicationReport.class);
    report.setApplicationId(applicationId);
    report.setCurrentApplicationAttemptId(applicationAttemptId);
    report.setUser(user);
    report.setQueue(queue);
    report.setName(name);
    report.setHost(host);
    report.setRpcPort(rpcPort);
    report.setClientToAMToken(clientToAMToken);
    report.setYarnApplicationState(state);
    report.setDiagnostics(diagnostics);
    report.setTrackingUrl(url);
    report.setStartTime(startTime);
    report.setFinishTime(finishTime);
    report.setFinalApplicationStatus(finalStatus);
    report.setApplicationResourceUsageReport(appResources);
    report.setOriginalTrackingUrl(origTrackingUrl);
    report.setProgress(progress);
    report.setApplicationType(appType);
    report.setAMRMToken(amRmToken);
    report.setApplicationTags(tags);
    report.setPriority(priority);
    return report;
  }
  
  public static ApplicationSubmissionContext newApplicationSubmissionContext(
      ApplicationId applicationId, String applicationName, String queue,
      Priority priority, ContainerLaunchContext amContainer,
      boolean isUnmanagedAM, boolean cancelTokensWhenComplete,
      int maxAppAttempts, Resource resource, String applicationType) {
    ApplicationSubmissionContext context =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    context.setApplicationId(applicationId);
    context.setApplicationName(applicationName);
    context.setQueue(queue);
    context.setPriority(priority);
    context.setAMContainerSpec(amContainer);
    context.setUnmanagedAM(isUnmanagedAM);
    context.setCancelTokensWhenComplete(cancelTokensWhenComplete);
    context.setMaxAppAttempts(maxAppAttempts);
    context.setResource(resource);
    context.setApplicationType(applicationType);
    return context;
  }

  public static ApplicationSubmissionContext newApplicationSubmissionContext(
      ApplicationId applicationId, String applicationName, String queue,
      Priority priority, ContainerLaunchContext amContainer,
      boolean isUnmanagedAM, boolean cancelTokensWhenComplete,
      int maxAppAttempts, Resource resource) {
    return newApplicationSubmissionContext(applicationId, applicationName,
      queue, priority, amContainer, isUnmanagedAM, cancelTokensWhenComplete,
      maxAppAttempts, resource, null);
  }
  
  public static ApplicationResourceUsageReport newApplicationResourceUsageReport(
      int numUsedContainers, int numReservedContainers, Resource usedResources,
      Resource reservedResources, Resource neededResources, long memorySeconds, 
      long vcoreSeconds) {
    ApplicationResourceUsageReport report =
        recordFactory.newRecordInstance(ApplicationResourceUsageReport.class);
    report.setNumUsedContainers(numUsedContainers);
    report.setNumReservedContainers(numReservedContainers);
    report.setUsedResources(usedResources);
    report.setReservedResources(reservedResources);
    report.setNeededResources(neededResources);
    report.setMemorySeconds(memorySeconds);
    report.setVcoreSeconds(vcoreSeconds);
    return report;
  }

  public static Resource newResource(long memory, int vCores) {
    Resource resource = recordFactory.newRecordInstance(Resource.class);
    resource.setMemorySize(memory);
    resource.setVirtualCores(vCores);
    return resource;
  }

  public static URL newURL(String scheme, String host, int port, String file) {
    URL url = recordFactory.newRecordInstance(URL.class);
    url.setScheme(scheme);
    url.setHost(host);
    url.setPort(port);
    url.setFile(file);
    return url;
  }

  public static AllocateResponse newAllocateResponse(int responseId,
      List<ContainerStatus> completedContainers,
      List<Container> allocatedContainers, List<NodeReport> updatedNodes,
      Resource availResources, AMCommand command, int numClusterNodes,
      PreemptionMessage preempt) {
    AllocateResponse response = recordFactory
        .newRecordInstance(AllocateResponse.class);
    response.setNumClusterNodes(numClusterNodes);
    response.setResponseId(responseId);
    response.setCompletedContainersStatuses(completedContainers);
    response.setAllocatedContainers(allocatedContainers);
    response.setUpdatedNodes(updatedNodes);
    response.setAvailableResources(availResources);
    response.setAMCommand(command);
    response.setPreemptionMessage(preempt);

    return response;
  }
}
