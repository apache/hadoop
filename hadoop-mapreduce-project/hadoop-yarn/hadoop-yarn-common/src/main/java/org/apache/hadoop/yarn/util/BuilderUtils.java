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

package org.apache.hadoop.yarn.util;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;

/**
 * Builder utilities to construct various objects.
 *
 */
public class BuilderUtils {

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  public static class ApplicationIdComparator implements
      Comparator<ApplicationId> {
    @Override
    public int compare(ApplicationId a1, ApplicationId a2) {
      return a1.compareTo(a2);
    }
  }

  public static class ContainerIdComparator implements
      java.util.Comparator<ContainerId> {

    @Override
    public int compare(ContainerId c1,
        ContainerId c2) {
      return c1.compareTo(c2);
    }
  }

  public static class ResourceRequestComparator
  implements java.util.Comparator<org.apache.hadoop.yarn.api.records.ResourceRequest> {
    @Override
    public int compare(org.apache.hadoop.yarn.api.records.ResourceRequest r1,
        org.apache.hadoop.yarn.api.records.ResourceRequest r2) {

      // Compare priority, host and capability
      int ret = r1.getPriority().compareTo(r2.getPriority());
      if (ret == 0) {
        String h1 = r1.getHostName();
        String h2 = r2.getHostName();
        ret = h1.compareTo(h2);
      }
      if (ret == 0) {
        ret = r1.getCapability().compareTo(r2.getCapability());
      }
      return ret;
    }
  }

  public static LocalResource newLocalResource(URL url, LocalResourceType type,
      LocalResourceVisibility visibility, long size, long timestamp) {
    LocalResource resource =
      recordFactory.newRecordInstance(LocalResource.class);
    resource.setResource(url);
    resource.setType(type);
    resource.setVisibility(visibility);
    resource.setSize(size);
    resource.setTimestamp(timestamp);
    return resource;
  }

  public static LocalResource newLocalResource(URI uri,
      LocalResourceType type, LocalResourceVisibility visibility, long size,
      long timestamp) {
    return newLocalResource(ConverterUtils.getYarnUrlFromURI(uri), type,
        visibility, size, timestamp);
  }

  public static ApplicationId newApplicationId(RecordFactory recordFactory,
      long clustertimestamp, CharSequence id) {
    ApplicationId applicationId =
        recordFactory.newRecordInstance(ApplicationId.class);
    applicationId.setId(Integer.valueOf(id.toString()));
    applicationId.setClusterTimestamp(clustertimestamp);
    return applicationId;
  }

  public static ApplicationId newApplicationId(RecordFactory recordFactory,
      long clusterTimeStamp, int id) {
    ApplicationId applicationId =
        recordFactory.newRecordInstance(ApplicationId.class);
    applicationId.setId(id);
    applicationId.setClusterTimestamp(clusterTimeStamp);
    return applicationId;
  }

  public static ApplicationId newApplicationId(long clusterTimeStamp, int id) {
    ApplicationId applicationId =
        recordFactory.newRecordInstance(ApplicationId.class);
    applicationId.setId(id);
    applicationId.setClusterTimestamp(clusterTimeStamp);
    return applicationId;
  }

  public static ApplicationAttemptId newApplicationAttemptId(
      ApplicationId appId, int attemptId) {
    ApplicationAttemptId appAttemptId =
        recordFactory.newRecordInstance(ApplicationAttemptId.class);
    appAttemptId.setApplicationId(appId);
    appAttemptId.setAttemptId(attemptId);
    return appAttemptId;
  }

  public static ApplicationId convert(long clustertimestamp, CharSequence id) {
    ApplicationId applicationId =
        recordFactory.newRecordInstance(ApplicationId.class);
    applicationId.setId(Integer.valueOf(id.toString()));
    applicationId.setClusterTimestamp(clustertimestamp);
    return applicationId;
  }

  public static ContainerId newContainerId(ApplicationAttemptId appAttemptId,
      int containerId) {
    ContainerId id = recordFactory.newRecordInstance(ContainerId.class);
    id.setId(containerId);
    id.setApplicationAttemptId(appAttemptId);
    return id;
  }

  public static ContainerId newContainerId(int appId, int appAttemptId,
      long timestamp, int id) {
    ApplicationId applicationId = newApplicationId(timestamp, appId);
    ApplicationAttemptId applicationAttemptId = newApplicationAttemptId(
        applicationId, appAttemptId);
    ContainerId cId = newContainerId(applicationAttemptId, id);
    return cId;
  }

  public static ContainerId newContainerId(RecordFactory recordFactory,
      ApplicationId appId, ApplicationAttemptId appAttemptId,
      int containerId) {
    ContainerId id = recordFactory.newRecordInstance(ContainerId.class);
    id.setId(containerId);
    id.setApplicationAttemptId(appAttemptId);
    return id;
  }

  public static ContainerId newContainerId(RecordFactory recordFactory,
      ApplicationAttemptId appAttemptId,
      int containerId) {
    ContainerId id = recordFactory.newRecordInstance(ContainerId.class);
    id.setApplicationAttemptId(appAttemptId);
    id.setId(containerId);
    return id;
  }

  public static NodeId newNodeId(String host, int port) {
    NodeId nodeId = recordFactory.newRecordInstance(NodeId.class);
    nodeId.setHost(host);
    nodeId.setPort(port);
    return nodeId;
  }

  public static Container newContainer(ContainerId containerId,
      NodeId nodeId, String nodeHttpAddress,
      Resource resource, Priority priority, ContainerToken containerToken) {
    Container container = recordFactory.newRecordInstance(Container.class);
    container.setId(containerId);
    container.setNodeId(nodeId);
    container.setNodeHttpAddress(nodeHttpAddress);
    container.setResource(resource);
    container.setPriority(priority);
    container.setState(ContainerState.NEW);
    ContainerStatus containerStatus = Records.newRecord(ContainerStatus.class);
    containerStatus.setContainerId(containerId);
    containerStatus.setState(ContainerState.NEW);
    container.setContainerStatus(containerStatus);
    container.setContainerToken(containerToken);
    return container;
  }

  public static ContainerToken newContainerToken(NodeId nodeId,
      ByteBuffer password, ContainerTokenIdentifier tokenIdentifier) {
    ContainerToken containerToken = recordFactory
        .newRecordInstance(ContainerToken.class);
    containerToken.setIdentifier(ByteBuffer.wrap(tokenIdentifier.getBytes()));
    containerToken.setKind(ContainerTokenIdentifier.KIND.toString());
    containerToken.setPassword(password);
    // RPC layer client expects ip:port as service for tokens
    InetSocketAddress addr = NetUtils.createSocketAddr(nodeId.getHost(),
        nodeId.getPort());
    containerToken.setService(addr.getAddress().getHostAddress() + ":"
        + addr.getPort());
    return containerToken;
  }

  public static ContainerLaunchContext newContainerLaunchContext(
      ContainerId containerID, String user, Resource assignedCapability,
      Map<String, LocalResource> localResources,
      Map<String, String> environment, List<String> commands,
      Map<String, ByteBuffer> serviceData, ByteBuffer containerTokens,
      Map<ApplicationAccessType, String> acls) {
    ContainerLaunchContext container = recordFactory
        .newRecordInstance(ContainerLaunchContext.class);
    container.setContainerId(containerID);
    container.setUser(user);
    container.setResource(assignedCapability);
    container.setLocalResources(localResources);
    container.setEnvironment(environment);
    container.setCommands(commands);
    container.setServiceData(serviceData);
    container.setContainerTokens(containerTokens);
    container.setApplicationACLs(acls);
    return container;
  }

  public static ResourceRequest newResourceRequest(Priority priority,
      String hostName, Resource capability, int numContainers) {
    ResourceRequest request = recordFactory
        .newRecordInstance(ResourceRequest.class);
    request.setPriority(priority);
    request.setHostName(hostName);
    request.setCapability(capability);
    request.setNumContainers(numContainers);
    return request;
  }

  public static ResourceRequest newResourceRequest(ResourceRequest r) {
    ResourceRequest request = recordFactory
        .newRecordInstance(ResourceRequest.class);
    request.setPriority(r.getPriority());
    request.setHostName(r.getHostName());
    request.setCapability(r.getCapability());
    request.setNumContainers(r.getNumContainers());
    return request;
  }

  public static ApplicationReport newApplicationReport(
      ApplicationId applicationId, String user, String queue, String name,
      String host, int rpcPort, String clientToken, YarnApplicationState state,
      String diagnostics, String url, long startTime, long finishTime,
      FinalApplicationStatus finalStatus, ApplicationResourceUsageReport appResources,
      String origTrackingUrl) {
    ApplicationReport report = recordFactory
        .newRecordInstance(ApplicationReport.class);
    report.setApplicationId(applicationId);
    report.setUser(user);
    report.setQueue(queue);
    report.setName(name);
    report.setHost(host);
    report.setRpcPort(rpcPort);
    report.setClientToken(clientToken);
    report.setYarnApplicationState(state);
    report.setDiagnostics(diagnostics);
    report.setTrackingUrl(url);
    report.setStartTime(startTime);
    report.setFinishTime(finishTime);
    report.setFinalApplicationStatus(finalStatus);
    report.setApplicationResourceUsageReport(appResources);
    report.setOriginalTrackingUrl(origTrackingUrl);
    return report;
  }

  public static Resource newResource(int memory) {
    Resource resource = recordFactory.newRecordInstance(Resource.class);
    resource.setMemory(memory);
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

  public static AllocateRequest newAllocateRequest(
      ApplicationAttemptId applicationAttemptId, int responseID,
      float appProgress, List<ResourceRequest> resourceAsk,
      List<ContainerId> containersToBeReleased) {
    AllocateRequest allocateRequest = recordFactory
        .newRecordInstance(AllocateRequest.class);
    allocateRequest.setApplicationAttemptId(applicationAttemptId);
    allocateRequest.setResponseId(responseID);
    allocateRequest.setProgress(appProgress);
    allocateRequest.addAllAsks(resourceAsk);
    allocateRequest.addAllReleases(containersToBeReleased);
    return allocateRequest;
  }
}
