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

import java.net.URI;
import java.util.Comparator;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

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

  public static LocalResource newLocalResource(RecordFactory recordFactory,
      URI uri, LocalResourceType type, LocalResourceVisibility visibility,
      long size, long timestamp) {
    LocalResource resource =
        recordFactory.newRecordInstance(LocalResource.class);
    resource.setResource(ConverterUtils.getYarnUrlFromURI(uri));
    resource.setType(type);
    resource.setVisibility(visibility);
    resource.setSize(size);
    resource.setTimestamp(timestamp);
    return resource;
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

  public static ApplicationId convert(long clustertimestamp, CharSequence id) {
    ApplicationId applicationId =
        recordFactory.newRecordInstance(ApplicationId.class);
    applicationId.setId(Integer.valueOf(id.toString()));
    applicationId.setClusterTimestamp(clustertimestamp);
    return applicationId;
  }

  public static ContainerId newContainerId(RecordFactory recordFactory,
      ApplicationAttemptId appAttemptId,
      int containerId) {
    ContainerId id = recordFactory.newRecordInstance(ContainerId.class);
    id.setAppAttemptId(appAttemptId);
    id.setAppId(appAttemptId.getApplicationId());
    id.setId(containerId);
    return id;
  }

  public static Container clone(Container c) {
    Container container = recordFactory.newRecordInstance(Container.class);
    container.setId(c.getId());
    container.setContainerToken(c.getContainerToken());
    container.setNodeId(c.getNodeId());
    container.setNodeHttpAddress(c.getNodeHttpAddress());
    container.setResource(c.getResource());
    container.setState(c.getState());
    return container;
  }

  public static Container newContainer(RecordFactory recordFactory,
      ApplicationAttemptId appAttemptId, int containerId, NodeId nodeId,
      String containerManagerAddress, String nodeHttpAddress,
      Resource resource) {
    ContainerId containerID =
        newContainerId(recordFactory, appAttemptId, containerId);
    return newContainer(containerID, nodeId, containerManagerAddress,
        nodeHttpAddress, resource);
  }

  public static Container newContainer(ContainerId containerId, NodeId nodeId,
      String containerManagerAddress, String nodeHttpAddress,
      Resource resource) {
    Container container = recordFactory.newRecordInstance(Container.class);
    container.setId(containerId);
    container.setNodeId(nodeId);
    container.setNodeHttpAddress(nodeHttpAddress);
    container.setResource(resource);
    container.setState(ContainerState.INITIALIZING);
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
      String host, int rpcPort, String clientToken, ApplicationState state,
      String diagnostics, String url) {
    ApplicationReport report = recordFactory
        .newRecordInstance(ApplicationReport.class);
    report.setApplicationId(applicationId);
    report.setUser(user);
    report.setQueue(queue);
    report.setName(name);
    report.setHost(host);
    report.setRpcPort(rpcPort);
    report.setClientToken(clientToken);
    report.setState(state);
    report.setDiagnostics(diagnostics);
    report.setTrackingUrl(url);
    return report;
  }
}
