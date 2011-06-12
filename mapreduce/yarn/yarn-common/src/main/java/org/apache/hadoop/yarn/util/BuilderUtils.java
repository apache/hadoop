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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
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

  public static class ContainerComparator implements
      java.util.Comparator<Container> {

    @Override
    public int compare(Container c1,
        Container c2) {
      return c1.compareTo(c2);
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
      ApplicationId applicationId,
      int containerId) {
    ContainerId id = recordFactory.newRecordInstance(ContainerId.class);
    id.setAppId(applicationId);
    id.setId(containerId);
    return id;
  }

  public static Container clone(Container c) {
    Container container = recordFactory.newRecordInstance(Container.class);
    container.setId(c.getId());
    container.setContainerToken(c.getContainerToken());
    container.setContainerManagerAddress(c.getContainerManagerAddress());
    container.setNodeHttpAddress(c.getNodeHttpAddress());
    container.setResource(c.getResource());
    container.setState(c.getState());
    return container;
  }

  public static Container newContainer(RecordFactory recordFactory,
      ApplicationId applicationId, int containerId,
      String containerManagerAddress, String nodeHttpAddress,
      Resource resource) {
    ContainerId containerID =
        newContainerId(recordFactory, applicationId, containerId);
    return newContainer(containerID, containerManagerAddress,
        nodeHttpAddress, resource);
  }

  public static Container newContainer(ContainerId containerId,
      String containerManagerAddress, String nodeHttpAddress,
      Resource resource) {
    Container container = recordFactory.newRecordInstance(Container.class);
    container.setId(containerId);
    container.setContainerManagerAddress(containerManagerAddress);
    container.setNodeHttpAddress(nodeHttpAddress);
    container.setResource(resource);
    container.setState(ContainerState.INITIALIZING);
    return container;
  }
}
