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

package org.apache.hadoop.yarn.service;

import com.google.common.base.Supplier;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.*;
import org.apache.hadoop.yarn.proto.ClientAMProtocol;
import org.apache.hadoop.yarn.service.component.Component;
import org.apache.hadoop.yarn.service.component.ComponentState;
import org.apache.slider.api.resource.Application;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.server.services.yarnregistry.YarnRegistryViewForProviders;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;

public class MockServiceAM extends ServiceMaster {

  Application application;
  // The list of containers fed by tests to be returned on
  // AMRMClientCallBackHandler#onContainersAllocated
  final List<Container> feedContainers =
      Collections.synchronizedList(new LinkedList<>());

  public MockServiceAM(Application application) {
    super(application.getName());
    this.application = application;
  }


  @Override
  protected ContainerId getAMContainerId()
      throws BadClusterStateException {
    return ContainerId.newContainerId(ApplicationAttemptId
        .newInstance(ApplicationId.fromString(application.getId()), 1), 1);
  }

  @Override
  protected Path getAppDir() {
    Path path = new Path(new Path("target", "apps"), application.getName());
    System.out.println("Application path: " + path);
    return path;
  }

  @Override
  protected ServiceScheduler createServiceScheduler(ServiceContext context)
      throws IOException, YarnException {
    return new ServiceScheduler(context) {

      @Override
      protected YarnRegistryViewForProviders createYarnRegistryOperations(
          ServiceContext context, RegistryOperations registryClient) {
        return mock(YarnRegistryViewForProviders.class);
      }

      @Override
      protected AMRMClientAsync<AMRMClient.ContainerRequest> createAMRMClient() {
        AMRMClientImpl client1 = new AMRMClientImpl() {
          @Override public AllocateResponse allocate(float progressIndicator)
              throws YarnException, IOException {

            AllocateResponse.AllocateResponseBuilder builder =
                AllocateResponse.newBuilder();
            synchronized (feedContainers) {
              if (feedContainers.isEmpty()) {
                System.out.println("Allocating........ no containers");
                return builder.build();
              } else {
                // The AMRMClient will return containers for compoenent that are
                // at FLEXING state
                List<Container> allocatedContainers = new LinkedList<>();
                Iterator<Container> itor = feedContainers.iterator();
                while (itor.hasNext()) {
                  Container c = itor.next();
                  org.apache.hadoop.yarn.service.component.Component component =
                      componentsById.get(c.getAllocationRequestId());
                  if (component.getState() == ComponentState.FLEXING) {
                    System.out.println("Allocated container " + c.getId());
                    allocatedContainers.add(c);
                    itor.remove();
                  }
                }
                return builder.allocatedContainers(allocatedContainers).build();
              }
            }
          }

          @Override
          public RegisterApplicationMasterResponse registerApplicationMaster(
              String appHostName, int appHostPort, String appTrackingUrl) {
            return mock(RegisterApplicationMasterResponse.class);
          }

          @Override public void unregisterApplicationMaster(
              FinalApplicationStatus appStatus, String appMessage,
              String appTrackingUrl) {
            // DO nothing
          }
        };

        return AMRMClientAsync
            .createAMRMClientAsync(client1, 1000,
                this.new AMRMClientCallback());
      }

      @Override
      public NMClientAsync createNMClient() {
        NMClientAsync nmClientAsync = super.createNMClient();
        nmClientAsync.setClient(mock(NMClient.class));
        return nmClientAsync;
      }
    };
  }

  @Override protected void loadApplicationJson(ServiceContext context,
      SliderFileSystem fs) throws IOException {
    context.application = application;
  }

  /**
   *
   * @param application The application for the component
   * @param id The id for the container
   * @param compName The component to which the container is fed
   * @return
   */
  public Container feedContainerToComp(Application application, int id,
      String compName) {
    ApplicationId applicationId = ApplicationId.fromString(application.getId());
    ContainerId containerId = ContainerId
        .newContainerId(ApplicationAttemptId.newInstance(applicationId, 1), id);
    NodeId nodeId = NodeId.newInstance("localhost", 1234);
    Container container = Container
        .newInstance(containerId, nodeId, "localhost",
            Resource.newInstance(100, 1), Priority.newInstance(0), null);

    long allocateId =
        context.scheduler.getAllComponents().get(compName).getAllocateId();
    container.setAllocationRequestId(allocateId);
    synchronized (feedContainers) {
      feedContainers.add(container);
    }
    return container;
  }

  public void flexComponent(String compName, long numberOfContainers)
      throws IOException {
    ClientAMProtocol.ComponentCountProto componentCountProto =
        ClientAMProtocol.ComponentCountProto.newBuilder().setName(compName)
            .setNumberOfContainers(numberOfContainers).build();
    ClientAMProtocol.FlexComponentsRequestProto requestProto =
        ClientAMProtocol.FlexComponentsRequestProto.newBuilder()
            .addComponents(componentCountProto).build();
    context.clientAMService.flexComponents(requestProto);
  }

  public Component getComponent(String compName) {
    return context.scheduler.getAllComponents().get(compName);
  }

  public void waitForDependenciesSatisfied(String compName)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override public Boolean get() {
        return context.scheduler.getAllComponents().get(compName)
            .areDependenciesReady();
      }
    }, 1000, 20000);
  }

  public void waitForNumDesiredContainers(String compName,
      int numDesiredContainers) throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override public Boolean get() {
        return context.scheduler.getAllComponents().get(compName)
            .getNumDesiredInstances() == numDesiredContainers;
      }
    }, 1000, 20000);
  }
}
