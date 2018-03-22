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
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
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
import org.apache.hadoop.yarn.proto.ClientAMProtocol;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.component.Component;
import org.apache.hadoop.yarn.service.component.ComponentState;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceState;
import org.apache.hadoop.yarn.service.exceptions.BadClusterStateException;
import org.apache.hadoop.yarn.service.registry.YarnRegistryViewForProviders;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockServiceAM extends ServiceMaster {

  private static final Logger LOG =
      LoggerFactory.getLogger(MockServiceAM.class);

  Service service;
  // The list of containers fed by tests to be returned on
  // AMRMClientCallBackHandler#onContainersAllocated
  final List<Container> feedContainers =
      Collections.synchronizedList(new LinkedList<>());

  final List<ContainerStatus> failedContainers =
      Collections.synchronizedList(new LinkedList<>());

  private final List<Container> recoveredContainers =
      Collections.synchronizedList(new LinkedList<>());

  private final Map<String, ServiceRecord> registryComponents =
      new ConcurrentHashMap<>();

  private Map<ContainerId, ContainerStatus> containerStatuses =
      new ConcurrentHashMap<>();

  public MockServiceAM(Service service) {
    super(service.getName());
    this.service = service;
  }

  @Override
  protected ContainerId getAMContainerId()
      throws BadClusterStateException {
    return ContainerId.newContainerId(ApplicationAttemptId
        .newInstance(ApplicationId.fromString(service.getId()), 1), 1);
  }

  @Override
  protected Path getAppDir() {
    Path path = new Path(new Path("target", "apps"), service.getName());
    LOG.info("Service path: {}", path);
    return path;
  }

  @Override
  protected ServiceScheduler createServiceScheduler(ServiceContext context)
      throws IOException, YarnException {
    return new ServiceScheduler(context) {

      @SuppressWarnings("SuspiciousMethodCalls")
      @Override
      protected YarnRegistryViewForProviders createYarnRegistryOperations(
          ServiceContext context, RegistryOperations registryClient) {
        YarnRegistryViewForProviders yarnRegistryView = mock(
            YarnRegistryViewForProviders.class);
        if (!registryComponents.isEmpty()) {
          try {
            when(yarnRegistryView.listComponents())
                .thenReturn(new LinkedList<>(registryComponents.keySet()));
            when(yarnRegistryView.getComponent(anyString())).thenAnswer(
                invocation ->
                    registryComponents.get(invocation.getArguments()[0]));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return yarnRegistryView;
      }

      @Override
      protected AMRMClientAsync<AMRMClient.ContainerRequest> createAMRMClient() {
        AMRMClientImpl client1 = new AMRMClientImpl() {
          @Override public AllocateResponse allocate(float progressIndicator)
              throws YarnException, IOException {

            AllocateResponse.AllocateResponseBuilder builder =
                AllocateResponse.newBuilder();
            // add new containers if any
            synchronized (feedContainers) {
              if (feedContainers.isEmpty()) {
                LOG.info("Allocating........ no containers");
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
                    LOG.info("Allocated container {} ", c.getId());
                    allocatedContainers.add(c);
                    itor.remove();
                  }
                }
                builder.allocatedContainers(allocatedContainers);
              }
            }

            // add recovered containers if any
            synchronized (recoveredContainers) {
              if (!recoveredContainers.isEmpty()) {
                List<Container> containersFromPrevAttempt = new LinkedList<>();
                containersFromPrevAttempt.addAll(recoveredContainers);
                recoveredContainers.clear();
                builder.containersFromPreviousAttempt(
                    containersFromPrevAttempt);
              }
            }

            // add failed containers if any
            synchronized (failedContainers) {
              if (!failedContainers.isEmpty()) {
                List<ContainerStatus> failed =
                    new LinkedList<>(failedContainers);
                failedContainers.clear();
                builder.completedContainersStatuses(failed);
              }
            }
            return builder.build();
          }

          @Override
          public RegisterApplicationMasterResponse registerApplicationMaster(
              String appHostName, int appHostPort, String appTrackingUrl,
              Map placementConstraintsMap) throws YarnException, IOException {
            return this.registerApplicationMaster(appHostName, appHostPort,
                appTrackingUrl);
          }

          @Override
            public RegisterApplicationMasterResponse registerApplicationMaster(
                String appHostName, int appHostPort, String appTrackingUrl) {
            RegisterApplicationMasterResponse response = mock(
                RegisterApplicationMasterResponse.class);
            when(response.getResourceTypes()).thenReturn(
                ResourceUtils.getResourcesTypeInfo());
            return response;
          }

          @Override public void unregisterApplicationMaster(
              FinalApplicationStatus appStatus, String appMessage,
              String appTrackingUrl) {
            // DO nothing
          }
        };

        AMRMClientAsync<AMRMClient.ContainerRequest> amrmClientAsync =
            AMRMClientAsync.createAMRMClientAsync(client1, 1000,
                this.new AMRMClientCallback());

        return amrmClientAsync;
      }

      @SuppressWarnings("SuspiciousMethodCalls")
      @Override
      public NMClientAsync createNMClient() {
        NMClientAsync nmClientAsync = super.createNMClient();
        NMClient nmClient = mock(NMClient.class);
        try {
          when(nmClient.getContainerStatus(anyObject(), anyObject()))
              .thenAnswer(invocation ->
                  containerStatuses.get(invocation.getArguments()[0]));
        } catch (YarnException | IOException e) {
          throw new RuntimeException(e);
        }
        nmClientAsync.setClient(nmClient);
        return nmClientAsync;
      }
    };
  }

  @Override protected void loadApplicationJson(ServiceContext context,
      SliderFileSystem fs) throws IOException {
    context.service = service;
  }

  public void feedRegistryComponent(ContainerId containerId, String compName,
      String compInstName) {
    ServiceRecord record = new ServiceRecord();
    record.set(YarnRegistryAttributes.YARN_ID, containerId.toString());
    record.description = compInstName;
    record.set(YarnRegistryAttributes.YARN_PERSISTENCE,
        PersistencePolicies.CONTAINER);
    record.set(YarnRegistryAttributes.YARN_IP, "localhost");
    record.set(YarnRegistryAttributes.YARN_HOSTNAME, "localhost");
    record.set(YarnRegistryAttributes.YARN_COMPONENT, compName);
    registryComponents.put(RegistryPathUtils.encodeYarnID(
        containerId.toString()), record);
  }

  /**
   * Simulates a recovered container that is sent to the AM in the heartbeat
   * response.
   *
   * @param containerId The ID for the container
   * @param compName    The component to which the recovered container is fed.
   */
  public void feedRecoveredContainer(ContainerId containerId, String compName) {
    Container container = createContainer(containerId, compName);
    recoveredContainers.add(container);
    addContainerStatus(container, ContainerState.RUNNING);
  }

  /**
   *
   * @param service The service for the component
   * @param id The id for the container
   * @param compName The component to which the container is fed
   * @return
   */
  public Container feedContainerToComp(Service service, int id,
      String compName) {
    ContainerId containerId = createContainerId(id);
    Container container = createContainer(containerId, compName);
    synchronized (feedContainers) {
      feedContainers.add(container);
    }
    addContainerStatus(container, ContainerState.RUNNING);
    return container;
  }

  public void feedFailedContainerToComp(Service service, int id, String
      compName) {
    ApplicationId applicationId = ApplicationId.fromString(service.getId());
    ContainerId containerId = ContainerId
        .newContainerId(ApplicationAttemptId.newInstance(applicationId, 1), id);
    ContainerStatus status = Records.newRecord(ContainerStatus.class);
    status.setContainerId(containerId);
    synchronized (failedContainers) {
      failedContainers.add(status);
    }
  }

  public ContainerId createContainerId(int id) {
    ApplicationId applicationId = ApplicationId.fromString(service.getId());
    return ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(applicationId, 1), id);
  }

  private Container createContainer(ContainerId containerId, String compName) {
    NodeId nodeId = NodeId.newInstance("localhost", 1234);
    Container container = Container.newInstance(
        containerId, nodeId, "localhost",
        Resource.newInstance(100, 1),
        Priority.newInstance(0), null);
    long allocateId =
        context.scheduler.getAllComponents().get(compName).getAllocateId();
    container.setAllocationRequestId(allocateId);
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


  public ComponentInstance getCompInstance(String compName, String
      instanceName) {
    return context.scheduler.getAllComponents().get(compName)
        .getComponentInstance(instanceName);
  }

  public void waitForCompInstanceState(ComponentInstance instance,
      ComponentInstanceState state)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return instance.getState().equals(state);
      }
    }, 1000, 20000);
  }

  private void addContainerStatus(Container container, ContainerState state) {
    ContainerStatus status = ContainerStatus.newInstance(container.getId(),
        state, "", 0);
    status.setHost(container.getNodeId().getHost());
    status.setIPs(Lists.newArrayList(container.getNodeId().getHost()));
    containerStatuses.put(container.getId(), status);
  }

}
